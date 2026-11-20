"use strict";

const crypto = require("node:crypto");

const {
    withGlobalConnectorLock
} = require("../../socketio/connectorOperationCoordinator");

const {
    fetchLedgerOpeningDurable
} = require("./fetchLedgerOpeningDurable");

const {
    fetchVouchersDurable
} = require("./fetchVouchersDurable");

const {
    startManualReconcileClaimed
} = require("./manualReconcileClaimedAdmission");

const {
    fetchSqlVoucherEntries
} = require("../../utils/reconciliations/fetchSqlVoucherEntries");

const {
    compareVoucherLedgerEntries
} = require("./serverCompareVoucherLedgerEntries");

const {
    applyLedgerOpeningDurable
} = require("./applyLedgerOpeningDurable");

// Voucher reconciliation is comparison-only.
// Monetary settlement is not authorized in this session.

const {
    getMismatchLedgerGuids
} = require("./getMismatchLedgerGuids");

const {
    getReconciliationVoucherGuids
} = require("./getReconciliationVoucherGuids");

const {
    getReconciliationVoucherDateRange
} = require("./getReconciliationVoucherDateRange");

const {
    bindManualReconcileExecution
} = require("./manualReconcileExecutionBinding");

const {
    getManualReconcileServerInstanceId
} = require("./manualReconcileServerIdentity");
const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function requireText(value, name) {
    if (typeof value !== "string" || !value.trim()) {
        throw new Error(`RECONCILE_${name}_INVALID`);
    }

    return value.trim();
}


async function runManualReconciliationSession({
    supabase,
    companyCode,
    tallyOwner,
    connectorId,
    tallyCompanyName,
    tallyCompanyGuid,
    booksBeginningFrom,
    batchId,
    executionClaimToken,
    ledgerGuids
}) {
    if (
        typeof batchId !== "string" ||
        !UUID.test(batchId) ||
        typeof executionClaimToken !== "string" ||
        !UUID.test(executionClaimToken)
    ) {
        throw new Error(
            "RECONCILE_BATCH_CLAIM_INPUT_INVALID"
        );
    }

    companyCode = requireText(companyCode, "COMPANY_CODE");
    connectorId = requireText(connectorId, "CONNECTOR_ID");
    tallyCompanyName = requireText(
        tallyCompanyName,
        "TALLY_COMPANY_NAME"
    );
    tallyCompanyGuid = requireText(
        tallyCompanyGuid,
        "TALLY_COMPANY_GUID"
    );

    if (!["USER", "CA"].includes(tallyOwner)) {
        throw new Error("RECONCILE_TALLY_OWNER_INVALID");
    }

    if (
        typeof booksBeginningFrom !== "string" ||
        !/^\d{8}$/.test(booksBeginningFrom)
    ) {
        throw new Error("RECONCILE_BOOKS_DATE_INVALID");
    }

       // Ledger GUIDs will be selected from
    // balance_difference_view after admission.
    // Caller-supplied GUIDs are not accepted.
    if (ledgerGuids !== undefined) {
        throw new Error(
            "RECONCILE_MANUAL_LEDGER_GUIDS_NOT_ALLOWED"
        );
    }

    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error("RECONCILE_SUPABASE_INVALID");
    }

    
    const workerId = `manual-reconcile:${crypto.randomUUID()}`;
    const attemptId = crypto.randomUUID();

    
    const admissionInput = {
        supabase,
        attemptId,
        companyCode,
        tallyOwner,
        reconcileType: "LEDGER",
        workerId,
        batchId,
        executionClaimToken
    };

    let admission;

    try {
        admission = await startManualReconcileClaimed(
            admissionInput
        );

    } catch (error) {
        // Admission may have committed even if the
        // RPC response was lost.
        // Preserve the same attempt identity for recovery.
        error.reconcileAdmissionUncertain = true;
        error.reconcileRecoveryRequired = true;
        error.reconcileAttemptId = attemptId;
        error.reconcileWorkerId = workerId;
        error.reconcileCompanyCode = companyCode;
        error.reconcileTallyOwner = tallyOwner;

        throw error;
    }

       if (admission.reason === "ALREADY_ACQUIRED") {
        return {
            success: false,
            reason: "RECONCILE_ATTEMPT_RECOVERY_REQUIRED",
            attemptId,
            recoveryRequired: true
        };
    }

    if (!admission.acquired) {
        return {
            success: false,
            reason: admission.reason
        };
    }

    if (admission.reason !== "ACQUIRED") {
        throw new Error(
            "RECONCILE_ADMISSION_STATE_INVALID"
        );
    }

    const reservationToken = admission.reservationToken;


    // Once admission succeeds, every unverified failure
    // must preserve the RUNNING reservation.
        let readings;
    let releaseIdentity;
    let releaseEvidence;

    const executionId = crypto.randomUUID();
    const serverInstanceId =
        getManualReconcileServerInstanceId();

    try {
        const envelope = await withGlobalConnectorLock(
            connectorId,
            async (socket, signal, lockIdentity) => {
                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                   if (
                    !Array.isArray(socket.companyGuids) ||
                    !socket.companyGuids.includes(tallyCompanyGuid)
                ) {
                    throw new Error(
                        "RECONCILE_COMPANY_GUID_MISMATCH"
                    );
                }

                             if (
                    !lockIdentity ||
                    lockIdentity.connectorId !== connectorId ||
                    typeof lockIdentity.socketId !== "string" ||
                    !lockIdentity.socketId ||
                    typeof lockIdentity.ownerToken !== "string" ||
                    !UUID.test(lockIdentity.ownerToken) ||
                    typeof lockIdentity.assertOwnership !== "function"
                ) {
                    throw new Error(
                        "RECONCILE_LOCK_IDENTITY_INVALID"
                    );
                }

                await lockIdentity.assertOwnership();

                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                await bindManualReconcileExecution({
                    supabase,
                    attemptId,
                    operationToken: reservationToken,
                    executionId,
                    connectorId,
                    socketId: lockIdentity.socketId,
                    ownerToken: lockIdentity.ownerToken,
                    serverInstanceId
                });

                await lockIdentity.assertOwnership();

                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                const mismatchLedgerGuids =
                    await getMismatchLedgerGuids({
                        supabase,
                        companyCode,
                        tallyOwner
                    });

                if (
                    !Array.isArray(mismatchLedgerGuids) ||
                    mismatchLedgerGuids.some(
                        guid =>
                            typeof guid !== "string" ||
                            !guid.trim()
                    )
                ) {
                    throw new Error(
                        "RECONCILE_MISMATCH_GUIDS_INVALID"
                    );
                }

                // ==============================================
                // RECONCILE — SELECT EXISTING VOUCHER GUIDS
                // ==============================================
                // Scope: company_code + tally_owner + ledger_guid
                // READ ONLY. No INSERT / UPDATE / DELETE.

                const reconciliationVoucherGuids =
                    await getReconciliationVoucherGuids({
                        supabase,
                        companyCode,
                        tallyOwner,
                        ledgerGuids: mismatchLedgerGuids
                    });

                if (
                    !Array.isArray(reconciliationVoucherGuids) ||
                    reconciliationVoucherGuids.some(
                        guid =>
                            typeof guid !== "string" ||
                            !guid.trim()
                    )
                ) {
                    throw new Error(
                        "RECONCILE_VOUCHER_GUIDS_INVALID"
                    );
                }

                            // ==============================================
                // RECONCILE — RESOLVE EXISTING VOUCHER DATES
                // ==============================================
                // Dates come from tally_vouchers.
                // Transaction details remain in tally_voucher_ledgers.

                let reconciliationVoucherDateRange = null;

                if (reconciliationVoucherGuids.length > 0) {
                    reconciliationVoucherDateRange =
                        await getReconciliationVoucherDateRange({
                            supabase,
                            companyCode,
                            tallyOwner,
                            voucherGuids: reconciliationVoucherGuids
                        });

                    if (
                        reconciliationVoucherDateRange.fromDate <
                        booksBeginningFrom
                    ) {
                        throw new Error(
                            "RECONCILE_VOUCHER_BEFORE_BOOKS_BEGINNING"
                        );
                    }
                }

                if (signal.aborted) {
                    throw new Error(
                        "RECONCILE_SESSION_ABORTED"
                    );
                }

                // Voucher reconciliation is comparison-only.
                // No voucher amount update or delete/resync is authorized.

                const results = [];
                let operationOrdinal = 0;

                for (const ledgerGuid of mismatchLedgerGuids) {
                    if (signal.aborted) {
                        throw new Error(
                            "RECONCILE_SESSION_ABORTED"
                        );
                    }

                    // Each GUID has two distinct journal operations:
                    // fetch ordinal 1, apply ordinal 2, etc.
                    const fetchOrdinal = ++operationOrdinal;

                    const result = await fetchLedgerOpeningDurable({
                        supabase,
                        socket,
                        signal,
                        attemptId,
                        executionId,
                        operationToken: reservationToken,
                        operationOrdinal: fetchOrdinal,
                        connectorId,
                        tallyCompanyGuid,
                        company: tallyCompanyName,
                        ledgerGuid: ledgerGuid.trim(),
                        booksBeginningFrom
                    });

                    if (signal.aborted) {
                        throw new Error(
                            "RECONCILE_SESSION_ABORTED"
                        );
                    }

                    const applyOrdinal = ++operationOrdinal;

                    const applied = await applyLedgerOpeningDurable({
                        supabase,
                        attemptId,
                        executionId,
                        operationToken: reservationToken,
                        operationOrdinal: applyOrdinal,
                        fetchOperationId: result.fetchOperationId
                    });

                    if (signal.aborted) {
                        throw new Error(
                            "RECONCILE_SESSION_ABORTED"
                        );
                    }

                    results.push({
                        ...result,
                        reconciliation: applied
                    });
                }
                              // ==============================================
                            // ==============================================
                // VOUCHER RECONCILIATION (READ-ONLY COMPARISON)
                // ==============================================
                // Durable Tally fetch is journaled.
                // No voucher amount settlement or resync occurs.

                const voucherResults = [];

                if (reconciliationVoucherGuids.length > 0) {

                    if (
                        !reconciliationVoucherDateRange ||
                        !reconciliationVoucherDateRange.fromDate ||
                        !reconciliationVoucherDateRange.toDate
                    ) {
                        throw new Error("RECONCILE_VOUCHER_DATE_RANGE_INVALID");
                    }

                    const uniqueVoucherGuids = new Set(
    reconciliationVoucherGuids
);

if (
    reconciliationVoucherGuids.some(
        guid =>
            typeof guid !== "string" ||
            !guid.trim()
    ) ||
    uniqueVoucherGuids.size !==
        reconciliationVoucherGuids.length
) {
    throw new Error(
        "RECONCILE_VOUCHER_GUIDS_INVALID"
    );
}

                    // SQL reader accepts at most 1000 voucher GUIDs per call.
                    // Voucher comparison performs a durable fetch only.
                    // No voucher amount-apply operation is authorized.
                    for (let offset = 0; offset < reconciliationVoucherGuids.length; offset += 1000) {
                        if (signal.aborted) {
                            throw new Error("RECONCILE_SESSION_ABORTED");
                        }
                        await lockIdentity.assertOwnership();

                        const voucherGuids = reconciliationVoucherGuids.slice(offset, offset + 1000);
                                              const sqlResult = await fetchSqlVoucherEntries({
                            supabase,
                            companyCode,
                            tallyOwner,
                            voucherGuids
                        });

                        if (
                            !sqlResult ||
                            !Array.isArray(sqlResult.entries) ||
                            sqlResult.sqlSnapshot?.complete !== true ||
                            !Array.isArray(sqlResult.sqlSnapshot.requestedVoucherGuids) ||
                            sqlResult.sqlSnapshot.requestedVoucherGuids.length !== voucherGuids.length ||
                            sqlResult.sqlSnapshot.requestedVoucherGuids.some(
                                (guid, index) => guid !== voucherGuids[index]
                            )
                        ) {
                            throw new Error(
                                "RECONCILE_VOUCHER_SQL_SNAPSHOT_INVALID"
                            );
                        }

                        const sqlEntries = sqlResult.entries;
                        if (
                            !Array.isArray(sqlEntries) ||
                            sqlEntries.length === 0 ||
                            sqlEntries.length > 10000
                        ) {
                            throw new Error("RECONCILE_VOUCHER_SQL_ROWS_INVALID");
                        }

                        // Do not perform a durable Tally fetch when the SQL
                        // snapshot is already structurally unsafe.
                        if (sqlEntries.some(row =>
                            row.is_deleted !== false ||
                            row.requiresStructuralReview !== false ||
                            !Array.isArray(row.bill_allocations) ||
                            !Array.isArray(row.costcentre_allocations) ||
                            row.bill_allocations.length !== 0 ||
                            row.costcentre_allocations.length !== 0
                        )) {
                            throw new Error("RECONCILE_VOUCHER_STRUCTURAL_REVIEW_REQUIRED");
                        }

                        const fetchOrdinal = ++operationOrdinal;
                        const fetched = await fetchVouchersDurable({
                            supabase,
                            socket,
                            signal,
                            attemptId,
                            operationToken: reservationToken,
                            executionId,
                            operationOrdinal: fetchOrdinal,
                            connectorId,
                            tallyCompanyGuid,
                            company: tallyCompanyName,
                            companyCode,
                            tallyOwner,
                            voucherGuids,
                            booksBeginningFrom,
                            fromDate: reconciliationVoucherDateRange.fromDate,
                            toDate: reconciliationVoucherDateRange.toDate
                        });

                        if (signal.aborted) {
                            throw new Error("RECONCILE_SESSION_ABORTED");
                        }

                                             const sqlPresentVoucherGuids = new Set(
                            sqlEntries.map(row => row.voucher_guid)
                        );

                        const comparison = compareVoucherLedgerEntries({
                            tallyEntries: fetched.entries,
                            sqlEntries,
                            companyCode,
                            tallyOwner,
                            snapshotCoverage: {
                                // SQL evidence is server-origin only.
                                sqlCompleteVoucherGuids: voucherGuids.filter(
                                    guid => sqlPresentVoucherGuids.has(guid)
                                ),

                                // Tally coverage must remain independent.
                                tallyCompleteVoucherGuids:
                                    fetched.snapshotCoverage?.tallyCompleteVoucherGuids || []
                            }
                        });

                        if (signal.aborted) {
                            throw new Error(
                                "RECONCILE_SESSION_ABORTED"
                            );
                        }

                        await lockIdentity.assertOwnership();

                        // Diagnostic comparison only.
                        // No voucher amount settlement is permitted.
                        voucherResults.push({
                            fetchOperationId: fetched.fetchOperationId,
                            voucherCount: voucherGuids.length,
                            comparisonSummary: comparison.summary,
                            voucherDecisions: comparison.voucherDecisions,
                            settlementStatus: "NOT_AUTHORIZED",
                            updatedRows: 0
                        });
                    }
                }
                                  // Final mismatch recheck while connector is locked.
                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                const remainingLedgerGuids =
                    await getMismatchLedgerGuids({
                        supabase,
                        companyCode,
                        tallyOwner
                    });

                if (
                    !Array.isArray(remainingLedgerGuids) ||
                    remainingLedgerGuids.some(
                        guid =>
                            typeof guid !== "string" ||
                            !guid.trim()
                    )
                ) {
                    throw new Error(
                        "RECONCILE_FINAL_MISMATCH_GUIDS_INVALID"
                    );
                }

                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                await lockIdentity.assertOwnership();

                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                let checkpointResponse;

                try {
                 checkpointResponse = await supabase.rpc(
                    "record_manual_reconcile_pre_release_checkpoint_v2",
                    {
                        p_attempt_id: attemptId,
                        p_operation_token: reservationToken,
                        p_execution_id: executionId,
                        p_connector_id: connectorId,
                        p_owner_token: lockIdentity.ownerToken,
                        p_remaining_mismatch_ledger_guids:
                            remainingLedgerGuids,
                        p_voucher_investigation_guids:
                            reconciliationVoucherGuids
                    }
                );
                } catch (cause) {
                    const error = new Error(
                        "RECONCILE_CHECKPOINT_OUTCOME_UNKNOWN"
                    );
                    error.cause = cause;
                    throw error;
                }

                if (checkpointResponse?.error) {
                    const error = new Error(
                        "RECONCILE_CHECKPOINT_OUTCOME_UNKNOWN"
                    );
                    error.cause = checkpointResponse.error;
                    throw error;
                }

                if (checkpointResponse?.data !== true) {
                    throw new Error(
                        "RECONCILE_CHECKPOINT_NOT_CONFIRMED"
                    );
                }

                if (signal.aborted) {
                    throw new Error("RECONCILE_SESSION_ABORTED");
                }

                return {
                    identity: {
                        attemptId,
                        executionId,
                        serverInstanceId,
                        connectorId,
                        socketId: lockIdentity.socketId,
                        ownerToken: lockIdentity.ownerToken
                    },
                    readings: {
                        success: true,
                        ledgers: results,
                        vouchers: voucherResults,
                    remainingMismatchLedgerGuids:
                        remainingLedgerGuids,

                    voucherInvestigationGuids: [
                        ...reconciliationVoucherGuids
                    ],

                    requiresTransactionInvestigation:
                        remainingLedgerGuids.length > 0 ||
                        reconciliationVoucherGuids.length > 0,

                    voucherSettlementStatus:
                        reconciliationVoucherGuids.length > 0
                            ? "NOT_AUTHORIZED"
                            : "NOT_REQUIRED",
                        ...(mismatchLedgerGuids.length === 0
                            ? { message: "NO_BALANCE_MISMATCHES" }
                            : {})
                    }
                };
            },
                        {
                exposeLockIdentity: true,
                releaseEvidence: true,

                completionIdentity: {
                    attemptId,
                    executionId,
                    serverInstanceId
                },

                manualCoordinatorReceiptWriter: async (receipt) => {
                                     if (
                        !receipt ||
                        receipt.attemptId !== attemptId ||
                        receipt.executionId !== executionId ||
                        receipt.serverInstanceId !== serverInstanceId ||
                        receipt.connectorId !== connectorId ||
                        receipt.coordinatorExited !== true ||
                        receipt.admissionSealed !== true ||
                        receipt.lateWorkDetected !== false ||
                        receipt.pendingWork !== 0 ||
                        receipt.locallySettled !== true ||
                        receipt.outcome !== "LOCALLY_SETTLED_UNVERIFIED" ||
                        receipt.callbackStarted !== true ||
                        receipt.callbackSettled !== true ||
                        typeof receipt.socketId !== "string" ||
                        !receipt.socketId ||
                        !UUID.test(receipt.ownerToken || "")
                    ) {
                        throw new Error(
                            "RECONCILE_COORDINATOR_RECEIPT_INVALID"
                        );
                    }
                    const response = await supabase.rpc(
                        "record_manual_reconcile_coordinator_exit_receipt_v1",
                        {
                            p_attempt_id: attemptId,
                            p_operation_token: reservationToken,
                            p_execution_id: executionId,
                            p_server_instance_id: serverInstanceId,
                            p_connector_id: connectorId,
                            p_socket_id: receipt.socketId,
                            p_owner_token: receipt.ownerToken,
                            p_coordinator_exited: true,
                            p_admission_sealed: true,
                            p_pending_work: 0,
                            p_callback_started: receipt.callbackStarted,
                            p_callback_settled: receipt.callbackSettled,
                            p_late_work_detected: false,
                            p_outcome: "LOCALLY_SETTLED_UNVERIFIED"
                        }
                    );

                    if (response?.error) {
                        throw response.error;
                    }

                    const result = Array.isArray(response?.data)
                        ? response.data.length === 1
                            ? response.data[0]
                            : null
                        : response?.data;

                    if (
                        result?.recorded !== true ||
                        result.reason !==
                            "RECEIPT_RECORDED_OR_ALREADY_PRESENT"
                    ) {
                        throw new Error(
                            "RECONCILE_COORDINATOR_RECEIPT_NOT_CONFIRMED"
                        );
                    }
                }
            }
        );

        if (
            !envelope ||
            !envelope.result ||
            !envelope.releaseEvidence
        ) {
            throw new Error(
                "RECONCILE_RELEASE_EVIDENCE_MISSING"
            );
        }

        const identity = envelope.result.identity;
        const receipt = envelope.releaseEvidence;

       if (
    !identity ||
    identity.attemptId !== attemptId ||
    identity.executionId !== executionId ||
    identity.serverInstanceId !== serverInstanceId ||
    identity.connectorId !== connectorId ||
    typeof identity.socketId !== "string" ||
    !identity.socketId ||
    !UUID.test(identity.ownerToken || "") ||
    !receipt ||
    receipt.redisReleaseConfirmed !== true ||
    receipt.attemptId !== attemptId ||
    receipt.executionId !== executionId ||
    receipt.serverInstanceId !== serverInstanceId ||
    receipt.connectorId !== connectorId ||
    receipt.socketId !== identity.socketId ||
    receipt.ownerToken !== identity.ownerToken
) {
    throw new Error(
        "RECONCILE_RELEASE_EVIDENCE_INVALID"
    );
}

           readings = envelope.result.readings;
        releaseIdentity = identity;
        releaseEvidence = receipt;
       } catch (error) {
        error.reconcileRecoveryRequired = true;
        error.reconcileAttemptId = attemptId;
        error.reconcileExecutionId = executionId;
        error.reconcileServerInstanceId = serverInstanceId;
        error.reconcileReservationToken = reservationToken;
        error.reconcileCompanyCode = companyCode;
        error.reconcileTallyOwner = tallyOwner;
        throw error;
    }

    // Coordinator has returned successfully, including
    // its final ownership and release checks.
    // No further Tally communication is allowed here.
        // Voucher reconciliation remains comparison-only.
    // Pending voucher investigation is persisted in finalization.
    // It must not authorize monetary settlement.

    try {
        const recordedResponse = await supabase.rpc(
            "record_manual_reconcile_finalization_v2",
            {
                p_attempt_id: attemptId,
                p_operation_token: reservationToken,
                p_remaining_mismatch_ledger_guids:
                    readings.remainingMismatchLedgerGuids,
                p_voucher_investigation_guids:
                    readings.voucherInvestigationGuids,
                p_final_recheck_completed: true,
                p_connector_release_verified: true,
                p_connector_id: connectorId,
                p_release_owner_token:
                    releaseIdentity.ownerToken,
                p_release_evidence: releaseEvidence
            }
        );

        if (recordedResponse.error) {
            throw recordedResponse.error;
        }

        const recorded = Array.isArray(recordedResponse.data)
            ? recordedResponse.data.length === 1
                ? recordedResponse.data[0]
                : null
            : recordedResponse.data;

        if (
            recorded?.recorded !== true ||
            !["EVIDENCE_RECORDED", "ALREADY_RECORDED"]
                .includes(recorded.reason)
        ) {
            throw new Error(
                "RECONCILE_FINALIZATION_NOT_CONFIRMED"
            );
        }

        const closeResponse = await supabase.rpc(
            "close_manual_reconcile_v2",
            {
                p_attempt_id: attemptId,
                p_operation_token: reservationToken
            }
        );

        if (closeResponse.error) {
            throw closeResponse.error;
        }

        const closed = Array.isArray(closeResponse.data)
            ? closeResponse.data.length === 1
                ? closeResponse.data[0]
                : null
            : closeResponse.data;

        if (
            closed?.closed !== true ||
            !["CLOSED", "ALREADY_CLOSED"].includes(closed.reason)
        ) {
            throw new Error(
                "RECONCILE_CLOSE_NOT_CONFIRMED"
            );
        }

        return readings;
      } catch (error) {
        error.reconcileCloseUncertain = true;
        error.reconcileRecoveryRequired = true;

        error.reconcileAttemptId = attemptId;
        error.reconcileExecutionId = executionId;
        error.reconcileServerInstanceId = serverInstanceId;
        error.reconcileReservationToken = reservationToken;
        error.reconcileCompanyCode = companyCode;
        error.reconcileTallyOwner = tallyOwner;

        // INTERNAL RECOVERY CONTEXT ONLY.
        // Never send this object in an API response or logs.
        // It contains the original Redis release evidence.
        error.reconcileRecoveryContext = {
            attemptId,
            executionId,
            serverInstanceId,
            companyCode,
            tallyOwner,
            reservationToken,
            releaseIdentity: { ...releaseIdentity },
            releaseEvidence: { ...releaseEvidence },
           remainingMismatchLedgerGuids: [
                ...readings.remainingMismatchLedgerGuids
            ],
            voucherInvestigationGuids: [
                ...readings.voucherInvestigationGuids
            ]
        };  

        throw error;
    }
}

module.exports = {
    runManualReconciliationSession
};