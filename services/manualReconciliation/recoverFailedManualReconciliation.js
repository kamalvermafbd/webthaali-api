
"use strict";


const {
    inspectManualReconcileRecovery,
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");
const {
    sendManualReconcileFenceRequest
} = require("../../socketio/sendManualReconcileFenceRequest");
const {
    sendManualReconcileExecutionObservationRequest
} = require(
    "../../socketio/sendManualReconcileExecutionObservationRequest"
);

const {
    getManualReconcileCoordinatorLifecycle
} = require("../../socketio/connectorOperationCoordinator");



const {
    inspectManualReconcileFailureRedis,
    stageManualReconcileFailureCleanup,
    readManualReconcileFailureCleanupReceipt,
    unblockManualReconcileAfterSqlClosure,
    readManualReconcileFailureUnblockReceipt
} = require("../../socketio/manualReconcileFailureRecoveryStore");


const {
    manualReconcileFailureSettlement
} = require("./manualReconcileFailureSettlement");


const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;


function blocked(attemptId, reason) {
    return {
        recovered: false,
        attemptId,
        reason,
        recoveryRequired: true,
        settlementEligible: false
    };
}

function sameExecution(before, after) {
    const a = before?.attempt;
    const e = before?.execution;
    const s = before?.status;

    const b = after?.attempt;
    const f = after?.execution;
    const t = after?.status;

    return Boolean(
        a && e && s && b && f && t &&
        a.attempt_id === b.attempt_id &&
        a.operation_token === b.operation_token &&
        a.reservation_id === b.reservation_id &&
        a.company_code === b.company_code &&
        a.tally_owner === b.tally_owner &&
        e.attempt_id === f.attempt_id &&
        e.reservation_id === f.reservation_id &&
        e.operation_token === f.operation_token &&
        e.execution_id === f.execution_id &&
        e.connector_id === f.connector_id &&
        e.socket_id === f.socket_id &&
        e.owner_token === f.owner_token &&
        e.server_instance_id === f.server_instance_id &&
        s.id === t.id &&
        s.company_code === t.company_code &&
        s.tally_owner === t.tally_owner &&
        s.operation_token === t.operation_token &&
        s.status === t.status
    );
}

/**
 * Explicit failure-recovery transport attempt only.
 * Never settles a reconciliation, releases Redis, or clears quarantine.
 * Do not invoke from an inspection endpoint or an automatic retry loop.
 */


function validFailureIdentity(attemptId, snapshot) {
    const a = snapshot?.attempt;
    const e = snapshot?.execution;
    const s = snapshot?.status;

    return Boolean(
        a && e && s &&
        a.attempt_id === attemptId &&
        e.attempt_id === attemptId &&
        e.reservation_id === a.reservation_id &&
        e.operation_token === a.operation_token &&
        s.id === a.reservation_id &&
        s.company_code === a.company_code &&
        s.tally_owner === a.tally_owner &&
        s.status === "FAILED" &&
        s.operation_token === null &&
        s.last_terminal_attempt_id === attemptId &&
        s.closed_at != null &&
        UUID.test(a.operation_token || "") &&
        UUID.test(e.execution_id || "") &&
        UUID.test(e.server_instance_id || "") &&
        UUID.test(e.owner_token || "") &&
        typeof e.connector_id === "string" &&
        Boolean(e.connector_id.trim()) &&
        typeof e.socket_id === "string" &&
        Boolean(e.socket_id.trim()) &&
        snapshot.finalization === null
    );
}


function failureRedisIdentity(attemptId, execution) {
    return {
        attemptId,
        executionId: execution.execution_id,
        serverInstanceId: execution.server_instance_id,
        connectorId: execution.connector_id,
        socketId: execution.socket_id,
        ownerToken: execution.owner_token
    };
}

function receiptMatchesIdentity(receipt, identity) {
    return Boolean(
        receipt &&
        typeof receipt === "object" &&
        !Array.isArray(receipt) &&
        Object.keys(receipt).length === 8 &&
        receipt.attemptId === identity.attemptId &&
        receipt.executionId === identity.executionId &&
        receipt.serverInstanceId === identity.serverInstanceId &&
        receipt.connectorId === identity.connectorId &&
        receipt.socketId === identity.socketId &&
        receipt.ownerToken === identity.ownerToken &&
        receipt.stage === "UNBLOCKED_AFTER_SQL_CLOSURE" &&
        receipt.markersCleared === true
    );
}

async function confirmFailureUnblock({
    supabase,
    attemptId,
    execution,
    redisIdentity,
    unblockResult,
    extra = {}
}) {
    const failure = reason => ({
        ...blocked(attemptId, reason),
        sqlSettled: true,
        ...extra
    });

    if (unblockResult?.unblocked !== true) {
        return {
            ...failure("REDIS_UNBLOCK_NOT_CONFIRMED"),
            unblockReason: unblockResult?.reason
        };
    }

    let receipt;

    try {
        receipt =
            await readManualReconcileFailureUnblockReceipt(
                attemptId
            );
    } catch (_) {
        return failure("REDIS_UNBLOCK_RECEIPT_READ_UNCERTAIN");
    }

    if (
        !receiptMatchesIdentity(receipt, redisIdentity) ||
        !receiptMatchesIdentity(
            unblockResult.receipt,
            redisIdentity
        )
    ) {
        return failure("REDIS_UNBLOCK_RECEIPT_INVALID");
    }

    // Recheck SQL closure before submitting confirmation.
    let recovery;

    try {
        const response = await supabase
            .from("manual_reconcile_failure_recovery_evidence")
            .select(
                "attempt_id,execution_id,connector_id," +
                "socket_id,owner_token," +
                "coordinator_server_instance_id," +
                "recovery_state,remote_execution_fenced," +
                "redis_cleanup_confirmed," +
                "redis_stage_confirmed_at,terminal_closed_at"
            )
            .eq("attempt_id", attemptId)
            .single();

        if (response.error) {
            return failure("SQL_CLOSURE_RECHECK_UNCERTAIN");
        }

        recovery = response.data;
    } catch (_) {
        return failure("SQL_CLOSURE_RECHECK_UNCERTAIN");
    }

    if (
        !recovery ||
        recovery.attempt_id !== attemptId ||
        recovery.execution_id !== execution.execution_id ||
        recovery.connector_id !== execution.connector_id ||
        recovery.socket_id !== execution.socket_id ||
        recovery.owner_token !== execution.owner_token ||
        recovery.coordinator_server_instance_id !==
            execution.server_instance_id ||
        recovery.recovery_state !== "SETTLED" ||
        recovery.remote_execution_fenced !== true ||
        recovery.redis_cleanup_confirmed !== true ||
        !recovery.redis_stage_confirmed_at ||
        !recovery.terminal_closed_at
    ) {
        return failure("SQL_CLOSURE_RECHECK_INVALID");
    }

    let response;

    try {
        response = await supabase.rpc(
            "confirm_manual_reconcile_failure_unblock_v1",
            {
                p_attempt_id: attemptId,
                p_execution_id: execution.execution_id,
                p_server_instance_id:
                    execution.server_instance_id,
                p_connector_id: execution.connector_id,
                p_socket_id: execution.socket_id,
                p_owner_token: execution.owner_token,
                p_redis_unblock_receipt: receipt
            }
        );
    } catch (_) {
        return failure("SQL_UNBLOCK_CONFIRMATION_OUTCOME_UNCERTAIN");
    }

    if (response?.error) {
        return failure("SQL_UNBLOCK_CONFIRMATION_OUTCOME_UNCERTAIN");
    }

    const result = Array.isArray(response?.data)
        ? response.data.length === 1
            ? response.data[0]
            : null
        : response?.data;

    if (
        result?.confirmed !== true ||
        ![
            "UNBLOCK_CONFIRMED",
            "ALREADY_CONFIRMED"
        ].includes(result.reason)
    ) {
        return {
            ...failure("SQL_UNBLOCK_CONFIRMATION_NOT_VERIFIED"),
            confirmationReason: result?.reason
        };
    }

    return {
        recovered: true,
        attemptId,
        reason: "FAILURE_SETTLED_REDIS_UNBLOCK_SQL_CONFIRMED",
        recoveryRequired: false,
        settlementEligible: false,
        redisUnblocked: true,
        sqlUnblockConfirmed: true,
        ...extra
    };
}

async function resumeSqlClosedFailure({
    supabase,
    attemptId
}) {
    let recovery;

    try {
        const response = await supabase
            .from("manual_reconcile_failure_recovery_evidence")
            .select(
                "attempt_id,execution_id,connector_id," +
                "socket_id,owner_token," +
                "coordinator_server_instance_id," +
                "recovery_state,remote_execution_fenced," +
                "redis_cleanup_confirmed," +
                "redis_stage_receipt," +
                "redis_stage_confirmed_at,terminal_closed_at," +
                "redis_unblock_receipt," +
                "redis_unblock_confirmed_at"
            )
            .eq("attempt_id", attemptId)
            .maybeSingle();

        if (response.error) {
            return blocked(
                attemptId,
                "SQL_CLOSED_RECOVERY_READ_UNCERTAIN"
            );
        }

        recovery = response.data;
    } catch (_) {
        return blocked(
            attemptId,
            "SQL_CLOSED_RECOVERY_READ_UNCERTAIN"
        );
    }

    if (!recovery) {
        return null;
    }

    const closureStarted =
        recovery.terminal_closed_at !== null ||
        recovery.recovery_state === "SETTLED" ||
        recovery.redis_unblock_receipt !== null ||
        recovery.redis_unblock_confirmed_at !== null;

    if (!closureStarted) {
        return null;
    }

    if (
        recovery.recovery_state !== "SETTLED" ||
        recovery.remote_execution_fenced !== true ||
        recovery.redis_cleanup_confirmed !== true ||
        !recovery.redis_stage_receipt ||
        !recovery.redis_stage_confirmed_at ||
        !recovery.terminal_closed_at
    ) {
        return blocked(
            attemptId,
            "SQL_CLOSED_RECOVERY_EVIDENCE_INVALID"
        );
    }

    let snapshot;

    try {
        snapshot = await readManualReconcileRecoverySnapshot(
            supabase,
            attemptId
        );
    } catch (_) {
        return blocked(
            attemptId,
            "SQL_CLOSED_SNAPSHOT_UNCERTAIN"
        );
    }

    if (
        !validFailureIdentity(attemptId, snapshot) ||
        snapshot.status.status !== "FAILED"
    ) {
        return blocked(
            attemptId,
            "SQL_CLOSED_EXECUTION_IDENTITY_INVALID"
        );
    }

    const execution = snapshot.execution;
    const redisIdentity =
        failureRedisIdentity(attemptId, execution);

    if (
        recovery.attempt_id !== attemptId ||
        recovery.execution_id !== execution.execution_id ||
        recovery.connector_id !== execution.connector_id ||
        recovery.socket_id !== execution.socket_id ||
        recovery.owner_token !== execution.owner_token ||
        recovery.coordinator_server_instance_id !==
            execution.server_instance_id
    ) {
        return blocked(
            attemptId,
            "SQL_CLOSED_RECOVERY_IDENTITY_MISMATCH"
        );
    }

    const expectedStage = {
        ...redisIdentity,
        stage: "LOCKS_CLEARED_MARKERS_RETAINED",
        markersRetained: true
    };

    const actualStage = recovery.redis_stage_receipt;

    if (
        !actualStage ||
        typeof actualStage !== "object" ||
        Array.isArray(actualStage) ||
        Object.keys(actualStage).length !==
            Object.keys(expectedStage).length ||
        Object.keys(expectedStage).some(
            key => actualStage[key] !== expectedStage[key]
        )
    ) {
        return blocked(
            attemptId,
            "SQL_CLOSED_STAGE_RECEIPT_INVALID"
        );
    }

    let unblockResult;

    try {
        unblockResult =
            await unblockManualReconcileAfterSqlClosure({
                ...redisIdentity,
                supabase
            });
    } catch (_) {
        return blocked(
            attemptId,
            "SQL_CLOSED_REDIS_UNBLOCK_UNCERTAIN"
        );
    }

    return confirmFailureUnblock({
        supabase,
        attemptId,
        execution,
        redisIdentity,
        unblockResult,
        extra: {
            sqlClosedResume: true,
            sqlSettled: true
        }
    });
}

async function recoverFailedManualReconciliation({
    supabase,
    attemptId,
    timeoutMs = 10000
} = {}) {
    if (
        !supabase || typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" || !UUID.test(attemptId) ||
        !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000
    ) {
        throw new Error("RECONCILE_FAILURE_RECOVERY_INPUT_INVALID");
    }

    attemptId = attemptId.toLowerCase();

    // SQL-closed recovery resumes without another remote fence.
    const closedResume = await resumeSqlClosedFailure({
        supabase,
        attemptId
    });

    if (closedResume !== null) {
        return closedResume;
    }

    const inspection = await inspectManualReconcileRecovery({
        supabase,
        attemptId
    });
    if (inspection.state !== "EXECUTION_UNCERTAIN" ||
        inspection.reservationOwned !== true) {
        return blocked(attemptId, `NOT_ELIGIBLE_${inspection.state}`);
    }

    const before = await readManualReconcileRecoverySnapshot(
        supabase,
        attemptId
    );
    const attempt = before.attempt;
    const execution = before.execution;
    const status = before.status;

    
    if (
    !attempt || !execution || !status ||
    execution.attempt_id !== attemptId ||
    execution.reservation_id !== attempt.reservation_id ||
    execution.operation_token !== attempt.operation_token ||
    status.id !== attempt.reservation_id ||
    attempt.attempt_id !== attemptId ||
    !UUID.test(attempt.operation_token || "") ||
    !UUID.test(execution.execution_id || "") ||
    typeof execution.connector_id !== "string" ||
    !execution.connector_id.trim() ||
    typeof execution.socket_id !== "string" ||
    !execution.socket_id.trim() ||
    status.status !== "RUNNING" ||
    status.operation_token !== attempt.operation_token ||
    status.company_code !== attempt.company_code ||
    status.tally_owner !== attempt.tally_owner ||
    before.finalization !== null
) {
    return blocked(attemptId, "ORIGINAL_EXECUTION_UNVERIFIED");
}

    // This RPC records intent only. It does NOT establish remote quiescence.
    let response;
    try {
        response = await supabase.rpc(
            "request_manual_reconcile_failure_fence_v2",
            {
                p_attempt_id: attemptId,
                p_operation_token: attempt.operation_token,
                p_execution_id: execution.execution_id
            }
        );
    } catch (_error) {
        return blocked(attemptId, "FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    if (response?.error) {
        return blocked(attemptId, "FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    const result = Array.isArray(response?.data)
        ? response.data.length === 1 ? response.data[0] : null
        : response?.data;
   if (
    !result ||
    typeof result !== "object" ||
    Array.isArray(result) ||
    result.fence_requested !== true ||
    !["FENCE_REQUESTED", "ALREADY_REQUESTED"].includes(result.reason) ||
    typeof result.fence_id !== "string" ||
    !UUID.test(result.fence_id)
) {
    return blocked(attemptId, "FENCE_INTENT_NOT_CONFIRMED");
}

    // Re-read after the intent RPC to avoid dispatch on a changed identity.
    const after = await readManualReconcileRecoverySnapshot(
        supabase,
        attemptId
    );
    if (
        !sameExecution(before, after) ||
        after.status?.status !== "RUNNING" ||
        after.status?.operation_token !== attempt.operation_token ||
        after.finalization !== null
    ) {
        return blocked(attemptId, "EXECUTION_CHANGED_AFTER_FENCE_INTENT");
    }

    const fenceId = result.fence_id.toLowerCase();
    try {
        const acknowledgement = await sendManualReconcileFenceRequest({
            connectorId: execution.connector_id,
            expectedSocketId: execution.socket_id,
            attemptId,
            executionId: execution.execution_id,
            fenceId,
            timeoutMs
        });

                       // Validate fence acknowledgement before observation.
        const fenceSnapshot = acknowledgement?.snapshot;

        if (
            acknowledgement?.settlementEligible !== false ||
            acknowledgement?.attemptId !== attemptId ||
            acknowledgement?.executionId !== execution.execution_id ||
            acknowledgement?.connectorId !== execution.connector_id ||
            acknowledgement?.socketId !== execution.socket_id ||
            acknowledgement?.fenceId !== fenceId ||
            !UUID.test(fenceSnapshot?.processEpoch || "") ||
            fenceSnapshot?.fenced !== true ||
            fenceSnapshot?.fenceId !== fenceId ||
            fenceSnapshot?.settlementEligible !== false ||
            typeof fenceSnapshot?.executionKnown !== "boolean" ||
            typeof fenceSnapshot?.identityConflict !== "boolean"
        ) {
            return {
                ...blocked(attemptId, "FENCE_ACK_IDENTITY_INVALID"),
                fenceAcknowledged: false
            };
        }

        // An installed safety fence does not prove that the
        // requested execution identity exists in this process.
        if (
            fenceSnapshot.executionKnown !== true ||
            fenceSnapshot.identityConflict !== false
        ) {
            return {
                ...blocked(
                    attemptId,
                    fenceSnapshot.identityConflict
                        ? "FENCE_EXECUTION_IDENTITY_CONFLICT"
                        : "FENCE_EXECUTION_UNKNOWN"
                ),
                fenceAcknowledged: true,
                executionKnown: fenceSnapshot.executionKnown,
                identityConflict: fenceSnapshot.identityConflict,
                observationVerified: false
            };
        }
        // =====================================================
        // DURABLE FAILURE FENCE EVIDENCE
        // Only after verified connector fence acknowledgement.
        // Does not authorize Redis cleanup or SQL closure.
        // =====================================================

        const processEpoch =
            fenceSnapshot.processEpoch.toLowerCase();

        const remoteFenceEvidence = {
            attemptId,
            executionId: execution.execution_id,
            fenceId,
            connectorId: execution.connector_id,
            socketId: execution.socket_id,
            processEpoch,
            fenced: true,
            executionKnown: true,
            identityConflict: false,
            settlementEligible: false
        };

        let fenceEvidenceResponse;

        try {
            fenceEvidenceResponse = await supabase.rpc(
                "record_manual_reconcile_failure_fence_evidence_v1",
                {
                    p_attempt_id: attemptId,
                    p_operation_token: attempt.operation_token,
                    p_execution_id: execution.execution_id,
                    p_fence_id: fenceId,
                    p_connector_process_epoch: processEpoch,
                    p_remote_fence_evidence: remoteFenceEvidence
                }
            );
        } catch (_error) {
            return {
                ...blocked(
                    attemptId,
                    "FENCE_EVIDENCE_OUTCOME_UNCERTAIN"
                ),
                fenceAcknowledged: true,
                observationVerified: false
            };
        }

        if (fenceEvidenceResponse?.error) {
            return {
                ...blocked(
                    attemptId,
                    "FENCE_EVIDENCE_OUTCOME_UNCERTAIN"
                ),
                fenceAcknowledged: true,
                observationVerified: false
            };
        }

        const fenceEvidenceResult =
            Array.isArray(fenceEvidenceResponse?.data)
                ? fenceEvidenceResponse.data.length === 1
                    ? fenceEvidenceResponse.data[0]
                    : null
                : fenceEvidenceResponse?.data;

        if (
            !fenceEvidenceResult ||
            fenceEvidenceResult.recorded !== true ||
            ![
                "FENCE_EVIDENCE_RECORDED",
                "ALREADY_RECORDED"
            ].includes(fenceEvidenceResult.reason)
        ) {
            return {
                ...blocked(
                    attemptId,
                    "FENCE_EVIDENCE_NOT_CONFIRMED"
                ),
                fenceAcknowledged: true,
                observationVerified: false
            };
        }
        let observationResult;

        try {
            observationResult =
                await sendManualReconcileExecutionObservationRequest({
                    connectorId: execution.connector_id,
                    expectedSocketId: execution.socket_id,
                    attemptId,
                    executionId: execution.execution_id,
                    expectedProcessEpoch:
                        acknowledgement.snapshot.processEpoch,
                    expectedFenceId: fenceId,
                    timeoutMs
                });
        } catch (error) {
            return {
                ...blocked(
                    attemptId,
                    "EXECUTION_OBSERVATION_UNVERIFIED"
                ),
                fenceAcknowledged: true,
                observationVerified: false,
                observationReason:
                    error?.message ||
                    "RECONCILE_OBSERVATION_FAILED"
            };
        }

                      // =====================================================
        // STEP 3D — VERIFIED REMOTE OBSERVATION + LOCAL DRAIN
        // =====================================================

        const observation = observationResult?.observation;

        if (
            observationResult?.settlementEligible !== false ||
            observationResult?.attemptId !== attemptId ||
            observationResult?.executionId !== execution.execution_id ||
            observationResult?.connectorId !== execution.connector_id ||
            observationResult?.socketId !== execution.socket_id ||
            observation?.attemptId !== attemptId ||
            observation?.executionId !== execution.execution_id ||
            observation?.processEpoch !== processEpoch ||
            observation?.fenceId !== fenceId ||
            observation?.fenced !== true ||
            observation?.executionKnown !== true ||
            observation?.pending !== 0 ||
            observation?.running !== 0 ||
            observation?.settled !== 1 ||
            observation?.locallyDrained !== true ||
            observation?.settlementEligible !== false
        ) {
            return {
                ...blocked(
                    attemptId,
                    "EXECUTION_OBSERVATION_IDENTITY_INVALID"
                ),
                fenceAcknowledged: true,
                fenceEvidenceRecorded: true,
                observationVerified: false
            };
        }

        const lifecycle =
            getManualReconcileCoordinatorLifecycle(attemptId);

        if (
            !lifecycle ||
            lifecycle.attemptId !== attemptId ||
            lifecycle.executionId !== execution.execution_id ||
            lifecycle.serverInstanceId !==
                execution.server_instance_id ||
            lifecycle.connectorId !== execution.connector_id ||
            lifecycle.socketId !== execution.socket_id ||
            lifecycle.ownerToken !== execution.owner_token ||
            lifecycle.coordinatorExited !== true ||
            lifecycle.pendingWork !== 0 ||
            lifecycle.locallySettled !== true ||
            lifecycle.outcome !== "LOCALLY_SETTLED_UNVERIFIED" ||
            lifecycle.recoveryAuthorized !== false
        ) {
            return {
                ...blocked(
                    attemptId,
                    "COORDINATOR_DRAIN_NOT_VERIFIED"
                ),
                fenceAcknowledged: true,
                fenceEvidenceRecorded: true,
                observationVerified: true,
                coordinatorDrainVerified: false
            };
        }

        // Recheck SQL execution identity after asynchronous
        // connector observation and coordinator inspection.
        const verifiedSnapshot =
            await readManualReconcileRecoverySnapshot(
                supabase,
                attemptId
            );

        if (
            !sameExecution(before, verifiedSnapshot) ||
            verifiedSnapshot.status?.status !== "RUNNING" ||
            verifiedSnapshot.finalization !== null
        ) {
            return {
                ...blocked(
                    attemptId,
                    "EXECUTION_CHANGED_AFTER_DRAIN"
                ),
                observationVerified: true,
                coordinatorDrainVerified: false
            };
        }

              // =====================================================
        // STEP 3E — DURABLE COORDINATOR DRAIN EVIDENCE
        // =====================================================

        // Recheck process-local lifecycle immediately before
        // recording durable evidence.
        const finalLifecycle =
            getManualReconcileCoordinatorLifecycle(attemptId);

        if (
            !finalLifecycle ||
            finalLifecycle.attemptId !== attemptId ||
            finalLifecycle.executionId !== execution.execution_id ||
            finalLifecycle.serverInstanceId !==
                execution.server_instance_id ||
            finalLifecycle.connectorId !== execution.connector_id ||
            finalLifecycle.socketId !== execution.socket_id ||
            finalLifecycle.ownerToken !== execution.owner_token ||
            finalLifecycle.coordinatorExited !== true ||
            finalLifecycle.pendingWork !== 0 ||
            finalLifecycle.locallySettled !== true ||
            finalLifecycle.outcome !== "LOCALLY_SETTLED_UNVERIFIED" ||
            finalLifecycle.recoveryAuthorized !== false
        ) {
            return {
                ...blocked(
                    attemptId,
                    "COORDINATOR_DRAIN_CHANGED"
                ),
                fenceAcknowledged: true,
                observationVerified: true,
                coordinatorDrainVerified: false
            };
        }
// FAIL-CLOSED: Durable original-coordinator quiescence
// receipt verification is not implemented yet.
//
// Process-local lifecycle observations cannot authorize
// Redis cleanup or SQL failure settlement.
//
// Remove this guard only after the original coordinator
// receipt writer and all SQL authorization checks are deployed.
return {
    ...blocked(
        attemptId,
        "ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED"
    ),
    fenceAcknowledged: true,
    observationVerified: true,
    coordinatorDrainVerified: false,
    durableDrainRecorded: false
};
        let drainResponse;

        try {
            drainResponse = await supabase.rpc(
                "record_manual_reconcile_coordinator_drain_v1",
                {
                    p_attempt_id: attemptId,
                    p_operation_token: attempt.operation_token,
                    p_execution_id: execution.execution_id,
                    p_fence_id: fenceId,
                    p_server_instance_id:
                        execution.server_instance_id,
                    p_connector_process_epoch: processEpoch
                }
            );
        } catch (_error) {
            return {
                ...blocked(
                    attemptId,
                    "COORDINATOR_DRAIN_RECORD_OUTCOME_UNCERTAIN"
                ),
                fenceAcknowledged: true,
                observationVerified: true,
                coordinatorDrainVerified: true,
                durableDrainRecorded: false
            };
        }

        if (drainResponse?.error) {
            return {
                ...blocked(
                    attemptId,
                    "COORDINATOR_DRAIN_RECORD_OUTCOME_UNCERTAIN"
                ),
                fenceAcknowledged: true,
                observationVerified: true,
                coordinatorDrainVerified: true,
                durableDrainRecorded: false
            };
        }

        const drainResult =
            Array.isArray(drainResponse?.data)
                ? drainResponse.data.length === 1
                    ? drainResponse.data[0]
                    : null
                : drainResponse?.data;

        if (
            !drainResult ||
            drainResult.recorded !== true ||
            ![
                "DRAIN_EVIDENCE_RECORDED",
                "ALREADY_RECORDED"
            ].includes(drainResult.reason)
        ) {
            return {
                ...blocked(
                    attemptId,
                    "COORDINATOR_DRAIN_RECORD_NOT_CONFIRMED"
                ),
                fenceAcknowledged: true,
                observationVerified: true,
                coordinatorDrainVerified: true,
                durableDrainRecorded: false
            };
        }

     
        // =====================================================
        // FAILURE RECOVERY — REDIS STAGE / SQL / UNBLOCK
        // =====================================================

        const redisIdentity = {
            attemptId,
            executionId: execution.execution_id,
            serverInstanceId: execution.server_instance_id,
            connectorId: execution.connector_id,
            socketId: execution.socket_id,
            ownerToken: execution.owner_token
        };

        const failureBlocked = (reason, extra = {}) => ({
            ...blocked(attemptId, reason),
            fenceAcknowledged: true,
            fenceEvidenceRecorded: true,
            observationVerified: true,
            coordinatorDrainVerified: true,
            durableDrainRecorded: true,
            fenceId,
            processEpoch,
            ...extra
        });

        // A previous execution may have staged Redis successfully
        // but lost its SQL response. Never stage twice.
        let existingReceipt;

        try {
            existingReceipt =
                await readManualReconcileFailureCleanupReceipt(
                    attemptId
                );
        } catch (_error) {
            return failureBlocked(
                "REDIS_STAGE_RECEIPT_READ_UNCERTAIN"
            );
        }

        let redisStageExecuted = false;
        let redisStageAuthorized = false;

        if (existingReceipt) {
            if (
                existingReceipt.attemptId !== attemptId ||
                existingReceipt.executionId !==
                    execution.execution_id ||
                existingReceipt.serverInstanceId !==
                    execution.server_instance_id ||
                existingReceipt.connectorId !==
                    execution.connector_id ||
                existingReceipt.socketId !==
                    execution.socket_id ||
                existingReceipt.ownerToken !==
                    execution.owner_token ||
                existingReceipt.stage !==
                    "LOCKS_CLEARED_MARKERS_RETAINED" ||
                existingReceipt.markersRetained !== true
            ) {
                return failureBlocked(
                    "EXISTING_REDIS_STAGE_IDENTITY_INVALID"
                );
            }

            redisStageExecuted = true;
        } else {
            // =================================================
            // DURABLE SQL AUTHORIZATION
            // =================================================

            let authorizationResponse;

            try {
                authorizationResponse = await supabase.rpc(
                    "authorize_manual_reconcile_failure_redis_stage_v1",
                    {
                        p_attempt_id: attemptId,
                        p_operation_token: attempt.operation_token,
                        p_execution_id: execution.execution_id,
                        p_fence_id: fenceId,
                        p_server_instance_id:
                            execution.server_instance_id,
                        p_connector_process_epoch: processEpoch
                    }
                );
            } catch (_error) {
                return failureBlocked(
                    "REDIS_STAGE_AUTHORIZATION_OUTCOME_UNCERTAIN"
                );
            }

            if (authorizationResponse?.error) {
                return failureBlocked(
                    "REDIS_STAGE_AUTHORIZATION_OUTCOME_UNCERTAIN"
                );
            }

            const authorization =
                Array.isArray(authorizationResponse?.data)
                    ? authorizationResponse.data.length === 1
                        ? authorizationResponse.data[0]
                        : null
                    : authorizationResponse?.data;

            if (
                !authorization ||
                authorization.authorized !== true ||
                authorization.reason !==
                    "REDIS_STAGE_AUTHORIZED" ||
                authorization.connector_id !==
                    execution.connector_id ||
                authorization.socket_id !==
                    execution.socket_id ||
                authorization.owner_token !==
                    execution.owner_token
            ) {
                return failureBlocked(
                    "REDIS_STAGE_AUTHORIZATION_NOT_CONFIRMED"
                );
            }

            redisStageAuthorized = true;

            // =================================================
            // ATOMIC READ-ONLY REDIS PREFLIGHT
            // =================================================

            let preflight;

            try {
                preflight =
                    await inspectManualReconcileFailureRedis(
                        redisIdentity
                    );
            } catch (_error) {
                return failureBlocked(
                    "REDIS_PREFLIGHT_OUTCOME_UNCERTAIN",
                    { redisStageAuthorized }
                );
            }

            if (
                !preflight ||
                preflight.attemptId !== attemptId ||
                preflight.executionId !==
                    execution.execution_id ||
                preflight.connectorId !==
                    execution.connector_id ||
                preflight.socketId !== execution.socket_id ||
                preflight.ownershipConsistent !== true ||
                preflight.connectorActiveOwned !== true ||
                preflight.socketActiveOwned !== true ||
                preflight.connectorLockCompatible !== true ||
                preflight.socketLockCompatible !== true ||
                preflight.quarantinesCompatible !== true ||
                preflight.stageReceiptExists !== false ||
                preflight.successReceiptExists !== false ||
                preflight.cleanupAuthorized !== false
            ) {
                return failureBlocked(
                    "REDIS_PREFLIGHT_NOT_SAFE",
                    {
                        redisStageAuthorized,
                        quarantineRepairCandidate:
                            preflight?.quarantineRepairCandidate === true
                    }
                );
            }

            // =================================================
            // ATOMIC REDIS STAGING
            // =================================================

            let stageResult;

            try {
                stageResult =
                    await stageManualReconcileFailureCleanup({
                        ...redisIdentity,
                        authorization
                    });
            } catch (_error) {
                return failureBlocked(
                    "REDIS_STAGE_OUTCOME_UNCERTAIN",
                    { redisStageAuthorized }
                );
            }

            if (
                stageResult?.staged !== true ||
                stageResult.reason !==
                    "LOCKS_CLEARED_MARKERS_RETAINED"
            ) {
                return failureBlocked(
                    "REDIS_STAGE_NOT_CONFIRMED",
                    {
                        redisStageAuthorized,
                        redisStageReason: stageResult?.reason
                    }
                );
            }

            redisStageExecuted = true;
        }

        // =====================================================
        // SQL TERMINAL FAILURE SETTLEMENT
        // =====================================================

        let settlement;

        try {
            settlement = await manualReconcileFailureSettlement({
                supabase,
                attemptId,
                fenceId,
                processEpoch
            });
        } catch (_error) {
            return failureBlocked(
                "SQL_SETTLEMENT_OUTCOME_UNCERTAIN",
                { redisStageExecuted }
            );
        }

        if (settlement?.settled !== true) {
            return failureBlocked(
                settlement?.reason ||
                    "SQL_SETTLEMENT_NOT_CONFIRMED",
                {
                    redisStageExecuted,
                    sqlSettled: false
                }
            );
        }

        // =====================================================
        // SQL CLOSED — REDIS UNBLOCK
        // =====================================================

        let unblockResult;

        try {
            unblockResult =
                await unblockManualReconcileAfterSqlClosure({
                    ...redisIdentity,
                    supabase
                });
        } catch (_error) {
            return failureBlocked(
                "REDIS_UNBLOCK_OUTCOME_UNCERTAIN",
                {
                    redisStageExecuted: true,
                    sqlSettled: true
                }
            );
        }

        if (unblockResult?.unblocked !== true) {
            return failureBlocked(
                "REDIS_UNBLOCK_NOT_CONFIRMED",
                {
                    redisStageExecuted: true,
                    sqlSettled: true,
                    unblockReason: unblockResult?.reason
                }
            );
        }

       
        return await confirmFailureUnblock({
            supabase,
            attemptId,
            execution,
            redisIdentity,
            unblockResult,
            extra: {
                fenceAcknowledged: true,
                fenceEvidenceRecorded: true,
                observationVerified: true,
                coordinatorDrainVerified: true,
                durableDrainRecorded: true,
                redisStageExecuted: true,
                sqlSettled: true,
                fenceId,
                processEpoch
            }
        });




    } catch (error) {
        return {
            ...blocked(attemptId, "FENCE_TRANSPORT_UNVERIFIED"),
            fenceAcknowledged: false,
            transportReason: error?.message || "UNKNOWN"
        };
    }
}

module.exports = { recoverFailedManualReconciliation };
