
"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const Module = require("node:module");
const path = require("node:path");

const SESSION = path.resolve(
    __dirname,
    "../../services/manualReconciliation/ManualReconciliationSession.js"
);

const TOKEN = "11111111-1111-4111-8111-111111111111";
const OWNER = "22222222-2222-4222-8222-222222222222";
const SERVER = "33333333-3333-4333-8333-333333333333";
const VOUCHER = "voucher-1";

function makeHarness(options = {}) {
    const events = [];

    let locked = false;
    let mismatchReads = 0;
    let receipt = null;
    let checkpoint = null;
    let finalization = null;
    let voucherFetches = 0;

    const initial = options.initialMismatch ?? ["ledger-1"];
    const vouchers = options.voucherGuids ?? [];

    const identity = {
        connectorId: "connector-1",
        socketId: "socket-1",
        ownerToken: OWNER,

        async assertOwnership() {
            events.push("ownership");

            if (options.ownershipFailure) {
                throw Error("OWNERSHIP_LOST");
            }
        }
    };

    const dependencies = {
        connectorOperationCoordinator: {
            async withGlobalConnectorLock(
                connectorId,
                callback,
                config
            ) {
                events.push("lock");

                assert.equal(connectorId, "connector-1");
                assert.equal(config.exposeLockIdentity, true);
                assert.equal(config.releaseEvidence, true);
                assert.equal(
                    config.completionIdentity.serverInstanceId,
                    SERVER
                );

                if (options.lockFailure) {
                    throw Error("GLOBAL_LOCK_FAILED");
                }

                locked = true;
                let result;

                try {
                    result = await callback(
                        { companyGuids: ["company-guid-1"] },
                        new AbortController().signal,
                        identity
                    );
                } finally {
                    locked = false;
                }

                events.push("released");

                if (options.releaseFailure) {
                    throw Error("RELEASE_OUTCOME_UNKNOWN");
                }

                receipt = {
                    ...config.completionIdentity,
                    connectorId,
                    socketId: identity.socketId,
                    ownerToken: identity.ownerToken,
                    redisReleaseConfirmed: true
                };

                if (options.invalidReceipt) {
                    receipt.executionId = TOKEN;
                }

                return {
                    result,
                    releaseEvidence: options.missingReceipt
                        ? null
                        : receipt
                };
            }
        },

        manualReconcileClaimedAdmission: {
            async startManualReconcileClaimed(args) {
                events.push("admission");

                assert.equal(locked, false);
                assert.equal(args.batchId, TOKEN);
                assert.equal(args.executionClaimToken, OWNER);

                if (options.admissionFailure) {
                    throw Error("ADMISSION_UNKNOWN");
                }

                return options.admission ?? {
                    acquired: true,
                    reason: "ACQUIRED",
                    reservationToken: TOKEN
                };
            }
        },

        manualReconcileExecutionBinding: {
            async bindManualReconcileExecution(args) {
                events.push("bind");

                assert.equal(locked, true);
                assert.equal(args.operationToken, TOKEN);
                assert.equal(args.ownerToken, OWNER);
                assert.equal(args.serverInstanceId, SERVER);

                if (options.bindingFailure) {
                    throw Error("BIND_OUTCOME_UNKNOWN");
                }

                return {
                    bound: true,
                    executionId: args.executionId
                };
            }
        },

        manualReconcileServerIdentity: {
            getManualReconcileServerInstanceId: () => SERVER
        },

        getMismatchLedgerGuids: {
            async getMismatchLedgerGuids() {
                events.push("mismatch");

                assert.equal(locked, true);

                return ++mismatchReads === 1
                    ? initial
                    : (options.remainingMismatch ?? []);
            }
        },

        getReconciliationVoucherGuids: {
            async getReconciliationVoucherGuids(args) {
                events.push("voucher-guids");

                assert.equal(locked, true);
                assert.deepEqual(args.ledgerGuids, initial);

                return vouchers;
            }
        },

        getReconciliationVoucherDateRange: {
            async getReconciliationVoucherDateRange(args) {
                events.push("voucher-dates");

                assert.equal(locked, true);
                assert.deepEqual(args.voucherGuids, vouchers);

                return options.dateRange ?? {
                    fromDate: "20260401",
                    toDate: "20260430"
                };
            }
        },

        fetchLedgerOpeningDurable: {
            async fetchLedgerOpeningDurable(args) {
                events.push("fetch");

                assert.equal(locked, true);
                assert.equal(args.operationOrdinal, 1);

                if (options.fetchFailure) {
                    throw Error("FETCH_UNCERTAIN");
                }

                return {
                    guid: args.ledgerGuid,
                    fetchOperationId:
                        "55555555-5555-4555-8555-555555555555",
                    openingBalance: "100.00",
                    openingBalanceType: "CR"
                };
            }
        },

        applyLedgerOpeningDurable: {
            async applyLedgerOpeningDurable(args) {
                events.push("apply");

                assert.equal(locked, true);
                assert.equal(args.operationOrdinal, 2);

                if (options.applyFailure) {
                    throw Error("APPLY_UNCERTAIN");
                }

                return { result: "UPDATED" };
            }
        },

        fetchSqlVoucherEntries: {
            async fetchSqlVoucherEntries(args) {
                events.push("sql-vouchers");

                assert.equal(locked, true);
                assert.deepEqual(args.voucherGuids, vouchers);

                if (options.invalidSqlSnapshot) {
                    return {
                        entries: [],
                        sqlSnapshot: { complete: false }
                    };
                }

                return {
                    entries: vouchers.map(voucher_guid => ({
                        voucher_guid,
                        is_deleted: false,
                        requiresStructuralReview:
                            !!options.structuralReview,
                        bill_allocations: [],
                        costcentre_allocations: []
                    })),

                    sqlSnapshot: {
                        complete: true,
                        requestedVoucherGuids: [...vouchers]
                    }
                };
            }
        },

        fetchVouchersDurable: {
            async fetchVouchersDurable(args) {
                events.push("voucher-fetch");

                assert.equal(locked, true);
                assert.equal(
                    args.operationOrdinal,
                    initial.length * 2 + 1
                );

                voucherFetches++;

                if (options.voucherFetchFailure) {
                    throw Error("VOUCHER_FETCH_UNCERTAIN");
                }

                return {
                    fetchOperationId:
                        "66666666-6666-4666-8666-666666666666",
                    entries: [],
                    snapshotCoverage: {
                        tallyCompleteVoucherGuids: [...vouchers]
                    }
                };
            }
        },

        serverCompareVoucherLedgerEntries: {
            compareVoucherLedgerEntries(args) {
                events.push("voucher-compare");

                assert.equal(locked, true);
                assert.ok(Array.isArray(args.sqlEntries));

                return {
                    summary: { compared: vouchers.length },
                    voucherDecisions: []
                };
            }
        }
    };

    const supabase = {
        async rpc(name, args) {
            events.push(name);

            if (
                name ===
                "record_manual_reconcile_pre_release_checkpoint_v2"
            ) {
                assert.equal(locked, true);

                checkpoint = args;

                assert.equal(args.p_operation_token, TOKEN);
                assert.equal(args.p_owner_token, OWNER);

                assert.deepEqual(
                    args.p_remaining_mismatch_ledger_guids,
                    options.remainingMismatch ?? []
                );

                assert.deepEqual(
                    args.p_voucher_investigation_guids,
                    vouchers
                );

                if (options.checkpointThrow) {
                    throw Error("CHECKPOINT_TRANSPORT_LOST");
                }

                if (options.checkpointError) {
                    return {
                        data: null,
                        error: Error("CHECKPOINT_DB_ERROR")
                    };
                }

                return {
                    data: !options.checkpointFalse,
                    error: null
                };
            }

            assert.equal(locked, false);

            if (
                name ===
                "record_manual_reconcile_finalization_v2"
            ) {
                finalization = args;

                assert.deepEqual(
                    args.p_release_evidence,
                    receipt
                );

                assert.equal(
                    args.p_connector_release_verified,
                    true
                );

                assert.deepEqual(
                    args.p_voucher_investigation_guids,
                    vouchers
                );

                assert.deepEqual(
                    args.p_remaining_mismatch_ledger_guids,
                    options.remainingMismatch ?? []
                );

                if (options.recorderThrow) {
                    throw Error("RECORDER_TRANSPORT_LOST");
                }

                if (options.recorderError) {
                    return {
                        data: null,
                        error: Error("RECORDER_DB_ERROR")
                    };
                }

                return {
                    data: {
                        recorded: true,
                        reason: options.recordReplay
                            ? "ALREADY_RECORDED"
                            : "EVIDENCE_RECORDED"
                    },
                    error: null
                };
            }

            if (name === "close_manual_reconcile_v2") {
                assert.equal(args.p_operation_token, TOKEN);

                if (options.closeThrow) {
                    throw Error("CLOSE_TRANSPORT_LOST");
                }

                return {
                    data: {
                        closed: true,
                        reason: options.closeReplay
                            ? "ALREADY_CLOSED"
                            : "CLOSED"
                    },
                    error: null
                };
            }

            throw Error(`UNEXPECTED_RPC:${name}`);
        }
    };

    const originalLoad = Module._load;
    const cached = require.cache[SESSION];

    let run;

    Module._load = function(request, parent, isMain) {
        if (parent?.filename === SESSION) {
            const key = path.basename(request);

            if (
                Object.prototype.hasOwnProperty.call(
                    dependencies,
                    key
                )
            ) {
                return dependencies[key];
            }
        }

        return originalLoad.apply(this, arguments);
    };

    try {
        delete require.cache[SESSION];

        run = require(SESSION)
            .runManualReconciliationSession;
    } finally {
        Module._load = originalLoad;

        delete require.cache[SESSION];

        if (cached) {
            require.cache[SESSION] = cached;
        }
    }

    return {
        state: {
            events,

            get locked() {
                return locked;
            },

            get receipt() {
                return receipt;
            },

            get checkpoint() {
                return checkpoint;
            },

            get finalization() {
                return finalization;
            },

            get voucherFetches() {
                return voucherFetches;
            }
        },

        run(extra = {}) {
            return run({
                supabase,
                companyCode: "C0053",
                tallyOwner: "USER",
                connectorId: "connector-1",
                tallyCompanyName: "Test Company",
                tallyCompanyGuid: "company-guid-1",
                booksBeginningFrom: "20260401",
                batchId: TOKEN,
                executionClaimToken: OWNER,
                ...extra
            });
        }
    };
}

test(
    "V2 ledger lifecycle: checkpoint before release, finalization and close after",
    async () => {
        const h = makeHarness();
        const result = await h.run();

        assert.equal(result.success, true);
        assert.equal(result.ledgers.length, 1);

        const e = h.state.events;

        const order = [
            "admission",
            "lock",
            "bind",
            "fetch",
            "apply",
            "record_manual_reconcile_pre_release_checkpoint_v2",
            "released",
            "record_manual_reconcile_finalization_v2",
            "close_manual_reconcile_v2"
        ];

        let previous = -1;

        for (const event of order) {
            const index = e.indexOf(event);

            assert.ok(
                index > previous,
                `Event out of order: ${event}`
            );

            previous = index;
        }

        assert.equal(
            result.requiresTransactionInvestigation,
            false
        );

        assert.equal(
            result.voucherSettlementStatus,
            "NOT_REQUIRED"
        );
    }
);

test(
    "zero mismatches still checkpoint, release, finalize and close",
    async () => {
        const h = makeHarness({
            initialMismatch: []
        });

        const result = await h.run();

        assert.equal(
            result.message,
            "NO_BALANCE_MISMATCHES"
        );

        assert.deepEqual(result.ledgers, []);

        assert.equal(
            h.state.events.includes("fetch"),
            false
        );

        assert.equal(
            h.state.events.includes("apply"),
            false
        );

        assert.ok(
            h.state.events.includes(
                "close_manual_reconcile_v2"
            )
        );
    }
);

test(
    "voucher investigation stays comparison-only and is persisted in V2 evidence",
    async () => {
        const h = makeHarness({
            voucherGuids: [VOUCHER]
        });

        const result = await h.run();

        assert.equal(result.success, true);

        assert.equal(
            result.requiresTransactionInvestigation,
            true
        );

        assert.equal(
            result.voucherSettlementStatus,
            "NOT_AUTHORIZED"
        );

        assert.deepEqual(
            result.voucherInvestigationGuids,
            [VOUCHER]
        );

        assert.equal(result.vouchers.length, 1);

        assert.equal(
            result.vouchers[0].settlementStatus,
            "NOT_AUTHORIZED"
        );

        assert.equal(
            result.vouchers[0].updatedRows,
            0
        );

        assert.equal(h.state.voucherFetches, 1);

        assert.deepEqual(
            h.state.checkpoint.p_voucher_investigation_guids,
            [VOUCHER]
        );

        assert.deepEqual(
            h.state.finalization.p_voucher_investigation_guids,
            [VOUCHER]
        );

        assert.ok(
            !h.state.events.some(
                x => /voucher.*apply|apply.*voucher/i.test(x)
            )
        );
    }
);

test(
    "remaining ledger mismatch requires investigation",
    async () => {
        const h = makeHarness({
            remainingMismatch: ["ledger-1"]
        });

        const result = await h.run();

        assert.equal(
            result.requiresTransactionInvestigation,
            true
        );

        assert.deepEqual(
            result.remainingMismatchLedgerGuids,
            ["ledger-1"]
        );
    }
);

test(
    "ledger and voucher investigation coexist",
    async () => {
        const h = makeHarness({
            remainingMismatch: ["ledger-1"],
            voucherGuids: [VOUCHER]
        });

        const result = await h.run();

        assert.equal(
            result.requiresTransactionInvestigation,
            true
        );

        assert.deepEqual(
            result.voucherInvestigationGuids,
            [VOUCHER]
        );

        assert.deepEqual(
            result.remainingMismatchLedgerGuids,
            ["ledger-1"]
        );
    }
);

test(
    "invalid voucher GUIDs fail before checkpoint",
    async () => {
        for (const voucherGuids of [
            [""],
            [VOUCHER, VOUCHER]
        ]) {
            const h = makeHarness({ voucherGuids });

            await assert.rejects(
                h.run(),
                e => e.reconcileRecoveryRequired === true
            );

            assert.ok(
                !h.state.events.includes(
                    "record_manual_reconcile_pre_release_checkpoint_v2"
                )
            );
        }
    }
);

test(
    "unsafe SQL voucher evidence fails before durable Tally voucher fetch",
    async () => {
        for (const option of [
            "invalidSqlSnapshot",
            "structuralReview"
        ]) {
            const h = makeHarness({
                voucherGuids: [VOUCHER],
                [option]: true
            });

            await assert.rejects(
                h.run(),
                e => e.reconcileRecoveryRequired === true
            );

            assert.equal(
                h.state.voucherFetches,
                0
            );

            assert.ok(
                !h.state.events.includes(
                    "record_manual_reconcile_finalization_v2"
                )
            );
        }
    }
);

test(
    "admission ambiguity prevents connector lock",
    async () => {
        const h = makeHarness({
            admissionFailure: true
        });

        await assert.rejects(
            h.run(),
            e =>
                e.reconcileAdmissionUncertain === true &&
                e.reconcileRecoveryRequired === true
        );

        assert.equal(
            h.state.events.includes("lock"),
            false
        );
    }
);

test(
    "ALREADY_ACQUIRED does not redispatch",
    async () => {
        const h = makeHarness({
            admission: {
                acquired: true,
                reason: "ALREADY_ACQUIRED",
                reservationToken: TOKEN
            }
        });

        const result = await h.run();

        assert.equal(result.recoveryRequired, true);

        assert.equal(
            h.state.events.includes("lock"),
            false
        );
    }
);

test(
    "lock failure preserves reservation",
    async () => {
        const h = makeHarness({
            lockFailure: true
        });

        await assert.rejects(
            h.run(),
            e =>
                e.message === "GLOBAL_LOCK_FAILED" &&
                e.reconcileRecoveryRequired === true
        );

        assert.equal(
            h.state.events.includes("bind"),
            false
        );
    }
);

test(
    "binding failure prevents fetch and checkpoint",
    async () => {
        const h = makeHarness({
            bindingFailure: true
        });

        await assert.rejects(
            h.run(),
            /BIND_OUTCOME_UNKNOWN/
        );

        assert.equal(
            h.state.events.includes("fetch"),
            false
        );

        assert.equal(
            h.state.events.includes(
                "record_manual_reconcile_pre_release_checkpoint_v2"
            ),
            false
        );
    }
);

for (const option of [
    "fetchFailure",
    "applyFailure",
    "voucherFetchFailure",
    "checkpointThrow",
    "checkpointError",
    "checkpointFalse",
    "releaseFailure",
    "missingReceipt",
    "invalidReceipt",
    "ownershipFailure"
]) {
    test(
        `${option} prevents finalization and close`,
        async () => {
            const h = makeHarness({
                [option]: true,
                voucherGuids:
                    option === "voucherFetchFailure"
                        ? [VOUCHER]
                        : []
            });

            await assert.rejects(
                h.run(),
                e => e.reconcileRecoveryRequired === true
            );

            assert.equal(
                h.state.events.includes(
                    "record_manual_reconcile_finalization_v2"
                ),
                false
            );

            assert.equal(
                h.state.events.includes(
                    "close_manual_reconcile_v2"
                ),
                false
            );
        }
    );
}

test(
    "recorder transport ambiguity retains voucher and Redis evidence",
    async () => {
        const h = makeHarness({
            recorderThrow: true,
            voucherGuids: [VOUCHER]
        });

        await assert.rejects(
            h.run(),
            e => {
                assert.equal(
                    e.reconcileRecoveryRequired,
                    true
                );

                assert.deepEqual(
                    e.reconcileRecoveryContext?.releaseEvidence,
                    h.state.receipt
                );

                assert.deepEqual(
                    e.reconcileRecoveryContext?.voucherInvestigationGuids,
                    [VOUCHER]
                );

                return true;
            }
        );

        assert.equal(
            h.state.events.includes(
                "close_manual_reconcile_v2"
            ),
            false
        );
    }
);

test(
    "recorder DB error prevents close",
    async () => {
        const h = makeHarness({
            recorderError: true
        });

        await assert.rejects(
            h.run(),
            e => e.reconcileRecoveryRequired === true
        );

        assert.equal(
            h.state.events.includes(
                "close_manual_reconcile_v2"
            ),
            false
        );
    }
);

test(
    "close ambiguity preserves recovery context",
    async () => {
        const h = makeHarness({
            closeThrow: true
        });

        await assert.rejects(
            h.run(),
            e =>
                e.reconcileRecoveryRequired === true &&
                e.reconcileRecoveryContext
                    ?.releaseEvidence
                    ?.redisReleaseConfirmed === true
        );
    }
);

test(
    "exact V2 recorder and close replay are accepted",
    async () => {
        const h = makeHarness({
            recordReplay: true,
            closeReplay: true
        });

        const result = await h.run();

        assert.equal(result.success, true);

        assert.ok(
            h.state.events.includes(
                "close_manual_reconcile_v2"
            )
        );
    }
);

test(
    "caller-supplied ledger GUIDs are rejected before admission",
    async () => {
        const h = makeHarness();

        await assert.rejects(
            h.run({
                ledgerGuids: ["ledger-1"]
            }),
            /RECONCILE_MANUAL_LEDGER_GUIDS_NOT_ALLOWED/
        );

        assert.deepEqual(
            h.state.events,
            []
        );
    }
);
