
"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const Module = require("node:module");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "../..");
const SESSION = path.join(
    ROOT,
    "services/manualReconciliation/ManualReconciliationSession.js"
);

const TOKEN = "11111111-1111-4111-8111-111111111111";
const OWNER = "22222222-2222-4222-8222-222222222222";
const SERVER = "33333333-3333-4333-8333-333333333333";

function makeHarness(options = {}) {
    const events = [];
    let lockActive = false;
    let mismatchCalls = 0;
    let receipt;
    let checkpointParams;
    let finalizationParams;

    const state = {
        events,
        get lockActive() { return lockActive; },
        get receipt() { return receipt; },
        get checkpointParams() { return checkpointParams; },
        get finalizationParams() { return finalizationParams; }
    };

    const identity = {
        connectorId: "connector-1",
        socketId: "socket-1",
        ownerToken: OWNER,
        async assertOwnership() {
            events.push("ownership");
            if (options.ownershipFailure) {
                throw new Error("OWNERSHIP_LOST");
            }
        }
    };

    const dependencies = {
        connectorOperationCoordinator: {
            async withGlobalConnectorLock(connectorId, callback, config) {
                events.push("lock");
                assert.equal(connectorId, "connector-1");
                assert.equal(config.exposeLockIdentity, true);
                assert.equal(config.releaseEvidence, true);
                assert.ok(config.completionIdentity.attemptId);
                assert.ok(config.completionIdentity.executionId);
                assert.equal(config.completionIdentity.serverInstanceId, SERVER);

                if (options.lockFailure) {
                    throw new Error("GLOBAL_LOCK_FAILED");
                }

                lockActive = true;
                let result;

                try {
                    result = await callback(
                        { companyGuids: ["company-guid-1"] },
                        new AbortController().signal,
                        identity
                    );
                } finally {
                    lockActive = false;
                }

                events.push("released");

                if (options.releaseFailure) {
                    throw new Error("RELEASE_OUTCOME_UNKNOWN");
                }

                receipt = {
                    ...config.completionIdentity,
                    connectorId,
                    socketId: identity.socketId,
                    ownerToken: identity.ownerToken,
                    redisReleaseConfirmed: true
                };

                if (options.invalidReceipt) {
                    receipt.executionId =
                        "44444444-4444-4444-8444-444444444444";
                }

                if (options.missingReceipt) {
                    return { result, releaseEvidence: null };
                }

                return { result, releaseEvidence: receipt };
            }
        },

        manualReconcileAdmissionV2: {
            async startManualReconcileV2() {
                events.push("admission");

                if (options.admissionFailure) {
                    throw new Error("ADMISSION_UNKNOWN");
                }

                return options.admission || {
                    acquired: true,
                    reason: "ACQUIRED",
                    reservationToken: TOKEN
                };
            }
        },

        manualReconcileExecutionBinding: {
            async bindManualReconcileExecution(args) {
                events.push("bind");
                assert.equal(lockActive, true);
                assert.equal(args.operationToken, TOKEN);
                assert.equal(args.ownerToken, OWNER);
                assert.equal(args.socketId, "socket-1");
                assert.equal(args.serverInstanceId, SERVER);

                if (options.bindingFailure) {
                    throw new Error("BIND_OUTCOME_UNKNOWN");
                }

                return {
                    bound: true,
                    executionId: args.executionId
                };
            }
        },

        manualReconcileServerIdentity: {
            getManualReconcileServerInstanceId() {
                return SERVER;
            }
        },

        getMismatchLedgerGuids: {
            async getMismatchLedgerGuids() {
                events.push("mismatch");
                assert.equal(lockActive, true);
                mismatchCalls++;

                return mismatchCalls === 1
                    ? (options.initialMismatch ?? ["ledger-1"])
                    : (options.remainingMismatch ?? []);
            }
        },

        fetchLedgerOpeningDurable: {
            async fetchLedgerOpeningDurable(args) {
                events.push("fetch");
                assert.equal(lockActive, true);
                assert.equal(args.operationOrdinal, 1);
                assert.ok(args.executionId);

                if (options.fetchFailure) {
                    throw new Error("FETCH_UNCERTAIN");
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
                assert.equal(lockActive, true);
                assert.equal(args.operationOrdinal, 2);
                assert.ok(args.executionId);

                if (options.applyFailure) {
                    throw new Error("APPLY_UNCERTAIN");
                }

                return { result: "UPDATED" };
            }
        }
    };

    const supabase = {
        async rpc(name, args) {
            events.push(name);

            if (
                name ===
                "record_manual_reconcile_pre_release_checkpoint_v1"
            ) {
                assert.equal(lockActive, true);
                checkpointParams = args;
                assert.equal(args.p_operation_token, TOKEN);
                assert.equal(args.p_owner_token, OWNER);

                if (options.checkpointThrow) {
                    throw new Error("CHECKPOINT_TRANSPORT_LOST");
                }

                if (options.checkpointError) {
                    return {
                        data: null,
                        error: new Error("CHECKPOINT_DB_ERROR")
                    };
                }

                return {
                    data: options.checkpointFalse ? false : true,
                    error: null
                };
            }

            assert.equal(lockActive, false);

            if (
                name ===
                "record_manual_reconcile_finalization_v1"
            ) {
                finalizationParams = args;
                assert.deepEqual(
                    args.p_release_evidence,
                    receipt
                );
                assert.equal(
                    args.p_connector_release_verified,
                    true
                );

                if (options.recorderThrow) {
                    throw new Error("RECORDER_TRANSPORT_LOST");
                }

                if (options.recorderError) {
                    return {
                        data: null,
                        error: new Error("RECORDER_DB_ERROR")
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
                if (options.closeThrow) {
                    throw new Error("CLOSE_TRANSPORT_LOST");
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

            throw new Error(`UNEXPECTED_RPC:${name}`);
        }
    };

    const originalLoad = Module._load;
    const originalCache = require.cache[SESSION];

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

    let run;

    try {
        delete require.cache[SESSION];
        run = require(SESSION).runManualReconciliationSession;
    } finally {
        Module._load = originalLoad;
        delete require.cache[SESSION];

        if (originalCache) {
            require.cache[SESSION] = originalCache;
        }
    }

    return {
        state,
        async run() {
            return run({
                supabase,
                companyCode: "C0053",
                tallyOwner: "USER",
                connectorId: "connector-1",
                tallyCompanyName: "Test Company",
                tallyCompanyGuid: "company-guid-1",
                booksBeginningFrom: "20260401"
            });
        }
    };
}

test("successful durable lifecycle runs in correct order", async () => {
    const h = makeHarness();
    const result = await h.run();

    assert.equal(result.success, true);
    assert.equal(result.ledgers.length, 1);

    assert.deepEqual(
        h.state.events.filter(x => [
            "admission",
            "lock",
            "bind",
            "fetch",
            "apply",
            "record_manual_reconcile_pre_release_checkpoint_v1",
            "released",
            "record_manual_reconcile_finalization_v1",
            "close_manual_reconcile_v2"
        ].includes(x)),
        [
            "admission",
            "lock",
            "bind",
            "fetch",
            "apply",
            "record_manual_reconcile_pre_release_checkpoint_v1",
            "released",
            "record_manual_reconcile_finalization_v1",
            "close_manual_reconcile_v2"
        ]
    );
});

test("zero mismatches still bind, checkpoint, release and close", async () => {
    const h = makeHarness({ initialMismatch: [] });
    const result = await h.run();

    assert.equal(result.message, "NO_BALANCE_MISMATCHES");
    assert.equal(result.ledgers.length, 0);
    assert.equal(h.state.events.includes("fetch"), false);
    assert.equal(h.state.events.includes("apply"), false);
    assert.equal(h.state.events.includes("bind"), true);
    assert.equal(h.state.events.includes("released"), true);
    assert.equal(
        h.state.events.includes("close_manual_reconcile_v2"),
        true
    );
});

test("admission ambiguity prevents connector work", async () => {
    const h = makeHarness({ admissionFailure: true });

    await assert.rejects(
        h.run(),
        e =>
            e.reconcileAdmissionUncertain === true &&
            e.reconcileRecoveryRequired === true
    );

    assert.equal(h.state.events.includes("lock"), false);
});

test("ALREADY_ACQUIRED never redispatches", async () => {
    const h = makeHarness({
        admission: {
            acquired: true,
            reason: "ALREADY_ACQUIRED",
            reservationToken: TOKEN
        }
    });

    const result = await h.run();

    assert.equal(result.recoveryRequired, true);
    assert.equal(h.state.events.includes("lock"), false);
});

test("lock failure preserves reservation", async () => {
    const h = makeHarness({ lockFailure: true });

    await assert.rejects(
        h.run(),
        e =>
            e.message === "GLOBAL_LOCK_FAILED" &&
            e.reconcileRecoveryRequired === true
    );

    assert.equal(h.state.events.includes("bind"), false);
});

test("binding failure prevents FETCH and checkpoint", async () => {
    const h = makeHarness({ bindingFailure: true });

    await assert.rejects(h.run(), /BIND_OUTCOME_UNKNOWN/);

    assert.equal(h.state.events.includes("fetch"), false);
    assert.equal(
        h.state.events.includes(
            "record_manual_reconcile_pre_release_checkpoint_v1"
        ),
        false
    );
});

for (const option of [
    "fetchFailure",
    "applyFailure",
    "checkpointThrow",
    "checkpointError",
    "checkpointFalse",
    "releaseFailure",
    "missingReceipt",
    "invalidReceipt"
]) {
    test(`${option} prevents finalization and close`, async () => {
        const h = makeHarness({ [option]: true });

        await assert.rejects(
            h.run(),
            e => e.reconcileRecoveryRequired === true
        );

        assert.equal(
            h.state.events.includes(
                "record_manual_reconcile_finalization_v1"
            ),
            false
        );

        assert.equal(
            h.state.events.includes("close_manual_reconcile_v2"),
            false
        );
    });
}

test("recorder ambiguity preserves original receipt for recovery", async () => {
    const h = makeHarness({ recorderThrow: true });

    await assert.rejects(h.run(), e => {
        assert.equal(e.reconcileRecoveryRequired, true);

        assert.deepEqual(
            e.reconcileRecoveryContext?.releaseEvidence,
            h.state.receipt
        );

        assert.deepEqual(
            e.reconcileRecoveryContext?.remainingMismatchLedgerGuids,
            []
        );

        return true;
    });

    assert.equal(
        h.state.events.includes("close_manual_reconcile_v2"),
        false
    );
});

test("recorder DB error prevents close", async () => {
    const h = makeHarness({ recorderError: true });

    await assert.rejects(
        h.run(),
        e => e.reconcileRecoveryRequired === true
    );

    assert.equal(
        h.state.events.includes("close_manual_reconcile_v2"),
        false
    );
});

test("close ambiguity preserves recovery context", async () => {
    const h = makeHarness({ closeThrow: true });

    await assert.rejects(
        h.run(),
        e =>
            e.reconcileRecoveryRequired === true &&
            e.reconcileRecoveryContext
                ?.releaseEvidence
                ?.redisReleaseConfirmed === true
    );
});

test("exact recorder and close replay are accepted", async () => {
    const h = makeHarness({
        recordReplay: true,
        closeReplay: true
    });

    const result = await h.run();

    assert.equal(result.success, true);
    assert.equal(
        h.state.events.includes("close_manual_reconcile_v2"),
        true
    );
});
