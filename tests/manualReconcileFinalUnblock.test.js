
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const Module = require("node:module");
const { randomUUID } = require("node:crypto");
const path = require("node:path");

const target = path.resolve(
    __dirname,
    "../services/manualReconciliation/recoverFailedManualReconciliation.js"
);

function setup(overrides = {}) {
    const attemptId = randomUUID();
    const operationToken = randomUUID();
    const executionId = randomUUID();
    const ownerToken = randomUUID();
    const serverInstanceId = randomUUID();

    const execution = {
        attempt_id: attemptId,
        reservation_id: "12345",
        operation_token: operationToken,
        execution_id: executionId,
        connector_id: "CON-TEST",
        socket_id: "socket-test",
        owner_token: ownerToken,
        server_instance_id: serverInstanceId
    };

    const snapshot = {
        attempt: {
            attempt_id: attemptId,
            operation_token: operationToken,
            reservation_id: "12345",
            company_code: "C-TEST",
            tally_owner: "USER"
        },
        execution,
        status: {
            id: "12345",
            company_code: "C-TEST",
            tally_owner: "USER",
            status: "FAILED",
            operation_token: null,
            last_terminal_attempt_id: attemptId,
            closed_at: "2026-01-01"
        },
        finalization: null
    };

    const identity = {
        attemptId,
        executionId,
        serverInstanceId,
        connectorId: "CON-TEST",
        socketId: "socket-test",
        ownerToken
    };

    const stage = {
        ...identity,
        stage: "LOCKS_CLEARED_MARKERS_RETAINED",
        markersRetained: true
    };

    const receipt = {
        ...identity,
        stage: "UNBLOCKED_AFTER_SQL_CLOSURE",
        markersCleared: true
    };

    const evidence = {
        attempt_id: attemptId,
        execution_id: executionId,
        connector_id: "CON-TEST",
        socket_id: "socket-test",
        owner_token: ownerToken,
        coordinator_server_instance_id: serverInstanceId,
        coordinator_unsafe_detected: false,
        coordinator_unsafe_detected_at: null,
        recovery_state: "SETTLED",
        remote_execution_fenced: true,
        redis_cleanup_confirmed: true,
        redis_stage_receipt: stage,
        redis_stage_confirmed_at: "2026-01-01",
        terminal_closed_at: "2026-01-01",
        redis_unblock_receipt: null,
        redis_unblock_confirmed_at: null
    };

    const calls = {
        unblock: 0,
        fence: 0,
        rpc: [],
        snapshot: 0
    };

    const supabase = {
        from(name) {
            assert.equal(
                name,
                "manual_reconcile_failure_recovery_evidence"
            );

            return {
                select() {
                    return this;
                },
                eq() {
                    return this;
                },
                async maybeSingle() {
                    return {
                        data: overrides.evidence === undefined
                            ? evidence
                            : overrides.evidence,
                        error: overrides.evidenceError || null
                    };
                },
                async single() {
                    return {
                        data: overrides.recheck === undefined
                            ? evidence
                            : overrides.recheck,
                        error: overrides.recheckError || null
                    };
                }
            };
        },

        async rpc(name) {
            calls.rpc.push(name);

            if (overrides.rpcThrows) {
                throw new Error("sql offline");
            }

            return overrides.rpcResponse || {
                data: {
                    confirmed: true,
                    reason: "UNBLOCK_CONFIRMED"
                },
                error: null
            };
        }
    };

    const mocks = {
        inspectManualReconcileRecovery: {
            inspectManualReconcileRecovery: async () => {
                throw new Error("must not inspect");
            },
            readManualReconcileRecoverySnapshot: async () => {
                calls.snapshot++;
                return overrides.snapshot || snapshot;
            }
        },

        sendManualReconcileFenceRequest: {
            sendManualReconcileFenceRequest: async () => {
                calls.fence++;
                throw new Error("must not fence");
            }
        },

        sendManualReconcileExecutionObservationRequest: {
            sendManualReconcileExecutionObservationRequest: async () => {
                throw new Error("must not observe");
            }
        },

        connectorOperationCoordinator: {
            getManualReconcileCoordinatorLifecycle: async () => {
                throw new Error("must not coordinate");
            }
        },

        manualReconcileFailureRecoveryStore: {
            inspectManualReconcileFailureRedis: async () => {},
            stageManualReconcileFailureCleanup: async () => {},
            readManualReconcileFailureCleanupReceipt: async () => {},

            unblockManualReconcileAfterSqlClosure: async () => {
                calls.unblock++;

                if (overrides.unblockThrows) {
                    throw new Error("redis offline");
                }

                return overrides.unblockResult === undefined
                    ? { unblocked: true, receipt }
                    : overrides.unblockResult;
            },

            readManualReconcileFailureUnblockReceipt: async () => {
                if (overrides.receiptThrows) {
                    throw new Error("redis offline");
                }

                return overrides.receipt === undefined
                    ? receipt
                    : overrides.receipt;
            }
        },

        manualReconcileFailureSettlement: {
            manualReconcileFailureSettlement: async () => {
                throw new Error("must not settle again");
            }
        }
    };

    const original = Module._load;

    delete require.cache[target];

    Module._load = function (request, parent, isMain) {
        if (parent?.filename === target) {
            const name = path.basename(request);

            if (mocks[name]) {
                return mocks[name];
            }
        }

        return original.apply(this, arguments);
    };

    let recover;

    try {
        recover = require(target)
            .recoverFailedManualReconciliation;
    } finally {
        Module._load = original;
    }

    return {
        run: () => recover({ supabase, attemptId }),
        calls
    };
}

const cases = [

    [
    "unsafe coordinator blocks Redis unblock",
    {
        evidence: {
            coordinator_unsafe_detected: true,
            recovery_state: "SETTLED",
            terminal_closed_at: "2026-01-01"
        }
    },
    false,
    "COORDINATOR_UNSAFE_OR_UNVERIFIED",
    0
],
[
    "unverified coordinator blocks Redis unblock",
    {
        evidence: {
            recovery_state: "SETTLED",
            terminal_closed_at: "2026-01-01"
        }
    },
    false,
    "COORDINATOR_UNSAFE_OR_UNVERIFIED",
    0
],
    [
        "confirmed SQL unblock succeeds",
        {},
        true,
        "FAILURE_SETTLED_REDIS_UNBLOCK_SQL_CONFIRMED",
        1
    ],
    [
        "Redis unblock throws",
        { unblockThrows: true },
        false,
        "SQL_CLOSED_REDIS_UNBLOCK_UNCERTAIN",
        1
    ],
    [
        "Redis unblock rejects",
        { unblockResult: { unblocked: false, reason: "CONFLICT" } },
        false,
        "REDIS_UNBLOCK_NOT_CONFIRMED",
        1
    ],
    [
        "Redis receipt read throws",
        { receiptThrows: true },
        false,
        "REDIS_UNBLOCK_RECEIPT_READ_UNCERTAIN",
        1
    ],
    [
        "SQL closure recheck fails",
        { recheckError: { message: "db error" } },
        false,
        "SQL_CLOSURE_RECHECK_UNCERTAIN",
        1
    ],
    [
        "SQL confirmation throws",
        { rpcThrows: true },
        false,
        "SQL_UNBLOCK_CONFIRMATION_OUTCOME_UNCERTAIN",
        1
    ],
    [
        "SQL confirmation rejects",
        {
            rpcResponse: {
                data: { confirmed: false, reason: "REJECTED" },
                error: null
            }
        },
        false,
        "SQL_UNBLOCK_CONFIRMATION_NOT_VERIFIED",
        1
    ],
    [
        "SQL evidence invalid",
        {
            evidence: {
    coordinator_unsafe_detected: false,
    recovery_state: "SETTLED",
    terminal_closed_at: "2026-01-01"
}
        },
        false,
        "SQL_CLOSED_RECOVERY_EVIDENCE_INVALID",
        0
    ],
    [
        "SQL read failure",
        { evidenceError: { message: "db error" } },
        false,
        "SQL_CLOSED_RECOVERY_READ_UNCERTAIN",
        0
    ],
    [
        "unblock receipt missing",
        { receipt: null },
        false,
        "REDIS_UNBLOCK_RECEIPT_INVALID",
        1
    ],
    [
        "already confirmed SQL response",
        {
            rpcResponse: {
                data: {
                    confirmed: true,
                    reason: "ALREADY_CONFIRMED"
                },
                error: null
            }
        },
        true,
        "FAILURE_SETTLED_REDIS_UNBLOCK_SQL_CONFIRMED",
        1
    ]
];

for (const [name, options, success, reason, unblocks] of cases) {
    test(name, async () => {
        const h = setup(options);
        const result = await h.run();

        assert.equal(result.recovered, success);
        assert.equal(result.reason, reason);
        assert.equal(h.calls.unblock, unblocks);
        assert.equal(h.calls.fence, 0);
        assert.equal(h.calls.snapshot, unblocks ? 1 : 0);

        const expectedRpc =
            success ||
            reason.startsWith("SQL_UNBLOCK_CONFIRMATION")
                ? ["confirm_manual_reconcile_failure_unblock_v1"]
                : [];

        assert.deepEqual(h.calls.rpc, expectedRpc);
    });
}
