
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const Module = require("node:module");

function fixture() {
    const attemptId = randomUUID();
    const executionId = randomUUID();
    const operationToken = randomUUID();
    const serverInstanceId = randomUUID();
    const ownerToken = randomUUID();

    const snapshot = {
        attempt: {
            attempt_id: attemptId,
            operation_token: operationToken,
            reservation_id: "12345"
        },
        execution: {
            attempt_id: attemptId,
            reservation_id: "12345",
            operation_token: operationToken,
            execution_id: executionId,
            server_instance_id: serverInstanceId,
            owner_token: ownerToken,
            connector_id: "CON-TEST",
            socket_id: "SOCKET-TEST"
        },
        status: {
            id: "12345",
            status: "RUNNING",
            operation_token: operationToken
        },
        finalization: null
    };

    const receipt = {
        attemptId,
        executionId,
        serverInstanceId,
        connectorId: "CON-TEST",
        socketId: "SOCKET-TEST",
        ownerToken,
        stage: "LOCKS_CLEARED_MARKERS_RETAINED",
        markersRetained: true
    };

    return {
        attemptId,
        executionId,
        operationToken,
        serverInstanceId,
        ownerToken,
        fenceId: randomUUID(),
        processEpoch: randomUUID(),
        snapshot,
        receipt
    };
}

function harness(options = {}) {
    const data = fixture();

    if (options.modifySnapshot) {
        options.modifySnapshot(data.snapshot);
    }

    if (options.modifyReceipt) {
        options.modifyReceipt(data.receipt);
    }

    const calls = {
        snapshots: 0,
        receiptReads: 0,
        rpc: []
    };

    const supabase = {
        async rpc(name, params) {
            calls.rpc.push({ name, params });

            if (options.rpcThrows) {
                throw new Error("TEST_SQL_RPC_TIMEOUT");
            }

            if (options.rpcError) {
                return {
                    data: null,
                    error: { message: "DATABASE_UNAVAILABLE" }
                };
            }

            if (options.rpcRejected) {
                return {
                    data: [{
                        settled: false,
                        reason: "SETTLEMENT_REJECTED"
                    }],
                    error: null
                };
            }

            return {
                data: [{
                    settled: true,
                    reason: "FAILURE_SETTLED"
                }],
                error: null
            };
        }
    };

    const inspectorPath = require.resolve(
        "../services/manualReconciliation/inspectManualReconcileRecovery"
    );

    const redisPath = require.resolve(
        "../socketio/manualReconcileFailureRecoveryStore"
    );

    const settlementPath = require.resolve(
        "../services/manualReconciliation/manualReconcileFailureSettlement"
    );

    const originalLoad = Module._load;

    delete require.cache[settlementPath];

    Module._load = function (request, parent, isMain) {
        const resolved = Module._resolveFilename(
            request,
            parent,
            isMain
        );

        if (resolved === inspectorPath) {
            return {
                readManualReconcileRecoverySnapshot:
                    async () => {
                        calls.snapshots++;

                        if (options.snapshotThrows) {
                            throw new Error(
                                "TEST_SNAPSHOT_UNAVAILABLE"
                            );
                        }

                        return data.snapshot;
                    }
            };
        }

        if (resolved === redisPath) {
            return {
                readManualReconcileFailureCleanupReceipt:
                    async () => {
                        calls.receiptReads++;

                        if (options.receiptThrows) {
                            throw new Error(
                                "TEST_REDIS_UNAVAILABLE"
                            );
                        }

                        if (options.receiptMissing) {
                            return null;
                        }

                        return data.receipt;
                    }
            };
        }

        return originalLoad.apply(this, arguments);
    };

    let settle;

    try {
        ({
            manualReconcileFailureSettlement: settle
        } = require(settlementPath));
    } finally {
        Module._load = originalLoad;
        delete require.cache[settlementPath];
    }

    const run = () => settle({
        supabase,
        attemptId: data.attemptId,
        fenceId: data.fenceId,
        processEpoch: data.processEpoch
    });

    return { data, calls, supabase, settle, run };
}

test("invalid input blocks before snapshot and RPC", async () => {
    const h = harness();

    const result = await h.settle({
        supabase: h.supabase,
        attemptId: "invalid",
        fenceId: h.data.fenceId,
        processEpoch: h.data.processEpoch
    });

    assert.equal(result.settled, false);
    assert.equal(result.reason, "INVALID_INPUT");
    assert.equal(result.recoveryRequired, true);
    assert.equal(h.calls.snapshots, 0);
    assert.equal(h.calls.receiptReads, 0);
    assert.equal(h.calls.rpc.length, 0);
});

test("snapshot read failure blocks settlement", async () => {
    const h = harness({ snapshotThrows: true });
    const result = await h.run();

    assert.equal(result.reason, "SNAPSHOT_UNVERIFIED");
    assert.equal(result.settled, false);
    assert.equal(h.calls.receiptReads, 0);
    assert.equal(h.calls.rpc.length, 0);
});

test("execution identity mismatch blocks settlement", async () => {
    const h = harness({
        modifySnapshot: snapshot => {
            snapshot.execution.operation_token = randomUUID();
        }
    });

    const result = await h.run();

    assert.equal(result.reason, "EXECUTION_NOT_ELIGIBLE");
    assert.equal(result.settled, false);
    assert.equal(h.calls.receiptReads, 0);
    assert.equal(h.calls.rpc.length, 0);
});

test("existing finalization blocks settlement", async () => {
    const h = harness({
        modifySnapshot: snapshot => {
            snapshot.finalization = { status: "COMPLETED" };
        }
    });

    const result = await h.run();

    assert.equal(result.reason, "EXECUTION_NOT_ELIGIBLE");
    assert.equal(h.calls.rpc.length, 0);
});

test("missing Redis receipt blocks SQL RPC", async () => {
    const h = harness({ receiptMissing: true });
    const result = await h.run();

    assert.equal(
        result.reason,
        "REDIS_STAGE_RECEIPT_NOT_VERIFIED"
    );
    assert.equal(result.settled, false);
    assert.equal(h.calls.receiptReads, 1);
    assert.equal(h.calls.rpc.length, 0);
});

test("Redis receipt read failure blocks SQL RPC", async () => {
    const h = harness({ receiptThrows: true });
    const result = await h.run();

    assert.equal(
        result.reason,
        "REDIS_STAGE_RECEIPT_READ_UNCERTAIN"
    );
    assert.equal(result.settled, false);
    assert.equal(h.calls.rpc.length, 0);
});

test("mismatched Redis receipt blocks SQL RPC", async () => {
    const h = harness({
        modifyReceipt: receipt => {
            receipt.ownerToken = randomUUID();
        }
    });

    const result = await h.run();

    assert.equal(
        result.reason,
        "REDIS_STAGE_RECEIPT_NOT_VERIFIED"
    );
    assert.equal(result.settled, false);
    assert.equal(h.calls.rpc.length, 0);
});

test("SQL RPC exception remains uncertain", async () => {
    const h = harness({ rpcThrows: true });
    const result = await h.run();

    assert.equal(
        result.reason,
        "SQL_SETTLEMENT_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.settled, false);
    assert.equal(h.calls.rpc.length, 1);
});

test("SQL RPC database error remains uncertain", async () => {
    const h = harness({ rpcError: true });
    const result = await h.run();

    assert.equal(
        result.reason,
        "SQL_SETTLEMENT_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.settled, false);
    assert.equal(h.calls.rpc.length, 1);
});

test("rejected SQL settlement remains blocked", async () => {
    const h = harness({ rpcRejected: true });
    const result = await h.run();

    assert.equal(
        result.reason,
        "SQL_SETTLEMENT_NOT_CONFIRMED"
    );
    assert.equal(result.settled, false);
    assert.equal(result.recoveryRequired, true);
    assert.equal(h.calls.rpc.length, 1);
});

test("confirmed SQL settlement returns verified result", async () => {
    const h = harness();
    const result = await h.run();

    assert.equal(result.settled, true);
    assert.equal(result.reason, "FAILURE_SETTLED");
    assert.equal(result.recoveryRequired, false);
    assert.equal(result.executionId, h.data.executionId);
    assert.equal(result.connectorId, "CON-TEST");
    assert.equal(result.socketId, "SOCKET-TEST");

    assert.equal(h.calls.snapshots, 1);
    assert.equal(h.calls.receiptReads, 1);
    assert.equal(h.calls.rpc.length, 1);

    assert.equal(
        h.calls.rpc[0].name,
        "settle_manual_reconcile_failure_v1"
    );

    assert.deepEqual(h.calls.rpc[0].params, {
        p_attempt_id: h.data.attemptId,
        p_operation_token: h.data.operationToken,
        p_execution_id: h.data.executionId,
        p_fence_id: h.data.fenceId,
        p_server_instance_id: h.data.serverInstanceId,
        p_connector_process_epoch: h.data.processEpoch,
        p_redis_stage_receipt: h.data.receipt
    });
});
