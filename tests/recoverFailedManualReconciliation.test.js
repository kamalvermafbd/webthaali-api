
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const Module = require("node:module");

const attemptId = randomUUID();
const executionId = randomUUID();
const operationToken = randomUUID();

function fixture() {
    const reservationId = "12345";

    return {
        attempt: {
            attempt_id: attemptId,
            operation_token: operationToken,
            reservation_id: reservationId,
            company_code: "C-TEST",
            tally_owner: "USER"
        },
        execution: {
            attempt_id: attemptId,
            reservation_id: reservationId,
            operation_token: operationToken,
            execution_id: executionId,
            connector_id: "CON-TEST",
            socket_id: "original-socket",
            owner_token: randomUUID(),
            server_instance_id: randomUUID()
        },
        status: {
            id: reservationId,
            status: "RUNNING",
            operation_token: operationToken,
            company_code: "C-TEST",
            tally_owner: "USER"
        },
        finalization: null
    };
}

function harness(options = {}) {
    const calls = {
    rpc: [],
    transport: [],
    observation: [],
    snapshots: 0
};
    const baseline = fixture();
    const snapshots = options.snapshots || [
        baseline,
        structuredClone(baseline)
    ];

    const inspection = options.inspection || {
        state: "EXECUTION_UNCERTAIN",
        reservationOwned: true
    };

    const supabase = {
        async rpc(name, params) {
            calls.rpc.push({ name, params });

            if (options.rpcThrows) {
                throw new Error("database unavailable");
            }

          return options.rpcResponse || {
    data: [
        {
            fence_requested: true,
            reason: "FENCE_REQUESTED"
        }
    ],
    error: null
};
        }
    };

    const inspectorPath = require.resolve(
        "../services/manualReconciliation/inspectManualReconcileRecovery"
    );

    const senderPath = require.resolve(
        "../socketio/sendManualReconcileFenceRequest"
    );

    const observationSenderPath = require.resolve(
    "../socketio/sendManualReconcileExecutionObservationRequest"
);

    const recoveryPath = require.resolve(
        "../services/manualReconciliation/recoverFailedManualReconciliation"
    );

    const originalLoad = Module._load;
    delete require.cache[recoveryPath];

    Module._load = function (request, parent, isMain) {
        const resolved = Module._resolveFilename(
            request,
            parent,
            isMain
        );

        if (resolved === inspectorPath) {
            return {
                inspectManualReconcileRecovery: async () => inspection,

                readManualReconcileRecoverySnapshot: async () => {
                    const index = calls.snapshots++;
                    return snapshots[
                        Math.min(index, snapshots.length - 1)
                    ];
                }
            };
        }

       if (resolved === senderPath) {
    return {
        sendManualReconcileFenceRequest: async args => {
            calls.transport.push(args);

            if (options.transportThrows) {
                throw new Error(
                    "RECONCILE_FENCE_ACK_TIMEOUT"
                );
            }

            const processEpoch = randomUUID();

            return {
                requestId: randomUUID(),
                fenceId: args.fenceId,
                attemptId: args.attemptId,
                executionId: args.executionId,
                connectorId: args.connectorId,
                socketId: args.expectedSocketId,
                settlementEligible: false,
                snapshot: {
                    processEpoch,
                    generation: 1,
                    fenced: true,
                    fenceId: args.fenceId,
                    pending: 0,
                    running: 0,
                    drained: true
                }
            };
        }
    };
}

if (resolved === observationSenderPath) {
    return {
        sendManualReconcileExecutionObservationRequest:
            async args => {
                calls.observation.push(args);

                if (options.observationThrows) {
                    throw new Error(
                        options.observationError ||
                        "RECONCILE_OBSERVATION_NOT_VERIFIED"
                    );
                }

                return {
                    requestId: randomUUID(),
                    attemptId: args.attemptId,
                    executionId: args.executionId,
                    connectorId: args.connectorId,
                    socketId: args.expectedSocketId,
                    settlementEligible: false,
                    observation: {
                        processEpoch: args.expectedProcessEpoch,
                        generation: 2,
                        fenceId: args.expectedFenceId,
                        executionKnown: true,
                        pending: 0,
                        running: 0,
                        settled: 1,
                        locallyDrained: true
                    }
                };
            }
    };
}

        return originalLoad.apply(this, arguments);
    };

    let recover;

    try {
        ({
            recoverFailedManualReconciliation: recover
        } = require(recoveryPath));
    } finally {
        Module._load = originalLoad;
        delete require.cache[recoveryPath];
    }

    return { recover, supabase, calls };
}

test("invalid input rejects before RPC", async () => {
    const h = harness();

    await assert.rejects(
        h.recover({
            supabase: h.supabase,
            attemptId: "invalid"
        }),
        /RECONCILE_FAILURE_RECOVERY_INPUT_INVALID/
    );

    assert.equal(h.calls.rpc.length, 0);
    assert.equal(h.calls.transport.length, 0);
});

test("ineligible inspection blocks without RPC", async () => {
    const h = harness({
        inspection: {
            state: "FINALIZATION_RECORDED",
            reservationOwned: true
        }
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.rpc.length, 0);
    assert.equal(h.calls.transport.length, 0);
});

test("original execution mismatch blocks before RPC", async () => {
    const snapshot = fixture();
    snapshot.status.operation_token = randomUUID();

    const h = harness({
        snapshots: [snapshot]
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "ORIGINAL_EXECUTION_UNVERIFIED"
    );

    assert.equal(h.calls.rpc.length, 0);
    assert.equal(h.calls.transport.length, 0);
});

test("fence intent RPC failure never dispatches", async () => {
    const h = harness({ rpcThrows: true });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "FENCE_INTENT_OUTCOME_UNCERTAIN"
    );

    assert.equal(h.calls.rpc.length, 1);
    assert.equal(h.calls.transport.length, 0);
});

test("unconfirmed RPC response never dispatches", async () => {
    const h = harness({
        rpcResponse: {
            data: { reason: "UNKNOWN" },
            error: null
        }
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "FENCE_INTENT_NOT_CONFIRMED"
    );

    assert.equal(h.calls.transport.length, 0);
});

test("identity change after intent prevents transport", async () => {
    const before = fixture();
    const after = structuredClone(before);

    after.execution.socket_id = "replacement-socket";

    const h = harness({
        snapshots: [before, after]
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "EXECUTION_CHANGED_AFTER_FENCE_INTENT"
    );

    assert.equal(h.calls.transport.length, 0);
});

test("socket timeout stays blocked without settlement", async () => {
    const h = harness({
        transportThrows: true
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "FENCE_TRANSPORT_UNVERIFIED"
    );

    assert.equal(result.fenceAcknowledged, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.transport.length, 1);
});

test("valid fence ACK and observation remain non-settling", async () => {
    const h = harness();

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "EXECUTION_LOCALLY_DRAINED");
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(result.recoveryRequired, true);
    assert.equal(result.fenceAcknowledged, true);
    assert.equal(result.observationVerified, true);

    assert.equal(result.observation.executionKnown, true);
    assert.equal(result.observation.locallyDrained, true);
    assert.equal(result.observation.pending, 0);
    assert.equal(result.observation.running, 0);
    assert.equal(result.observation.settled, 1);

    assert.equal(h.calls.rpc.length, 1);
    assert.equal(h.calls.transport.length, 1);
    assert.equal(h.calls.observation.length, 1);

    assert.equal(
        h.calls.rpc[0].name,
        "request_manual_reconcile_failure_fence_v1"
    );

    assert.deepEqual(h.calls.rpc[0].params, {
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_execution_id: executionId
    });

    assert.equal(
        h.calls.transport[0].expectedSocketId,
        "original-socket"
    );

    assert.equal(
        h.calls.observation[0].expectedSocketId,
        "original-socket"
    );

    assert.equal(
        h.calls.observation[0].expectedFenceId,
        h.calls.transport[0].fenceId
    );

    assert.equal(
        h.calls.observation[0].expectedProcessEpoch,
        result.observation.processEpoch
    );
});

test("observation failure remains blocked", async () => {
    const h = harness({
        observationThrows: true
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "EXECUTION_OBSERVATION_UNVERIFIED"
    );
    assert.equal(result.fenceAcknowledged, true);
    assert.equal(result.observationVerified, false);
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);

    assert.equal(h.calls.transport.length, 1);
    assert.equal(h.calls.observation.length, 1);
    assert.equal(h.calls.rpc.length, 1);
});

test("unknown execution observation remains blocked", async () => {
    const h = harness({
        observationThrows: true,
        observationError: "RECONCILE_OBSERVATION_NOT_VERIFIED"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "EXECUTION_OBSERVATION_UNVERIFIED"
    );
    assert.equal(
        result.observationReason,
        "RECONCILE_OBSERVATION_NOT_VERIFIED"
    );
    assert.equal(result.settlementEligible, false);
    assert.equal(result.recovered, false);
});

test("connector observation timeout remains blocked", async () => {
    const h = harness({
        observationThrows: true,
        observationError: "RECONCILE_OBSERVATION_ACK_TIMEOUT"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "EXECUTION_OBSERVATION_UNVERIFIED"
    );
    assert.equal(
        result.observationReason,
        "RECONCILE_OBSERVATION_ACK_TIMEOUT"
    );
    assert.equal(result.fenceAcknowledged, true);
    assert.equal(result.observationVerified, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(result.recovered, false);
});
