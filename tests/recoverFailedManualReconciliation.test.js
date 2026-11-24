
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
        snapshots: 0,
        redisReceiptReads: 0,
        redisPreflights: 0,
        redisStages: 0,
        sqlSettlements: 0
    };

    const baseline = fixture();
    const snapshots = options.snapshots || [
        baseline,
        structuredClone(baseline)
    ];

    const UUID_REGEX =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

const fenceId = randomUUID();

let durableFenceEvidence = null;

    const inspection = options.inspection || {
        state: "EXECUTION_UNCERTAIN",
        reservationOwned: true
    };

   
    const supabase = {
        from(table) {
            if (table === "manual_reconcile_coordinator_exit_receipts") {
                const e = snapshots[0].execution;
                const data = options.missingExitReceipt ? null : {
                    attempt_id: attemptId, operation_token: operationToken,
                    execution_id: executionId,
                    server_instance_id: e.server_instance_id,
                    connector_id: e.connector_id, socket_id: e.socket_id,
                    owner_token: e.owner_token, coordinator_exited: true,
                    admission_sealed: true, pending_work: 0,
                    callback_started: true, callback_settled: true,
                    late_work_detected: false,
                    outcome: "LOCALLY_SETTLED_UNVERIFIED",
                    recorded_at: "2026-11-20T00:00:00Z",
                    ...options.exitReceiptOverride
                };
                return { select() { return { eq() { return {
                    async maybeSingle() { return { data, error: null }; }
                }; } }; } };
            }
            if (
                table !==
                "manual_reconcile_failure_recovery_evidence"
            ) {
                throw new Error(
                    "UNEXPECTED_TEST_TABLE: " + table
                );
            }

            return {
                select() {
                    return {
                        eq() {
                            return {
                                async maybeSingle() {
                                return {
                                    data: durableFenceEvidence
                                        ? structuredClone(durableFenceEvidence)
                                        : null,
                                    error: null
                                };
                            }
                            };
                        }
                    };
                }
            };
        },
        
        async rpc(name, params) {
            calls.rpc.push({ name, params });

            if (
                name ===
                "authorize_manual_reconcile_failure_redis_stage_v1"
            ) {
                if (options.authorizationMode === "throws") {
                    throw new Error("AUTHORIZATION_RPC_TIMEOUT");
                }

                if (options.authorizationMode === "error") {
                    return {
                        data: null,
                        error: { message: "DATABASE_UNAVAILABLE" }
                    };
                }

                if (options.authorizationMode === "confirmed") {
                    const execution = snapshots[0].execution;

                    return {
                        data: [{
                            authorized: true,
                            reason: "REDIS_STAGE_AUTHORIZED",
                            connector_id: execution.connector_id,
                            socket_id: execution.socket_id,
                            owner_token: execution.owner_token
                        }],
                        error: null
                    };
                }

                return {
                    data: [{
                        authorized: false,
                        reason: "REDIS_STAGE_NOT_AUTHORIZED"
                    }],
                    error: null
                };
            }


            if (
                name ===
                "record_manual_reconcile_coordinator_drain_v1" &&
                options.drainRpcMode
            ) {
                if (options.drainRpcMode === "throws") {
                    throw new Error("DRAIN_RPC_CONNECTION_LOST");
                }

                if (options.drainRpcMode === "error") {
                    return {
                        data: null,
                        error: {
                            message: "DRAIN_RPC_DATABASE_ERROR"
                        }
                    };
                }

                if (options.drainRpcMode === "rejected") {
                    return {
                        data: [{
                            recorded: false,
                            reason: "DRAIN_EVIDENCE_REJECTED"
                        }],
                        error: null
                    };
                }

                if (options.drainRpcMode === "confirmed") {
                    return {
                        data: [{
                            recorded: true,
                            reason: "DRAIN_EVIDENCE_RECORDED"
                        }],
                        error: null
                    };
                }
            }

            if (
                name ===
                "record_manual_reconcile_failure_fence_evidence_v1" &&
                !options.rpcThrows &&
                !options.rpcResponse
            ) {
                return {
                    data: [{
                        recorded: true,
                        reason: "FENCE_EVIDENCE_RECORDED"
                    }],
                    error: null
                };
            }


            if (options.rpcThrows) {
                throw new Error("database unavailable");
            }

          const response = options.rpcResponse || {
    data: [{
        fence_requested: true,
        reason: "FENCE_REQUESTED",
        fence_id: fenceId
    }],
    error: null
};

if (
    name === "request_manual_reconcile_failure_fence_v2" &&
    !response.error
) {
    const reply = Array.isArray(response.data)
        ? response.data.length === 1
            ? response.data[0]
            : null
        : response.data;

    if (
        reply?.fence_requested === true &&
        ["FENCE_REQUESTED", "ALREADY_REQUESTED"]
            .includes(reply.reason) &&
        typeof reply.fence_id === "string" &&
        UUID_REGEX.test(reply.fence_id)
    ) {
        const execution = snapshots[0].execution;

        durableFenceEvidence = {
            attempt_id: attemptId,
            execution_id: execution.execution_id,
            connector_id: execution.connector_id,
            socket_id: execution.socket_id,
            owner_token: execution.owner_token,
            fence_id: reply.fence_id,
            fence_requested_at: "2026-11-20T00:00:00Z"
        };
    }
}

return response;
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

    const coordinatorPath = require.resolve(
        "../socketio/connectorOperationCoordinator"
    );

    const coordinatorLifecycle =
        options.lifecycle === undefined
            ? null
            : options.lifecycle;

    
    const failureStorePath = require.resolve(
        "../socketio/manualReconcileFailureRecoveryStore"
    );


    const settlementPath = require.resolve(
        "../services/manualReconciliation/manualReconcileFailureSettlement"
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
                settlementEligible: false,
                executionKnown: true,
                identityConflict: false,
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
                    attemptId: args.attemptId,
                    executionId: args.executionId,
                    processEpoch: args.expectedProcessEpoch,
                    generation: 2,
                    fenceId: args.expectedFenceId,
                    fenced: true,
                    executionKnown: true,
                    pending: 0,
                    running: 0,
                    settled: 1,
                    locallyDrained: true,
                    transportOutcomeKnown: true,
                    settlementEligible: false
                }

                };
            }
    };
}

        if (resolved === coordinatorPath) {
            return {
                getManualReconcileCoordinatorLifecycle:
                    requestedAttemptId => {
                        if (requestedAttemptId !== attemptId) {
                            return null;
                        }

                        return coordinatorLifecycle;
                    }
            };
        }

   
        if (
            resolved === failureStorePath &&
            options.drainRpcMode === "confirmed"
        ) {
            return {
                
                readManualReconcileFailureCleanupReceipt:
                    async () => {
                        calls.redisReceiptReads++;

                        if (options.receiptReadMode === "existing") {
                            const e = snapshots[0].execution;

                            return {
                                attemptId,
                                executionId,
                                serverInstanceId:
                                    e.server_instance_id,
                                connectorId: e.connector_id,
                                socketId: e.socket_id,
                                ownerToken: e.owner_token,
                                stage: "LOCKS_CLEARED_MARKERS_RETAINED",
                                markersRetained: true
                            };
                        }

                        if (options.receiptReadMode === "mismatch") {
                            return {
                                attemptId,
                                executionId: randomUUID(),
                                stage: "LOCKS_CLEARED_MARKERS_RETAINED",
                                markersRetained: true
                            };
                        }

                        if (options.receiptReadMode === "null") {
                            return null;
                        }

                        throw new Error(
                            "TEST_REDIS_RECEIPT_READ_UNAVAILABLE"
                        );
                    },


                inspectManualReconcileFailureRedis:
                    async identity => {
                        calls.redisPreflights++;

                        if (options.preflightMode === "throws") {
                            throw new Error(
                                "TEST_REDIS_PREFLIGHT_UNAVAILABLE"
                            );
                        }

                        const safe = {
                            attemptId: identity.attemptId,
                            executionId: identity.executionId,
                            connectorId: identity.connectorId,
                            socketId: identity.socketId,
                            ownershipConsistent: true,
                            connectorActiveOwned: true,
                            socketActiveOwned: true,
                            connectorLockCompatible: true,
                            socketLockCompatible: true,
                            quarantinesCompatible: true,
                            stageReceiptExists: false,
                            successReceiptExists: false,
                            connectorQuarantinePresent: true,
                            socketQuarantinePresent: true,
                            quarantineRepairCandidate: false,
                            cleanupAuthorized: false
                        };

                        if (options.preflightMode === "unsafe") {
                            return {
                                ...safe,
                                ownershipConsistent: false,
                                connectorActiveOwned: false
                            };
                        }

                        if (options.preflightMode === "repair") {
                            return {
                                ...safe,
                                ownershipConsistent: false,
                                quarantinesCompatible: false,
                                connectorQuarantinePresent: false,
                                socketQuarantinePresent: false,
                                quarantineRepairCandidate: true
                            };
                        }

                        return safe;
                    },

              
                stageManualReconcileFailureCleanup:
                    async ({ authorization }) => {
                        calls.redisStages++;

                        if (options.stageMode === "throws") {
                            throw new Error("TEST_REDIS_STAGE_TIMEOUT");
                        }

                        if (options.stageMode === "rejected") {
                            return {
                                staged: false,
                                reason: "LOCK_OWNER_MISMATCH"
                            };
                        }

                        if (options.stageMode === "confirmed") {
                            assert.equal(
                                authorization?.authorized,
                                true
                            );

                            return {
                                staged: true,
                                reason: "LOCKS_CLEARED_MARKERS_RETAINED"
                            };
                        }

                        throw new Error(
                            "TEST_REDIS_STAGE_MUST_NOT_EXECUTE"
                        );
                    }

            };
        }


        if (
            resolved === settlementPath &&
            options.drainRpcMode === "confirmed"
        ) {
            return {
                manualReconcileFailureSettlement:
                    async () => {
                        calls.sqlSettlements++;

                        if (options.settlementMode === "throws") {
                            throw new Error(
                                "TEST_SQL_SETTLEMENT_TIMEOUT"
                            );
                        }

                        return {
                            settled: false,
                            reason: "SQL_SETTLEMENT_NOT_CONFIRMED"
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
    "FENCE_INTENT_OUTCOME_UNCERTAIN"
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
    const h = harness({ missingExitReceipt: true });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED");
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(result.recoveryRequired, true);
    assert.equal(result.fenceAcknowledged, true);
    assert.equal(result.observationVerified, true);

   assert.equal(result.coordinatorDrainVerified, false);
assert.equal(result.fenceEvidenceRecorded, true);
    assert.equal(h.calls.rpc.length, 2);
    assert.equal(h.calls.transport.length, 1);
    assert.equal(h.calls.observation.length, 1);

    assert.equal(
        h.calls.rpc[0].name,
        "request_manual_reconcile_failure_fence_v2"
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
    h.calls.rpc[1].params.p_connector_process_epoch
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
    assert.equal(h.calls.rpc.length, 2);
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

function validCoordinatorLifecycle(snapshot) {
    return {
        attemptId,
        executionId,
        serverInstanceId:
            snapshot.execution.server_instance_id,
        connectorId:
            snapshot.execution.connector_id,
        socketId:
            snapshot.execution.socket_id,
        ownerToken:
            snapshot.execution.owner_token,
        coordinatorExited: true,
        pendingWork: 0,
        callbackSettled: true,
        locallySettled: true,
        outcome: "LOCALLY_SETTLED_UNVERIFIED",
        recoveryAuthorized: false
    };
}

test("missing lifecycle and durable exit receipt remain blocked", async () => {
    const h = harness({ lifecycle: null, missingExitReceipt: true });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "ORIGINAL_COORDINATOR_DURABLE_RECEIPT_REQUIRED"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.observationVerified, true);
    assert.equal(result.coordinatorDrainVerified, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.rpc.length, 2);
});

test("pending coordinator work blocks recovery", async () => {
    const snapshot = fixture();

    const lifecycle = {
        ...validCoordinatorLifecycle(snapshot),
        pendingWork: 1,
        locallySettled: false,
        outcome: "PENDING_WORK_UNVERIFIED"
    };

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_NOT_VERIFIED"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.coordinatorDrainVerified, false);
    assert.equal(h.calls.rpc.length, 2);
});

test("coordinator identity mismatch blocks recovery", async () => {
    const snapshot = fixture();

    const lifecycle = {
        ...validCoordinatorLifecycle(snapshot),
        ownerToken: randomUUID()
    };

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_NOT_VERIFIED"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.coordinatorDrainVerified, false);
    assert.equal(h.calls.rpc.length, 2);
});

test("valid coordinator drain reaches durable SQL evidence", async () => {
    const snapshot = fixture();

    const lifecycle = validCoordinatorLifecycle(snapshot);

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_RECORD_NOT_CONFIRMED"
    );

    assert.equal(result.recovered, false);
    assert.equal(result.observationVerified, true);
    assert.equal(result.coordinatorDrainVerified, true);
    assert.equal(result.durableDrainRecorded, false);
    assert.equal(result.settlementEligible, false);

    assert.equal(h.calls.rpc.length, 3);

    assert.equal(
        h.calls.rpc[2].name,
        "record_manual_reconcile_coordinator_drain_v1"
    );

    assert.equal(
        h.calls.rpc[2].params.p_attempt_id,
        attemptId
    );

    assert.equal(
        h.calls.rpc[2].params.p_execution_id,
        executionId
    );

    assert.equal(
        h.calls.rpc[2].params.p_server_instance_id,
        lifecycle.serverInstanceId
    );
});

test("durable drain RPC exception remains blocked", async () => {
    const snapshot = fixture();

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle: validCoordinatorLifecycle(snapshot),
        drainRpcMode: "throws"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_RECORD_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.durableDrainRecorded, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.rpc.length, 3);
});

test("durable drain RPC error remains blocked", async () => {
    const snapshot = fixture();

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle: validCoordinatorLifecycle(snapshot),
        drainRpcMode: "error"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_RECORD_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.durableDrainRecorded, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.rpc.length, 3);
});

test("rejected durable drain evidence remains blocked", async () => {
    const snapshot = fixture();

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle: validCoordinatorLifecycle(snapshot),
        drainRpcMode: "rejected"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "COORDINATOR_DRAIN_RECORD_NOT_CONFIRMED"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.durableDrainRecorded, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.rpc.length, 3);
});

test("confirmed durable drain cannot bypass Redis receipt uncertainty", async () => {
    const snapshot = fixture();

    const h = harness({
        snapshots: [snapshot, structuredClone(snapshot)],
        lifecycle: validCoordinatorLifecycle(snapshot),
        drainRpcMode: "confirmed"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "REDIS_STAGE_RECEIPT_READ_UNCERTAIN"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.durableDrainRecorded, true);
    assert.equal(result.settlementEligible, false);

    assert.equal(h.calls.rpc.length, 3);

    assert.equal(
        h.calls.rpc[2].name,
        "record_manual_reconcile_coordinator_drain_v1"
    );

    assert.equal(
        h.calls.rpc.some(
            call => call.name ===
                "authorize_manual_reconcile_failure_redis_stage_v1"
        ),
        false
    );
});

function authorizationHarness(options = {}) {
    const snapshot = fixture();

    return harness({
        snapshots: [
            snapshot,
            structuredClone(snapshot)
        ],
        lifecycle: validCoordinatorLifecycle(snapshot),
        drainRpcMode: "confirmed",
        receiptReadMode: "null",
        ...options
    });
}

test("SQL Redis authorization timeout blocks staging", async () => {
    const h = authorizationHarness({
        authorizationMode: "throws"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "REDIS_STAGE_AUTHORIZATION_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisReceiptReads, 1);
    assert.equal(h.calls.redisPreflights, 0);
    assert.equal(h.calls.redisStages, 0);
    assert.equal(h.calls.rpc.length, 4);
});

test("SQL Redis authorization database error blocks staging", async () => {
    const h = authorizationHarness({
        authorizationMode: "error"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "REDIS_STAGE_AUTHORIZATION_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.recovered, false);
    assert.equal(h.calls.redisPreflights, 0);
    assert.equal(h.calls.redisStages, 0);
});

test("rejected SQL Redis authorization blocks staging", async () => {
    const h = authorizationHarness({
        authorizationMode: "rejected"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "REDIS_STAGE_AUTHORIZATION_NOT_CONFIRMED"
    );
    assert.equal(result.recovered, false);
    assert.equal(h.calls.redisPreflights, 0);
    assert.equal(h.calls.redisStages, 0);
});

test("Redis preflight exception blocks staging", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        preflightMode: "throws"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "REDIS_PREFLIGHT_OUTCOME_UNCERTAIN"
    );
    assert.equal(result.redisStageAuthorized, true);
    assert.equal(result.recovered, false);
    assert.equal(h.calls.redisPreflights, 1);
    assert.equal(h.calls.redisStages, 0);
});

test("unsafe Redis ownership preflight blocks staging", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        preflightMode: "unsafe"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "REDIS_PREFLIGHT_NOT_SAFE");
    assert.equal(result.redisStageAuthorized, true);
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisPreflights, 1);
    assert.equal(h.calls.redisStages, 0);
});

test("missing quarantine repair candidate never authorizes cleanup", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        preflightMode: "repair"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "REDIS_PREFLIGHT_NOT_SAFE");
    assert.equal(result.quarantineRepairCandidate, true);
    assert.equal(result.redisStageAuthorized, true);
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisPreflights, 1);
    assert.equal(h.calls.redisStages, 0);
});

test("Redis staging exception prevents SQL settlement", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        stageMode: "throws"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "REDIS_STAGE_OUTCOME_UNCERTAIN");
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisStages, 1);
    assert.equal(h.calls.sqlSettlements, 0);
});

test("rejected Redis staging prevents SQL settlement", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        stageMode: "rejected"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "REDIS_STAGE_NOT_CONFIRMED");
    assert.equal(result.redisStageReason, "LOCK_OWNER_MISMATCH");
    assert.equal(result.recovered, false);
    assert.equal(h.calls.redisStages, 1);
    assert.equal(h.calls.sqlSettlements, 0);
});

test("confirmed Redis staging cannot bypass uncertain SQL settlement", async () => {
    const h = authorizationHarness({
        authorizationMode: "confirmed",
        stageMode: "confirmed",
        settlementMode: "throws"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "SQL_SETTLEMENT_OUTCOME_UNCERTAIN");
    assert.equal(result.recovered, false);
    assert.equal(result.redisStageExecuted, true);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisStages, 1);
    assert.equal(h.calls.sqlSettlements, 1);
});

test("existing valid Redis stage receipt skips duplicate staging", async () => {
    const h = authorizationHarness({
        receiptReadMode: "existing"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(result.reason, "SQL_SETTLEMENT_NOT_CONFIRMED");
    assert.equal(result.redisStageExecuted, true);
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisStages, 0);
    assert.equal(h.calls.redisPreflights, 0);
    assert.equal(h.calls.sqlSettlements, 1);

    assert.equal(
        h.calls.rpc.some(
            call => call.name ===
                "authorize_manual_reconcile_failure_redis_stage_v1"
        ),
        false
    );
});

test("existing mismatched Redis receipt blocks recovery", async () => {
    const h = authorizationHarness({
        receiptReadMode: "mismatch"
    });

    const result = await h.recover({
        supabase: h.supabase,
        attemptId
    });

    assert.equal(
        result.reason,
        "EXISTING_REDIS_STAGE_IDENTITY_INVALID"
    );
    assert.equal(result.recovered, false);
    assert.equal(result.settlementEligible, false);
    assert.equal(h.calls.redisStages, 0);
    assert.equal(h.calls.sqlSettlements, 0);
});


test("server restart with valid durable exit receipt reaches SQL drain verification", async () => {
    const h = harness({ lifecycle: null });
    const result = await h.recover({ supabase: h.supabase, attemptId });
    assert.equal(result.reason, "COORDINATOR_DRAIN_RECORD_NOT_CONFIRMED");
    assert.equal(h.calls.rpc.at(-1).name, "record_manual_reconcile_coordinator_drain_v1");
    assert.equal(h.calls.redisStages, 0);
});

test("foreign durable exit receipt cannot authorize cleanup after restart", async () => {
    const h = harness({ lifecycle: null, exitReceiptOverride: { owner_token: randomUUID() } });
    const result = await h.recover({ supabase: h.supabase, attemptId });
    assert.equal(result.reason, "COORDINATOR_DURABLE_RECEIPT_INVALID");
    assert.equal(h.calls.rpc.length, 2);
    assert.equal(h.calls.redisStages, 0);
    assert.equal(h.calls.sqlSettlements, 0);
});
