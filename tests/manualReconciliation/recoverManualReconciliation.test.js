
"use strict";

// LOCAL ONLY: in-memory Supabase and Redis boundaries.
// No real database, Redis, Tally, HTTP or server access.
// Run alone: node --test --test-concurrency=1 tests/manualReconciliation/recoverManualReconciliation.test.js
const assert = require("node:assert/strict");
const { test } = require("node:test");
const Module = require("node:module");
const path = require("node:path");

const ROOT = path.resolve(__dirname, "../..");
const INSPECT = path.join(ROOT, "services/manualReconciliation/inspectManualReconcileRecovery.js");
const RECOVER = path.join(ROOT, "services/manualReconciliation/recoverManualReconciliation.js");
const HEALTH = path.join(ROOT, "socketio/connectorHealthStore.js");

const ATTEMPT = "11111111-1111-4111-8111-111111111111";
const RESERVATION = "22222222-2222-4222-8222-222222222222";
const TOKEN = "33333333-3333-4333-8333-333333333333";
const EXECUTION = "44444444-4444-4444-8444-444444444444";
const OWNER = "55555555-5555-4555-8555-555555555555";
const SERVER = "66666666-6666-4666-8666-666666666666";
const FETCH = "77777777-7777-4777-8777-777777777777";
const APPLY = "88888888-8888-4888-8888-888888888888";
const CONNECTOR = "synthetic-connector";
const SOCKET = "synthetic-socket";

function fixture() {
    const attempt = {
        attempt_id: ATTEMPT, reservation_id: RESERVATION,
        operation_token: TOKEN, company_code: "LOCAL_TEST_COMPANY", tally_owner: "USER"
    };
    const execution = {
        attempt_id: ATTEMPT, reservation_id: RESERVATION,
        operation_token: TOKEN, execution_id: EXECUTION,
        owner_token: OWNER, server_instance_id: SERVER,
        connector_id: CONNECTOR, socket_id: SOCKET
    };
    const checkpoint = {
        attempt_id: ATTEMPT, reservation_id: RESERVATION,
        operation_token: TOKEN, execution_id: EXECUTION,
        owner_token: OWNER, connector_id: CONNECTOR,
        final_recheck_completed: true, remaining_mismatch_ledger_guids: []
    };
    const receipt = {
        attemptId: ATTEMPT, executionId: EXECUTION,
        serverInstanceId: SERVER, connectorId: CONNECTOR,
        socketId: SOCKET, ownerToken: OWNER, redisReleaseConfirmed: true
    };
    const finalization = {
        attempt_id: ATTEMPT, reservation_id: RESERVATION,
        operation_token: TOKEN, state: "EVIDENCE_RECORDED",
        final_recheck_completed: true, connector_release_verified: true,
        connector_id: CONNECTOR, release_owner_token: OWNER,
        remaining_mismatch_ledger_guids: [], release_evidence: { ...receipt }
    };
    const base = {
        attempt_id: ATTEMPT, reservation_id: RESERVATION,
        operation_token: TOKEN, execution_id: EXECUTION, state: "RESULT_VERIFIED"
    };
    const operations = [
        { ...base, operation_id: FETCH, operation_ordinal: 0, operation_type: "TALLY_FETCH" },
        { ...base, operation_id: APPLY, operation_ordinal: 1, operation_type: "LEDGER_APPLY", source_fetch_operation_id: FETCH }
    ];
    return {
        version: 1, attemptId: ATTEMPT, complete: true,
        operationCount: String(operations.length), operations,
        attempt, execution, checkpoint, finalization,
        status: { company_code: attempt.company_code, tally_owner: attempt.tally_owner,
            status: "RUNNING", operation_token: TOKEN },
        receipt
    };
}

// Only intercept the exact connector health module used by the inspector.
// Restore require cache and Module._load after every test.
async function withHarness(options, run) {
    const saved = new Map([INSPECT, RECOVER, HEALTH].map(name => [name, require.cache[name]]));
    const originalLoad = Module._load;
    const calls = { snapshot: 0, close: 0, receipt: 0 };
    const snapshot = options.snapshot === undefined ? fixture() : options.snapshot;
    const receipt = options.receipt === undefined ? snapshot?.receipt ?? null : options.receipt;
    const supabase = {
        async rpc(name, args) {
            if (name === "read_manual_reconcile_recovery_snapshot_v1") {
                calls.snapshot++;
                assert.equal(args.p_attempt_id, ATTEMPT);
                if (options.snapshotThrows) throw new Error("SYNTHETIC_SNAPSHOT_FAILURE");
                if (options.snapshotError) return { data: null, error: new Error("SYNTHETIC_SNAPSHOT_ERROR") };
                return { data: snapshot, error: null };
            }
            if (name === "close_manual_reconcile_v2") {
                calls.close++;
                assert.deepEqual(args, { p_attempt_id: ATTEMPT, p_operation_token: TOKEN });
                if (options.closeThrows) throw new Error("SYNTHETIC_CLOSE_FAILURE");
                if (options.closeError) return { data: null, error: new Error("SYNTHETIC_CLOSE_ERROR") };
                return { data: options.closeResult === undefined
                    ? [{ closed: true, reason: "CLOSED" }] : options.closeResult, error: null };
            }
            assert.fail("Unexpected RPC: " + name);
        }
    };
    try {
        delete require.cache[INSPECT];
        delete require.cache[RECOVER];
        Module._load = function(request, parent, isMain) {
            const resolved = Module._resolveFilename(request, parent, isMain);
            if (resolved === HEALTH) {
                return {
                    async readManualReconcileReleaseReceipt(attemptId) {
                        calls.receipt++;
                        assert.equal(attemptId, ATTEMPT);
                        if (options.receiptThrows) throw new Error("SYNTHETIC_REDIS_FAILURE");
                        return receipt;
                    }
                };
            }
            return originalLoad.apply(this, arguments);
        };
        const { recoverManualReconciliation } = require(RECOVER);
        const { inspectManualReconcileRecovery } = require(INSPECT);
        return await run({
    recoverManualReconciliation,
    inspectManualReconcileRecovery,
    supabase,
    calls
});
    } finally {
        Module._load = originalLoad;
        for (const [name, cached] of saved) {
            delete require.cache[name];
            if (cached) require.cache[name] = cached;
        }
    }
}

async function recover(options = {}) {
    return withHarness(options, async ({ recoverManualReconciliation, supabase, calls }) => {
        const result = await recoverManualReconciliation({ supabase, attemptId: ATTEMPT });
        return { result, calls };
    });
}

test("finalization recorded: re-read immutable attempt and close", async () => {
    const { result, calls } = await recover();
    assert.deepEqual(result, { recovered: true, reason: "CLOSED", attemptId: ATTEMPT });
    assert.equal(calls.snapshot, 2);
    assert.equal(calls.receipt, 1);
    assert.equal(calls.close, 1);
});

test("already closed: no close RPC", async () => {
    const snapshot = fixture();
    snapshot.finalization.state = "CLOSED";
    const { result, calls } = await recover({ snapshot });
    assert.equal(result.reason, "ALREADY_CLOSED");
    assert.equal(result.recovered, true);
    assert.equal(calls.close, 0);
    assert.equal(calls.snapshot, 1);
});

test("execution uncertain: no automatic close", async () => {
    const snapshot = fixture();
    snapshot.checkpoint = null;
    snapshot.finalization = null;
    const { result, calls } = await recover({ snapshot, receipt: null });
    assert.equal(result.reason, "EXECUTION_UNCERTAIN");
    assert.equal(result.recovered, false);
    assert.equal(calls.close, 0);
});

test("unverified journal: no automatic close", async () => {
    const snapshot = fixture();
    snapshot.operations[1].state = "DISPATCH_MAY_HAVE_OCCURRED";
    const { result, calls } = await recover({ snapshot });
    assert.equal(result.recovered, false);
    assert.equal(calls.close, 0);
});

test("missing apply: no automatic close", async () => {
    const snapshot = fixture();
    snapshot.operations.pop();
    snapshot.operationCount = "1";
    const { result, calls } = await recover({ snapshot });
    assert.equal(result.recovered, false);
    assert.equal(calls.close, 0);
});

test("Redis receipt identity mismatch: fail closed", async () => {
    const receipt = { ...fixture().receipt, ownerToken: TOKEN };
    await withHarness({ receipt }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /REDIS_RELEASE_RECEIPT_IDENTITY_MISMATCH/);
        assert.equal(calls.close, 0);
    });
});

test("finalization evidence mismatch: fail closed", async () => {
    const snapshot = fixture();
    snapshot.finalization.release_evidence.socketId = "other-socket";
    await withHarness({ snapshot }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /FINALIZATION_EVIDENCE_INVALID/);
        assert.equal(calls.close, 0);
    });
});

test("snapshot RPC error: fail closed", async () => {
    await withHarness({ snapshotError: true }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /SNAPSHOT_READ_FAILED/);
        assert.equal(calls.close, 0);
    });
});

test("snapshot RPC throw: fail closed", async () => {
    await withHarness({ snapshotThrows: true }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /SNAPSHOT_READ_FAILED/);
        assert.equal(calls.close, 0);
    });
});

test("invalid snapshot: fail closed", async () => {
    const snapshot = fixture();
    snapshot.operationCount = "999";
    await withHarness({ snapshot }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /SNAPSHOT_RESULT_INVALID/);
        assert.equal(calls.close, 0);
    });
});

test("close RPC error: uncertain, never claim recovery", async () => {
    await withHarness({ closeError: true }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /CLOSE_RPC_UNCERTAIN/);
        assert.equal(calls.close, 1);
    });
});

test("close RPC throw: error propagates, never claim recovery", async () => {
    await withHarness({ closeThrows: true }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /SYNTHETIC_CLOSE_FAILURE/);
        assert.equal(calls.close, 1);
    });
});

test("close RPC unconfirmed: recovery required", async () => {
    const { result, calls } = await recover({ closeResult: [{ closed: false, reason: "RESERVATION_NOT_OWNED" }] });
    assert.equal(result.recovered, false);
    assert.equal(result.reason, "CLOSE_NOT_CONFIRMED");
    assert.equal(result.closeReason, "RESERVATION_NOT_OWNED");
    assert.equal(calls.close, 1);
});

test("reservation ownership lost: no automatic close", async () => {
    const snapshot = fixture();
    snapshot.status.operation_token = OWNER;
    const { result, calls } = await recover({ snapshot });
    assert.equal(result.reason, "RESERVATION_NOT_OWNED");
    assert.equal(calls.close, 0);
});

test("invalid attempt UUID: no RPC or Redis access", async () => {
    await withHarness({}, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: "invalid" }),
            /INPUT_INVALID/);
        assert.deepEqual(calls, { snapshot: 0, close: 0, receipt: 0 });
    });
});

test("missing Redis receipt: DB finalization permits close (current contract)", async () => {
    const { result, calls } = await recover({ receipt: null });
    assert.equal(result.recovered, true);
    assert.equal(result.reason, "CLOSED");
    assert.equal(calls.receipt, 1);
    assert.equal(calls.close, 1);
});

test("Redis read failure: fail closed", async () => {
    await withHarness({ receiptThrows: true }, async ({ recoverManualReconciliation, supabase, calls }) => {
        await assert.rejects(recoverManualReconciliation({ supabase, attemptId: ATTEMPT }),
            /SYNTHETIC_REDIS_FAILURE/);
        assert.equal(calls.close, 0);
    });
});

test("second snapshot invalid: no close", async () => {
    await withHarness({}, async ({ recoverManualReconciliation, supabase, calls }) => {
        let reads = 0;
        const wrapped = {
            rpc: async (name, args) => {
                if (name === "read_manual_reconcile_recovery_snapshot_v1") {
                    reads++;
                    return { data: reads === 1 ? fixture() : { ...fixture(), attempt: null }, error: null };
                }
                assert.fail("Unexpected RPC: " + name);
            }
        };
        await assert.rejects(recoverManualReconciliation({ supabase: wrapped, attemptId: ATTEMPT }),
            /SNAPSHOT_RESULT_INVALID|ATTEMPT_IDENTITY_UNVERIFIED/);
        assert.equal(reads, 2);
        assert.equal(calls.close, 0);
    });
});
