"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
    startManualReconcileV2
} = require("../../services/manualReconciliation/manualReconcileAdmissionV2");

const ATTEMPT_ID = "11111111-1111-4111-8111-111111111111";
const TOKEN = "22222222-2222-4222-8222-222222222222";

const INPUT = {
    attemptId: ATTEMPT_ID,
    companyCode: "TEST_COMPANY",
    tallyOwner: "USER",
    reconcileType: "LEDGER",
    workerId: "test-worker",
    batchId: null
};

function mockSupabase(data, error = null) {
    const calls = [];

    return {
        calls,
        rpc: async (name, params) => {
            calls.push({ name, params });
            return { data, error };
        }
    };
}

test("ACQUIRED returns the reservation token", async () => {
    const supabase = mockSupabase([{
        acquired: true,
        reason: "ACQUIRED",
        reservation_token: TOKEN
    }]);

    const result = await startManualReconcileV2({
        supabase,
        ...INPUT
    });

    assert.deepEqual(result, {
        acquired: true,
        reason: "ACQUIRED",
        reservationToken: TOKEN
    });

    assert.equal(supabase.calls.length, 1);
    assert.equal(
        supabase.calls[0].name,
        "start_manual_reconcile_v2"
    );
    assert.deepEqual(supabase.calls[0].params, {
        p_attempt_id: ATTEMPT_ID,
        p_company_code: "TEST_COMPANY",
        p_tally_owner: "USER",
        p_reconcile_type: "LEDGER",
        p_worker_id: "test-worker",
        p_batch_id: null
    });
});

test("ALREADY_ACQUIRED recognizes an identical attempt", async () => {
    const supabase = mockSupabase([{
        acquired: true,
        reason: "ALREADY_ACQUIRED",
        reservation_token: TOKEN
    }]);

    const result = await startManualReconcileV2({
        supabase,
        ...INPUT
    });

    assert.deepEqual(result, {
        acquired: true,
        reason: "ALREADY_ACQUIRED",
        reservationToken: TOKEN
    });
});

for (const reason of [
    "ATTEMPT_ID_CONFLICT",
    "ATTEMPT_NOT_CURRENT",
    "RECONCILE_ALREADY_RUNNING",
    "SYNC_IN_PROGRESS",
    "HTTP_EXECUTION_UNRESOLVED",
    "SYNC_OUTCOME_UNKNOWN",
    "BATCH_NOT_FOUND",
    "INVALID_INPUT"
]) {
    test(`${reason} returns a safe rejection`, async () => {
        const supabase = mockSupabase([{
            acquired: false,
            reason,
            reservation_token: null
        }]);

        const result = await startManualReconcileV2({
            supabase,
            ...INPUT
        });

        assert.deepEqual(result, {
            acquired: false,
            reason
        });
    });
}

test("RPC transport error retains uncertain admission identity", async () => {
    const supabase = mockSupabase(
        null,
        new Error("connection lost")
    );

    await assert.rejects(
        startManualReconcileV2({
            supabase,
            ...INPUT
        }),
        error => {
            assert.equal(
                error.message,
                "RECONCILE_ADMISSION_OUTCOME_UNVERIFIED"
            );
            assert.equal(error.reconcileAdmissionUncertain, true);
            assert.equal(error.reconcileRecoveryRequired, true);
            assert.equal(error.attemptId, ATTEMPT_ID);
            assert.deepEqual(error.admissionInput, INPUT);
            return true;
        }
    );
});

test("Malformed RPC response fails closed", async () => {
    const supabase = mockSupabase([{
        acquired: true,
        reason: "ACQUIRED",
        reservation_token: null
    }]);

    await assert.rejects(
        startManualReconcileV2({
            supabase,
            ...INPUT
        }),
        error => {
            assert.equal(
                error.message,
                "RECONCILE_ADMISSION_OUTCOME_UNVERIFIED"
            );
            assert.equal(error.reconcileAdmissionUncertain, true);
            return true;
        }
    );
});

test("Invalid input does not call the RPC", async () => {
    const supabase = mockSupabase([]);

    await assert.rejects(
        startManualReconcileV2({
            supabase,
            ...INPUT,
            attemptId: "invalid-uuid"
        }),
        {
            message: "RECONCILE_ADMISSION_INPUT_INVALID"
        }
    );

    assert.equal(supabase.calls.length, 0);
});

test("Thrown RPC exception retains the original attempt ID", async () => {
    const supabase = {
        rpc: async () => {
            throw new Error("network timeout");
        }
    };

    await assert.rejects(
        startManualReconcileV2({
            supabase,
            ...INPUT
        }),
        error => {
            assert.equal(error.reconcileAdmissionUncertain, true);
            assert.equal(error.attemptId, ATTEMPT_ID);
            return true;
        }
    );
});

test("Multiple response rows are rejected as uncertain", async () => {
    const supabase = mockSupabase([
        {
            acquired: true,
            reason: "ACQUIRED",
            reservation_token: TOKEN
        },
        {
            acquired: true,
            reason: "ACQUIRED",
            reservation_token: TOKEN
        }
    ]);

    await assert.rejects(
        startManualReconcileV2({
            supabase,
            ...INPUT
        }),
        {
            message: "RECONCILE_ADMISSION_OUTCOME_UNVERIFIED"
        }
    );
});