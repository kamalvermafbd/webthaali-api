
"use strict";

const {
    inspectManualReconcileRecovery,
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function fail(reason) {
    throw new Error(`RECONCILE_RECOVERY_${reason}`);
}

function getRpcRow(data) {
    const row = Array.isArray(data)
        ? data.length === 1
            ? data[0]
            : null
        : data;

    if (!row || typeof row !== "object") {
        fail("RPC_RESULT_INVALID");
    }

    return row;
}

async function recoverManualReconciliation({
    supabase,
    attemptId
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        fail("INPUT_INVALID");
    }

    const inspection =
        await inspectManualReconcileRecovery({
            supabase,
            attemptId
        });

    if (inspection.state === "ALREADY_CLOSED") {
        return {
            recovered: true,
            reason: "ALREADY_CLOSED",
            attemptId
        };
    }

    if (inspection.state !== "FINALIZATION_RECORDED") {
        return {
            recovered: false,
            reason: inspection.state,
            attemptId,
            recoveryRequired: true
        };
    }

        // Re-read the immutable original attempt identity
    // through the privileged recovery snapshot RPC.
    // Never accept an operation token from an HTTP client.

    const snapshot =
        await readManualReconcileRecoverySnapshot(
            supabase,
            attemptId
        );

    const attempt = snapshot.attempt;

    if (
        !attempt ||
        attempt.attempt_id !== attemptId ||
        !UUID.test(attempt.operation_token || "")
    ) {
        fail("ATTEMPT_IDENTITY_UNVERIFIED");
    }

    // Recheck the original attempt and reservation
    // through the database close RPC.
    const { data, error } = await supabase.rpc(
        "close_manual_reconcile_v2",
        {
            p_attempt_id: attemptId,
            p_operation_token: attempt.operation_token
        }
    );

    if (error) {
        throw new Error(
            "RECONCILE_RECOVERY_CLOSE_RPC_UNCERTAIN",
            { cause: error }
        );
    }

    const result = getRpcRow(data);

    if (
        result.closed !== true ||
        !["CLOSED", "ALREADY_CLOSED"].includes(
            result.reason
        )
    ) {
        return {
            recovered: false,
            reason: "CLOSE_NOT_CONFIRMED",
            closeReason: result.reason || null,
            attemptId,
            recoveryRequired: true
        };
    }

    return {
        recovered: true,
        reason: result.reason,
        attemptId
    };
}

module.exports = {
    recoverManualReconciliation
};
