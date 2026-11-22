
"use strict";

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

async function ensureManualReconcileFailureFenceIntent({
    supabase, attemptId, attempt, execution, onEvent = () => {}
}) {
    const event = (name, extra = {}) => {
        try {
            onEvent({
                event: name,
                attemptId,
                executionId: execution?.execution_id || null,
                ...extra
            });
        } catch (_) {
            // Logging must not affect recovery.
        }
    };

    const stopped = reason => ({
        fence_requested: false,
        reason
    });

    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof supabase.from !== "function" ||
        !UUID.test(attemptId || "") ||
        attempt?.attempt_id !== attemptId ||
        execution?.attempt_id !== attemptId ||
        execution.reservation_id !== attempt.reservation_id ||
        execution.operation_token !== attempt.operation_token ||
        !UUID.test(attempt.operation_token || "") ||
        !UUID.test(execution.execution_id || "") ||
        !UUID.test(execution.owner_token || "") ||
        !UUID.test(execution.server_instance_id || "") ||
        typeof execution.connector_id !== "string" ||
        !execution.connector_id.trim() ||
        typeof execution.socket_id !== "string" ||
        !execution.socket_id.trim()
    ) {
        return stopped("FENCE_INTENT_INPUT_INVALID");
    }

    let response;
    let rpcError = null;

    event("RPC_STARTED");

    try {
        response = await supabase.rpc(
            "request_manual_reconcile_failure_fence_v2",
            {
                p_attempt_id: attemptId,
                p_operation_token: attempt.operation_token,
                p_execution_id: execution.execution_id
            }
        );

        rpcError = response?.error || null;
    } catch (error) {
        rpcError = error;
    }

    const reply = Array.isArray(response?.data)
        ? response.data.length === 1
            ? response.data[0]
            : null
        : response?.data;

    if (!rpcError && reply?.fence_requested === false) {
        event("RPC_REJECTED", {
            reason: reply.reason || null
        });

        return stopped("FENCE_INTENT_NOT_CONFIRMED");
    }

    const acknowledged =
        !rpcError &&
        reply?.fence_requested === true &&
        ["FENCE_REQUESTED", "ALREADY_REQUESTED"]
            .includes(reply.reason) &&
        typeof reply.fence_id === "string" &&
        UUID.test(reply.fence_id);

    event(
        acknowledged ? "RPC_ACKNOWLEDGED" : "RPC_UNCERTAIN",
        { code: rpcError?.code || null }
    );

    let row;

    try {
        const read = await supabase
            .from("manual_reconcile_failure_recovery_evidence")
            .select(
                "attempt_id,execution_id,connector_id,socket_id," +
                "owner_token,fence_id,fence_requested_at"
            )
            .eq("attempt_id", attemptId)
            .maybeSingle();

        if (read.error) throw read.error;

        row = read.data;
    } catch (error) {
        event("READBACK_UNCERTAIN", {
            code: error?.code || null
        });

        return stopped("FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    if (!row) {
        event("INTENT_NOT_OBSERVED");

        return stopped("FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    if (
        row.attempt_id !== attemptId ||
        row.execution_id !== execution.execution_id ||
        row.connector_id !== execution.connector_id ||
        row.socket_id !== execution.socket_id ||
        row.owner_token !== execution.owner_token ||
        typeof row.fence_id !== "string" ||
        !UUID.test(row.fence_id) ||
        typeof row.fence_requested_at !== "string" ||
        !Number.isFinite(Date.parse(row.fence_requested_at)) ||
        (
            acknowledged &&
            row.fence_id.toLowerCase() !==
                reply.fence_id.toLowerCase()
        )
    ) {
        event("INTENT_IDENTITY_CONFLICT");

        return stopped("FENCE_INTENT_NOT_CONFIRMED");
    }

    event("DURABLE_INTENT_VERIFIED", {
        fenceId: row.fence_id,
        recoveredFromUncertainty: !acknowledged
    });

    return {
        fence_requested: true,
        reason: acknowledged
            ? reply.reason
            : "ALREADY_REQUESTED",
        fence_id: row.fence_id.toLowerCase()
    };
}

module.exports = {
    ensureManualReconcileFailureFenceIntent
};
