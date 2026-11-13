
"use strict";

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function requireUUID(value, name) {
    if (
        typeof value !== "string" ||
        !UUID.test(value)
    ) {
        throw new Error(
            `DURABLE_RECONCILE_${name}_INVALID`
        );
    }

    return value;
}

/**
 * Mark a prepared Tally fetch as potentially dispatched.
 *
 * Only a freshly committed PREPARED ->
 * DISPATCH_MAY_HAVE_OCCURRED transition authorizes emit.
 *
 * No automatic retries.
 */
async function markDurableReconcileFetchDispatch({
    supabase,
    operationId,
    attemptId,
    operationToken,
    executionId,
    requestId
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error(
            "DURABLE_RECONCILE_SUPABASE_INVALID"
        );
    }

   requireUUID(operationId, "OPERATION_ID");
requireUUID(attemptId, "ATTEMPT_ID");
requireUUID(operationToken, "OPERATION_TOKEN");
requireUUID(executionId, "EXECUTION_ID");
requireUUID(requestId, "REQUEST_ID");

    let data;
    let rpcError;

    try {
       const response = await supabase.rpc(
    "mark_manual_reconcile_fetch_dispatch_v2",
    {
        p_operation_id: operationId,
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_request_id: requestId,
        p_execution_id: executionId
    }
);

        data = response?.data;
        rpcError = response?.error;
    } catch (cause) {
        const error = new Error(
            "DURABLE_RECONCILE_DISPATCH_RPC_UNVERIFIED",
            { cause }
        );

     error.reconcileOutcomeUncertain = true;
error.reconcileRecoveryRequired = true;
error.operationId = operationId;
error.attemptId = attemptId;
error.executionId = executionId;
error.requestId = requestId;

        throw error;
    }

    if (rpcError) {
        const error = new Error(
            "DURABLE_RECONCILE_DISPATCH_RPC_ERROR",
            { cause: rpcError }
        );

       error.reconcileOutcomeUncertain = true;
error.reconcileRecoveryRequired = true;
error.operationId = operationId;
error.attemptId = attemptId;
error.executionId = executionId;
error.requestId = requestId;
        throw error;
    }

    const row =
        Array.isArray(data) && data.length === 1
            ? data[0]
            : null;

    if (
        row?.marked === true &&
        row.reason === "DISPATCH_MARKED" &&
        row.operation_state ===
            "DISPATCH_MAY_HAVE_OCCURRED"
    ) {
        return {
            dispatchAuthorized: true,
            freshTransition: true,
            requestId
        };
    }

    // No other response permits a socket emit.
    const error = new Error(
        "DURABLE_RECONCILE_DISPATCH_NOT_AUTHORIZED"
    );

  error.reconcileRecoveryRequired = true;
error.reconcileOutcomeUncertain = true;
error.operationId = operationId;
error.attemptId = attemptId;
error.executionId = executionId;
error.requestId = requestId;

    if (row) {
        error.dispatchReason =
            typeof row.reason === "string"
                ? row.reason
                : "INVALID_RESPONSE";

        error.dispatchState =
            typeof row.operation_state === "string"
                ? row.operation_state
                : null;
    }

    throw error;
}

module.exports = {
    markDurableReconcileFetchDispatch
};
