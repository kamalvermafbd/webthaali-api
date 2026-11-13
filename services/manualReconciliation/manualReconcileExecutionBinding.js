
"use strict";

const crypto = require("node:crypto");

function requireUuid(value, name) {
    if (
        typeof value !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    ) {
        throw new Error(`RECONCILE_${name}_INVALID`);
    }

    return value;
}

function requireText(value, name) {
    if (typeof value !== "string" || !value.trim()) {
        throw new Error(`RECONCILE_${name}_INVALID`);
    }

    return value.trim();
}

async function bindManualReconcileExecution({
    supabase,
    attemptId,
    operationToken,
    executionId,
    connectorId,
    socketId,
    ownerToken,
    serverInstanceId
}) {
    if (!supabase || typeof supabase.rpc !== "function") {
        throw new Error("RECONCILE_SUPABASE_INVALID");
    }

    const params = {
        p_attempt_id: requireUuid(attemptId, "ATTEMPT_ID"),
        p_operation_token: requireUuid(
            operationToken, "OPERATION_TOKEN"
        ),
        p_execution_id: requireUuid(
            executionId, "EXECUTION_ID"
        ),
        p_connector_id: requireText(
            connectorId, "CONNECTOR_ID"
        ),
        p_socket_id: requireText(socketId, "SOCKET_ID"),
        p_owner_token: requireUuid(
            ownerToken, "OWNER_TOKEN"
        ),
        p_server_instance_id: requireUuid(
            serverInstanceId, "SERVER_INSTANCE_ID"
        )
    };

    let response;

    try {
        response = await supabase.rpc(
            "bind_manual_reconcile_execution_v1",
            params
        );
    } catch (cause) {
        const error = new Error(
            "RECONCILE_EXECUTION_BIND_OUTCOME_UNKNOWN"
        );
        error.cause = cause;
        error.reconcileRecoveryRequired = true;
        throw error;
    }

    if (response.error) {
        const error = new Error(
            "RECONCILE_EXECUTION_BIND_OUTCOME_UNKNOWN"
        );
        error.cause = response.error;
        error.reconcileRecoveryRequired = true;
        throw error;
    }

    const result = Array.isArray(response.data)
        ? response.data.length === 1
            ? response.data[0]
            : null
        : response.data;

    if (
        !result ||
        result.bound !== true ||
        result.reason !== "BOUND" ||
        result.execution_id !== executionId
    ) {
        const error = new Error(
            `RECONCILE_EXECUTION_BIND_NOT_CONFIRMED:${
                result?.reason || "INVALID_RESPONSE"
            }`
        );
        error.reconcileRecoveryRequired = true;
        throw error;
    }

    return {
        bound: true,
        executionId
    };
}

module.exports = {
    bindManualReconcileExecution
};
