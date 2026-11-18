
"use strict";

const {
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");

const {
    readManualReconcileFailureCleanupReceipt
} = require("../../socketio/manualReconcileFailureRecoveryStore");

const UUID =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function blocked(attemptId, reason) {
    return {
        settled: false,
        attemptId,
        reason,
        recoveryRequired: true
    };
}

function record(value) {
    return value !== null &&
        typeof value === "object" &&
        !Array.isArray(value);
}

async function manualReconcileFailureSettlement({
    supabase,
    attemptId,
    fenceId,
    processEpoch
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId) ||
        typeof fenceId !== "string" ||
        !UUID.test(fenceId) ||
        typeof processEpoch !== "string" ||
        !UUID.test(processEpoch)
    ) {
        return blocked(attemptId, "INVALID_INPUT");
    }

    attemptId = attemptId.toLowerCase();
    fenceId = fenceId.toLowerCase();
    processEpoch = processEpoch.toLowerCase();

    let snapshot;

    try {
        snapshot = await readManualReconcileRecoverySnapshot(
            supabase,
            attemptId
        );
    } catch (_error) {
        return blocked(attemptId, "SNAPSHOT_UNVERIFIED");
    }

    const {
        attempt,
        execution,
        status,
        finalization
    } = snapshot;

    if (
        !record(attempt) ||
        !record(execution) ||
        !record(status) ||
        finalization !== null ||
        attempt.attempt_id !== attemptId ||
        execution.attempt_id !== attemptId ||
        execution.reservation_id !== attempt.reservation_id ||
        execution.operation_token !== attempt.operation_token ||
        status.id !== attempt.reservation_id ||
        status.status !== "RUNNING" ||
        status.operation_token !== attempt.operation_token ||
        !UUID.test(attempt.operation_token || "") ||
        !UUID.test(execution.execution_id || "") ||
        !UUID.test(execution.server_instance_id || "") ||
        !UUID.test(execution.owner_token || "") ||
        typeof execution.connector_id !== "string" ||
        !execution.connector_id ||
        typeof execution.socket_id !== "string" ||
        !execution.socket_id
    ) {
        return blocked(attemptId, "EXECUTION_NOT_ELIGIBLE");
    }

    let receipt;

    try {
        receipt =
            await readManualReconcileFailureCleanupReceipt(
                attemptId
            );
    } catch (_error) {
        return blocked(
            attemptId,
            "REDIS_STAGE_RECEIPT_READ_UNCERTAIN"
        );
    }

    if (
        !record(receipt) ||
        receipt.attemptId !== attemptId ||
        receipt.executionId !== execution.execution_id ||
        receipt.serverInstanceId !== execution.server_instance_id ||
        receipt.connectorId !== execution.connector_id ||
        receipt.socketId !== execution.socket_id ||
        receipt.ownerToken !== execution.owner_token ||
        receipt.stage !==
            "LOCKS_CLEARED_MARKERS_RETAINED" ||
        receipt.markersRetained !== true
    ) {
        return blocked(
            attemptId,
            "REDIS_STAGE_RECEIPT_NOT_VERIFIED"
        );
    }

    let response;

    try {
        response = await supabase.rpc(
            "settle_manual_reconcile_failure_v1",
            {
                p_attempt_id: attemptId,
                p_operation_token:
                    attempt.operation_token,
                p_execution_id:
                    execution.execution_id,
                p_fence_id: fenceId,
                p_server_instance_id:
                    execution.server_instance_id,
                p_connector_process_epoch:
                    processEpoch,
                p_redis_stage_receipt: receipt
            }
        );
    } catch (_error) {
        return blocked(
            attemptId,
            "SQL_SETTLEMENT_OUTCOME_UNCERTAIN"
        );
    }

    if (response?.error) {
        return blocked(
            attemptId,
            "SQL_SETTLEMENT_OUTCOME_UNCERTAIN"
        );
    }

    const result = Array.isArray(response?.data)
        ? response.data.length === 1
            ? response.data[0]
            : null
        : response?.data;

    if (
        !record(result) ||
        result.settled !== true ||
        result.reason !== "FAILURE_SETTLED"
    ) {
        return blocked(
            attemptId,
            "SQL_SETTLEMENT_NOT_CONFIRMED"
        );
    }

    return {
        settled: true,
        attemptId,
        reason: "FAILURE_SETTLED",
        recoveryRequired: false,
        executionId: execution.execution_id,
        connectorId: execution.connector_id,
        socketId: execution.socket_id
    };
}

module.exports = {
    manualReconcileFailureSettlement
};
