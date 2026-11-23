
"use strict";

const {
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");

const UUID =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function isRecord(value) {
    return value !== null &&
        typeof value === "object" &&
        !Array.isArray(value);
}

async function inspectHistoricalManualReconcileFailure({
    supabase,
    attemptId
} = {}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        throw new Error(
            "HISTORICAL_RECOVERY_INSPECTION_INPUT_INVALID"
        );
    }

    attemptId = attemptId.toLowerCase();

    const snapshot =
        await readManualReconcileRecoverySnapshot(
            supabase,
            attemptId
        );

    const { attempt, execution, status, finalization } =
        snapshot;

    const blockers = [];

    const identityValid = Boolean(
        attempt &&
        execution &&
        status &&
        attempt.attempt_id === attemptId &&
        execution.attempt_id === attemptId &&
        execution.reservation_id === attempt.reservation_id &&
        execution.operation_token === attempt.operation_token &&
        status.id === attempt.reservation_id &&
        status.company_code === attempt.company_code &&
        status.tally_owner === attempt.tally_owner &&
        status.status === "FAILED" &&
        status.operation_token === null &&
        status.last_terminal_attempt_id === attemptId &&
        status.closed_at != null &&
        UUID.test(attempt.operation_token || "") &&
        UUID.test(execution.execution_id || "") &&
        UUID.test(execution.server_instance_id || "") &&
        UUID.test(execution.owner_token || "") &&
        typeof execution.connector_id === "string" &&
        Boolean(execution.connector_id.trim()) &&
        typeof execution.socket_id === "string" &&
        Boolean(execution.socket_id.trim()) &&
        finalization === null
    );

    if (!identityValid) {
        blockers.push("HISTORICAL_FAILED_IDENTITY_INVALID");
    }

    const readOne = async (table, columns) => {
        const response = await supabase
            .from(table)
            .select(columns)
            .eq("attempt_id", attemptId)
            .maybeSingle();

        if (response.error) {
            throw response.error;
        }

        return response.data;
    };

    const [recovery, receipt] = await Promise.all([
        readOne(
            "manual_reconcile_failure_recovery_evidence",
            "attempt_id,execution_id,connector_id," +
            "socket_id,owner_token,recovery_state," +
            "remote_execution_fenced,fence_confirmed_at," +
            "remote_fence_evidence," +
            "coordinator_drain_confirmed_at," +
            "coordinator_unsafe_detected," +
            "coordinator_unsafe_detected_at," +
            "redis_stage_receipt,terminal_closed_at"
        ),
        readOne(
            "manual_reconcile_coordinator_exit_receipts",
            "attempt_id,operation_token,execution_id," +
            "server_instance_id,connector_id,socket_id," +
            "owner_token,coordinator_exited," +
            "admission_sealed,pending_work," +
            "callback_started,callback_settled," +
            "late_work_detected,outcome,recorded_at"
        )
    ]);

    const recoveryIdentityValid = Boolean(
        isRecord(recovery) &&
        identityValid &&
        recovery.attempt_id === attemptId &&
        recovery.execution_id === execution.execution_id &&
        recovery.connector_id === execution.connector_id &&
        recovery.socket_id === execution.socket_id &&
        recovery.owner_token === execution.owner_token
    );

    if (!recoveryIdentityValid) {
        blockers.push("RECOVERY_EVIDENCE_IDENTITY_INVALID");
    }

    const coordinatorReceiptValid = Boolean(
        isRecord(receipt) &&
        identityValid &&
        receipt.attempt_id === attemptId &&
        receipt.operation_token === attempt.operation_token &&
        receipt.execution_id === execution.execution_id &&
        receipt.server_instance_id ===
            execution.server_instance_id &&
        receipt.connector_id === execution.connector_id &&
        receipt.socket_id === execution.socket_id &&
        receipt.owner_token === execution.owner_token &&
        receipt.coordinator_exited === true &&
        receipt.admission_sealed === true &&
        receipt.pending_work === 0 &&
        receipt.callback_started === true &&
        receipt.callback_settled === true &&
        receipt.late_work_detected === false &&
        receipt.outcome === "LOCALLY_SETTLED_UNVERIFIED" &&
        typeof receipt.recorded_at === "string" &&
        Number.isFinite(Date.parse(receipt.recorded_at))
    );

    if (!coordinatorReceiptValid) {
        blockers.push("ORIGINAL_COORDINATOR_RECEIPT_MISSING_OR_INVALID");
    }

    const remoteFenceRecorded = Boolean(
        recoveryIdentityValid &&
        recovery.remote_execution_fenced === true &&
        recovery.fence_confirmed_at != null &&
        isRecord(recovery.remote_fence_evidence)
    );

    if (!remoteFenceRecorded) {
        blockers.push("REMOTE_FENCE_EVIDENCE_MISSING");
    }

    const coordinatorDrainRecorded = Boolean(
        recoveryIdentityValid &&
        recovery.coordinator_drain_confirmed_at != null
    );

    if (!coordinatorDrainRecorded) {
        blockers.push("COORDINATOR_DRAIN_EVIDENCE_MISSING");
    }

    if (
        recovery &&
        (
            recovery.coordinator_unsafe_detected !== false ||
            recovery.coordinator_unsafe_detected_at != null
        )
    ) {
        blockers.push("COORDINATOR_UNSAFE_OR_UNVERIFIED");
    }

    return {
        attemptId,
        reservationStatus: status?.status || null,
        identityValid,
        recoveryState: recovery?.recovery_state || null,
        coordinatorReceiptValid,
        remoteFenceRecorded,
        coordinatorDrainRecorded,
        blockers,
        readyForRecovery: false,
        diagnosticOnly: true
    };
}

module.exports = {
    inspectHistoricalManualReconcileFailure
};
