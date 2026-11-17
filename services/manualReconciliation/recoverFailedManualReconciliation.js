
"use strict";


const {
    inspectManualReconcileRecovery,
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");
const {
    sendManualReconcileFenceRequest
} = require("../../socketio/sendManualReconcileFenceRequest");
const {
    sendManualReconcileExecutionObservationRequest
} = require(
    "../../socketio/sendManualReconcileExecutionObservationRequest"
);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function blocked(attemptId, reason) {
    return {
        recovered: false,
        attemptId,
        reason,
        recoveryRequired: true,
        settlementEligible: false
    };
}

function sameExecution(before, after) {
    const a = before?.attempt;
    const e = before?.execution;
    const s = before?.status;

    const b = after?.attempt;
    const f = after?.execution;
    const t = after?.status;

    return Boolean(
        a && e && s && b && f && t &&
        a.attempt_id === b.attempt_id &&
        a.operation_token === b.operation_token &&
        a.reservation_id === b.reservation_id &&
        a.company_code === b.company_code &&
        a.tally_owner === b.tally_owner &&
        e.attempt_id === f.attempt_id &&
        e.reservation_id === f.reservation_id &&
        e.operation_token === f.operation_token &&
        e.execution_id === f.execution_id &&
        e.connector_id === f.connector_id &&
        e.socket_id === f.socket_id &&
        e.owner_token === f.owner_token &&
        e.server_instance_id === f.server_instance_id &&
        s.id === t.id &&
        s.company_code === t.company_code &&
        s.tally_owner === t.tally_owner &&
        s.operation_token === t.operation_token &&
        s.status === t.status
    );
}

/**
 * Explicit failure-recovery transport attempt only.
 * Never settles a reconciliation, releases Redis, or clears quarantine.
 * Do not invoke from an inspection endpoint or an automatic retry loop.
 */
async function recoverFailedManualReconciliation({
    supabase,
    attemptId,
    timeoutMs = 10000
} = {}) {
    if (
        !supabase || typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" || !UUID.test(attemptId) ||
        !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000
    ) {
        throw new Error("RECONCILE_FAILURE_RECOVERY_INPUT_INVALID");
    }

    attemptId = attemptId.toLowerCase();

    const inspection = await inspectManualReconcileRecovery({
        supabase,
        attemptId
    });
    if (inspection.state !== "EXECUTION_UNCERTAIN" ||
        inspection.reservationOwned !== true) {
        return blocked(attemptId, `NOT_ELIGIBLE_${inspection.state}`);
    }

    const before = await readManualReconcileRecoverySnapshot(
        supabase,
        attemptId
    );
    const attempt = before.attempt;
    const execution = before.execution;
    const status = before.status;

    
    if (
    !attempt || !execution || !status ||
    execution.attempt_id !== attemptId ||
    execution.reservation_id !== attempt.reservation_id ||
    execution.operation_token !== attempt.operation_token ||
    status.id !== attempt.reservation_id ||
    attempt.attempt_id !== attemptId ||
    !UUID.test(attempt.operation_token || "") ||
    !UUID.test(execution.execution_id || "") ||
    typeof execution.connector_id !== "string" ||
    !execution.connector_id.trim() ||
    typeof execution.socket_id !== "string" ||
    !execution.socket_id.trim() ||
    status.status !== "RUNNING" ||
    status.operation_token !== attempt.operation_token ||
    status.company_code !== attempt.company_code ||
    status.tally_owner !== attempt.tally_owner ||
    before.finalization !== null
) {
    return blocked(attemptId, "ORIGINAL_EXECUTION_UNVERIFIED");
}

    // This RPC records intent only. It does NOT establish remote quiescence.
    let response;
    try {
        response = await supabase.rpc(
            "request_manual_reconcile_failure_fence_v2",
            {
                p_attempt_id: attemptId,
                p_operation_token: attempt.operation_token,
                p_execution_id: execution.execution_id
            }
        );
    } catch (_error) {
        return blocked(attemptId, "FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    if (response?.error) {
        return blocked(attemptId, "FENCE_INTENT_OUTCOME_UNCERTAIN");
    }

    const result = Array.isArray(response?.data)
        ? response.data.length === 1 ? response.data[0] : null
        : response?.data;
   if (
    !result ||
    typeof result !== "object" ||
    Array.isArray(result) ||
    result.fence_requested !== true ||
    !["FENCE_REQUESTED", "ALREADY_REQUESTED"].includes(result.reason) ||
    typeof result.fence_id !== "string" ||
    !UUID.test(result.fence_id)
) {
    return blocked(attemptId, "FENCE_INTENT_NOT_CONFIRMED");
}

    // Re-read after the intent RPC to avoid dispatch on a changed identity.
    const after = await readManualReconcileRecoverySnapshot(
        supabase,
        attemptId
    );
    if (
        !sameExecution(before, after) ||
        after.status?.status !== "RUNNING" ||
        after.status?.operation_token !== attempt.operation_token ||
        after.finalization !== null
    ) {
        return blocked(attemptId, "EXECUTION_CHANGED_AFTER_FENCE_INTENT");
    }

    const fenceId = result.fence_id.toLowerCase();
    try {
        const acknowledgement = await sendManualReconcileFenceRequest({
            connectorId: execution.connector_id,
            expectedSocketId: execution.socket_id,
            attemptId,
            executionId: execution.execution_id,
            fenceId,
            timeoutMs
        });

                       // Validate fence acknowledgement before observation.
        const fenceSnapshot = acknowledgement?.snapshot;

        if (
            acknowledgement?.settlementEligible !== false ||
            acknowledgement?.attemptId !== attemptId ||
            acknowledgement?.executionId !== execution.execution_id ||
            acknowledgement?.connectorId !== execution.connector_id ||
            acknowledgement?.socketId !== execution.socket_id ||
            acknowledgement?.fenceId !== fenceId ||
            !UUID.test(fenceSnapshot?.processEpoch || "") ||
            fenceSnapshot?.fenced !== true ||
            fenceSnapshot?.fenceId !== fenceId ||
            fenceSnapshot?.settlementEligible !== false ||
            typeof fenceSnapshot?.executionKnown !== "boolean" ||
            typeof fenceSnapshot?.identityConflict !== "boolean"
        ) {
            return {
                ...blocked(attemptId, "FENCE_ACK_IDENTITY_INVALID"),
                fenceAcknowledged: false
            };
        }

        // An installed safety fence does not prove that the
        // requested execution identity exists in this process.
        if (
            fenceSnapshot.executionKnown !== true ||
            fenceSnapshot.identityConflict !== false
        ) {
            return {
                ...blocked(
                    attemptId,
                    fenceSnapshot.identityConflict
                        ? "FENCE_EXECUTION_IDENTITY_CONFLICT"
                        : "FENCE_EXECUTION_UNKNOWN"
                ),
                fenceAcknowledged: true,
                executionKnown: fenceSnapshot.executionKnown,
                identityConflict: fenceSnapshot.identityConflict,
                observationVerified: false
            };
        }

        let observationResult;

        try {
            observationResult =
                await sendManualReconcileExecutionObservationRequest({
                    connectorId: execution.connector_id,
                    expectedSocketId: execution.socket_id,
                    attemptId,
                    executionId: execution.execution_id,
                    expectedProcessEpoch:
                        acknowledgement.snapshot.processEpoch,
                    expectedFenceId: fenceId,
                    timeoutMs
                });
        } catch (error) {
            return {
                ...blocked(
                    attemptId,
                    "EXECUTION_OBSERVATION_UNVERIFIED"
                ),
                fenceAcknowledged: true,
                observationVerified: false,
                observationReason:
                    error?.message ||
                    "RECONCILE_OBSERVATION_FAILED"
            };
        }

        return {
            ...blocked(
                attemptId,
                "EXECUTION_LOCALLY_DRAINED"
            ),
            fenceAcknowledged: true,
            executionKnown: true,
            identityConflict: false,
            observationVerified: true,
            observation: observationResult.observation
        };
    } catch (error) {
        return {
            ...blocked(attemptId, "FENCE_TRANSPORT_UNVERIFIED"),
            fenceAcknowledged: false,
            transportReason: error?.message || "UNKNOWN"
        };
    }
}

module.exports = { recoverFailedManualReconciliation };
