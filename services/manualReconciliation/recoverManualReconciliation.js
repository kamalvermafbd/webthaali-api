
"use strict";

const {
    inspectManualReconcileRecovery,
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");

const {
    readManualReconcileReleaseReceipt
} = require("../../socketio/connectorHealthStore");

const UUID =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function fail(reason) {
    throw new Error(`RECONCILE_RECOVERY_${reason}`);
}

function getRpcRow(data) {
    const row = Array.isArray(data)
        ? data.length === 1
            ? data[0]
            : null
        : data;

    if (!row || typeof row !== "object" || Array.isArray(row)) {
        fail("RPC_RESULT_INVALID");
    }

    return row;
}

   function sameJson(a, b) {
    if (a === b) return true;

    if (Array.isArray(a) || Array.isArray(b)) {
        return Array.isArray(a) &&
            Array.isArray(b) &&
            a.length === b.length &&
            a.every((value, i) => sameJson(value, b[i]));
    }

    if (
        a && b &&
        typeof a === "object" &&
        typeof b === "object"
    ) {
        const keysA = Object.keys(a);
        const keysB = Object.keys(b);

        return keysA.length === keysB.length &&
            keysA.every(key =>
                Object.prototype.hasOwnProperty.call(b, key) &&
                sameJson(a[key], b[key])
            );
    }

    return false;
}
async function inspectConfirmedClosure(supabase, attemptId) {
    const inspection = await inspectManualReconcileRecovery({
        supabase,
        attemptId
    });

    return inspection.state === "ALREADY_CLOSED";
}

async function recoverManualReconciliation({
    supabase,
    attemptId,
    cancelAdmissionOnly = false
}) {
        if (typeof cancelAdmissionOnly !== "boolean") {
        fail("INPUT_INVALID");
    }
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        fail("INPUT_INVALID");
    }

   attemptId = attemptId.toLowerCase();

    if (cancelAdmissionOnly) {
        const snapshot = await readManualReconcileRecoverySnapshot(
            supabase,
            attemptId
        );

        const attempt = snapshot.attempt;

        if (!attempt) {
            return {
                recovered: false,
                reason: "ATTEMPT_NOT_FOUND",
                attemptId,
                recoveryRequired: true
            };
        }

        // Preliminary validation only.
        // SQL RPC performs atomic eligibility checks.
        if (
            attempt.attempt_id !== attemptId ||
            !UUID.test(attempt.operation_token || "") ||
            snapshot.execution !== null ||
            snapshot.checkpoint !== null ||
            snapshot.finalization !== null ||
            !Array.isArray(snapshot.operations) ||
            snapshot.operations.length !== 0
        ) {
            return {
                recovered: false,
                reason: "ADMISSION_CANCELLATION_NOT_ELIGIBLE",
                attemptId,
                recoveryRequired: true
            };
        }

        let response;

        try {
            response = await supabase.rpc(
                "cancel_manual_reconcile_admission_only_v1",
                {
                    p_attempt_id: attemptId,
                    p_operation_token: attempt.operation_token
                }
            );
        } catch {
            return {
                recovered: false,
                reason: "ADMISSION_CANCELLATION_OUTCOME_UNCERTAIN",
                attemptId,
                recoveryRequired: true
            };
        }

        if (response?.error) {
            return {
                recovered: false,
                reason: "ADMISSION_CANCELLATION_OUTCOME_UNCERTAIN",
                attemptId,
                recoveryRequired: true
            };
        }

        const result = getRpcRow(response?.data);

        if (result.attempt_id !== attemptId) {
            fail("CANCELLATION_RESPONSE_IDENTITY_INVALID");
        }

        if (
            result.cancelled === true &&
            [
                "CANCELLED_ADMISSION_ONLY",
                "ALREADY_CANCELLED_ADMISSION_ONLY"
            ].includes(result.reason)
        ) {
            return {
                recovered: true,
                reason: result.reason,
                attemptId
            };
        }

        if (
            result.cancelled === false &&
            [
                "INVALID_INPUT",
                "ATTEMPT_NOT_FOUND",
                "NOT_ADMISSION_ONLY",
                "RESERVATION_NOT_OWNED"
            ].includes(result.reason)
        ) {
            return {
                recovered: false,
                reason: result.reason,
                attemptId,
                recoveryRequired: true
            };
        }

        fail("CANCELLATION_RPC_RESULT_INVALID");
    }

let inspection =
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

    
        // Only independently verified Redis release evidence
    // may authorize post-release finalization recovery.

    if (
        inspection.state ===
        "CHECKPOINT_PRESENT_RELEASE_VERIFIED"
    ) {
        const snapshot =
            await readManualReconcileRecoverySnapshot(
                supabase,
                attemptId
            );

        const {
            attempt,
            execution,
            checkpoint,
            finalization,
            status
        } = snapshot;

        const receipt =
            await readManualReconcileReleaseReceipt(
                attemptId
            );

        if (
            inspection.reservationOwned !== true ||
            inspection.redisReleaseReceiptVerified !== true ||
            inspection.journal?.allVerified !== true ||
            inspection.finalizationState !== null ||
            !attempt ||
            !execution ||
            !checkpoint ||
            finalization !== null ||
            !status ||
            attempt.attempt_id !== attemptId ||
            !UUID.test(attempt.operation_token || "") ||
                        execution.attempt_id !== attemptId ||
            checkpoint.attempt_id !== attemptId ||
            checkpoint.reservation_id !== attempt.reservation_id ||
            checkpoint.operation_token !== attempt.operation_token ||
            status.company_code !== attempt.company_code ||
            status.tally_owner !== attempt.tally_owner ||
            execution.reservation_id !== attempt.reservation_id ||
            execution.operation_token !== attempt.operation_token ||
            checkpoint.execution_id !== execution.execution_id ||
            checkpoint.connector_id !== execution.connector_id ||
            checkpoint.owner_token !== execution.owner_token ||
            checkpoint.final_recheck_completed !== true ||
            !Array.isArray(
                checkpoint.remaining_mismatch_ledger_guids
            ) ||
            !Array.isArray(
                checkpoint.voucher_investigation_guids
            ) ||
            status.id !== attempt.reservation_id ||
            status.status !== "RUNNING" ||
            status.operation_token !== attempt.operation_token ||
            !receipt ||
            receipt.redisReleaseConfirmed !== true ||
            receipt.attemptId !== attemptId ||
            receipt.executionId !== execution.execution_id ||
            receipt.serverInstanceId !== execution.server_instance_id ||
            receipt.connectorId !== execution.connector_id ||
            receipt.socketId !== execution.socket_id ||
            receipt.ownerToken !== execution.owner_token
        ) {
            return {
                recovered: false,
                reason: "RECOVERY_EVIDENCE_NOT_VERIFIED",
                attemptId,
                recoveryRequired: true
            };
        }

        const beforeRecord =
            await inspectManualReconcileRecovery({
                supabase,
                attemptId
            });

        if (
            beforeRecord.state !==
                "CHECKPOINT_PRESENT_RELEASE_VERIFIED" ||
            beforeRecord.reservationOwned !== true ||
            beforeRecord.redisReleaseReceiptVerified !== true ||
            beforeRecord.journal?.allVerified !== true
        ) {
            return {
                recovered: false,
                reason: "RECOVERY_STATE_CHANGED",
                attemptId,
                recoveryRequired: true
            };
        }

        let recordResponse;
        let recordCause;

        try {
            recordResponse = await supabase.rpc(
                "record_manual_reconcile_finalization_v2",
                {
                    p_attempt_id: attemptId,
                    p_operation_token: attempt.operation_token,
                    p_remaining_mismatch_ledger_guids:
                        checkpoint.remaining_mismatch_ledger_guids,
                    p_voucher_investigation_guids:
                        checkpoint.voucher_investigation_guids,
                    p_final_recheck_completed: true,
                    p_connector_release_verified: true,
                    p_connector_id: execution.connector_id,
                    p_release_owner_token: execution.owner_token,
                    p_release_evidence: receipt
                }
            );

            if (recordResponse?.error) {
                recordCause = recordResponse.error;
            }
        } catch (cause) {
            recordCause = cause;
        }

        // A committed RPC may have lost its response.
        // Confirm finalization from durable state.

        let afterRecord;

        try {
            afterRecord =
                await inspectManualReconcileRecovery({
                    supabase,
                    attemptId
                });
        } catch (cause) {
            throw new Error(
                "RECONCILE_RECOVERY_FINALIZATION_UNCERTAIN",
                { cause: recordCause || cause }
            );
        }

        if (
            afterRecord.state !== "FINALIZATION_RECORDED" &&
            afterRecord.state !== "ALREADY_CLOSED"
        ) {
            if (recordCause) {
                throw new Error(
                    "RECONCILE_RECOVERY_FINALIZATION_RPC_UNCERTAIN",
                    { cause: recordCause }
                );
            }

            return {
                recovered: false,
                reason: "FINALIZATION_NOT_CONFIRMED",
                attemptId,
                recoveryRequired: true
            };
        }

     

        const confirmedSnapshot =
            await readManualReconcileRecoverySnapshot(
                supabase,
                attemptId
            );

        const storedFinal = confirmedSnapshot.finalization;

        if (
            !storedFinal ||
            !["EVIDENCE_RECORDED", "CLOSED"].includes(
                storedFinal.state
            ) ||
            storedFinal.attempt_id !== attemptId ||
            storedFinal.reservation_id !== attempt.reservation_id ||
            storedFinal.operation_token !== attempt.operation_token ||
            storedFinal.connector_id !== execution.connector_id ||
            storedFinal.release_owner_token !== execution.owner_token ||
            storedFinal.connector_release_verified !== true ||
            storedFinal.final_recheck_completed !== true ||
            !sameJson(
                storedFinal.remaining_mismatch_ledger_guids,
                checkpoint.remaining_mismatch_ledger_guids
            ) ||
            !sameJson(
                storedFinal.voucher_investigation_guids,
                checkpoint.voucher_investigation_guids
            ) ||
            !sameJson(
                storedFinal.release_evidence,
                receipt
            )
        ) {
            return {
                recovered: false,
                reason: "FINALIZATION_EVIDENCE_NOT_CONFIRMED",
                attemptId,
                recoveryRequired: true
            };
        }

        inspection = afterRecord;
    }
    if (inspection.state === "ALREADY_CLOSED") {
        return {
            recovered: true,
            reason: "ALREADY_CLOSED",
            attemptId
        };
    }
    // Never close uncertain execution.
    if (
        inspection.state !== "FINALIZATION_RECORDED" ||
        inspection.reservationOwned !== true ||
        inspection.journal?.allVerified !== true
    ) {
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
    
    let response;
    let closeCause;

    try {
        response = await supabase.rpc("close_manual_reconcile_v2", {
            p_attempt_id: attemptId,
            p_operation_token: attempt.operation_token
        });
    } catch (cause) {
        closeCause = cause;
    }

    if (response?.error) {
        closeCause = response.error;
    }

    if (closeCause) {
        // RPC response lost ho sakta hai even if DB close succeeded.
        try {
            if (await inspectConfirmedClosure(supabase, attemptId)) {
                return {
                    recovered: true,
                    reason: "ALREADY_CLOSED",
                    attemptId
                };
            }
        } catch {
            // Preserve uncertain-close error.
        }

        throw new Error(
            "RECONCILE_RECOVERY_CLOSE_RPC_UNCERTAIN",
            { cause: closeCause }
        );
    }

    const result = getRpcRow(response?.data);

    if (
        result.closed !== true ||
        !["CLOSED", "ALREADY_CLOSED"].includes(result.reason)
    ) {
        return {
            recovered: false,
            reason: "CLOSE_NOT_CONFIRMED",
            closeReason: result.reason || null,
            attemptId,
            recoveryRequired: true
        };
    }

    if (!(await inspectConfirmedClosure(supabase, attemptId))) {
        return {
            recovered: false,
            reason: "CLOSE_POSTCHECK_NOT_CONFIRMED",
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
