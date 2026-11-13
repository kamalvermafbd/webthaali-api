"use strict";

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const REJECTIONS = new Set([
    "INVALID_INPUT",
    "RECONCILE_ALREADY_RUNNING",
    "SYNC_IN_PROGRESS",
    "HTTP_EXECUTION_UNRESOLVED",
    "SYNC_OUTCOME_UNKNOWN",
    "BATCH_NOT_FOUND",
    "ATTEMPT_ID_CONFLICT",
    "ATTEMPT_NOT_CURRENT"
]);

/**
 * Phase 1: Durable manual reconciliation admission.
 *
 * This adapter is intentionally NOT wired to the reconciliation
 * executor yet.
 *
 * The caller must retain attemptId and identical inputs before
 * invoking this function.
 *
 * A transport failure must never cause a fresh attemptId to be
 * generated automatically.
 *
 * ALREADY_ACQUIRED confirms admission identity only.
 * It does NOT authorize another executor to perform Tally work.
 */
async function startManualReconcileV2({
    supabase,
    attemptId,
    companyCode,
    tallyOwner,
    reconcileType,
    workerId,
    batchId = null
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId) ||
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        typeof reconcileType !== "string" ||
        !reconcileType.trim() ||
        typeof workerId !== "string" ||
        !workerId.trim() ||
        (
            batchId !== null &&
            typeof batchId !== "string"
        )
    ) {
        throw new Error(
            "RECONCILE_ADMISSION_INPUT_INVALID"
        );
    }

    try {
        const { data, error } = await supabase.rpc(
            "start_manual_reconcile_v2",
            {
                p_attempt_id: attemptId,
                p_company_code: companyCode,
                p_tally_owner: tallyOwner,
                p_reconcile_type: reconcileType,
                p_worker_id: workerId,
                p_batch_id: batchId
            }
        );

        if (error) {
            throw error;
        }

        const row =
            Array.isArray(data) && data.length === 1
                ? data[0]
                : null;

        if (
            row?.acquired === true &&
            ["ACQUIRED", "ALREADY_ACQUIRED"].includes(
                row.reason
            ) &&
            typeof row.reservation_token === "string" &&
            UUID.test(row.reservation_token)
        ) {
            return {
                acquired: true,
                reason: row.reason,
                reservationToken: row.reservation_token
            };
        }

        if (
            row?.acquired === false &&
            row.reservation_token === null &&
            REJECTIONS.has(row.reason)
        ) {
            return {
                acquired: false,
                reason: row.reason
            };
        }

        throw new Error(
            "RECONCILE_ADMISSION_RESULT_INVALID"
        );

    } catch (cause) {
        const error = new Error(
            "RECONCILE_ADMISSION_OUTCOME_UNVERIFIED",
            { cause }
        );

        error.reconcileAdmissionUncertain = true;
        error.reconcileRecoveryRequired = true;
        error.attemptId = attemptId;

        // Internal recovery context.
        // Never serialize this object directly to frontend.
        error.admissionInput = {
            attemptId,
            companyCode,
            tallyOwner,
            reconcileType,
            workerId,
            batchId
        };

        throw error;
    }
}

module.exports = {
    startManualReconcileV2
};