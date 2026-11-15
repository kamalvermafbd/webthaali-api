
"use strict";

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const REJECTIONS = new Set([
    "INVALID_INPUT",
    "BATCH_NOT_FOUND",
    "BATCH_CLAIM_INVALID",
    "BATCH_NOT_ELIGIBLE",
    "BATCH_OPERATION_ACTIVE",
    "RECONCILE_ALREADY_RUNNING",
    "SYNC_IN_PROGRESS",
    "HTTP_EXECUTION_UNRESOLVED",
    "SYNC_OUTCOME_UNKNOWN",
    "ATTEMPT_ID_CONFLICT",
    "ATTEMPT_NOT_CURRENT"
]);

async function startManualReconcileClaimed({
    supabase,
    attemptId,
    companyCode,
    tallyOwner,
    reconcileType,
    workerId,
    batchId,
    executionClaimToken
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        !UUID.test(attemptId || "") ||
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        typeof reconcileType !== "string" ||
        !reconcileType.trim() ||
        typeof workerId !== "string" ||
        !workerId.trim() ||
        !UUID.test(batchId || "") ||
        !UUID.test(executionClaimToken || "")
    ) {
        throw new Error(
            "RECONCILE_CLAIMED_ADMISSION_INPUT_INVALID"
        );
    }

    try {
        const { data, error } = await supabase.rpc(
            "start_manual_reconcile_claimed_v1",
            {
                p_attempt_id: attemptId,
                p_company_code: companyCode,
                p_tally_owner: tallyOwner,
                p_reconcile_type: reconcileType,
                p_worker_id: workerId,
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken
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
            "RECONCILE_CLAIMED_ADMISSION_RESULT_INVALID"
        );

    } catch (cause) {
        const error = new Error(
            "RECONCILE_CLAIMED_ADMISSION_OUTCOME_UNVERIFIED",
            { cause }
        );

        error.reconcileAdmissionUncertain = true;
        error.reconcileRecoveryRequired = true;
        error.attemptId = attemptId;

        // Internal recovery context only.
        // Never serialize this to the frontend.
        error.admissionInput = {
            attemptId,
            companyCode,
            tallyOwner,
            reconcileType,
            workerId,
            batchId,
            executionClaimToken
        };

        throw error;
    }
}

module.exports = {
    startManualReconcileClaimed
};
