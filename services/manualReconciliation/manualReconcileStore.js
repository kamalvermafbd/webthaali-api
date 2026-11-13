"use strict";

function getRpcRow(data) {
    if (!Array.isArray(data) || data.length !== 1) {
        throw new Error("RECONCILE_RPC_RESULT_INVALID");
    }

    const row = data[0];

    if (!row || typeof row !== "object") {
        throw new Error("RECONCILE_RPC_RESULT_INVALID");
    }

    return row;
}

async function startManualReconcile({
    supabase,
    companyCode,
    tallyOwner,
    reconcileType,
    workerId,
    batchId = null
}) {
    const { data, error } = await supabase.rpc(
        "start_manual_reconcile",
        {
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

    const row = getRpcRow(data);

    if (row.acquired === true) {
        if (
            row.reason !== "ACQUIRED" ||
            typeof row.reservation_token !== "string" ||
            !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                row.reservation_token
            )
        ) {
            throw new Error("RECONCILE_ADMISSION_RESULT_INVALID");
        }

        return {
            acquired: true,
            reservationToken: row.reservation_token
        };
    }

    if (
        row.acquired === false &&
        row.reservation_token === null &&
        [
            "INVALID_INPUT",
            "RECONCILE_ALREADY_RUNNING",
            "SYNC_IN_PROGRESS",
            "HTTP_EXECUTION_UNRESOLVED",
            "SYNC_OUTCOME_UNKNOWN",
            "BATCH_NOT_FOUND"
        ].includes(row.reason)
    ) {
        return {
            acquired: false,
            reason: row.reason
        };
    }

    throw new Error("RECONCILE_ADMISSION_RESULT_INVALID");
}

async function closeManualReconcile({
    supabase,
    companyCode,
    tallyOwner,
    reservationToken,
    errorMessage = null
}) {
    const { data, error } = await supabase.rpc(
        "close_manual_reconcile",
        {
            p_company_code: companyCode,
            p_tally_owner: tallyOwner,
            p_operation_token: reservationToken,
            p_error_message: errorMessage
        }
    );

    if (error) {
        throw error;
    }

    const row = getRpcRow(data);

    if (row.closed === true && row.reason === "CLOSED") {
        return { closed: true };
    }

    if (
        row.closed === false &&
        [
            "INVALID_INPUT",
            "RESERVATION_NOT_FOUND",
            "NOT_RUNNING",
            "TOKEN_MISMATCH",
            "CLOSE_FAILED"
        ].includes(row.reason)
    ) {
        return {
            closed: false,
            reason: row.reason
        };
    }

    throw new Error("RECONCILE_CLOSE_RESULT_INVALID");
}

async function inspectManualReconcile({
    supabase,
    companyCode,
    tallyOwner
}) {
    const { data, error } = await supabase.rpc(
        "inspect_manual_reconcile",
        {
            p_company_code: companyCode,
            p_tally_owner: tallyOwner
        }
    );

    if (error) {
        throw error;
    }

    if (!Array.isArray(data) || data.length > 1) {
        throw new Error("RECONCILE_INSPECTION_RESULT_INVALID");
    }

    if (data.length === 0) {
        return {
            found: false
        };
    }

    const row = data[0];

    if (
        !row ||
        typeof row !== "object" ||
        !["RUNNING", "CLOSED"].includes(row.status)
    ) {
        throw new Error("RECONCILE_INSPECTION_RESULT_INVALID");
    }

    if (
        row.status === "RUNNING" &&
        (
            typeof row.operation_token !== "string" ||
            !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                row.operation_token
            )
        )
    ) {
        throw new Error("RECONCILE_INSPECTION_TOKEN_INVALID");
    }

    if (
        row.status === "CLOSED" &&
        row.operation_token !== null
    ) {
        throw new Error("RECONCILE_INSPECTION_TOKEN_INVALID");
    }

    return {
        found: true,
        status: row.status,
        reservationToken: row.operation_token,
        workerId: row.worker_id,
        reconcileType: row.reconcile_type,
        startedAt: row.started_at,
        updatedAt: row.updated_at,
        errorMessage: row.error_message
    };
}

module.exports = {
    startManualReconcile,
    closeManualReconcile,
    inspectManualReconcile
};