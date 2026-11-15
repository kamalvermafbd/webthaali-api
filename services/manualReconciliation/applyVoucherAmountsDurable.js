
"use strict";

const crypto = require("node:crypto");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function uncertainError(code, context, cause) {
    const error = new Error(code, cause ? { cause } : undefined);

    error.reconcileRecoveryRequired = true;
    error.reconcileOutcomeUncertain = true;

    Object.assign(error, context);
    return error;
}

async function applyVoucherAmountsDurable({
    supabase,
    signal,
    attemptId,
    executionId,
    operationToken,
    operationOrdinal,
    fetchOperationId,
    sqlEntries
}) {
    if (!supabase || typeof supabase.rpc !== "function") {
        throw new Error("RECONCILE_VOUCHER_APPLY_DEPENDENCY_INVALID");
    }

    for (const value of [
        attemptId,
        executionId,
        operationToken,
        fetchOperationId
    ]) {
        if (typeof value !== "string" || !UUID.test(value)) {
            throw new Error("RECONCILE_VOUCHER_APPLY_ID_INVALID");
        }
    }

    if (
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal < 1
    ) {
        throw new Error("RECONCILE_VOUCHER_APPLY_ORDINAL_INVALID");
    }

    if (
        !Array.isArray(sqlEntries) ||
        sqlEntries.length === 0 ||
        sqlEntries.length > 10000
    ) {
        throw new Error("RECONCILE_VOUCHER_APPLY_ROWS_INVALID");
    }

    const seenIds = new Set();
    const seenPairs = new Set();

    const expectedRows = sqlEntries.map(row => {
        if (
            !row ||
            typeof row !== "object" ||
            typeof row.id !== "string" ||
            !/^[1-9]\d*$/.test(row.id) ||
            BigInt(row.id) > 9223372036854775807n ||
            typeof row.voucher_guid !== "string" ||
            !row.voucher_guid ||
            typeof row.ledger_guid !== "string" ||
            !row.ledger_guid ||
            typeof row.amount !== "string" ||
            typeof row.debit !== "string" ||
            typeof row.credit !== "string" ||
            row.is_deleted !== false ||
            !Array.isArray(row.bill_allocations) ||
            !Array.isArray(row.costcentre_allocations) ||
            row.bill_allocations.length !== 0 ||
            row.costcentre_allocations.length !== 0
        ) {
            throw new Error("RECONCILE_VOUCHER_APPLY_ROW_UNSAFE");
        }

        const pair = JSON.stringify([
            row.voucher_guid,
            row.ledger_guid
        ]);

        if (seenIds.has(row.id) || seenPairs.has(pair)) {
            throw new Error("RECONCILE_VOUCHER_APPLY_DUPLICATE_ROW");
        }

        seenIds.add(row.id);
        seenPairs.add(pair);

        return {
            id: row.id,
            voucherGuid: row.voucher_guid,
            ledgerGuid: row.ledger_guid,
            amount: row.amount,
            debit: row.debit,
            credit: row.credit
        };
    });

    if (signal?.aborted) {
        throw new Error("RECONCILE_VOUCHER_APPLY_ABORTED");
    }

    const operationId = crypto.randomUUID();

    const context = {
        attemptId,
        executionId,
        operationId,
        operationOrdinal,
        fetchOperationId
    };

    let response;

    try {
        response = await supabase.rpc(
            "apply_manual_reconcile_voucher_amounts_v1",
            {
                p_operation_id: operationId,
                p_attempt_id: attemptId,
                p_operation_token: operationToken,
                p_execution_id: executionId,
                p_operation_ordinal: operationOrdinal,
                p_source_fetch_operation_id: fetchOperationId,
                p_expected_rows: expectedRows
            }
        );
    } catch (cause) {
        throw uncertainError(
            "RECONCILE_VOUCHER_APPLY_OUTCOME_UNKNOWN",
            context,
            cause
        );
    }

    if (response?.error) {
        throw uncertainError(
            "RECONCILE_VOUCHER_APPLY_OUTCOME_UNKNOWN",
            context,
            response.error
        );
    }

    const result =
        Array.isArray(response?.data) &&
        response.data.length === 1
            ? response.data[0]
            : null;

    if (
        !result ||
        result.applied !== true ||
        result.reason !== "RESULT_VERIFIED" ||
        result.operation_state !== "RESULT_VERIFIED" ||
        !Number.isSafeInteger(result.updated_rows) ||
        result.updated_rows < 0 ||
        result.updated_rows > expectedRows.length
    ) {
        throw uncertainError(
            "RECONCILE_VOUCHER_APPLY_NOT_VERIFIED",
            context
        );
    }

    if (signal?.aborted) {
        throw uncertainError(
            "RECONCILE_VOUCHER_APPLY_ABORTED_AFTER_SETTLEMENT",
            context
        );
    }

    return {
        operationId,
        operationOrdinal,
        fetchOperationId,
        updatedRows: result.updated_rows,
        matchedRows: expectedRows.length,
        verified: true
    };
}

module.exports = {
    applyVoucherAmountsDurable
};
