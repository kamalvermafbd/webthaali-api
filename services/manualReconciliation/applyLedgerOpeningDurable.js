
"use strict";

const crypto = require("node:crypto");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const DECIMAL = /^\d+(?:\.\d+)?$/;

function requireUUID(value, name) {
    if (typeof value !== "string" || !UUID.test(value)) {
        throw new Error(
            `DURABLE_RECONCILE_${name}_INVALID`
        );
    }
}

function recoveryError(code, context, cause) {
    const error = new Error(
        code,
        cause ? { cause } : undefined
    );

    error.reconcileRecoveryRequired = true;
    error.reconcileOutcomeUncertain = true;
   error.attemptId = context.attemptId;
error.operationId = context.operationId;
error.executionId = context.executionId;
error.fetchOperationId = context.fetchOperationId;
error.operationOrdinal = context.operationOrdinal;

    return error;
}

function singleRow(response, context, code) {
    if (response?.error) {
        throw recoveryError(
            code,
            context,
            response.error
        );
    }

    const data = response?.data;

    if (!Array.isArray(data) || data.length !== 1) {
        throw recoveryError(
            `${code}_INVALID_RESPONSE`,
            context
        );
    }

    return data[0];
}

function validateApplyResult(row, context) {
    if (
        !row ||
        typeof row !== "object" ||
        !["UPDATED", "UNCHANGED", "NOT_FOUND"].includes(
            row.result
        )
    ) {
        throw recoveryError(
            "DURABLE_RECONCILE_APPLY_RESULT_INVALID",
            context
        );
    }

    for (const key of [
        "previous_balance",
        "current_balance"
    ]) {
        const value = row[key];

        if (
            value !== null &&
            (
                typeof value !== "string" ||
                !DECIMAL.test(value)
            )
        ) {
            throw recoveryError(
                "DURABLE_RECONCILE_APPLY_DECIMAL_INVALID",
                context
            );
        }
    }

    for (const key of [
        "previous_type",
        "current_type"
    ]) {
        if (
            row[key] !== null &&
            !["DR", "CR", ""].includes(row[key])
        ) {
            throw recoveryError(
                "DURABLE_RECONCILE_APPLY_TYPE_INVALID",
                context
            );
        }
    }

    if (
        row.result !== "NOT_FOUND" &&
        (
            row.current_balance === null ||
            row.current_type === null
        )
    ) {
        throw recoveryError(
            "DURABLE_RECONCILE_APPLY_CURRENT_VALUE_MISSING",
            context
        );
    }

    return {
        result: row.result,
        previousBalance: row.previous_balance,
        previousType: row.previous_type,
        currentBalance: row.current_balance,
        currentType: row.current_type
    };
}

/**
 * Apply one durably verified ledger opening balance.
 *
 * A new apply operation is linked to exactly one
 * verified TALLY_FETCH operation.
 *
 * The actual ledger update and result journal write
 * occur inside the same PostgreSQL RPC transaction.
 *
 * No automatic retry on an uncertain outcome.
 */
async function applyLedgerOpeningDurable({
    supabase,
    attemptId,
    operationToken,
    executionId,
    operationOrdinal,
    fetchOperationId
}) {
    if (!supabase || typeof supabase.rpc !== "function") {
        throw new Error(
            "DURABLE_RECONCILE_SUPABASE_INVALID"
        );
    }

   requireUUID(attemptId, "ATTEMPT_ID");
requireUUID(operationToken, "OPERATION_TOKEN");
requireUUID(executionId, "EXECUTION_ID");
requireUUID(fetchOperationId, "FETCH_OPERATION_ID");

    if (
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal <= 0 ||
        operationOrdinal > 2147483647
    ) {
        throw new Error(
            "DURABLE_RECONCILE_OPERATION_ORDINAL_INVALID"
        );
    }

    const operationId = crypto.randomUUID();

   const context = {
    attemptId,
    operationId,
    executionId,
    fetchOperationId,
    operationOrdinal
};

    let prepareResponse;

    try {
        prepareResponse = await supabase.rpc(
    "prepare_manual_reconcile_apply_v2",
    {
        p_operation_id: operationId,
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_execution_id: executionId,
        p_operation_ordinal: operationOrdinal,
        p_source_fetch_operation_id:
            fetchOperationId
    }
);
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_APPLY_PREPARE_UNVERIFIED",
            context,
            cause
        );
    }

    const prepared = singleRow(
        prepareResponse,
        context,
        "DURABLE_RECONCILE_APPLY_PREPARE_FAILED"
    );

    if (
        prepared?.prepared !== true ||
        prepared.reason !== "PREPARED" ||
        prepared.operation_state !== "PREPARED"
    ) {
        throw recoveryError(
            "DURABLE_RECONCILE_APPLY_PREPARE_NOT_AUTHORIZED",
            context
        );
    }

    // Preparation is committed before the mutation RPC.
    // The mutation and RESULT_VERIFIED journal write
    // are atomic within PostgreSQL.
    let applyResponse;

    try {
       applyResponse = await supabase.rpc(
    "apply_manual_reconcile_ledger_opening_v3",
    {
        p_operation_id: operationId,
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_execution_id: executionId
    }
);
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_APPLY_OUTCOME_UNVERIFIED",
            context,
            cause
        );
    }

    const row = singleRow(
        applyResponse,
        context,
        "DURABLE_RECONCILE_APPLY_RPC_FAILED"
    );

    const result = validateApplyResult(row, context);

    if (result.result === "NOT_FOUND") {
        throw recoveryError(
            "DURABLE_RECONCILE_LEDGER_NOT_FOUND",
            context
        );
    }

    return {
        ...result,
        applyOperationId: operationId,
        fetchOperationId,
        applyOperationOrdinal: operationOrdinal
    };
}

module.exports = {
    applyLedgerOpeningDurable
};
