
"use strict";

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function requireUUID(value, name) {
    if (
        typeof value !== "string" ||
        !UUID.test(value)
    ) {
        throw new Error(
            `DURABLE_RECONCILE_${name}_INVALID`
        );
    }
}

function requireText(value, name) {
    if (
        typeof value !== "string" ||
        !value.trim()
    ) {
        throw new Error(
            `DURABLE_RECONCILE_${name}_INVALID`
        );
    }

    return value;
}

function recoveryError(code, context, cause) {
    const error = new Error(
        code,
        cause ? { cause } : undefined
    );

    error.reconcileRecoveryRequired = true;
    error.reconcileOutcomeUncertain = true;

   error.attemptId = context.attemptId;
error.executionId = context.executionId;
error.operationId = context.operationId;
error.requestId = context.requestId;
error.operationOrdinal = context.operationOrdinal;

    return error;
}

/**
 * Prepare one durable ledger-opening fetch.
 *
 * The caller must generate and retain operationId,
 * requestId, attemptId and operationOrdinal.
 *
 * This function never generates replacement IDs,
 * never retries an uncertain RPC, and never emits
 * a socket request.
 */
async function prepareDurableReconcileFetch({
    supabase,
    operationId,
    attemptId,
    operationToken,
    executionId,
    operationOrdinal,
    requestId,
    ledgerGuid,
    connectorId,
    tallyCompanyGuid,
    inputPayload,
    inputFingerprint
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error(
            "DURABLE_RECONCILE_SUPABASE_INVALID"
        );
    }

 requireUUID(operationId, "OPERATION_ID");
requireUUID(attemptId, "ATTEMPT_ID");
requireUUID(operationToken, "OPERATION_TOKEN");
requireUUID(executionId, "EXECUTION_ID");
requireUUID(requestId, "REQUEST_ID");

    if (
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal <= 0 ||
        operationOrdinal > 2147483647
    ) {
        throw new Error(
            "DURABLE_RECONCILE_OPERATION_ORDINAL_INVALID"
        );
    }

    requireText(ledgerGuid, "LEDGER_GUID");
    requireText(connectorId, "CONNECTOR_ID");
    requireText(tallyCompanyGuid, "TALLY_COMPANY_GUID");
    requireText(inputFingerprint, "INPUT_FINGERPRINT");

    if (
        !inputPayload ||
        typeof inputPayload !== "object" ||
        Array.isArray(inputPayload)
    ) {
        throw new Error(
            "DURABLE_RECONCILE_INPUT_PAYLOAD_INVALID"
        );
    }

    const context = {
    attemptId,
    executionId,
    operationId,
    requestId,
    operationOrdinal
};

    let response;

    try {
     response = await supabase.rpc(
    "prepare_manual_reconcile_fetch_v2",
    {
        p_operation_id: operationId,
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_operation_ordinal: operationOrdinal,
        p_request_id: requestId,
        p_ledger_guid: ledgerGuid,
        p_connector_id: connectorId,
        p_tally_company_guid: tallyCompanyGuid,
        p_input_payload: inputPayload,
        p_input_fingerprint: inputFingerprint,
        p_execution_id: executionId
    }
);
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_PREPARE_RPC_UNVERIFIED",
            context,
            cause
        );
    }

    if (response?.error) {
        throw recoveryError(
            "DURABLE_RECONCILE_PREPARE_RPC_ERROR",
            context,
            response.error
        );
    }

    const data = response?.data;

    const row =
        Array.isArray(data) && data.length === 1
            ? data[0]
            : null;

    if (
        row?.prepared === true &&
        row.reason === "PREPARED" &&
        row.operation_state === "PREPARED"
    ) {
        return {
            prepared: true,
            freshPreparation: true,
            operationId,
            attemptId,
            requestId,
            operationOrdinal
        };
    }

    // An existing prepared operation is not authority
    // for a second executor to dispatch the request.
    if (
        row?.prepared === true &&
        row.reason === "ALREADY_PREPARED"
    ) {
        throw recoveryError(
            "DURABLE_RECONCILE_PREPARE_ALREADY_EXISTS",
            context
        );
    }

    throw recoveryError(
        "DURABLE_RECONCILE_PREPARE_NOT_AUTHORIZED",
        context
    );
}

module.exports = {
    prepareDurableReconcileFetch
};
