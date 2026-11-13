
"use strict";

const crypto = require("node:crypto");

const {
    prepareDurableReconcileFetch
} = require("./manualReconcileDurablePrepare");

const {
    markDurableReconcileFetchDispatch
} = require("./manualReconcileDurableDispatch");

const {
    sendDurableReconcileRequest
} = require("../../socketio/sendDurableReconcileRequest");

function requireText(value, name) {
    if (typeof value !== "string" || !value.trim()) {
        throw new Error(`DURABLE_RECONCILE_$DIL0_INVALID`);
    }

    return value.trim();
}

function normalizeDecimal(value) {
    if (
        typeof value !== "string" ||
        !/^\d+(?:\.\d+)?$/.test(value)
    ) {
        throw new Error("DURABLE_RECONCILE_DECIMAL_INVALID");
    }

    const [whole, fraction] = value.split(".");

    return whole.replace(/^0+(?=\d)/, "") +
        (fraction === undefined ? "" : "." + fraction);
}

function validateLedgerResult(result, ledgerGuid) {
    if (
        !result ||
        typeof result !== "object" ||
        Array.isArray(result)
    ) {
        throw new Error("DURABLE_RECONCILE_RESULT_INVALID");
    }

    if (
        typeof result.guid !== "string" ||
        result.guid.trim() !== ledgerGuid
    ) {
        throw new Error("DURABLE_RECONCILE_GUID_MISMATCH");
    }

    const openingBalance =
        normalizeDecimal(result.openingBalance);

    const openingBalanceType = result.openingBalanceType;

    if (!["DR", "CR", ""].includes(openingBalanceType)) {
        throw new Error("DURABLE_RECONCILE_TYPE_INVALID");
    }

    const isZero =
        /^0+$/.test(openingBalance.replace(".", ""));

    if (
        (isZero && openingBalanceType !== "") ||
        (!isZero && openingBalanceType === "")
    ) {
        throw new Error("DURABLE_RECONCILE_TYPE_MISMATCH");
    }

    return {
        guid: ledgerGuid,
        openingBalance,
        openingBalanceType
    };
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
error.requestId = context.requestId;
error.operationOrdinal = context.operationOrdinal;

    return error;
}

/**
 * Durable Tally ledger opening fetch.
 *
 * Execution sequence:
 * 1. Prepare journal operation.
 * 2. Commit dispatch marker.
 * 3. Emit socket request once.
 * 4. Validate connector result.
 * 5. Verify result durably in database.
 *
 * No automatic retry at any stage.
 */
async function fetchLedgerOpeningDurable({
    supabase,
    socket,
    signal,
    attemptId,
    operationToken,
    executionId,
    operationOrdinal,
    connectorId,
    tallyCompanyGuid,
    company,
    ledgerGuid,
    booksBeginningFrom
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error("DURABLE_RECONCILE_SUPABASE_INVALID");
    }

   if (!socket || typeof socket.emit !== "function") {
    throw new Error("DURABLE_RECONCILE_SOCKET_INVALID");
}

if (
    typeof executionId !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(executionId)
) {
    throw new Error("DURABLE_RECONCILE_EXECUTION_ID_INVALID");
}

company = requireText(company, "COMPANY");
    ledgerGuid = requireText(ledgerGuid, "LEDGER_GUID");
    connectorId = requireText(connectorId, "CONNECTOR_ID");
    tallyCompanyGuid = requireText(
        tallyCompanyGuid,
        "TALLY_COMPANY_GUID"
    );

    if (
        typeof booksBeginningFrom !== "string" ||
        !/^\d{8}$/.test(booksBeginningFrom)
    ) {
        throw new Error("DURABLE_RECONCILE_BOOKS_DATE_INVALID");
    }

    if (signal?.aborted) {
        throw new Error("DURABLE_RECONCILE_ABORTED");
    }

    const operationId = crypto.randomUUID();
    const requestId = crypto.randomUUID();

    const context = {
    attemptId,
    operationId,
    executionId,
    requestId,
    operationOrdinal
};

    const payload = {
        company,
        ledgerGuid,
        booksBeginningFrom
    };

    const inputFingerprint = crypto
        .createHash("sha256")
        .update(JSON.stringify(payload))
        .digest("hex");

    // 1. Prepare durable journal operation.
   await prepareDurableReconcileFetch({
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
    inputPayload: payload,
    inputFingerprint
});

    // 2. Dispatch must commit before socket.emit.
    // 3. Sender emits only after fresh authorization.
    let rawResult;

    try {
        rawResult = await sendDurableReconcileRequest(
            socket,
            "reconcileLedgerOpening",
            payload,
            {
                requestId,
                signal,
               dispatch: () =>
    markDurableReconcileFetchDispatch({
        supabase,
        operationId,
        attemptId,
        operationToken,
        executionId,
        requestId
    })
            }
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_FETCH_OUTCOME_UNVERIFIED",
            context,
            cause
        );
    }

    // The connector response is not yet durable.
    let ledger;

    try {
        ledger = validateLedgerResult(
            rawResult,
            ledgerGuid
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_FETCH_RESULT_INVALID",
            context,
            cause
        );
    }

    if (signal?.aborted) {
        throw recoveryError(
            "DURABLE_RECONCILE_ABORTED_BEFORE_VERIFICATION",
            context
        );
    }

    // 4. Persist and verify exact decimal-string result.
    let response;

    try {
        response = await supabase.rpc(
    "verify_manual_reconcile_fetch_result_v2",
    {
        p_operation_id: operationId,
        p_attempt_id: attemptId,
        p_operation_token: operationToken,
        p_execution_id: executionId,
        p_request_id: requestId,
        p_result_payload: ledger
    }
);
    } catch (cause) {
        throw recoveryError(
            "DURABLE_RECONCILE_VERIFY_RPC_UNVERIFIED",
            context,
            cause
        );
    }

    if (response?.error) {
        throw recoveryError(
            "DURABLE_RECONCILE_VERIFY_RPC_ERROR",
            context,
            response.error
        );
    }

    const rows = response?.data;

    const row =
        Array.isArray(rows) && rows.length === 1
            ? rows[0]
            : null;

  if (
    row?.verified !== true ||
    row.reason !== "RESULT_VERIFIED" ||
    row.operation_state !== "RESULT_VERIFIED"
) {
        throw recoveryError(
            "DURABLE_RECONCILE_FETCH_VERIFICATION_NOT_CONFIRMED",
            context
        );
    }

    if (signal?.aborted) {
        throw recoveryError(
            "DURABLE_RECONCILE_ABORTED_AFTER_VERIFICATION",
            context
        );
    }

    return {
        ...ledger,
        fetchOperationId: operationId,
        fetchRequestId: requestId,
        fetchOperationOrdinal: operationOrdinal
    };
}

module.exports = {
    fetchLedgerOpeningDurable
};
