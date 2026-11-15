
"use strict";

const crypto = require("node:crypto");
const { sendDurableReconcileRequest } = require("../../socketio/sendDurableReconcileRequest");

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const DATE = /^\d{8}$/;
const GUID = /^[A-Za-z0-9][A-Za-z0-9-]{0,127}$/;
const MONEY = /^(?:0|[1-9][0-9]*)\.[0-9]{2}$/;
const MAX_PAISE = 9007199254740991n;
const SIGNED_MONEY = /^-?(?:0|[1-9][0-9]*)\.[0-9]{2}$/;
function requireString(value, label) {
    if (typeof value !== "string" || !value.trim()) {
        throw new Error(`DURABLE_VOUCHER_${label}_INVALID`);
    }
    return value.trim();
}

function requireUUID(value, label) {
    if (typeof value !== "string" || !UUID.test(value)) {
        throw new Error(`DURABLE_VOUCHER_${label}_INVALID`);
    }
}

function recoveryError(code, context, cause) {
    const error = new Error(code, cause ? { cause } : undefined);
    error.reconcileRecoveryRequired = true;
    error.reconcileOutcomeUncertain = true;
    Object.assign(error, context);
    return error;
}

function oneRow(response, field, reason, state) {
    const row = Array.isArray(response?.data) && response.data.length === 1
        ? response.data[0] : null;

    return response?.error == null &&
        row?.[field] === true &&
        row.reason === reason &&
        row.operation_state === state;
}


function validateEntries(raw, companyCode, tallyOwner, requested) {
    if (
        !raw ||
        typeof raw !== "object" ||
        Array.isArray(raw) ||
        !Array.isArray(raw.entries) ||
        !raw.entries.length
    ) {
        throw new Error("DURABLE_VOUCHER_RESULT_ENTRIES_INVALID");
    }

   const seen = new Set();
const seenPairs = new Set();
const requestedCaseIndex = new Map();
const ledgerCaseIndex = new Map();

    for (const voucherGuid of requested) {
        requestedCaseIndex.set(voucherGuid.toLowerCase(), voucherGuid);
    }

    const entries = Array.from(raw.entries).map(entry => {
        if (
            !entry ||
            typeof entry !== "object" ||
            Array.isArray(entry) ||
            entry.companyCode !== companyCode ||
            entry.tallyOwner !== tallyOwner ||
            typeof entry.voucherGuid !== "string" ||
            entry.voucherGuid !== entry.voucherGuid.trim() ||
            !GUID.test(entry.voucherGuid) ||
            typeof entry.ledgerGuid !== "string" ||
            entry.ledgerGuid !== entry.ledgerGuid.trim() ||
            !GUID.test(entry.ledgerGuid)
        ) {
            throw new Error("DURABLE_VOUCHER_ENTRY_SCOPE_INVALID");
        }

        const foldedVoucherGuid = entry.voucherGuid.toLowerCase();

        if (
            requestedCaseIndex.has(foldedVoucherGuid) &&
            requestedCaseIndex.get(foldedVoucherGuid) !== entry.voucherGuid
        ) {
            throw new Error("DURABLE_VOUCHER_RESPONSE_GUID_CASE_CONFLICT");
        }

        if (!requested.has(entry.voucherGuid)) {
            throw new Error("DURABLE_VOUCHER_ENTRY_SCOPE_INVALID");
        }

        const foldedLedgerGuid = entry.ledgerGuid.toLowerCase();

        if (
            ledgerCaseIndex.has(foldedLedgerGuid) &&
            ledgerCaseIndex.get(foldedLedgerGuid) !== entry.ledgerGuid
        ) {
            throw new Error("DURABLE_VOUCHER_LEDGER_GUID_CASE_CONFLICT");
        }

        ledgerCaseIndex.set(foldedLedgerGuid, entry.ledgerGuid);

     const amount = entry.amount;
const debit = entry.debit;
const credit = entry.credit;

if (
    typeof amount !== "string" ||
    amount !== amount.trim() ||
    !SIGNED_MONEY.test(amount) ||
    typeof debit !== "string" ||
    typeof credit !== "string" ||
    debit !== debit.trim() ||
    credit !== credit.trim() ||
    !MONEY.test(debit) ||
    !MONEY.test(credit)
) {
    throw new Error("DURABLE_VOUCHER_ENTRY_AMOUNT_INVALID");
}

const amountPaise = BigInt(
    amount.replace(/^-/, "").replace(".", "")
);
const debitPaise = BigInt(debit.replace(".", ""));
const creditPaise = BigInt(credit.replace(".", ""));

if (
    amountPaise > MAX_PAISE ||
    debitPaise > MAX_PAISE ||
    creditPaise > MAX_PAISE ||
    (debitPaise > 0n && creditPaise > 0n) ||
   (
    amountPaise !== (debitPaise + creditPaise)
)
) {
    throw new Error("DURABLE_VOUCHER_ENTRY_AMOUNT_INVALID");
}
const pairKey = JSON.stringify([
    entry.voucherGuid,
    entry.ledgerGuid
]);

if (seenPairs.has(pairKey)) {
    throw new Error("DURABLE_VOUCHER_DUPLICATE_LEDGER_PAIR");
}

seenPairs.add(pairKey);
seen.add(entry.voucherGuid);

return {
    companyCode,
    tallyOwner,
    voucherGuid: entry.voucherGuid,
    ledgerGuid: entry.ledgerGuid,
    amount,
    debit,
    credit
};
    });

    if (seen.size !== requested.size) {
        throw new Error("DURABLE_VOUCHER_COVERAGE_INCOMPLETE");
    }

    return entries;
}


/**
 * One-shot, execution-fenced voucher fetch.
 * No automatic retry or DB voucher mutation.
 */
async function fetchVouchersDurable({
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
    companyCode,
    tallyOwner,
    voucherGuids,
    booksBeginningFrom,
    fromDate,
    toDate
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        !socket ||
        typeof socket.emit !== "function"
    ) {
        throw new Error("DURABLE_VOUCHER_DEPENDENCY_INVALID");
    }

    for (const [value, name] of [
        [attemptId, "ATTEMPT_ID"],
        [operationToken, "OPERATION_TOKEN"],
        [executionId, "EXECUTION_ID"]
    ]) {
        requireUUID(value, name);
    }

    if (
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal <= 0
    ) {
        throw new Error("DURABLE_VOUCHER_ORDINAL_INVALID");
    }

    company = requireString(company, "COMPANY");
    companyCode = requireString(companyCode, "COMPANY_CODE");
    connectorId = requireString(connectorId, "CONNECTOR_ID");
    tallyCompanyGuid = requireString(
        tallyCompanyGuid,
        "TALLY_COMPANY_GUID"
    );

    if (!["USER", "CA"].includes(tallyOwner)) {
        throw new Error("DURABLE_VOUCHER_TALLY_OWNER_INVALID");
    }

    if (
        ![booksBeginningFrom, fromDate, toDate].every(
            x => typeof x === "string" && DATE.test(x)
        ) ||
        fromDate > toDate ||
        fromDate < booksBeginningFrom
    ) {
        throw new Error("DURABLE_VOUCHER_DATE_INVALID");
    }

    if (
        !Array.isArray(voucherGuids) ||
        !voucherGuids.length ||
        Array.from(voucherGuids).some(
            g => typeof g !== "string" ||
                !g.trim() ||
                g !== g.trim() ||
                !/^[A-Za-z0-9][A-Za-z0-9-]{0,127}$/.test(g)
        ) ||
        new Set(voucherGuids).size !== voucherGuids.length
    ) {
        throw new Error("DURABLE_VOUCHER_GUIDS_INVALID");
    }

    const foldedVoucherGuids = new Set();

    for (const guid of voucherGuids) {
        const folded = guid.toLowerCase();

        if (foldedVoucherGuids.has(folded)) {
            throw new Error("DURABLE_VOUCHER_GUID_CASE_COLLISION");
        }

        foldedVoucherGuids.add(folded);
    }

    if (signal?.aborted) {
        throw new Error("DURABLE_VOUCHER_ABORTED");
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
        expectedCompanyGuid: tallyCompanyGuid,
        companyCode,
        tallyOwner,
        voucherGuids: [...voucherGuids],
        booksBeginningFrom,
        fromDate,
        toDate
    };

    const inputFingerprint = crypto
        .createHash("sha256")
        .update(JSON.stringify(payload))
        .digest("hex");

    let response;

    try {
        response = await supabase.rpc(
            "prepare_manual_reconcile_voucher_fetch_v1",
            {
                p_operation_id: operationId,
                p_attempt_id: attemptId,
                p_operation_token: operationToken,
                p_operation_ordinal: operationOrdinal,
                p_request_id: requestId,
                p_connector_id: connectorId,
                p_tally_company_guid: tallyCompanyGuid,
                p_input_payload: payload,
                p_input_fingerprint: inputFingerprint,
                p_execution_id: executionId
            }
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_VOUCHER_PREPARE_UNVERIFIED",
            context,
            cause
        );
    }

    if (
        !oneRow(
            response,
            "prepared",
            "PREPARED",
            "PREPARED"
        )
    ) {
        throw recoveryError(
            "DURABLE_VOUCHER_PREPARE_NOT_CONFIRMED",
            context,
            response?.error
        );
    }

    let raw;

    try {
        raw = await sendDurableReconcileRequest(
            socket,
            "reconcileVouchers",
            payload,
            {
                requestId,
                signal,

                dispatch: async () => {
                    const marked = await supabase.rpc(
                        "mark_manual_reconcile_voucher_dispatch_v1",
                        {
                            p_operation_id: operationId,
                            p_attempt_id: attemptId,
                            p_operation_token: operationToken,
                            p_request_id: requestId,
                            p_execution_id: executionId
                        }
                    );

                    if (
                        !oneRow(
                            marked,
                            "marked",
                            "DISPATCH_MARKED",
                            "DISPATCH_MAY_HAVE_OCCURRED"
                        )
                    ) {
                        throw new Error(
                            "DURABLE_VOUCHER_DISPATCH_NOT_AUTHORIZED"
                        );
                    }

                    return {
                        dispatchAuthorized: true,
                        freshTransition: true,
                        requestId
                    };
                }
            }
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_VOUCHER_FETCH_OUTCOME_UNVERIFIED",
            context,
            cause
        );
    }

    let entries;

    try {
        entries = validateEntries(
            raw,
            companyCode,
            tallyOwner,
            new Set(voucherGuids)
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_VOUCHER_RESULT_INVALID",
            context,
            cause
        );
    }

    if (signal?.aborted) {
        throw recoveryError(
            "DURABLE_VOUCHER_ABORTED_BEFORE_VERIFY",
            context
        );
    }

    try {
        response = await supabase.rpc(
            "verify_manual_reconcile_voucher_result_v1",
            {
                p_operation_id: operationId,
                p_attempt_id: attemptId,
                p_operation_token: operationToken,
                p_request_id: requestId,
                p_result_payload: { entries },
                p_execution_id: executionId
            }
        );
    } catch (cause) {
        throw recoveryError(
            "DURABLE_VOUCHER_VERIFY_UNVERIFIED",
            context,
            cause
        );
    }

    if (
        !oneRow(
            response,
            "verified",
            "RESULT_VERIFIED",
            "RESULT_VERIFIED"
        )
    ) {
        throw recoveryError(
            "DURABLE_VOUCHER_VERIFY_NOT_CONFIRMED",
            context,
            response?.error
        );
    }

    if (signal?.aborted) {
        throw recoveryError(
            "DURABLE_VOUCHER_ABORTED_AFTER_VERIFY",
            context
        );
    }

    return {
        entries,
        fetchOperationId: operationId,
        fetchRequestId: requestId,
        fetchOperationOrdinal: operationOrdinal
    };
}

module.exports = { fetchVouchersDurable };
