"use strict";

const {
    sendReconcileRequest
} = require("../../socketio/sendReconcileRequest");

function normalizeDecimal(value) {
    if (
        typeof value !== "string" ||
        !/^\d+(?:\.\d+)?$/.test(value)
    ) {
        throw new Error("RECONCILE_DECIMAL_INVALID");
    }

    const [whole, fraction] = value.split(".");

    const normalizedWhole =
        whole.replace(/^0+(?=\d)/, "");

    return normalizedWhole +
        (fraction === undefined ? "" : "." + fraction);
}

async function fetchLedgerOpening({
    socket,
    signal,
    company,
    ledgerGuid,
    booksBeginningFrom
}) {
    if (!socket || typeof socket.emit !== "function") {
        throw new Error("RECONCILE_SOCKET_INVALID");
    }

    if (signal?.aborted) {
        throw new Error("RECONCILE_SESSION_ABORTED");
    }

    if (
        typeof company !== "string" ||
        !company.trim() ||
        typeof ledgerGuid !== "string" ||
        !ledgerGuid.trim() ||
        typeof booksBeginningFrom !== "string" ||
        !/^\d{8}$/.test(booksBeginningFrom)
    ) {
        throw new Error("RECONCILE_LEDGER_REQUEST_INVALID");
    }

    const result = await sendReconcileRequest(
        socket,
        "reconcileLedgerOpening",
        {
            company: company.trim(),
            ledgerGuid: ledgerGuid.trim(),
            booksBeginningFrom
        },
        { signal }
    );

    if (
        !result ||
        typeof result !== "object" ||
        Array.isArray(result)
    ) {
        throw new Error("RECONCILE_LEDGER_RESULT_INVALID");
    }

    if (
        typeof result.guid !== "string" ||
        result.guid.trim() !== ledgerGuid.trim()
    ) {
        throw new Error("RECONCILE_LEDGER_GUID_MISMATCH");
    }

    const openingBalance =
        normalizeDecimal(result.openingBalance);

    if (
        !["DR", "CR", ""].includes(
            result.openingBalanceType
        )
    ) {
        throw new Error("RECONCILE_LEDGER_TYPE_INVALID");
    }

    const isZero =
        /^0+$/.test(openingBalance.replace(".", ""));

    if (
        (isZero && result.openingBalanceType !== "") ||
        (!isZero && result.openingBalanceType === "")
    ) {
        throw new Error("RECONCILE_LEDGER_TYPE_MISMATCH");
    }

    if (signal?.aborted) {
        throw new Error("RECONCILE_SESSION_ABORTED");
    }

    return {
        guid: result.guid.trim(),
        openingBalance,
        openingBalanceType: result.openingBalanceType
    };
}

module.exports = {
    fetchLedgerOpening
};