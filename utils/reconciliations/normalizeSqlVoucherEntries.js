"use strict";

function validateSqlAmount(value) {
    if (
        value === null ||
        value === undefined ||
        (typeof value !== "string" &&
            typeof value !== "number")
    ) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    if (
        typeof value === "number" &&
        (!Number.isFinite(value) ||
            value >= 2 ** 46 ||
            Object.is(value, -0))
    ) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    const text = String(value).trim();

    const match =
        /^\+?(?:(\d+)(?:\.(\d{1,2}))?|\.(\d{1,2}))$/.exec(text);

    if (!match) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    const whole = match[1] || "0";
    const fraction = (match[2] || match[3] || "").padEnd(2, "0");

    const paise =
        BigInt(whole) * 100n + BigInt(fraction);

    if (paise > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_OUT_OF_RANGE");
    }

    return text;
}
function normalizeSqlVoucherEntries(
    rows,
    { companyCode, tallyOwner } = {}
) {
    companyCode = String(companyCode || "").trim();
    tallyOwner = String(tallyOwner || "").trim();

    if (
        !companyCode ||
        !["USER", "CA"].includes(tallyOwner)
    ) {
        throw new Error("RECONCILE_SQL_SCOPE_INVALID");
    }

    if (!Array.isArray(rows)) {
        throw new TypeError("RECONCILE_SQL_ROWS_INVALID");
    }

    return rows.map(row => {

        const id = row?.id;

if (
    !Number.isSafeInteger(id) ||
    id <= 0
) {
    throw new Error(
        "RECONCILE_SQL_ENTRY_ID_INVALID"
    );
}
        const voucherGuid = String(
            row?.voucher_guid || ""
        ).trim();

        const ledgerGuid = String(
            row?.ledger_guid || ""
        ).trim();

        if (!voucherGuid || !ledgerGuid) {
            throw new Error(
                "RECONCILE_SQL_ENTRY_IDENTITY_MISSING"
            );
        }

        if (
            row.debit === null ||
            row.debit === undefined ||
            row.credit === null ||
            row.credit === undefined
        ) {
            throw new Error(
                "RECONCILE_SQL_ENTRY_AMOUNT_MISSING"
            );
        }

        const debit = validateSqlAmount(row.debit);
const credit = validateSqlAmount(row.credit);

        return {
    id,
    companyCode,
    tallyOwner,
    voucherGuid,
    ledgerGuid,
    debit,
    credit
};
    });
}

module.exports = {
    normalizeSqlVoucherEntries
};