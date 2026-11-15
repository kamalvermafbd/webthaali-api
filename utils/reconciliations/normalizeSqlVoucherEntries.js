
"use strict";

const MAX_PAISE = BigInt(Number.MAX_SAFE_INTEGER);
const MAX_SQL_ID = 9223372036854775807n;

function validateSqlAmount(value, signed = false) {
    if (typeof value !== "string") {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    if (value !== value.trim()) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    const pattern = signed
        ? /^-?(?:0|[1-9]\d*)\.\d{2}$/
        : /^(?:0|[1-9]\d*)\.\d{2}$/;

    if (!pattern.test(value)) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_INVALID");
    }

    const negative = value.startsWith("-");
    const unsigned = negative ? value.slice(1) : value;
    const [whole, fraction] = unsigned.split(".");

    const paise =
        BigInt(whole) * 100n + BigInt(fraction);

    if (paise > MAX_PAISE) {
        throw new Error("RECONCILE_SQL_ENTRY_AMOUNT_OUT_OF_RANGE");
    }

    // Preserve signed zero and original signed amount.
    return value;
}

function validateSqlId(value) {
    if (typeof value !== "string") {
        throw new Error("RECONCILE_SQL_ENTRY_ID_INVALID");
    }

    if (!/^[1-9]\d*$/.test(value)) {
        throw new Error("RECONCILE_SQL_ENTRY_ID_INVALID");
    }

    const id = BigInt(value);

    if (id > MAX_SQL_ID) {
        throw new Error("RECONCILE_SQL_ENTRY_ID_INVALID");
    }

    // Never convert PostgreSQL bigint IDs to JS Number.
    return value;
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
        if (
            !row ||
            typeof row !== "object" ||
            Array.isArray(row)
        ) {
            throw new Error("RECONCILE_SQL_ROW_INVALID");
        }

        const id = validateSqlId(row.id);

        const voucherGuid = String(
            row.voucher_guid || ""
        ).trim();

        const ledgerGuid = String(
            row.ledger_guid || ""
        ).trim();

        if (!voucherGuid || !ledgerGuid) {
            throw new Error(
                "RECONCILE_SQL_ENTRY_IDENTITY_MISSING"
            );
        }

        if (
            row.amount === null ||
            row.amount === undefined ||
            row.debit === null ||
            row.debit === undefined ||
            row.credit === null ||
            row.credit === undefined
        ) {
            throw new Error(
                "RECONCILE_SQL_ENTRY_AMOUNT_MISSING"
            );
        }

        const amount = validateSqlAmount(row.amount, true);
        const debit = validateSqlAmount(row.debit);
        const credit = validateSqlAmount(row.credit);

        const amountPaise = BigInt(
            amount.replace(/^-/, "").replace(".", "")
        );
        const debitPaise = BigInt(debit.replace(".", ""));
        const creditPaise = BigInt(credit.replace(".", ""));

        if (
            (debitPaise > 0n && creditPaise > 0n) ||
            (
                amountPaise !== debitPaise &&
                amountPaise !== creditPaise
            )
        ) {
            throw new Error(
                "RECONCILE_SQL_ENTRY_AMOUNT_INCONSISTENT"
            );
        }

        return {
            id,
            companyCode,
            tallyOwner,
            voucherGuid,
            ledgerGuid,
            amount,
            debit,
            credit
        };
    });
}

module.exports = {
    normalizeSqlVoucherEntries
};
