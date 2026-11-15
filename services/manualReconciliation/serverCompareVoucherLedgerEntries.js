
"use strict";

// Pure, read-only voucher-entry comparator.
// No database access and no mutation.
//
// All monetary comparisons use exact BigInt decimal units.
// Duplicate voucher-ledger groups are ambiguous.
// Structural-review rows cannot become UPDATE candidates.
// UPDATE authorization must be implemented separately.

const DECIMAL = /^-?\d+(?:\.\d+)?$/;
const MAX_DECIMAL_PLACES = 6;
const MAX_SQL_ID = 9223372036854775807n;

function requiredText(value, field) {
    if (
        typeof value !== "string" ||
        !value.trim()
    ) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${field}`
        );
    }

    return value.trim();
}

function decimalToUnits(value, field) {
    if (
        typeof value !== "string" &&
        typeof value !== "number" &&
        typeof value !== "bigint"
    ) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${field}`
        );
    }

    if (
        typeof value === "number" &&
        !Number.isSafeInteger(value)
    ) {
        throw new Error(
            `VOUCHER_COMPARE_UNSAFE_${field}`
        );
    }

    const text = String(value).trim();

    if (!DECIMAL.test(text)) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${field}`
        );
    }

    const negative = text.startsWith("-");
    const unsigned = negative
        ? text.slice(1)
        : text;

    const [whole, fraction = ""] =
        unsigned.split(".");

    if (fraction.length > MAX_DECIMAL_PLACES) {
        throw new Error(
            `VOUCHER_COMPARE_PRECISION_${field}`
        );
    }

    const units =
        BigInt(whole) * 1000000n +
        BigInt(
            fraction.padEnd(MAX_DECIMAL_PLACES, "0")
        );

    return negative ? -units : units;
}

function validateSqlId(value) {
    if (
        typeof value !== "string" ||
        !/^[1-9]\d*$/.test(value)
    ) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_SQL_ID"
        );
    }

    const id = BigInt(value);

    if (id > MAX_SQL_ID) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_SQL_ID"
        );
    }

    return value;
}

function validateAllocations(value) {
    if (value === null) {
        return [];
    }

    if (!Array.isArray(value)) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_ALLOCATIONS"
        );
    }

    return value;
}

function normalizeEntry(entry, source) {
    if (
        !entry ||
        typeof entry !== "object" ||
        Array.isArray(entry)
    ) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${source}_ENTRY`
        );
    }

    const companyCode = requiredText(
        entry.companyCode ?? entry.company_code,
        "COMPANY_CODE"
    );

    const tallyOwner = requiredText(
        entry.tallyOwner ?? entry.tally_owner,
        "TALLY_OWNER"
    );

    if (
        tallyOwner !== "USER" &&
        tallyOwner !== "CA"
    ) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_TALLY_OWNER"
        );
    }

    const voucherGuid = requiredText(
        entry.voucherGuid ?? entry.voucher_guid,
        "VOUCHER_GUID"
    );

    const ledgerGuid = requiredText(
        entry.ledgerGuid ?? entry.ledger_guid,
        "LEDGER_GUID"
    );

    const rawAmount = entry.amount;

    if (
        typeof rawAmount !== "string" ||
        rawAmount !== rawAmount.trim()
    ) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_AMOUNT"
        );
    }

    const amount = decimalToUnits(
        rawAmount,
        "AMOUNT"
    );

const amountNegative =
    amount !== 0n && rawAmount.startsWith("-");

    const debit = decimalToUnits(
        entry.debit,
        "DEBIT"
    );

    const credit = decimalToUnits(
        entry.credit,
        "CREDIT"
    );

    let id = null;
    let requiresStructuralReview = false;

    if (source === "SQL") {
        id = validateSqlId(
            entry.id ?? entry.ledger_entry_id
        );

        if (
            entry.is_deleted !== undefined &&
            entry.is_deleted !== false
        ) {
            throw new Error(
                "VOUCHER_COMPARE_SQL_DELETED_STATE_UNSAFE"
            );
        }

        const billAllocations =
            validateAllocations(
                entry.bill_allocations ?? null
            );

        const costcentreAllocations =
            validateAllocations(
                entry.costcentre_allocations ?? null
            );

        const derivedStructuralReview =
            billAllocations.length > 0 ||
            costcentreAllocations.length > 0;

        if (
            entry.requiresStructuralReview !== undefined &&
            typeof entry.requiresStructuralReview !== "boolean"
        ) {
            throw new Error(
                "VOUCHER_COMPARE_INVALID_STRUCTURAL_REVIEW"
            );
        }

        if (
            entry.requiresStructuralReview === false &&
            derivedStructuralReview
        ) {
            throw new Error(
                "VOUCHER_COMPARE_STRUCTURAL_REVIEW_MISMATCH"
            );
        }

        requiresStructuralReview =
            derivedStructuralReview ||
            entry.requiresStructuralReview === true;
    }

    return {
        id,
        companyCode,
        tallyOwner,
        voucherGuid,
        ledgerGuid,
        amount,
        amountNegative,
        debit,
        credit,
        requiresStructuralReview
    };
}

function groupKey(entry) {
    return JSON.stringify([
        entry.companyCode,
        entry.tallyOwner,
        entry.voucherGuid,
        entry.ledgerGuid
    ]);
}

function groupEntries(entries, source) {
    if (!Array.isArray(entries)) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${source}_ARRAY`
        );
    }

    const groups = new Map();
    const seenIds = new Set();

    for (const rawEntry of entries) {
        const entry = normalizeEntry(
            rawEntry,
            source
        );

        if (source === "SQL") {
            if (seenIds.has(entry.id)) {
                throw new Error(
                    "VOUCHER_COMPARE_DUPLICATE_SQL_ID"
                );
            }

            seenIds.add(entry.id);
        }

        const key = groupKey(entry);

        if (!groups.has(key)) {
            groups.set(key, []);
        }

        groups.get(key).push(entry);
    }

    return groups;
}

function compareVoucherLedgerEntries({
    tallyEntries,
    sqlEntries,
    companyCode,
    tallyOwner
}) {
    const expectedCompanyCode = requiredText(
        companyCode,
        "EXPECTED_COMPANY_CODE"
    );

    const expectedTallyOwner = requiredText(
        tallyOwner,
        "EXPECTED_TALLY_OWNER"
    );

    if (
        expectedTallyOwner !== "USER" &&
        expectedTallyOwner !== "CA"
    ) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_EXPECTED_OWNER"
        );
    }

    const tallyGroups = groupEntries(
        tallyEntries,
        "TALLY"
    );

    const sqlGroups = groupEntries(
        sqlEntries,
        "SQL"
    );

    for (const groups of [
        tallyGroups,
        sqlGroups
    ]) {
        for (const entries of groups.values()) {
            for (const entry of entries) {
                if (
                    entry.companyCode !== expectedCompanyCode ||
                    entry.tallyOwner !== expectedTallyOwner
                ) {
                    throw new Error(
                        "VOUCHER_COMPARE_SCOPE_MISMATCH"
                    );
                }
            }
        }
    }

    const unchanged = [];
    const updateCandidates = [];
    const ambiguous = [];
    const missingInSql = [];
    const missingInTally = [];
    const structuralReview = [];

    const allKeys = new Set([
        ...tallyGroups.keys(),
        ...sqlGroups.keys()
    ]);

    for (const key of allKeys) {
        const tally = tallyGroups.get(key) || [];
        const sql = sqlGroups.get(key) || [];

        const [
            scopedCompanyCode,
            scopedTallyOwner,
            voucherGuid,
            ledgerGuid
        ] = JSON.parse(key);

        const identity = {
            companyCode: scopedCompanyCode,
            tallyOwner: scopedTallyOwner,
            voucherGuid,
            ledgerGuid
        };

        if (tally.length === 0) {
            missingInTally.push({
                ...identity,
                sqlRowIds: sql.map(
                    entry => entry.id
                ),
                sqlCount: sql.length
            });
            continue;
        }

        if (sql.length === 0) {
            missingInSql.push({
                ...identity,
                tallyCount: tally.length
            });
            continue;
        }

        if (
            tally.length !== 1 ||
            sql.length !== 1
        ) {
            ambiguous.push({
                ...identity,
                tallyCount: tally.length,
                sqlCount: sql.length,
                sqlRowIds: sql.map(
                    entry => entry.id
                )
            });
            continue;
        }

        const fresh = tally[0];
        const existing = sql[0];

        // Structural evidence takes precedence over
        // unchanged and UPDATE classifications.
        if (existing.requiresStructuralReview) {
            structuralReview.push({
                ...identity,
                sqlRowId: existing.id,
                reason:
                    "SQL_ENTRY_HAS_STRUCTURAL_ALLOCATIONS"
            });
            continue;
        }

      if (
    fresh.amount === existing.amount &&
    fresh.amountNegative === existing.amountNegative &&
    fresh.debit === existing.debit &&
    fresh.credit === existing.credit
) {
            unchanged.push({
                ...identity,
                sqlRowId: existing.id
            });
            continue;
        }

        updateCandidates.push({
            ...identity,
            sqlRowId: existing.id,

            // Exact decimal units (1/1,000,000).
            // Diagnostic only, NOT SQL rupee values.
            expectedAmountUnits:
                existing.amount.toString(),
            newAmountUnits:
                fresh.amount.toString(),

            expectedAmountNegative:
                existing.amountNegative,
            newAmountNegative:
                fresh.amountNegative,

            expectedDebitUnits:
                existing.debit.toString(),
            expectedCreditUnits:
                existing.credit.toString(),

            newDebitUnits:
                fresh.debit.toString(),
            newCreditUnits:
                fresh.credit.toString()
        });
    }

    return {
        unchanged,
        updateCandidates,
        ambiguous,
        missingInSql,
        missingInTally,
        structuralReview,
        summary: {
            unchanged: unchanged.length,
            updateCandidates:
                updateCandidates.length,
            ambiguous: ambiguous.length,
            missingInSql: missingInSql.length,
            missingInTally: missingInTally.length,
            structuralReview:
                structuralReview.length
        }
    };
}

module.exports = {
    compareVoucherLedgerEntries
};
