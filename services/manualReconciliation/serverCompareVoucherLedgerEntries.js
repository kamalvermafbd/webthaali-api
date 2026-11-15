
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

    let parsed = value;

    if (typeof value === "string") {
        try {
            parsed = JSON.parse(value);
        } catch {
            throw new Error(
                "VOUCHER_COMPARE_INVALID_ALLOCATIONS_JSON"
            );
        }
    }

    if (!Array.isArray(parsed)) {
        throw new Error(
            "VOUCHER_COMPARE_INVALID_ALLOCATIONS"
        );
    }

    return parsed;
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

    // Debit and credit are the matching source of truth.
    // Existing SQL amount must follow the stored sign convention.
    if (debit < 0n || credit < 0n) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${source}_DEBIT_CREDIT`
        );
    }

    if (debit !== 0n && credit !== 0n) {
        throw new Error(
            `VOUCHER_COMPARE_INVALID_${source}_DIRECTION`
        );
    }

    if (
        source === "SQL" &&
        amount !== credit - debit
    ) {
        throw new Error(
            "VOUCHER_COMPARE_SQL_AMOUNT_INCONSISTENT"
        );
    }

    let id = null;
    let requiresStructuralReview = false;

    if (source === "TALLY") {
        if (
            typeof entry.requiresStructuralReview !== "boolean"
        ) {
            throw new Error(
                "VOUCHER_COMPARE_TALLY_STRUCTURAL_EVIDENCE_MISSING"
            );
        }

        requiresStructuralReview =
            entry.requiresStructuralReview;
    }



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
    tallyOwner,
    snapshotCoverage
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

    for (const groups of [tallyGroups, sqlGroups]) {
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
    const voucherDecisions = [];

    const tallyByVoucher = new Map();
    const sqlByVoucher = new Map();

    for (const [key, entries] of tallyGroups) {
        const [company, owner, voucher] = JSON.parse(key);
        const voucherKey = JSON.stringify([
            company,
            owner,
            voucher
        ]);

        if (!tallyByVoucher.has(voucherKey)) {
            tallyByVoucher.set(voucherKey, []);
        }

        tallyByVoucher.get(voucherKey).push(...entries);
    }

    for (const [key, entries] of sqlGroups) {
        const [company, owner, voucher] = JSON.parse(key);
        const voucherKey = JSON.stringify([
            company,
            owner,
            voucher
        ]);

        if (!sqlByVoucher.has(voucherKey)) {
            sqlByVoucher.set(voucherKey, []);
        }

        sqlByVoucher.get(voucherKey).push(...entries);
    }

    function sameValue(a, b) {
        return (
            a.ledgerGuid === b.ledgerGuid &&
            a.debit === b.debit &&
            a.credit === b.credit
        );
    }


   
    function updateCandidate(existing, fresh) {
        const expectedAmount =
            existing.credit - existing.debit;

        const newAmount =
            fresh.credit - fresh.debit;

        return {
            companyCode: existing.companyCode,
            tallyOwner: existing.tallyOwner,
            voucherGuid: existing.voucherGuid,
            ledgerGuid: existing.ledgerGuid,
            sqlRowId: existing.id,

            expectedAmountUnits:
                expectedAmount.toString(),

            newAmountUnits:
                newAmount.toString(),

            expectedAmountNegative:
                expectedAmount < 0n,

            newAmountNegative:
                newAmount < 0n,

            expectedDebitUnits:
                existing.debit.toString(),

            expectedCreditUnits:
                existing.credit.toString(),

            newDebitUnits:
                fresh.debit.toString(),

            newCreditUnits:
                fresh.credit.toString()
        };
    }


    const allVouchers = new Set([
        ...tallyByVoucher.keys(),
        ...sqlByVoucher.keys()
    ]);

    for (const voucherKey of allVouchers) {
        const [company, owner, voucherGuid] =
            JSON.parse(voucherKey);

        const identity = {
            companyCode: company,
            tallyOwner: owner,
            voucherGuid
        };

       
        const sql =
            sqlByVoucher.get(voucherKey) || [];

        const tally =
            tallyByVoucher.get(voucherKey) || [];

        // Coverage is evidence supplied by the fetch layer.
        // Only this comparator decides whether it is sufficient.
        const coverage = snapshotCoverage || {};

        const sqlComplete =
            Array.isArray(coverage.sqlCompleteVoucherGuids) &&
            coverage.sqlCompleteVoucherGuids.includes(voucherGuid);

        const tallyComplete =
            Array.isArray(coverage.tallyCompleteVoucherGuids) &&
            coverage.tallyCompleteVoucherGuids.includes(voucherGuid);

        if (!sqlComplete || !tallyComplete) {
            voucherDecisions.push({
                ...identity,
                action: "VERIFY_REQUIRED",
                reason: !sqlComplete
                    ? "SQL_VOUCHER_SNAPSHOT_UNVERIFIED"
                    : "TALLY_VOUCHER_SNAPSHOT_UNVERIFIED"
            });

            continue;
        }

        // Structural equality has not yet been established.
        // Do not authorize monetary UPDATE or UNCHANGED
        // when either snapshot contains structural allocations.
        if (sql.length > 0 && tally.length > 0) {
            const structuralEvidenceUnverified =
                sql.some(entry =>
                    entry.requiresStructuralReview
                ) ||
                tally.some(entry =>
                    entry.requiresStructuralReview
                );

            if (structuralEvidenceUnverified) {
                voucherDecisions.push({
                    ...identity,
                    action: "VERIFY_REQUIRED",
                    reason: "STRUCTURAL_COMPARISON_UNVERIFIED"
                });

                continue;
            }
        }

        if (!sql.length) {

            missingInSql.push({
                ...identity,
                tallyCount: tally.length
            });

            voucherDecisions.push({
                ...identity,
                action: "OPTIONAL_SYNC",
                tallyCount: tally.length
            });

            continue;
        }

      
        if (!tally.length) {
            // Both voucher snapshots have already passed
            // the completeness guard above.
            // Therefore Tally absence is verified.
            missingInTally.push({
                ...identity,
                sqlRowIds: sql.map(e => e.id),
                sqlCount: sql.length
            });

            voucherDecisions.push({
                ...identity,
                action: "DELETE_ONLY",
                reason: "VOUCHER_ABSENT_IN_VERIFIED_TALLY",
                sqlRowIds: sql.map(e => e.id)
            });

            continue;
        }

        // Repeated voucher-ledger occurrences are not
        // eligible for amount-only settlement.
        const hasRepeatedLedger = entries => {
            const seen = new Set();

            for (const entry of entries) {
                if (seen.has(entry.ledgerGuid)) {
                    return true;
                }

                seen.add(entry.ledgerGuid);
            }

            return false;
        };

        if (
            hasRepeatedLedger(sql) ||
            hasRepeatedLedger(tally)
        ) {
            ambiguous.push({
                ...identity,
                reason: "REPEATED_LEDGER_OCCURRENCE",
                sqlCount: sql.length,
                tallyCount: tally.length,
                sqlRowIds: sql.map(entry => entry.id)
            });

            voucherDecisions.push({
                ...identity,
                action: "DELETE_AND_RESYNC",
                reason: "REPEATED_LEDGER_OCCURRENCE"
            });

            continue;
        }
        const localUnchanged = [];
        const localUpdates = [];
        const localStructural = [];

        let rebuildReason = null;

        if (sql.length !== tally.length) {
            rebuildReason = "ENTRY_COUNT_MISMATCH";
        } else {
            const sqlByLedger = new Map();
            const tallyByLedger = new Map();

            for (const entry of sql) {
                if (!sqlByLedger.has(entry.ledgerGuid)) {
                    sqlByLedger.set(entry.ledgerGuid, []);
                }

                sqlByLedger.get(entry.ledgerGuid)
                    .push(entry);
            }

            for (const entry of tally) {
                if (!tallyByLedger.has(entry.ledgerGuid)) {
                    tallyByLedger.set(entry.ledgerGuid, []);
                }

                tallyByLedger.get(entry.ledgerGuid)
                    .push(entry);
            }

            const allLedgers = new Set([
                ...sqlByLedger.keys(),
                ...tallyByLedger.keys()
            ]);

            for (const ledgerGuid of allLedgers) {
                const oldEntries =
                    sqlByLedger.get(ledgerGuid) || [];

                const newEntries =
                    tallyByLedger.get(ledgerGuid) || [];

                if (
                    oldEntries.length !==
                    newEntries.length
                ) {
                    rebuildReason =
                        "LEDGER_ENTRY_COUNT_MISMATCH";
                    break;
                }

                const unmatchedFresh = [...newEntries];
                const unmatchedOld = [];

                for (const existing of oldEntries) {
                    const index =
                        unmatchedFresh.findIndex(
                            fresh =>
                                sameValue(existing, fresh)
                        );

                    if (index >= 0) {
                        unmatchedFresh.splice(index, 1);

                        localUnchanged.push({
                            ...identity,
                            ledgerGuid,
                            sqlRowId: existing.id
                        });
                    } else {
                        unmatchedOld.push(existing);
                    }
                }

                if (unmatchedOld.length > 1) {
                    rebuildReason =
                        "MULTIPLE_UNMATCHED_ENTRIES";
                    break;
                }

             
if (unmatchedOld.length === 1) {
    const existing = unmatchedOld[0];
    const fresh = unmatchedFresh[0];

    // A single unmatched pair must exist on both sides.
    if (!fresh || unmatchedFresh.length !== 1) {
        rebuildReason = "UNMATCHED_PAIR_COUNT_INVALID";
        break;
    }

    // Ledger identity must remain unchanged.
    if (existing.ledgerGuid !== fresh.ledgerGuid) {
        rebuildReason = "LEDGER_IDENTITY_CHANGED";
        break;
    }

    // Existing allocations cannot be corrected by
    // an amount-only UPDATE.
    if (existing.requiresStructuralReview) {
        localStructural.push({
            ...identity,
            ledgerGuid,
            sqlRowId: existing.id,
            reason: "SQL_ENTRY_HAS_STRUCTURAL_ALLOCATIONS"
        });

        rebuildReason = "STRUCTURAL_REVIEW";
        break;
    }

    // Debit/credit direction must remain compatible.
    function direction(entry) {
        if (entry.debit !== 0n && entry.credit === 0n) {
            return "DEBIT";
        }

        if (entry.credit !== 0n && entry.debit === 0n) {
            return "CREDIT";
        }

        if (entry.debit === 0n && entry.credit === 0n) {
            return "ZERO";
        }

        return "INVALID";
    }

    const oldDirection = direction(existing);
    const newDirection = direction(fresh);

   
if (
    oldDirection === "INVALID" ||
    newDirection === "INVALID" ||
    oldDirection !== newDirection
) {
    rebuildReason = "DEBIT_CREDIT_DIRECTION_MISMATCH";
    break;
}

    // One unmatched pair, same ledger, compatible
    // direction, no structural allocations.
    localUpdates.push(
        updateCandidate(existing, fresh)
    );
}

            }
        }

        if (rebuildReason) {
            ambiguous.push({
                ...identity,
                reason: rebuildReason,
                sqlCount: sql.length,
                tallyCount: tally.length,
                sqlRowIds: sql.map(e => e.id)
            });

            structuralReview.push(
                ...localStructural
            );

            voucherDecisions.push({
                ...identity,
                action: "DELETE_AND_RESYNC",
                reason: rebuildReason
            });
        } else {
            unchanged.push(...localUnchanged);
            updateCandidates.push(...localUpdates);

            voucherDecisions.push({
                ...identity,
                action:
                    localUpdates.length
                        ? "UPDATE"
                        : "UNCHANGED",
                updateCount: localUpdates.length
            });
        }
    }

    return {
        unchanged,
        updateCandidates,
        ambiguous,
        missingInSql,
        missingInTally,
        structuralReview,
        voucherDecisions,

        summary: {
            unchanged: unchanged.length,

            updateCandidates:
                updateCandidates.length,

            ambiguous: ambiguous.length,

            missingInSql:
                missingInSql.length,

            missingInTally:
                missingInTally.length,

            structuralReview:
                structuralReview.length,

            deleteAndResync:
                voucherDecisions.filter(
                    v =>
                        v.action ===
                        "DELETE_AND_RESYNC"
                ).length,

           
            deleteOnly:
                voucherDecisions.filter(
                    v =>
                        v.action === "DELETE_ONLY"
                ).length,


            
            optionalSync:
                voucherDecisions.filter(
                    v => v.action === "OPTIONAL_SYNC"
                ).length,

            verifyRequired:
                voucherDecisions.filter(
                    v => v.action === "VERIFY_REQUIRED"
                ).length

        }
    };
}

module.exports = {
    compareVoucherLedgerEntries
};
