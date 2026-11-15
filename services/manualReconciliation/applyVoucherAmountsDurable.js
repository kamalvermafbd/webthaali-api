
"use strict";

const crypto = require("node:crypto");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const SQL_ID = /^[1-9]\d*$/;
const DECIMAL = /^-?\d+(?:\.\d{1,6})?$/;
const UNITS = 1000000n;
const MAX_SQL_ID = 9223372036854775807n;
const MAX_AMOUNT_UNITS = 9007199254740991n;

function uncertainError(code, context, cause) {
    const error = new Error(
        code,
        cause ? { cause } : undefined
    );

    error.reconcileRecoveryRequired = true;
    error.reconcileOutcomeUncertain = true;

    Object.assign(error, context);
    return error;
}

function amountUnits(value, field) {
    if (
        typeof value !== "string" ||
        !DECIMAL.test(value)
    ) {
        throw new Error(
            `RECONCILE_VOUCHER_APPLY_INVALID_${field}`
        );
    }

    const negative = value.startsWith("-");
    const [whole, fraction = ""] =
        (negative ? value.slice(1) : value).split(".");

    const absolute =
        BigInt(whole) * UNITS +
        BigInt(fraction.padEnd(6, "0"));

    if (absolute > MAX_AMOUNT_UNITS) {
        throw new Error(
            `RECONCILE_VOUCHER_APPLY_RANGE_${field}`
        );
    }

    return negative ? -absolute : absolute;
}

function candidateUnits(value, field) {
    if (
        typeof value !== "string" ||
        !/^-?\d+$/.test(value)
    ) {
        throw new Error(
            `RECONCILE_VOUCHER_APPLY_INVALID_${field}`
        );
    }

    const units = BigInt(value);

    if (
        units < -MAX_AMOUNT_UNITS ||
        units > MAX_AMOUNT_UNITS
    ) {
        throw new Error(
            `RECONCILE_VOUCHER_APPLY_RANGE_${field}`
        );
    }

    return units;
}

function unitsToDecimal(units) {
    const negative = units < 0n;
    const absolute = negative ? -units : units;

    return (
        `${negative ? "-" : ""}` +
        `${absolute / UNITS}.` +
        (absolute % UNITS).toString().padStart(6, "0")
    );
}

async function applyVoucherAmountsDurable({
    supabase,
    signal,
    attemptId,
    executionId,
    operationToken,
    operationOrdinal,
    fetchOperationId,
    sqlEntries,
    updateCandidates
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_APPLY_DEPENDENCY_INVALID"
        );
    }

    for (const value of [
        attemptId,
        executionId,
        operationToken,
        fetchOperationId
    ]) {
        if (
            typeof value !== "string" ||
            !UUID.test(value)
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_ID_INVALID"
            );
        }
    }

    if (
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal < 1
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_APPLY_ORDINAL_INVALID"
        );
    }

    if (
        !Array.isArray(sqlEntries) ||
        sqlEntries.length === 0 ||
        sqlEntries.length > 10000
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_APPLY_ROWS_INVALID"
        );
    }

    if (
        !Array.isArray(updateCandidates) ||
        updateCandidates.length > sqlEntries.length
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_APPLY_CANDIDATES_INVALID"
        );
    }

    const seenIds = new Set();
    const rowsById = new Map();

    const expectedRows = sqlEntries.map(row => {
        if (
            !row ||
            typeof row !== "object" ||
            Array.isArray(row) ||
            typeof row.id !== "string" ||
            !SQL_ID.test(row.id) ||
            BigInt(row.id) > MAX_SQL_ID ||
            typeof row.voucher_guid !== "string" ||
            !row.voucher_guid.trim() ||
            typeof row.ledger_guid !== "string" ||
            !row.ledger_guid.trim() ||
            row.is_deleted !== false ||
            !Array.isArray(row.bill_allocations) ||
            !Array.isArray(row.costcentre_allocations) ||
            row.bill_allocations.length !== 0 ||
            row.costcentre_allocations.length !== 0
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_ROW_UNSAFE"
            );
        }

        if (seenIds.has(row.id)) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_DUPLICATE_ROW"
            );
        }

        const amount = amountUnits(
            row.amount,
            "AMOUNT"
        );

        const debit = amountUnits(
            row.debit,
            "DEBIT"
        );

        const credit = amountUnits(
            row.credit,
            "CREDIT"
        );

        if (
            debit < 0n ||
            credit < 0n ||
            (debit !== 0n && credit !== 0n) ||
            amount !== credit - debit
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_AMOUNT_INCONSISTENT"
            );
        }

        seenIds.add(row.id);

        const expected = {
            id: row.id,
            voucherGuid: row.voucher_guid,
            ledgerGuid: row.ledger_guid,
            amount: row.amount,
            debit: row.debit,
            credit: row.credit
        };

        rowsById.set(row.id, {
            expected,
            amount,
            debit,
            credit,
            companyCode:
                row.company_code ?? row.companyCode,
            tallyOwner:
                row.tally_owner ?? row.tallyOwner
        });

        return expected;
    });

    const seenCandidates = new Set();

    for (const candidate of updateCandidates) {
        if (
            !candidate ||
            typeof candidate !== "object" ||
            Array.isArray(candidate) ||
            typeof candidate.sqlRowId !== "string" ||
            seenCandidates.has(candidate.sqlRowId)
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_CANDIDATE_INVALID"
            );
        }

        const original = rowsById.get(
            candidate.sqlRowId
        );

        if (!original) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_UNKNOWN_ROW"
            );
        }

        if (
            candidate.voucherGuid !==
                original.expected.voucherGuid ||
            candidate.ledgerGuid !==
                original.expected.ledgerGuid ||
            candidate.companyCode !==
                original.companyCode ||
            candidate.tallyOwner !==
                original.tallyOwner
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_IDENTITY_MISMATCH"
            );
        }

        const expectedAmount = candidateUnits(
            candidate.expectedAmountUnits,
            "EXPECTED_AMOUNT"
        );

        const expectedDebit = candidateUnits(
            candidate.expectedDebitUnits,
            "EXPECTED_DEBIT"
        );

        const expectedCredit = candidateUnits(
            candidate.expectedCreditUnits,
            "EXPECTED_CREDIT"
        );

        const newAmount = candidateUnits(
            candidate.newAmountUnits,
            "NEW_AMOUNT"
        );

        const newDebit = candidateUnits(
            candidate.newDebitUnits,
            "NEW_DEBIT"
        );

        const newCredit = candidateUnits(
            candidate.newCreditUnits,
            "NEW_CREDIT"
        );

        if (
            expectedAmount !== original.amount ||
            expectedDebit !== original.debit ||
            expectedCredit !== original.credit ||
            newDebit < 0n ||
            newCredit < 0n ||
            (newDebit !== 0n && newCredit !== 0n) ||
            newAmount !== newCredit - newDebit ||
            (
                newAmount === original.amount &&
                newDebit === original.debit &&
                newCredit === original.credit
            )
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_APPLY_CANDIDATE_UNSAFE"
            );
        }

        original.expected.newAmount =
            unitsToDecimal(newAmount);

        original.expected.newDebit =
            unitsToDecimal(newDebit);

        original.expected.newCredit =
            unitsToDecimal(newCredit);

        seenCandidates.add(candidate.sqlRowId);
    }

    if (signal?.aborted) {
        throw new Error(
            "RECONCILE_VOUCHER_APPLY_ABORTED"
        );
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
                p_source_fetch_operation_id:
                    fetchOperationId,
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
        result.updated_rows !== updateCandidates.length
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
