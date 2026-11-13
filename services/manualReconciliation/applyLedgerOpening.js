"use strict";

const UUID_REGEX =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function applyLedgerOpening({
    supabase,
    companyCode,
    tallyOwner,
    reservationToken,
    ledger
}) {
    if (!supabase || typeof supabase.rpc !== "function") {
        throw new Error("RECONCILE_SUPABASE_INVALID");
    }

    if (
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        typeof reservationToken !== "string" ||
        !UUID_REGEX.test(reservationToken)
    ) {
        throw new Error("RECONCILE_LEDGER_CONTEXT_INVALID");
    }

    if (
        !ledger ||
        typeof ledger !== "object" ||
        typeof ledger.guid !== "string" ||
        !ledger.guid.trim() ||
        typeof ledger.openingBalance !== "string" ||
        !/^\d+(?:\.\d+)?$/.test(ledger.openingBalance) ||
        !["DR", "CR", ""].includes(ledger.openingBalanceType)
    ) {
        throw new Error("RECONCILE_LEDGER_PAYLOAD_INVALID");
    }

    const isZero =
        /^0+$/.test(ledger.openingBalance.replace(".", ""));

    if (
        (isZero && ledger.openingBalanceType !== "") ||
        (!isZero && ledger.openingBalanceType === "")
    ) {
        throw new Error("RECONCILE_LEDGER_TYPE_MISMATCH");
    }

    const { data, error } = await supabase.rpc(
        "apply_manual_reconcile_ledger_opening",
        {
            p_company_code: companyCode.trim(),
            p_tally_owner: tallyOwner,
            p_operation_token: reservationToken,
            p_ledger_guid: ledger.guid.trim(),
            p_opening_balance: ledger.openingBalance,
            p_opening_balance_type: ledger.openingBalanceType
        }
    );

    if (error) {
        throw error;
    }

    if (!Array.isArray(data) || data.length !== 1) {
        throw new Error("RECONCILE_LEDGER_APPLY_RESULT_INVALID");
    }

        const row = data[0];

    if (
        !row ||
        !["UPDATED", "UNCHANGED", "NOT_FOUND"].includes(row.result)
    ) {
        throw new Error("RECONCILE_LEDGER_APPLY_RESULT_INVALID");
    }

    if (row.result === "NOT_FOUND") {
        const error = new Error(
            "RECONCILE_LEDGER_NOT_FOUND"
        );

        error.reconcileLedgerGuid = ledger.guid.trim();
        error.reconcileStructuralMismatch = true;

        throw error;
    }

    return {
        result: row.result,
        previousBalance: row.previous_balance,
        previousType: row.previous_type,
        currentBalance: row.current_balance,
        currentType: row.current_type
    };
}

module.exports = {
    applyLedgerOpening
};