"use strict";

async function fetchSqlLedgerTransactions({
    supabase,
    companyCode,
    tallyOwner,
    ledgerGuid
}) {
    companyCode = String(companyCode || "").trim();
    tallyOwner = String(tallyOwner || "").trim();
    ledgerGuid = String(ledgerGuid || "").trim();

    if (
        !supabase ||
        !companyCode ||
        !["USER", "CA"].includes(tallyOwner) ||
        !ledgerGuid
    ) {
        throw new Error("RECONCILE_SQL_REQUEST_INVALID");
    }

    const pageSize = 500;
    const allRows = [];
    const seenIds = new Set();

    let afterEntryId = 0;

    while (true) {
        const { data, error } = await supabase.rpc(
            "get_reconcile_ledger_transactions_page",
            {
                p_company_code: companyCode,
                p_tally_owner: tallyOwner,
                p_ledger_guid: ledgerGuid,
                p_after_entry_id: afterEntryId,
                p_page_size: pageSize
            }
        );

        if (error) {
            throw error;
        }

        if (
            !Array.isArray(data) ||
            data.length > pageSize
        ) {
            throw new Error("RECONCILE_SQL_PAGE_INVALID");
        }

        let previousEntryId = afterEntryId;

for (const row of data) {
    const id = Number(row.ledger_entry_id);

    if (
        !Number.isSafeInteger(id) ||
        id <= previousEntryId ||
        seenIds.has(id)
    ) {
        throw new Error(
            "RECONCILE_SQL_PAGINATION_INVALID"
        );
    }

    previousEntryId = id;

            if (
                !row.voucher_guid ||
                !row.ledger_guid ||
                String(row.ledger_guid).trim().toLowerCase() !==
                    ledgerGuid.toLowerCase()
            ) {
                throw new Error(
                    "RECONCILE_SQL_ENTRY_SCOPE_INVALID"
                );
            }

            seenIds.add(id);

            allRows.push({
                id,
                voucher_guid: row.voucher_guid,
                ledger_guid: row.ledger_guid,
                amount: row.amount,
                debit: row.debit,
                credit: row.credit,
                voucherDate: row.voucher_date,
                alterId: row.alter_id
            });
        }

        if (data.length === 0) {
            break;
        }

        afterEntryId = Number(
            data[data.length - 1].ledger_entry_id
        );

        if (data.length < pageSize) {
            break;
        }
    }

    return allRows;
}

module.exports = {
    fetchSqlLedgerTransactions
};