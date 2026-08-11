function safeTrim(value) {
    if (value == null) return "";

    if (typeof value === "string") {
        return value.trim();
    }

    if (
        typeof value === "number" ||
        typeof value === "boolean"
    ) {
        return String(value).trim();
    }

    return "";
}


function buildLedgerRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    for (const ledger of (voucher.ledgers || [])) {

        rows.push({

            voucher_guid:
             safeTrim(header.guid),

            company_code,

            tally_owner,

            ledger_name:
                 safeTrim(ledger.ledgerName) || null,

            ledger_masterid:
                ledger.ledgerMasterId ?? null,

            ledger_guid:
                ledger.ledgerGuid ?? null,

            ledger_alterid:
                ledger.ledgerAlterId ?? null,

            amount:
                ledger.amount ?? null,

            debit:
                ledger.debit ?? null,

            credit:
                ledger.credit ?? null,

            bill_allocations:
                ledger.billAllocations ?? [],

            costcentre_allocations:
                ledger.costCentreAllocations ?? []

        });

    }

    return rows;

}

module.exports = {

    buildLedgerRows

};