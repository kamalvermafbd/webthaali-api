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
                header.guid.trim(),

            company_code,

            tally_owner,

            ledger_name:
                ledger.ledgerName?.trim() || null,

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