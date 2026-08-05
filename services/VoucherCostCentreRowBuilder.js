function buildCostCentreRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    for (const ledger of (voucher.ledgers || [])) {

        for (const cost of (ledger.costCentreAllocations || [])) {

            rows.push({

                voucher_guid:
                    header.guid.trim(),

                ledger_guid:
                    ledger.ledgerGuid ?? null,

                company_code,

                tally_owner,

                ledger_name:
                    ledger.ledgerName?.trim() || null,

                voucher_date:
                    header.voucherDate || null,

                voucher_number:
                    header.voucherNumber ?? null,

                voucher_type:
                    header.voucherType ?? null,

                debit_credit:
                    ledger.isDeemedPositive === "Yes"
                        ? "DR"
                        : "CR",

                cost_category:
                    cost.costCategory ?? null,

                cost_centre:
                    cost.costCentre ?? null,

                amount:
                    cost.amount ?? 0

            });

        }

    }

    return rows;

}

module.exports = {

    buildCostCentreRows

};