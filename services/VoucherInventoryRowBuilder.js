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

function buildInventoryRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    const skipStockConsumption = [

        "Multi Consumption Voucher View",

        "Consumption Voucher View"

    ].includes(header.persistedView);

    if (skipStockConsumption) {

        return rows;

    }

    for (const item of (voucher.inventory || [])) {

        if (

            item.inventoryNode !==

            "ALLINVENTORYENTRIES.LIST"

        ) {

            continue;

        }

        const gstRates = item.gstRates ?? [];

        const cgstRate =

            gstRates.find(

                r => r.dutyHead === "CGST"

            )?.rate ?? null;

        const sgstRate =

            gstRates.find(

                r =>

                    r.dutyHead === "SGST/UTGST"

            )?.rate ?? null;

        const igstRate =

            gstRates.find(

                r => r.dutyHead === "IGST"

            )?.rate ?? null;

        const hsnCode =

            item.hsnCode || null;

        rows.push({

            voucher_guid:
                safeTrim(header.guid),

            company_code,

            tally_owner,

            stock_item:
                safeTrim(item.stockItem) || null,

            actual_qty:

                item.actualQty || null,

            actual_qty_value:

                item.actualQtyValue || null,

            billed_qty:

                item.billedQty || null,

            billed_qty_value:

                item.billedQtyValue || null,

                unit:
                safeTrim(item.unit) || null,

            rate:

                item.rate || null,

            rate_value:

                item.rateValue || null,

            amount:

                item.amount ?? null,

            hsn_code:

                hsnCode,

            discount:

                item.discount ?? null,

            godown:
                  safeTrim(item.godown) || null,

            batch_id:

                item.batchId ?? null,

            batches:

                item.batches ?? [],

            accounting:

                item.accounting ?? [],

            stock_guid:

                item.stockGuid ?? null,

            stock_masterid:

                item.stockMasterIdResolved ?? null,

            stock_alterid:

                item.stockAlterId ?? null,

            transaction_type:

                item.transactionType ?? null,

            ledger_name:

                item.ledgerName ?? null,

            ledger_guid:

                item.ledgerGuid ?? null,

            ledger_master_id:

                item.ledgerMasterId ?? null,

            ledger_alter_id:

                item.ledgerAlterId ?? null,

            party_name:

                item.partyName ?? null,

            party_guid:

                item.partyGuid ?? null,

            party_master_id:

                item.partyMasterId ?? null,

            party_alter_id:

                item.partyAlterId ?? null,

            cgst_rate:

                cgstRate,

            sgst_rate:

                sgstRate,

            igst_rate:

                igstRate,

            gst_rate:

                igstRate ??

                (

                    (cgstRate || 0)

                    +

                    (sgstRate || 0)

                ),

            cgst_amount:

                item.cgstAmount ?? 0,

            sgst_amount:

                item.sgstAmount ?? 0,

            igst_amount:

                item.igstAmount ?? 0,

            taxable_amount:

                item.taxableAmount ?? 0,

            gst_rates:

                item.gstRates ?? [],

            costcentre_allocations:

                item.costCentreAllocations ?? []

        });

    }

    return rows;

}

module.exports = {

    buildInventoryRows

};