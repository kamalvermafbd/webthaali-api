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

function buildStockVoucherRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    for (const item of (voucher.inventory || [])) {

        if (
            item.inventoryNode ===
            "ALLINVENTORYENTRIES.LIST"
        ) {

            continue;

        }

        rows.push({

            voucher_guid:
                safeTrim(header.guid),

            company_code,

            tally_owner,

            stock_guid:
                item.stockGuid ?? null,

            stock_masterid:
                item.stockMasterIdResolved ?? null,

            stock_alterid:
                item.stockAlterId ?? null,

            stock_item:
                  safeTrim(item.stockItem) || null,

            movement_type:
                item.movementType ?? null,

            actual_qty:
                item.actualQty ?? null,

            actual_qty_value:
                item.actualQtyValue ?? null,

            billed_qty:
                item.billedQty ?? null,

            billed_qty_value:
                item.billedQtyValue ?? null,

        unit:
    safeTrim(item.unit) || null,

            rate:
                item.rate ?? null,

            rate_value:
                item.rateValue ?? null,

            amount:
                item.amount ?? null,

          godown:
    safeTrim(item.godown) || null,

            batch_name:
                item.batchName ?? null,

            batch_id:
                item.batchId ?? null,

            inventory_node:
                item.inventoryNode ?? null,

            xml_payload:
                item.raw ?? null,

            voucher_type_name:
                header.voucherTypeName ?? null,

            voucher_type:
                header.voucherType ?? null,

            voucher_number:
                header.voucherNumber ?? null,

            voucher_date:
                header.voucherDate || null,

            effective_date:
                header.effectiveDate || null,

            reference:
                header.reference || null,

            narration:
                header.narration || null,

            party_ledger_name:
                header.partyLedger || null,

            party_gstin:
                header.gstin || null,

            place_of_supply:
                header.placeOfSupply || null,

            gst_registration_type:
                header.gstRegistrationType || null,

            persisted_view:
                header.persistedView || null,

            is_invoice:
                header.isInvoice === "Yes",

            is_cancelled:
                header.isCancelled === "Yes",

            is_optional:
                header.isOptional === "Yes",

            is_deleted:
                header.isDeleted === "Yes",

            ledger_name:
                item.ledgerName ?? null,

            discount:
                item.discount ?? null,

            additional_amount:
                item.additionalAmount ?? null,

            batch_rate:
                item.batchRate ?? null,

            batch_rate_value:
                item.batchRateValue ?? null,

            batch_amount:
                item.batchAmount ?? null

        });

    }

    return rows;

}

module.exports = {

    buildStockVoucherRows

};