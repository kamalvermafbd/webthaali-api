const {
    buildLedgerRows
} = require("./VoucherLedgerRowBuilder");


const {
    buildInventoryRows
} = require("./VoucherInventoryRowBuilder");

const {
    buildStockVoucherRows
} = require("./VoucherStockRowBuilder");


const {
    buildBillAllocationRows
} = require("./VoucherBillAllocationRowBuilder");

const {
    buildCostCentreRows
} = require("./VoucherCostCentreRowBuilder");



function buildVoucherRow({

    header,

    company_code,

    tally_owner,

    sync_batch_id,

    now

}) {

    return {

        company_code,

        tally_owner,

        guid: header.guid.trim(),

        masterid: header.masterid ?? null,

        alterid: header.alterid ?? null,

        voucher_type: header.voucherType ?? null,

        voucher_number: header.voucherNumber ?? null,

        voucher_date: header.voucherDate || null,

        effective_date: header.effectiveDate || null,

        reference: header.reference || null,

        reference_date: header.referenceDate || null,

        party_ledger: header.partyLedger || null,

        narration: header.narration || null,

        gstin: header.gstin || null,

        place_of_supply: header.placeOfSupply || null,

        buyer_name: header.buyerName || null,

        buyer_address: header.buyerAddress || null,

        gst_registration_type:
            header.gstRegistrationType || null,

        persisted_view:
            header.persistedView || null,

        is_invoice:
            header.isInvoice === "Yes",

        is_optional:
            header.isOptional === "Yes",

        is_cancelled:
            header.isCancelled === "Yes",

        is_deleted: false,

        last_synced_at: now,

        sync_batch_id,

        updated_at: now

    };

}



module.exports = {

    buildVoucherRow,

    buildLedgerRows,

    buildBillAllocationRows,

    buildCostCentreRows,

    buildInventoryRows,

    buildStockVoucherRows

};