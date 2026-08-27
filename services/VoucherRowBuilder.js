// ======================================================
// VOUCHER ROW BUILDER
// ======================================================
//
// Converts raw Tally Voucher Header
// into database row.
//
// Shared by:
// saveVouchers.js
// BatchManager
//
// ======================================================

class VoucherRowBuilder {

    build({

        header,

        stockInCount = null,
        stockOutCount = null,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    }) {

        const timestamp =
            now || new Date().toISOString();

        return {

            company_code,

            tally_owner,

            guid:
                header.guid?.trim() || null,

            masterid:
                header.masterid ?? null,

            alterid:
                header.alterid ?? null,

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

            reference_date:
                header.referenceDate || null,

            party_ledger:
                header.partyLedger || null,

            narration:
                header.narration || null,

            gstin:
                header.gstin || null,

            place_of_supply:
                header.placeOfSupply || null,

            buyer_name:
                header.buyerName || null,

            buyer_address:
                header.buyerAddress || null,

            gst_registration_type:
                header.gstRegistrationType || null,

            persisted_view:
                header.persistedView || null,

            stock_in_count:
                stockInCount,

            stock_out_count:
                stockOutCount,

            is_invoice:
                header.isInvoice === "Yes",

            is_optional:
                header.isOptional === "Yes",

            is_cancelled:
                header.isCancelled === "Yes",

            is_deleted: false,

            created_at:
                timestamp,

            updated_at:
                timestamp,

            last_synced_at:
                timestamp,

            sync_batch_id

        };

    }

}

module.exports =
    new VoucherRowBuilder();