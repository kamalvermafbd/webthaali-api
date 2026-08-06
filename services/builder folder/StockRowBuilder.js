// ======================================================
// STOCK ROW BUILDER
// ======================================================
//
// Converts raw Tally Stock
// into database row.
//
// Shared by:
// saveStocks.js
// BatchManager
//
// ======================================================

class StockRowBuilder {

    build({

        stock,

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
                stock.guid?.trim() || null,

            masterid:
                (
                    stock.masterId
                    ??
                    stock.masterid
                )?.toString()
                ||
                null,

            alterid:
                stock.alterId
                ??
                stock.alterid
                ??
                null,

            name:
                stock.name?.trim() || null,

            parent:
                stock.parent?.trim() || null,

            parent_group_guid:
                stock.parentGroupGuid || null,

            parent_group_master_id:
                stock.parentGroupMasterId || null,

            parent_group_alter_id:
                stock.parentGroupAlterId || null,

            base_unit:
                stock.baseUnit?.trim() || null,

            hsn_code:
                stock.hsnCode?.trim() || null,

            gst_applicable:
                stock.gstApplicable?.trim() || null,

            type_of_supply:
                stock.typeOfSupply?.trim() || null,

            taxability:
                stock.taxability?.trim() || null,

            state_name:
                stock.stateName?.trim() || null,

            applicable_from:
                stock.applicableFrom || null,

            cgst:
                stock.cgst === ""
                    ? null
                    : Number(stock.cgst),

            sgst:
                stock.sgst === ""
                    ? null
                    : Number(stock.sgst),

            igst:
                stock.igst === ""
                    ? null
                    : Number(stock.igst),

            gst_rate:
                stock.gstRate === ""
                    ? null
                    : Number(stock.gstRate),

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
    new StockRowBuilder();