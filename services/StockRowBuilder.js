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
    safeTrim(stock.guid) || null,

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
    safeTrim(stock.name) || null,

            parent:
    safeTrim(stock.parent) || null,

            parent_group_guid:
                stock.parentGroupGuid || null,

            parent_group_master_id:
                stock.parentGroupMasterId || null,

            parent_group_alter_id:
                stock.parentGroupAlterId || null,

            base_unit:
    safeTrim(stock.baseUnit) || null,

            hsn_code:
    safeTrim(stock.hsnCode) || null,

           gst_applicable:
    safeTrim(stock.gstApplicable) || null,

            type_of_supply:
    safeTrim(stock.typeOfSupply) || null,

            taxability:
    safeTrim(stock.taxability) || null,


           state_name:
    safeTrim(stock.stateName) || null,

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