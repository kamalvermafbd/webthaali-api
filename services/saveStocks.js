const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveStocks({
    company_code,
    tally_owner,
    sync_batch_id,
    stocks = []
}) {

    if (!Array.isArray(stocks) || stocks.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];

    for (const stock of stocks) {

        const row = {

            company_code,
            tally_owner,

            guid: stock.guid?.trim() || null,
            masterid: stock.masterid ?? null,
            alterid: stock.alterid ?? null,

            name: stock.name?.trim() || null,
            parent: stock.parent?.trim() || null,
            base_unit: stock.baseUnit?.trim() || null,

            hsn_code: stock.hsnCode?.trim() || null,

            gst_applicable: stock.gstApplicable?.trim() || null,
            type_of_supply: stock.typeOfSupply?.trim() || null,

            taxability: stock.taxability?.trim() || null,
            state_name: stock.stateName?.trim() || null,

            applicable_from: stock.applicableFrom || null,

            cgst: stock.cgst === "" ? null : Number(stock.cgst),
            sgst: stock.sgst === "" ? null : Number(stock.sgst),
            igst: stock.igst === "" ? null : Number(stock.igst),

            is_deleted: false,

            last_synced_at: now,
            sync_batch_id,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

        } else {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Stock "${row.name}" because GUID is missing.`
            );

        }

    }

    let success = 0;

    if (withGuid.length > 0) {

        const { error } = await supabase

            .from("tally_sync_stocks")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save Stocks (GUID Upsert): " +
                error.message
            );

        }

        success += withGuid.length;

    }

    return {

        total: stocks.length,

        success,

        failed: stocks.length - success

    };

}

module.exports = {

    saveStocks

};