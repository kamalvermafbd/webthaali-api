const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveUnits({
    company_code,
    tally_owner,
    sync_batch_id,
    units = []
}) {

    if (!Array.isArray(units) || units.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];

    for (const unit of units) {

        const row = {

            company_code,
            tally_owner,

            guid: unit.guid?.trim() || null,
            masterid: unit.masterid ?? null,
            alterid: unit.alterid ?? null,
         

            name: unit.name?.trim() || null,
            formal_name: unit.formalName?.trim() || null,
            decimal_places:
    unit.decimalPlaces === "" ? null : Number(unit.decimalPlaces),

            is_deleted: false,

            last_synced_at: now,
            sync_batch_id,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

        } else {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Unit "${row.name}" because GUID is missing.`
            );

        }

    }

    let success = 0;

        if (withGuid.length > 0) {

           // console.log(JSON.stringify(withGuid, null, 2));

        const { error } = await supabase

            .from("tally_sync_units")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save Units (GUID Upsert): " +
                error.message
            );

        }

        success += withGuid.length;

    }

    return {

        total: units.length,

        success,

        failed: units.length - success

    };

}

module.exports = {

    saveUnits

};