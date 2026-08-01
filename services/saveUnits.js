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

        if (!sync_batch_id) {

        throw new Error(
            "sync_batch_id missing in saveUnits"
        );

    }

   if (!Array.isArray(units) || units.length === 0) {

    return {

        total: 0,

        success: 0,

        failed: 0,

        skipped: true

    };

}

    const now = new Date().toISOString();

    const withGuid = [];

    const withoutGuid = [];

    for (const unit of units) {

        const row = {

            company_code,
            tally_owner,

            guid: unit.guid?.trim() || null,

            masterid:
            (
                unit.masterId
                ??
                unit.master_id
                ??
                unit.masterid
            )?.toString()
            ||
            null,

            alterid:
                unit.alterId
                ??
                unit.alter_id
                ??
                unit.alterid
                ??
                null,

            name: unit.name?.trim() || null,
            formal_name:
            (
                unit.formalName
                ??
                unit.formal_name
            )?.trim()
            ||
            null,

          decimal_places:
            (
                unit.decimalPlaces
                ??
                unit.decimal_places
            )
            ==
            null
            ?
            null
            :
            Number(
                unit.decimalPlaces
                ??
                unit.decimal_places
            ),

            is_deleted: false,

            last_synced_at: now,
            sync_batch_id,

            updated_at: now

        };

       if (row.guid) {

            withGuid.push(row);

        }
        else {

            withoutGuid.push(row);

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


    if (withoutGuid.length > 0) {

    const { error } = await supabase

        .from("tally_sync_units")

        .upsert(
            withoutGuid,
            {
                onConflict:
                "company_code,tally_owner,name"
            }
        );

    if (error) {

        throw new Error(
            "Failed to save Units (NAME Upsert): "
            +
            error.message
        );

    }

    success += withoutGuid.length;

}

   return {

    total:
        units.length,

    success,

    failed:
        units.length - success,

    sync_batch_id

};

}

module.exports = {

    saveUnits

};