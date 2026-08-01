const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveCostCentres({
    company_code,
    tally_owner,
    sync_batch_id,
    costCentres = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveCostCentres"
    );

}

    if (!Array.isArray(costCentres) || costCentres.length === 0) {

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

    for (const costCentre of costCentres) {

        const row = {

            company_code,
            tally_owner,

           guid:
                costCentre.guid?.trim() || null,

            masterid:
            (
                costCentre.masterId
                ??
                costCentre.master_id
                ??
                costCentre.masterid
            )?.toString()
            ||
            null,

            alterid:
                costCentre.alterId
                ??
                costCentre.alter_id
                ??
                costCentre.alterid
                ??
                null,

            name:
                costCentre.name?.trim() || null,

            parent:
                costCentre.parent?.trim() || null,

            category:
                costCentre.category?.trim() || null,

            reserved_name:
            (
                costCentre.reservedName
                ??
                costCentre.reserved_name
            )?.trim()
            ||
            null,

            is_deleted:
                false,

            last_synced_at:
                now,

            sync_batch_id,

            updated_at:
                now
                

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

        const { error } = await supabase

            .from("tally_sync_cost_centres")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save cost centres (GUID Upsert): " +
                error.message
            );

        }

        success += withGuid.length;

    }

    if (withoutGuid.length > 0) {

    const { error } = await supabase

        .from("tally_sync_cost_centres")

        .upsert(
            withoutGuid,
            {
                onConflict:
                "company_code,tally_owner,name"
            }
        );

    if (error) {

        throw new Error(
            "Failed to save Cost Centres (NAME Upsert): "
            +
            error.message
        );

        }

        success += withoutGuid.length;

    }

    return {

        total:
            costCentres.length,

        success,

        failed:
            costCentres.length - success,

        sync_batch_id

    };

}

module.exports = {

    saveCostCentres

};