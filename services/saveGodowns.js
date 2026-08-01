const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveGodowns({
    company_code,
    tally_owner,
    sync_batch_id,
    godowns = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveGodowns"
    );

}

  if (!Array.isArray(godowns) || godowns.length === 0) {

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

    for (const godown of godowns) {

        const row = {

            company_code,
            tally_owner,

         guid:
            godown.guid?.trim() || null,

        masterid:
            (
                godown.masterId
                ??
                godown.master_id
                ??
                godown.masterid
            )?.toString()
            ||
            null,

            alterid:
                godown.alterId
                ??
                godown.alter_id
                ??
                godown.alterid
                ??
                null,

        name:
            godown.name?.trim() || null,

        parent:
            godown.parent?.trim() || null,

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

            .from("tally_sync_godowns")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save godowns (GUID Upsert): " +
                error.message
            );

        }

        success += withGuid.length;

    }

    if (withoutGuid.length > 0) {

        const { error } = await supabase

            .from("tally_sync_godowns")

            .upsert(
                withoutGuid,
                {
                    onConflict:
                    "company_code,tally_owner,name"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save Godowns (NAME Upsert): "
                +
                error.message
            );

        }

        success += withoutGuid.length;

    }

    return {

        total:
            godowns.length,

        success,

        failed:
            godowns.length - success,

        sync_batch_id

    };

}

module.exports = {

    saveGodowns

};