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

    if (!Array.isArray(godowns) || godowns.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];

    for (const godown of godowns) {

        const row = {

            company_code,
            tally_owner,

            guid: godown.guid?.trim() || null,
            masterid: godown.masterid ?? null,
            alterid: godown.alterid ?? null,

            name: godown.name?.trim() || null,
            parent: godown.parent?.trim() || null,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

        } else {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Godown "${row.name}" because GUID is missing.`
            );

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

    return {

        total: godowns.length,

        success,

        failed: godowns.length - success

    };

}

module.exports = {

    saveGodowns

};