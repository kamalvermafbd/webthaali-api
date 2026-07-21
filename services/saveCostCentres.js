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

    if (!Array.isArray(costCentres) || costCentres.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];

    for (const costCentre of costCentres) {

        const row = {

            company_code,
            tally_owner,

            guid: costCentre.guid?.trim() || null,
            masterid: costCentre.masterid ?? null,
            alterid: costCentre.alterid ?? null,

            name: costCentre.name?.trim() || null,
            parent: costCentre.parent?.trim() || null,
            category: costCentre.category?.trim() || null,
            reserved_name: costCentre.reservedName?.trim() || null,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

        } else {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Cost Centre "${row.name}" because GUID is missing.`
            );

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

    return {

        total: costCentres.length,

        success,

        failed: costCentres.length - success

    };

}

module.exports = {

    saveCostCentres

};