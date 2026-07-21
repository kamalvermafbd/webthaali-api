//const { supabase } = require("../config/supabase");

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveGroups({
    company_code,
    tally_owner,
    sync_batch_id,
    groups = []
}) {

    if (!Array.isArray(groups) || groups.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];
    const withoutGuid = [];

    for (const group of groups) {

        const row = {

            company_code,
            tally_owner,

            guid: group.guid?.trim() || null,
            alter_id: group.alterId ?? null,
            master_id: group.masterId?.toString() || null,

            name: group.name?.trim(),
            parent: group.parent?.trim() || null,
            reserved_name: group.reservedName?.trim() || null,

            is_revenue: group.isRevenue ?? null,
            is_deemed_positive: group.isDeemedPositive ?? null,

            is_deleted: false,

            last_synced_at: now,
            sync_batch_id,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

        } else {

            withoutGuid.push(row);

        }

    }

    let success = 0;

    // ===========================
    // GUID BASED UPSERT
    // ===========================

    if (withGuid.length > 0) {

        const { error } = await supabase

            .from("tally_sync_groups")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

    throw new Error(
        "Failed to save groups (GUID Upsert): " +
        error.message
    );

}

        success += withGuid.length;

    }

  // ===========================
// NAME BASED FALLBACK
// ===========================

if (withoutGuid.length > 0) {

    const { error } = await supabase

        .from("tally_sync_groups")

        .upsert(
            withoutGuid,
            {
                onConflict: "company_code,tally_owner,name"
            }
        );

    if (error) {

        throw new Error(
            "Failed to save groups (Name Upsert): " +
            error.message
        );

    }

    success += withoutGuid.length;

}

    return {

        total: groups.length,

        success,

        failed: groups.length - success

    };

}

module.exports = {

    saveGroups

};