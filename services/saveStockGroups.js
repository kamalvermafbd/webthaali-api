const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveStockGroups({
    company_code,
    tally_owner,
    sync_batch_id,
    stockGroups = []
}) {

    if (!Array.isArray(stockGroups) || stockGroups.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    /* remove 01.08.26
    const rows = stockGroups.map(group => ({

        company_code,
        tally_owner,

        guid: group.guid?.trim() || null,
        alter_id: group.alter_id ?? null,
        master_id: group.master_id?.toString() || null,

        name: group.name?.trim(),
        parent: group.parent?.trim() || null,
        reserved_name: group.reserved_name?.trim() || null,

        is_deleted: false,

        last_synced_at: now,
        sync_batch_id,

        updated_at: now

    }));

    */

    const withGuid = [];

const withoutGuid = [];


for (const group of stockGroups) {


    const row = {

        company_code,

        tally_owner,


        guid:
            group.guid?.trim() || null,


        alter_id:
            group.alterId
            ??
            group.alter_id
            ??
            null,


        master_id:
            (
                group.masterId
                ??
                group.master_id
            )?.toString()
            ||
            null,


        name:
            group.name?.trim(),


        parent:
            group.parent?.trim()
            ||
            null,


        reserved_name:
            group.reservedName
            ??
            group.reserved_name
            ??
            null,


        is_deleted:false,


        last_synced_at:now,

        sync_batch_id,

        updated_at:now

    };


    if (row.guid) {

        withGuid.push(row);

    }
    else {

        withoutGuid.push(row);

    }

}

    const { error } = await supabase

        .from("tally_sync_stock_groups")

        .upsert(
            rows,
            {
                onConflict: "company_code,tally_owner,guid"
            }
        );

    if (error) {

        throw new Error(
            "Failed to save Stock Groups: " +
            error.message
        );

    }

    return {

    total:
        stockGroups.length,

    success,

    failed:
        stockGroups.length - success,

    sync_batch_id

};

}

if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveStockGroups"
    );

}


if (
    !Array.isArray(stockGroups) ||
    stockGroups.length === 0
) {

    return {

        total:0,

        success:0,

        failed:0,

        skipped:true

    };

}

module.exports = {

    saveStockGroups

};