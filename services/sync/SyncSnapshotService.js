const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);





// ======================================================
// SAVE SYNC SNAPSHOT CHUNK
// ======================================================
//
// Generic snapshot storage
//
// Used for:
// VOUCHER
// LEDGER
// GROUP
// STOCK
//
// Chunk wise insert
// No full memory storage
// ======================================================


async function saveSyncSnapshotChunk({

    sync_batch_id,

    company_code,

    tally_owner,

    module,

    entity_type,

    rows = []

}) {


    if (
        !sync_batch_id ||
        !company_code ||
        !tally_owner ||
        !module ||
        !entity_type
    ) {

        throw new Error(
            "Snapshot parameters missing"
        );

    }



    if (
        !Array.isArray(rows) ||
        rows.length === 0
    ) {

        return {

            inserted:0,

            skipped:true,

            reason:
            "No snapshot rows"

        };

    }



    // =========================
    // PREPARE ROWS
    // =========================


    const snapshotRows = rows

    .filter(
        row => row.guid
    )

    .map(row => ({

        sync_batch_id,

        company_code,

        tally_owner,


        module,

        entity_type,


        guid:
            row.guid,


        alter_id:
            row.alterId
            ??
            row.alter_id
            ??
            null,


        master_id:
            row.masterId
            ??
            row.master_id
            ??
            null


    }));




    if (snapshotRows.length === 0) {

        return {

            inserted:0,

            skipped:true,

            reason:
            "No valid GUID rows"

        };

    }


    

    // =========================
    // INSERT CHUNK
    // =========================


    const {

        error

    } = await supabase

        .from(
            "tally_sync_snapshot"
        )

        .insert(
            snapshotRows
        );



    if (error) {

        throw new Error(

            "Snapshot insert failed : "

            +

            error.message

        );

    }




    return {


        success:true,


        inserted:

            snapshotRows.length


    };


}

// ======================================================
// CLEAR OLD SYNC SNAPSHOT
// ======================================================

async function clearSyncSnapshot({

    company_code,

    tally_owner,

    module,

    entity_type

}) {


    const {

        error

    } = await supabase

        .from(
            "tally_sync_snapshot"
        )

        .delete()

        .eq(
            "company_code",
            company_code
        )

        .eq(
            "tally_owner",
            tally_owner
        )

        .eq(
            "module",
            module
        )

        .eq(
            "entity_type",
            entity_type
        );


    if (error) {

        throw new Error(
            "Snapshot delete failed : "
            +
            error.message
        );

    }


    console.log(
        "Old snapshot cleared :",
        module,
        entity_type
    );


}

module.exports = {


    saveSyncSnapshotChunk,

    clearSyncSnapshot


};