const SnapshotManager =
    require("../../sync-engine/SnapshotManager");

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


    console.log("================================");
console.log("SNAPSHOT CHUNK");
console.log("ENTITY :", entity_type);
console.log("MODULE :", module);
console.log("ROWS RECEIVED :", rows.length);
console.log("FIRST GUID :", rows[0]?.guid);
console.log("LAST GUID :", rows[rows.length - 1]?.guid);
console.log("================================");



    // =========================
    // PREPARE ROWS
    // =========================

/* 180826
    const snapshotRows = rows

    .filter(
        row => row.guid
    )

    .map(row => ({

        sync_batch_id,

        last_sync_batch_id:
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
            row.alterid
            ??
            null,


        master_id:
            row.masterId
            ??
            row.master_id
            ??
            row.masterid
            ??
            null


    }));

*/


const snapshotRows = rows
    .filter(row =>
        entity_type === "VOUCHER"
            ? row.header?.guid
            : row.guid
    )
    .map(row => ({

        sync_batch_id,

        last_sync_batch_id:
            sync_batch_id,

        company_code,

        tally_owner,

        module,

        entity_type,

        guid:
            entity_type === "VOUCHER"
                ? row.header?.guid
                : row.guid,

        alter_id:
            entity_type === "VOUCHER"
                ? row.header?.alterid
                : (
                    row.alterId ??
                    row.alter_id ??
                    row.alterid ??
                    null
                ),

        master_id:
            entity_type === "VOUCHER"
                ? row.header?.masterid
                : (
                    row.masterId ??
                    row.master_id ??
                    row.masterid ??
                    null
                )

    }));
    
const fs = require("fs");

/*
fs.writeFileSync(

    `./logs/01-${entity_type}-incoming-snapshot.json`,

    JSON.stringify(

        {

            sync_batch_id,
company_code,
tally_owner,

            entity_type,

            module,

            rowsReceived: rows.length,

            snapshotRows: snapshotRows.length,

            data: snapshotRows

        },

        null,

        2

    )

);
*/

    if (snapshotRows.length === 0) {

        return {

            inserted:0,

            skipped:true,

            reason:
            "No valid GUID rows"

        };

    }

 // =========================
// UPSERT CHUNK
// =========================

/* 04.08.26
const {

    error

} = await supabase

    .from(
        "tally_sync_snapshot"
    )

    .upsert(
        snapshotRows,
        {
            onConflict:
            "company_code,tally_owner,module,entity_type,guid"
        }
    );


    if (error) {

        throw new Error(

            "Snapshot upsert failed : "

            +

            error.message

        );

    }
*/
await SnapshotManager.saveSnapshotRows({

    rows: snapshotRows

});


    return {


        success:true,


        inserted:

            snapshotRows.length


    };


}

// ======================================================
// REMOVE MISSING SNAPSHOT GUIDS
// ======================================================
async function removeMissingSnapshotGuids({

    sync_batch_id,

    company_code,

    tally_owner,

    module,

    entity_type

}) {

/*
    const {
        error
    } = await supabase

        .from("tally_sync_snapshot")

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
        )
        .neq(
            "last_sync_batch_id",
            sync_batch_id
        );


    if (error) {

        throw new Error(
            "Snapshot cleanup failed : "
            +
            error.message
        );

    }
*/


    await SnapshotManager.removeMissingGuids({

        company_code,

        tally_owner,

        module,

        entity_type,

        sync_batch_id

    });

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

/*
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
*/

    await SnapshotManager.clearSnapshot({

        company_code,

        tally_owner,

        module,

        entity_type

    });

}

module.exports = {

    saveSyncSnapshotChunk,

    clearSyncSnapshot,

    removeMissingSnapshotGuids

};