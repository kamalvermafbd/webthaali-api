const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);



async function reconcileMasters({

    table,

    entity_type,

    company_code,

    tally_owner,

    sync_batch_id,

//    tallyRows = []

}) {


    if (!table) {

        throw new Error(
            "Master table missing"
        );

    }


    if (!company_code || !tally_owner) {

        throw new Error(
            "Company code or tally owner missing"
        );

    }


/*
    if (
        !Array.isArray(tallyRows) ||
        tallyRows.length === 0
    ) {

        return {

            skipped: true,

            reason:
                "No tally master data received"

        };

    }
*/

// ============================
// LOAD TALLY SNAPSHOT DATA
// ============================

const {

    data: snapshotRows,

    error: snapshotError

} = await supabase

.from("tally_sync_snapshot")

.select(
    "guid,alter_id,master_id"
)

.eq(
    "sync_batch_id",
    sync_batch_id
)

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
    "MASTER"
)

.eq(
    "entity_type",
    entity_type
);


if (snapshotError) {

    throw new Error(
        "Failed loading snapshot : "
        +
        snapshotError.message
    );

}


if (
    !snapshotRows ||
    snapshotRows.length === 0
) {

    return {

        skipped:true,

        reason:
        "No snapshot master data found"

    };

}


    // ============================
    // TALLY GUID MAP
    // ============================


    const tallyMap = new Map();


    for (const row of snapshotRows) {


        if (!row.guid)
            continue;


        tallyMap.set(

            row.guid,

            {

                alter_id:
                    row.alter_id ?? null,

                master_id:
                    row.master_id ?? null

            }

        );

    }



    const tallyGuids =
        Array.from(
            tallyMap.keys()
        );



    // ============================
    // DB FETCH
    // ============================


    const {

        data: dbRows,

        error

    } = await supabase


    .from(table)


    .select(
        "guid,alter_id,master_id,is_deleted"
    )


    .eq(
        "company_code",
        company_code
    )


    .eq(
        "tally_owner",
        tally_owner
    );



    if (error) {

        throw new Error(
            "Failed to fetch master data : "
            +
            error.message
        );

    }



    const dbMap = new Map();


    for (const row of dbRows || []) {


        if (!row.guid)
            continue;

        if (
            row.is_deleted === true
        ) {
            continue;
        }

        dbMap.set(

            row.guid,

            {

                alter_id:
                    row.alter_id,

                master_id:
                    row.master_id,

                is_deleted:
                    row.is_deleted

            }

        );

    }



    const missingGuids = [];

    const extraGuids = [];

    const alterChanged = [];




    // ============================
    // CHECK TALLY GUIDS
    // ============================


    for (const [guid, tallyData] of tallyMap) {


        if (!dbMap.has(guid)) {


            missingGuids.push({

                guid,

                ...tallyData

            });


            continue;

        }



        const dbData =
            dbMap.get(guid);



        if (

            String(dbData.alter_id)
            !==
            String(tallyData.alter_id)

        ) {


            alterChanged.push({

                guid,

                oldAlterId:
                    dbData.alter_id,

                newAlterId:
                    tallyData.alter_id

            });


        }


    }




    // ============================
    // CHECK EXTRA DB GUIDS
    // ============================


    for (const [guid, dbData] of dbMap) {


        if (!tallyMap.has(guid)) {


            extraGuids.push({

                guid,

                alter_id:
                    dbData.alter_id

            });


        }


    }


// ============================
// UPDATE ALTER ID CHANGES
// ============================
/*
if (alterChanged.length > 0) {

for (const item of alterChanged) {

    const { error } = await supabase

        .from(table)

        .update({

            alter_id: item.newAlterId,

            sync_batch_id,

            last_synced_at:
                new Date().toISOString(),

            updated_at:
                new Date().toISOString()

        })

        .eq(
            "company_code",
            company_code
        )

        .eq(
            "tally_owner",
            tally_owner
        )

        .eq(
            "guid",
            item.guid
        );


    if (error) {

        throw new Error(
            "Failed to update alter_id : "
            +
            error.message
        );

    }

}

}
*/

    return {


        table,

        sync_batch_id,


        tallyCount:
            tallyGuids.length,


        dbCount:
            dbMap.size,


        missingGuids,


        extraGuids,


        alterChanged,


        summary: {

            missing:
                missingGuids.length,

            extra:
                extraGuids.length,

            alterChanged:
                alterChanged.length

        }


    };


}



module.exports = {

    reconcileMasters

};