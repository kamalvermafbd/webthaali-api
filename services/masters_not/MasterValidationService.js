const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);



async function validateMasters({

    table,

    company_code,

    tally_owner,

    rows = []

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



    if (
        !Array.isArray(rows) ||
        rows.length === 0
    ) {

        return {

            newRows: [],

            changedRows: [],

            unchangedRows: []

        };

    }



    // ============================
    // LOAD EXISTING DB DATA
    // ============================


    const {

        data: existingRows,

        error

    } = await supabase

        .from(table)

        .select(
            "guid,alter_id,master_id"
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
            "Failed loading master data : "
            +
            error.message
        );

    }



    const dbMap = new Map();



    for (const row of existingRows || []) {


        if (!row.guid)
            continue;


        dbMap.set(

            row.guid,

            {

                alter_id:
                    row.alter_id,

                master_id:
                    row.master_id

            }

        );

    }




    const newRows = [];

    const changedRows = [];

    const unchangedRows = [];




    // ============================
    // VALIDATE TALLY ROWS
    // ============================


    for (const row of rows) {


        const guid =
            row.guid?.trim();



        if (!guid) {

            continue;

        }



        const existing =
            dbMap.get(guid);



        // =====================
        // NEW MASTER
        // =====================

        if (!existing) {


            newRows.push(row);


            continue;

        }




        // =====================
        // ALTER ID CHANGE
        // =====================


        if (

            String(existing.alter_id)

            !==

            String(row.alterId)

        ) {


            changedRows.push({

                ...row,

                oldAlterId:
                    existing.alter_id,

                newAlterId:
                    row.alterId ?? null

            });


            continue;

        }




        // =====================
        // NO CHANGE
        // =====================


        unchangedRows.push(row);


    }




    return {


        total:
            rows.length,


        newRows,


        changedRows,


        unchangedRows,


        summary: {

            new:
                newRows.length,


            changed:
                changedRows.length,


            unchanged:
                unchangedRows.length

        }


    };


}



module.exports = {

    validateMasters

};