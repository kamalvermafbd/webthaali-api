const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);



async function applyMasterActions({

    table,

    company_code,

    tally_owner,

    sync_batch_id,

    reconciliationResult

}) {


    if (!reconciliationResult) {

        throw new Error(
            "Reconciliation result missing"
        );

    }



    const result = {

        alterUpdated: 0,

        extraMarkedDeleted: 0,

        missingCount:
            reconciliationResult.missingGuids?.length || 0

    };



    const now =
        new Date().toISOString();



    // ============================
    // ALTER ID UPDATE
    // ============================

    const alterChanged =
        reconciliationResult.alterChanged || [];



    for (const item of alterChanged) {


        const {

            error

        } = await supabase


        .from(table)


        .update({

            alter_id:
                item.newAlterId,

            sync_batch_id,

            last_synced_at:
                now,

            updated_at:
                now

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
                "Failed alter update : "
                +
                error.message
            );

        }


        result.alterUpdated++;


    }




    // ============================
    // EXTRA GUID SOFT DELETE
    // ============================


    const extraGuids =
        reconciliationResult.extraGuids || [];



    for (const item of extraGuids) {


        const {

            error

        } = await supabase


        .from(table)


        .update({

            is_deleted:true,

            sync_batch_id,

            updated_at:
                now

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
        )

         .eq(
            "is_deleted",
            false
        );

        if (error) {

            throw new Error(
                "Failed extra GUID delete : "
                +
                error.message
            );

        }



        result.extraMarkedDeleted++;


    }



    return result;


}



module.exports = {

    applyMasterActions

};