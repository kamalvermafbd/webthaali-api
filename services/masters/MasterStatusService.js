const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);



async function updateMasterStatus({

    sync_batch_id,

    module = "MASTERS",

    action = "COMPLETED"

}) {


    if (!sync_batch_id) {

        throw new Error(
            "sync_batch_id missing"
        );

    }



    const now =
        new Date().toISOString();



    const { error } = await supabase

        .from("sync_batches")

        .update({

            current_module:
                module,

            current_action:
                action,

            last_successful_module:
                module,

            last_activity_at:
                now

        })

        .eq(
            "batch_id",
            sync_batch_id
        );



    if (error) {

        throw new Error(
            "Failed to update master status : "
            +
            error.message
        );

    }



    return {

        success:true,

        sync_batch_id,

        module,

        action

    };

}



module.exports = {

    updateMasterStatus

};