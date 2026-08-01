const { createClient } = require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);



// ======================================================
// MASTER TABLES CLEANUP
// ======================================================

const MASTER_TABLES = [

    "tally_sync_groups",

    "tally_sync_ledgers",

    "tally_sync_units",

    "tally_sync_godowns",

    "tally_sync_cost_centres",

   // "tally_sync_stock_groups",

    //"tally_sync_stocks"

];




// ======================================================
// TODO FUTURE:
//
// Add transaction cleanup also:
//
// - tally_vouchers
// - tally_voucher_ledgers
// - tally_voucher_inventory
// - tally_stock_vouchers
// - tally_costcentres_allocations
// - tally_bill_allocations
//
//
// Before transaction hard delete,
// preserve saveVouchers.js controls:
//
// 1. incremental_post_completed check
// 2. guid_scan_completed check
// 3. Missing voucher recovery flow
// 4. ReconciliationEngine validation
// 5. Skip delete if GUID scan failed
//
// Current service handles only master cleanup.
// ======================================================




async function cleanupDeletedMasters({

    company_code,

    tally_owner,

    sync_batch_id

}) {



    if (
        !company_code ||
        !tally_owner ||
        !sync_batch_id
    ) {

        throw new Error(
            "Cleanup parameters missing"
        );

    }




    // =========================
    // VERIFY BATCH COMPLETED
    // =========================


    const {

        data: batch,

        error: batchError

    } = await supabase

        .from("sync_batches")

        .select(
            "batch_status,current_action,reconciliation_completed"
        )

        .eq(
            "batch_id",
            sync_batch_id
        )

        .single();



    if (batchError) {

        throw batchError;

    }




    if (

        batch.batch_status !== "COMPLETED"

        ||

        batch.current_action !== "COMPLETED"

        ||

        batch.reconciliation_completed !== true

    ) {


        return {


            success:false,


            skipped:true,


            reason:
            "Sync batch reconciliation not completed"


        };


    }





    const result = [];




    // =========================
    // HARD DELETE CLEANUP
    // =========================
    //
    // NOTE:
    //
    // Delete only after:
    // - sync batch completed
    // - reconciliation completed
    // - is_deleted=true records only
    //
    // Transaction tables will be added later.
    // =========================




    for (const table of MASTER_TABLES) {



        const {

            error,

            count


        } = await supabase


            .from(table)


            .delete({

                count:"exact"

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

                "sync_batch_id",

                sync_batch_id

            )


            .eq(

                "is_deleted",

                true

            );




        if (error) {


            throw new Error(

                `Cleanup failed for ${table}: `

                +

                error.message

            );


        }





        result.push({

            table,


            deleted:

                count || 0


        });



    }





    return {


        success:true,


        sync_batch_id,


        cleanedTables:

            result


    };



}




module.exports = {


    cleanupDeletedMasters


};