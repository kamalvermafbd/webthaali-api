const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

console.log("✅ saveGroups.js LOADED");

async function saveGroups({

    company_code,

    tally_owner,

    sync_batch_id,

    groups = []

}) {

     console.log("🚀 saveGroups() CALLED");
    console.log({
        company_code,
        tally_owner,
        sync_batch_id,
        totalGroups: groups.length
    });

    if (!sync_batch_id) {

        throw new Error(
            "sync_batch_id missing in saveGroups"
        );

    }


    if (
        !Array.isArray(groups) ||
        groups.length === 0
    ) {

        return {

            total: 0,

            success: 0,

            failed: 0,

            skipped: true

        };

    }



    const now =
        new Date().toISOString();



    const withGuid = [];

    const withoutGuid = [];



    for (const group of groups) {


        const row = {


            company_code,

            tally_owner,


            guid:
                group.guid?.trim() || null,


            alter_id:
                group.alterId ?? null,


            master_id:
                group.masterId?.toString() || null,



            name:
                group.name?.trim(),


            parent:
                group.parent?.trim() || null,


            reserved_name:
                group.reservedName?.trim() || null,



            parent_guid:
                group.parentGuid?.trim() || null,


            parent_master_id:
                group.parentMasterId?.toString() || null,


            parent_alter_id:
                group.parentAlterId ?? null,



            is_subledger:
                group.isSubledger ?? false,


            is_billwise_on:
                group.isBillwiseOn ?? false,


            track_negative_balances:
                group.trackNegativeBalances ?? false,


            is_condensed:
                group.isCondensed ?? false,



            is_revenue:
                group.isRevenue ?? null,


            is_deemed_positive:
                group.isDeemedPositive ?? null,



            is_deleted:false,


            created_at:
                now,


            last_synced_at:
                now,


            sync_batch_id,


            updated_at:
                now

        };



        if (row.guid) {

            withGuid.push(row);

        }
        else {

            withoutGuid.push(row);

        }

    }



    let success = 0;



    // =========================
    // GUID UPSERT
    // =========================

    if (withGuid.length > 0) {

console.log("Saving GUID groups:", withGuid.length);
        const {

            error

        } = await supabase


        .from(
            "tally_sync_groups"
        )


        .upsert(

            withGuid,

            {

                onConflict:
                "company_code,tally_owner,guid"

            }

        );

console.log("GUID UPSERT ERROR:", error);

        if (error) {

            throw new Error(

                "Groups GUID save failed : "
                +
                error.message

            );

        }



        success += withGuid.length;

    }




    // =========================
    // NAME FALLBACK
    // =========================

    if (withoutGuid.length > 0) {

console.log("Saving NAME groups:", withoutGuid.length);
        const {

            error

        } = await supabase


        .from(
            "tally_sync_groups"
        )


        .upsert(

            withoutGuid,

            {

                onConflict:
                "company_code,tally_owner,name"

            }

        );

console.log("NAME UPSERT ERROR:", error);

        if (error) {


            throw new Error(

                "Groups NAME save failed : "
                +
                error.message

            );


        }



        success += withoutGuid.length;


    }




    return {


        total:
            groups.length,


        success,


        failed:
            groups.length - success,


        sync_batch_id


    };


}



module.exports = {

    saveGroups

};