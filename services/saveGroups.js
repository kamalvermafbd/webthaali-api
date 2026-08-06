
const BatchManager =
    require("../sync-engine/BatchManager");

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


    const result =

    await BatchManager.run({

        batch_id:

            sync_batch_id,

        entity:

            "GROUP",

        company_code,

        tally_owner,

        sync_batch_id,

        rows: groups

    });
  
   return result;


}



module.exports = {

    saveGroups

};