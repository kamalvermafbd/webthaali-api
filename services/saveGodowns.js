const BatchManager =
    require("../sync-engine/BatchManager");


async function saveGodowns({
    company_code,
    tally_owner,
    sync_batch_id,
    godowns = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveGodowns"
    );

}

  if (!Array.isArray(godowns) || godowns.length === 0) {

    return {

        total: 0,

        success: 0,

        failed: 0,

        skipped: true

    };

}


  return await BatchManager.run({

    batch_id:

        sync_batch_id,

    entity:

        "GODOWN",

    company_code,

    tally_owner,

    sync_batch_id,

    rows: godowns

});
}

module.exports = {

    saveGodowns

};