const BatchManager =
    require("../sync-engine/BatchManager");


async function saveCostCentres({
    company_code,
    tally_owner,
    sync_batch_id,
    costCentres = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveCostCentres"
    );

}

    if (!Array.isArray(costCentres) || costCentres.length === 0) {

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

        "COST_CENTRE",

    company_code,

    tally_owner,

    sync_batch_id,

    rows: costCentres

});
}

module.exports = {

    saveCostCentres

};