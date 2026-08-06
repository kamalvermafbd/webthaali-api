const BatchManager =
    require("../sync-engine/BatchManager");

async function saveStockGroups({
    company_code,
    tally_owner,
    sync_batch_id,
    stockGroups = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveStockGroups"
    );

    }


    if (!Array.isArray(stockGroups) || stockGroups.length === 0) {

       return {
        total: 0,
        success: 0,
        failed: 0,
        skipped:true
    };

    }

    return await BatchManager.run({

        batch_id:

            sync_batch_id,

        entity:

            "STOCK_GROUP",

        company_code,

        tally_owner,

        sync_batch_id,

        rows: stockGroups

    });

}


module.exports = {

    saveStockGroups

};