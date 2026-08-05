const StockGroupRowBuilder =
    require("./StockGroupRowBuilder");

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

    const now = new Date().toISOString();

   const rows = [];

    for (const group of stockGroups) {


    const row =

    StockGroupRowBuilder.build({

        group,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    });

   if (row.guid) {

    rows.push(row);

    } else {

        console.warn(

            `[${company_code}] [${tally_owner}] Skipping Stock Group "${row.name}" because GUID is missing.`

        );

    }

}



    return await BatchManager.run({

        batch_id:

            sync_batch_id,

        module:

            "MASTER",

        entity:

            "STOCK_GROUP",

        table:

            "tally_sync_stock_groups",

        company_code,

        tally_owner,

        sync_batch_id,

        rows

    });

}


module.exports = {

    saveStockGroups

};