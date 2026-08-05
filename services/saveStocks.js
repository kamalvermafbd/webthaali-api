const BatchManager =
    require("../sync-engine/BatchManager");

const StockRowBuilder =
    require("./StockRowBuilder");

async function saveStocks({
    company_code,
    tally_owner,
    sync_batch_id,
    stocks = []
}) {

    if (!sync_batch_id) {

    throw new Error(
        "sync_batch_id missing in saveStocks"
    );

}

    if (!Array.isArray(stocks) || stocks.length === 0) {

      return {

            total: 0,

            success: 0,

            failed: 0,

            skipped: true

        };

    }

    const now = new Date().toISOString();

    const rows = [];



// ===========================
// BUILD ROWS
// ===========================

    for (const stock of stocks) {

       const row = StockRowBuilder.build({

            stock,

            company_code,

            tally_owner,

            sync_batch_id,

            now

        });

        if (row.guid) {

            rows.push(row);

        } else {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Stock "${row.name}" because GUID is missing.`
            );

        }

    }

    return await BatchManager.run({

    batch_id:

        sync_batch_id,

    module:

        "MASTER",

    entity:

        "STOCK",

    table:

        "tally_sync_stocks",

    company_code,

    tally_owner,

    sync_batch_id,

    rows

});

}

module.exports = {

    saveStocks

};