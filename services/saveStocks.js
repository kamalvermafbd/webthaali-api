const BatchManager =
    require("../sync-engine/BatchManager");


async function saveStocks({
    company_code,
    tally_owner,
    sync_batch_id,
    execution_claim_token,
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
return await BatchManager.run({

    batch_id: sync_batch_id,
    execution_claim_token,
    entity:

        "STOCK",

    company_code,

    tally_owner,

    sync_batch_id,

    rows: stocks

});

}

module.exports = {

    saveStocks

};