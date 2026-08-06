
const BatchManager =
    require("../sync-engine/BatchManager");


async function saveLedgers({
    company_code,
    tally_owner,
    sync_batch_id,
    ledgers = []
}) {

    if (!sync_batch_id) {

    throw new Error(

        "sync_batch_id missing in saveLedgers"

    );

}

    if (!Array.isArray(ledgers) || ledgers.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }



return await BatchManager.run({

    batch_id:

        sync_batch_id,

    entity:

        "LEDGER",

    company_code,

    tally_owner,

    sync_batch_id,

    rows: ledgers

});

}


module.exports = {

    saveLedgers

};