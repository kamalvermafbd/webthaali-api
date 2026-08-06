const BatchManager =
    require("../sync-engine/BatchManager");
    
async function saveUnits({
    company_code,
    tally_owner,
    sync_batch_id,
    units = []
}) {

        if (!sync_batch_id) {

        throw new Error(
            "sync_batch_id missing in saveUnits"
        );

    }

   if (!Array.isArray(units) || units.length === 0) {

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

        "UNIT",

    company_code,

    tally_owner,

    sync_batch_id,

    rows: units

});


}

module.exports = {

    saveUnits

};