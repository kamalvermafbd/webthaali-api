const BatchManager =
    require("../sync-engine/BatchManager");
    
const UnitRowBuilder =
    require("./UnitRowBuilder");
    
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

    const now = new Date().toISOString();
    
    const rows = [];

    for (const unit of units) {

        const row =

    UnitRowBuilder.build({

        unit,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    });

      if (row.guid) {

            rows.push(row);

        } else {

            console.warn(

                `[${company_code}] [${tally_owner}] Skipping Unit "${row.name}" because GUID is missing.`

            );

        }

    }

   return await BatchManager.run({

        batch_id:

            sync_batch_id,

        module:

            "MASTER",

        entity:

            "UNIT",

        table:

            "tally_sync_units",

        company_code,

        tally_owner,

        sync_batch_id,

        rows

    });


}

module.exports = {

    saveUnits

};