const BatchManager =
    require("../sync-engine/BatchManager");

const CostCentreRowBuilder =
    require("./CostCentreRowBuilder");

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

    const now = new Date().toISOString();

    const rows = [];

    for (const costCentre of costCentres) {

        const row =

        CostCentreRowBuilder.build({

            costCentre,

            company_code,

            tally_owner,

            sync_batch_id,

            now

        });

       if (row.guid) {

            rows.push(row);

        } else {

            console.warn(

                `[${company_code}] [${tally_owner}] Skipping Cost Centre "${row.name}" because GUID is missing.`

            );

        }

    }

    return await BatchManager.run({

        batch_id:

            sync_batch_id,

        module:

            "MASTER",

        entity:

            "COST_CENTRE",

        table:

            "tally_sync_cost_centres",

        company_code,

        tally_owner,

        sync_batch_id,

        rows

    });

}

module.exports = {

    saveCostCentres

};