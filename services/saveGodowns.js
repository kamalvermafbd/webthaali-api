const GodownRowBuilder =
    require("./GodownRowBuilder");

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

    const now = new Date().toISOString();

    const rows = [];

    for (const godown of godowns) {

        const row =

        GodownRowBuilder.build({

            godown,

            company_code,

            tally_owner,

            sync_batch_id,

            now

        });

     if (row.guid) {

        rows.push(row);

    } else {

        console.warn(

            `[${company_code}] [${tally_owner}] Skipping Godown "${row.name}" because GUID is missing.`

        );

    }

    }

   return await BatchManager.run({

    batch_id:

        sync_batch_id,

    module:

        "MASTER",

    entity:

        "GODOWN",

    table:

        "tally_sync_godowns",

    company_code,

    tally_owner,

    sync_batch_id,

    rows

});

}

module.exports = {

    saveGodowns

};