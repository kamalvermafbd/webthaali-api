const LedgerRowBuilder =
    require("./LedgerRowBuilder");


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

const rows = [];

const now =

    new Date().toISOString();

for (const ledger of ledgers) {

    const row =

        LedgerRowBuilder.build({

            ledger,

            company_code,

            tally_owner,

            sync_batch_id,

            now

        });

    if (row.guid) {

        rows.push(row);

    } else {

        console.warn(

            `[${company_code}] [${tally_owner}] Skipping Ledger "${row.name}" because GUID is missing.`

        );

    }

}

return await BatchManager.run({

    batch_id:

        sync_batch_id,

    module:

        "MASTER",

    entity:

        "LEDGER",

    table:

        "tally_sync_ledgers",

    company_code,

    tally_owner,

    sync_batch_id,

    rows

});

}


module.exports = {

    saveLedgers

};