const { createClient } =
    require("@supabase/supabase-js");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

const SnapshotManager =
    require("./SnapshotManager");


const {

    VALIDATION_SELECT_COLUMNS

} = require("./constants");


  // TODO
// Enable after queue execution integration.
//
// const BatchQueue =
//     require("./BatchQueue");
//
// const BatchExecutor =
//     require("./BatchExecutor");



class ReconciliationManager {


    
buildGuidMap(rows = []) {

    const map = new Map();

    for (const row of rows) {

        const guid =

    String(

        row.guid || ""

            ).trim();

        if (!guid) {

            continue;

        }

        map.set(

            guid,

            row

        );

       


    }

    return map;

}

    // ----------------------------------
    // Reconcile
    // ----------------------------------

    async reconcile({

        table,

        company_code,

        tally_owner,

        module,

        entity_type,

        sync_batch_id

    }) {

        if (!table) {

            throw new Error(

                "table is required"

            );

        }

        if (!company_code) {

            throw new Error(

                "company_code is required"

            );

        }

        if (!tally_owner) {

            throw new Error(

                "tally_owner is required"

            );

        }

        if (!module) {

            throw new Error(

                "module is required"

            );

        }

        if (!sync_batch_id) {

            throw new Error(

                "sync_batch_id is required"

            );

        }

        const snapshotRows =

            await SnapshotManager.loadSnapshot({

                company_code,

                tally_owner,

                module,

                entity_type

            });

                const {

                    data: dbRows,

                    error

                } = await supabase

                    .from(table)

                    .select(

                        VALIDATION_SELECT_COLUMNS[table] || "*"

                    )

                    .eq(

                        "company_code",

                        company_code

                    )

                    .eq(

                        "tally_owner",

                        tally_owner

                    )

                    .eq(

                        "is_deleted",

                        false

                    );

                if (error) {

                    throw new Error(

                        "Failed to load database rows : " +

                        error.message

                    );

                }

                if (

                    snapshotRows.length === 0 &&

                    (dbRows?.length || 0) === 0

                ) {

                    return {

                        success: true,

                        table,

                        module,

                        entity_type,

                        sync_batch_id,

                        snapshotCount: 0,

                        dbCount: 0,

                        missingGuids: [],

                        extraGuids: [],

                        alterChanged: [],

                        summary: {

                            missing: 0,

                            extra: 0,

                            alterChanged: 0

                        }

                    };

                }




/*
    const snapshotMap =

            new Map();

        for (const row of snapshotRows) {

            if (!row.guid) {

                continue;

            }

            snapshotMap.set(

                row.guid,

                row

            );

        }

    const dbMap =

            new Map();

        for (const row of dbRows || []) {

            if (!row.guid) {

                continue;

            }

            dbMap.set(

                row.guid,

                row

            );

        }
*/

        const snapshotMap =
            this.buildGuidMap(snapshotRows);

       const dbMap =

    this.buildGuidMap(

        dbRows || []

    );
            
        const missingGuids = [];

        const extraGuids = [];

        const alterChanged = [];

        // ----------------------------------
        // Snapshot -> DB
        // ----------------------------------

        for (const [guid, snapshot] of snapshotMap) {

            if (!dbMap.has(guid)) {

                missingGuids.push(snapshot);

                continue;

            }

            const dbRow = dbMap.get(guid);

// TODO
// Extract common alter-id resolver
// after all reconciliation modules
// are migrated.

          const dbAlterId =

                dbRow.alter_id ??

                dbRow.alterid ??

                null;

            const snapshotAlterId =

                snapshot.alter_id ??

                snapshot.alterid ??

                null;

                if (

                    String(dbAlterId)

                    !==

                    String(snapshotAlterId)

                ) {

                    alterChanged.push({

                        guid,

                        oldAlterId:

                            dbAlterId,

                        newAlterId:

                            snapshotAlterId

                    });

                }

        }

        // ----------------------------------
        // DB -> Snapshot
        // ----------------------------------

        for (const [guid, dbRow] of dbMap) {

            if (!snapshotMap.has(guid)) {

                extraGuids.push(dbRow);

            }

        }



        return {

            success: true,

            table,

            module,

            entity_type,

            sync_batch_id,

            snapshotCount:

                snapshotRows.length,

            dbCount:

                dbRows?.length || 0,

            missingGuids,

            extraGuids,

            alterChanged,

            summary: {

                missing:

                    missingGuids.length,

                extra:

                    extraGuids.length,

                alterChanged:

                    alterChanged.length

            }

        };


    }

}

module.exports =
    new ReconciliationManager();