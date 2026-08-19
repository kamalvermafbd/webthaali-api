const { createClient } =
    require("@supabase/supabase-js");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

const SnapshotManager =
    require("./SnapshotManager");
const fs = require("fs");

const {
    VALIDATION_SELECT_COLUMNS,
    ALTER_ID_COLUMN,
    VOUCHER_RECONCILIATION
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

            /*

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

                    */

                const PAGE_SIZE = 1000;

                const dbRows = [];

                let dbFrom = 0;

                while (true) {

                    const {
                        data,
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
                        )
                        .order(
                            "guid",
                            {
                                ascending: true
                            }
                        )
                        .range(
                            dbFrom,
                            dbFrom + PAGE_SIZE - 1
                        );

                    if (error) {

                        throw new Error(
                            `Failed to load DB rows from ${table}: ` +
                            error.message
                        );

                    }

                    const page =
                        data || [];

                    dbRows.push(
                        ...page
                    );

                    if (
                        page.length < PAGE_SIZE
                    ) {
                        break;
                    }

                    dbFrom += PAGE_SIZE;

                }

               
                
                fs.writeFileSync(

              `./logs/01-${entity_type}-reconciliation-input.json`,

                JSON.stringify(

                    {

                        table,

                        company_code,

                        tally_owner,

                        module,

                        entity_type,

                        sync_batch_id,

                        snapshotCount: snapshotRows.length,

                        dbCount: dbRows?.length || 0,

                        snapshotGuids: snapshotRows.map(r => r.guid),

                        dbGuids: (dbRows || []).map(r => r.guid)

                    },

                    null,

                    2

                )

            );
            

            /* 080826

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
                */

        const snapshotMap =
            this.buildGuidMap(snapshotRows);

       const dbMap =

            this.buildGuidMap(

                dbRows || []

            );

            const TARGET_GUID =
    "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b6e";

fs.writeFileSync(
    `./logs/TARGET-RECON-${entity_type}-${sync_batch_id}.json`,
    JSON.stringify({
        targetGuid: TARGET_GUID,

        snapshotHasTarget:
            snapshotMap.has(TARGET_GUID),

        dbHasTarget:
            dbMap.has(TARGET_GUID),

        snapshotTarget:
            snapshotMap.get(TARGET_GUID) || null,

        dbTarget:
            dbMap.get(TARGET_GUID) || null,

        snapshotCount:
            snapshotRows.length,

        dbCount:
            dbRows?.length || 0
    }, null, 2)
);

// ----------------------------------
// Child Orphan Detection
// ----------------------------------

        const orphanGuids = {};

        if (
            entity_type === "VOUCHER" &&
            table === VOUCHER_RECONCILIATION.ROOT.table
        ) {

        const childTables =
            Object.entries(
                VOUCHER_RECONCILIATION.CHILDREN
            );

        for (const [childTable, config] of childTables) {

            const {
                data: childRows,
                error: childError
            } = await supabase
                .from(childTable)
                .select(config.guidColumn)
                .eq(
                    "company_code",
                    company_code
                )
                .eq(
                    "tally_owner",
                    tally_owner
                );

            if (childError) {

                throw new Error(
                    `Failed to load child rows for orphan detection (${childTable}) : ` +
                    childError.message
                );

            }

            const orphanSet =
                new Set();

            for (const row of childRows || []) {

                const voucherGuid =
                    String(
                        row[config.guidColumn] || ""
                    ).trim();

                if (
                    voucherGuid &&
                    !dbMap.has(voucherGuid)
                ) {

                    orphanSet.add(
                        voucherGuid
                    );

                }

            }

            orphanGuids[childTable] =
                [...orphanSet];

            }

        }
            
        const missingGuids = [];

        const extraGuids = [];

        const alterChanged = [];

        const traceFile =
    `./logs/reconciliation-${entity_type}-${table}.json`;

                
        const alterColumn =
            ALTER_ID_COLUMN[table];

        if (!alterColumn) {

            throw new Error(
                `ALTER_ID_COLUMN missing for table: ${table}`
            );

        }

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

            dbRow[alterColumn] ??

            null;

            
        const snapshotAlterId =
    snapshot.alter_id ??
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

        fs.writeFileSync(

    traceFile,

    JSON.stringify(

        {

            stage:
                "RECONCILIATION_RESULT",

            table,

            module,

            entity_type,

            sync_batch_id,

            snapshotCount:
                snapshotRows.length,

            dbCount:
                dbRows?.length || 0,

            missingCount:
                missingGuids.length,

            extraCount:
                extraGuids.length,

            alterChangedCount:
                alterChanged.length,

            missingGuids:
                missingGuids.map(
                    row => ({
                        guid:
                            row.guid,

                        alter_id:
                            row[alterColumn],

                        master_id:
                            row.master_id
                            ?? null
                    })
                ),

            extraGuids:
                extraGuids.map(
                    row => ({
                        guid:
                            row.guid,

                        alter_id:
                            row[alterColumn],

                        master_id:
                            row.master_id
                            ?? null
                    })
                )

        },

        null,

        2

    )

);

        
        fs.writeFileSync(

            `./logs/02-${entity_type}-reconciliation-result.json`,

            JSON.stringify(

                {

                    table,

                    module,

                    entity_type,

                    snapshotCount: snapshotRows.length,

                    dbCount: dbRows?.length || 0,

                    matched:

                        snapshotRows.length - missingGuids.length,

                    missingCount:

                        missingGuids.length,

                    extraCount:

                        extraGuids.length,

                    alterChangedCount:

                        alterChanged.length,

                    missingGuids:

                        missingGuids.map(r => ({

                            guid: r.guid,

                            alter_id: r[alterColumn]

                        })),

                    extraGuids:

                        extraGuids.map(r => ({

                            guid: r.guid,

                            alter_id: r[alterColumn]

                        })),

                    alterChanged

                },

                null,

                2

            )

        );



        fs.writeFileSync(

    `./logs/03-${entity_type}-reconciliation-summary.json`,

    JSON.stringify(

        {

            table,

            module,

            entity_type,

            summary: {

                snapshotCount:

                    snapshotRows.length,

                dbCount:

                    dbRows?.length || 0,

                missing:

                    missingGuids.length,

                extra:

                    extraGuids.length,

                alterChanged:

                    alterChanged.length

            }

        },

        null,

        2

    )

);

/*080826
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

        */

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

    orphanGuids,

    summary: {
        missing:
            missingGuids.length,

        extra:
            extraGuids.length,

        alterChanged:
            alterChanged.length,

        orphan:
            Object.values(orphanGuids)
                .reduce(
                    (total, guids) =>
                        total + guids.length,
                    0
                )
            }

        };


    }

}

module.exports =
    new ReconciliationManager();