const { createClient } =
    require("@supabase/supabase-js");

const fs = require("fs");

const {

    TABLES,

    SNAPSHOT_STATUS,

    CONFLICT_KEYS,

    DB_CONFIG

} = require("./constants");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

class SnapshotManager {

    // ----------------------------------
// Process Snapshot Chunks
// ----------------------------------

async processChunks({

    rows,

    callback

}) {

    if (!Array.isArray(rows)) {

        throw new Error(

            "rows must be an array"

        );

    }

    if (typeof callback !== "function") {

        throw new Error(

            "callback must be a function"

        );

    }

    for (

        let i = 0;

        i < rows.length;

        i += DB_CONFIG.CHUNK_SIZE

    ) {

        const chunk =

            rows.slice(

                i,

                i + DB_CONFIG.CHUNK_SIZE

            );

        await callback(

            chunk

        );

    }

}

buildSnapshotRows({

    sync_batch_id,

    company_code,

    tally_owner,

    module,

    entity_type,

    rows = []

}) {

    if (!Array.isArray(rows) || rows.length === 0) {

        return [];

    }

    return rows

        .filter(row => row.guid?.trim())

        .map(row => ({

            sync_batch_id,

            last_sync_batch_id:
                sync_batch_id,

            company_code,

            tally_owner,

            module,

            entity_type,

            guid: row.guid.trim(),

            alter_id:
                row.alterId ??
                row.alter_id ??
                row.alterid ??
                null,

            master_id:
                row.masterId ??
                row.master_id ??
                row.masterid ??
                null,

            status:
                SNAPSHOT_STATUS.COMPLETED,

            is_deleted:
                false

        }));

}

    // ----------------------------------
    // Save Snapshot Rows
    // ----------------------------------

    
    async saveSnapshotRows({

        rows = [],

        onConflict =

    CONFLICT_KEYS[TABLES.SNAPSHOT]

    }) {

        console.log("SAVE SNAPSHOT:", rows.length);
console.dir(rows[0], { depth: null });

const snapshotRows = rows;

/*
        fs.writeFileSync(

            `./logs/BEFORE_SAVE_${rows[0]?.entity_type || "UNKNOWN"}.json`,

            JSON.stringify(

                {

                    receivedRows: rows.length,

                    guids: rows.map(r => r.guid)

                },

                null,

                2

            )

        );
        */

        if (!Array.isArray(rows)) {

            throw new Error(

                "Snapshot rows must be an array"

            );

        }

        if (rows.length === 0) {

            return {

                inserted: 0,

                skipped: true

            };

        }

       await this.processChunks({



        rows,

        callback: async (chunk) => {

            /*
            fs.writeFileSync(

                `./logs/02-before-upsert-${chunk[0]?.entity_type || "UNKNOWN"}.json`,

                JSON.stringify(

                    {

                        total: chunk.length,

                        rows: chunk

                    },

                    null,

                    2

                )

            );
*/

            const {

                error

            } = await supabase

                .from(

                    TABLES.SNAPSHOT

                )

                

                .upsert(

                    chunk,

                    {

                        onConflict

                    }

                );



                console.log("SNAPSHOT UPSERT ERROR:", error);
/*
                fs.writeFileSync(

                        `./logs/03-after-upsert-${chunk[0]?.entity_type || "UNKNOWN"}.json`,

                        JSON.stringify(

                            {

                                total: chunk.length,

                                success: !error

                            },

                            null,

                            2

                        )

                    );
*/

                const { data: verifyRows } = await supabase

                    .from(TABLES.SNAPSHOT)

                    .select("guid")

                    .eq("company_code", chunk[0].company_code)

                    .eq("tally_owner", chunk[0].tally_owner)

                    .eq("module", chunk[0].module)

                    .eq("entity_type", chunk[0].entity_type)

                    .eq("is_deleted", false);

                    /*
                fs.writeFileSync(

                    `./logs/AFTER_SAVE_${chunk[0].entity_type}.json`,

                    JSON.stringify(

                        {

                            rowsAfterSave: verifyRows?.length || 0,

                            guids: (verifyRows || []).map(r => r.guid)

                        },

                        null,

                        2

                    )

                );
                */
                
            if (error) {

                throw new Error(

                    "Snapshot upsert failed : " +

                    error.message

                );

            }

        }

    });

    return {

        inserted:

            rows.length

    };

    }

        // ----------------------------------
    // Load Snapshot
    // ----------------------------------

    async loadSnapshot({

        company_code,

        tally_owner,

        module,

        entity_type

    }) {

        let query = supabase

            .from(

                TABLES.SNAPSHOT

            )

            .select("*")

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "module",

                module

            )

            .eq(

                "is_deleted",

                false

            );

        if (entity_type) {

            query = query.eq(

                "entity_type",

                entity_type

            );

        }

   const PAGE_SIZE = 1000;

const allRows = [];

let from = 0;

while (true) {

    const {
        data,
        error
    } = await query
        .order("guid", {
            ascending: true
        })
        .range(
            from,
            from + PAGE_SIZE - 1
        );

    if (error) {

        throw new Error(
            "Failed to load snapshot : " +
            error.message
        );

    }

    const page =
        data || [];

    allRows.push(
        ...page
    );

    if (
        page.length < PAGE_SIZE
    ) {
        break;
    }

    from += PAGE_SIZE;

}

        /*
        fs.writeFileSync(
        `./logs/00-${entity_type}-snapshot-loaded.json`,
        JSON.stringify(
                {
                company_code,
                tally_owner,
                module,
                entity_type,
                rowsFound: data?.length || 0,
                guids: (data || []).map(row => ({
                    guid: row.guid,
                    alter_id: row.alter_id,
                    master_id: row.master_id,
                    is_deleted: row.is_deleted
                }))
            },
            null,
            2
        )
    );
    */


        return allRows;

        fs.writeFileSync(

    `./logs/00-${entity_type}-snapshot-loaded.json`,

    JSON.stringify({

        company_code,
        tally_owner,
        module,
        entity_type,

        rowsFound:
            allRows.length,

        guids:
            allRows.map(row => ({
                guid:
                    row.guid,

                alter_id:
                    row.alter_id,

                master_id:
                    row.master_id,

                is_deleted:
                    row.is_deleted
            }))

    }, null, 2)

);

    }

    // ----------------------------------
    // Load GUIDs
    // ----------------------------------

    async loadGuids({

    company_code,

    tally_owner,

    module,

    entity_type

    }) {

        let query =

            supabase

                .from(

                    TABLES.SNAPSHOT

                )

                .select(

                    "guid"

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

                    "module",

                    module

                )

                .eq(

                    "is_deleted",

                    false

                );

        if (entity_type) {

            query = query.eq(

                "entity_type",

                entity_type

            );

        }

        const {

            data,

            error

        } = await query;
        
        /*
        fs.appendFileSync(

        "./logs/snapshot-load-guids.jsonl",

        JSON.stringify({

            company_code,

            tally_owner,

            module,

            entity_type,

            rowsFound: data?.length || 0,

            guids: (data || []).map(row => row.guid)

        }) + "\n"

    );
    */

        if (error) {

            throw new Error(

                "Failed to load snapshot GUIDs : " +

                error.message

            );

        }

        return (

            data || []

        ).map(

            row => row.guid

        );

    }

    // ----------------------------------
    // Update Snapshot Status
    // ----------------------------------

    async updateStatus({

        company_code,

        tally_owner,

        module,

        entity_type,

        status

    }) {

        const now =
    new Date().toISOString();

        if (!status) {

            throw new Error(

                "Snapshot status is required"

            );

        }

        let query = supabase

            .from(

                TABLES.SNAPSHOT

            )

            .update({

                status,

               updated_at:

                now

            })

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "module",

                module

            );

        if (entity_type) {

            query = query.eq(

                "entity_type",

                entity_type

            );

        }

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                "Failed to update snapshot status : " +

                error.message

            );

        }

        return true;

    }

    // ----------------------------------
    // Mark Snapshot Deleted
    // ----------------------------------

    async markDeleted({

        company_code,

        tally_owner,

        module,

        entity_type,

        guids = []

    }) {

        const now =

     new Date().toISOString();

        if (!Array.isArray(guids)) {

        throw new Error(

            "guids must be an array"

        );

    }

    if (guids.length === 0) {

        return true;

    }

            let query = supabase

            .from(

                TABLES.SNAPSHOT

            )

            .update({

                is_deleted: true,

                status:

                    SNAPSHOT_STATUS.DELETED,

                updated_at:
                     now

            })

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "module",

                module

            );



            if (entity_type) {

                query = query.eq(

                    "entity_type",

                    entity_type

                );

            }

            query = query.in(

                "guid",

                guids

            );

            const {

                error

            } = await query;

        if (error) {

            throw new Error(

                "Failed to mark snapshot deleted : " +

                error.message

            );

        }

        return true;

    }


        // ----------------------------------
    // Clear Snapshot
    // ----------------------------------

    async clearSnapshot({

        company_code,

        tally_owner,

        module,

        entity_type

    }) {

        let query = supabase

            .from(

                TABLES.SNAPSHOT

            )

            .delete()

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "module",

                module

            );

        if (entity_type) {

            query = query.eq(

                "entity_type",

                entity_type

            );

        }

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                "Failed to clear snapshot : " +

                error.message

            );

        }

        return true;

    }

    // ----------------------------------
    // Remove Missing GUIDs
    // ----------------------------------

      async removeMissingGuids({

        sync_batch_id,

        company_code,

        tally_owner,

        module,

        entity_type

    }) {

        const now =

    new Date().toISOString();

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

        let query = supabase

            .from(

                TABLES.SNAPSHOT

            )

            .update({

                is_deleted: true,

                status:

                    SNAPSHOT_STATUS.DELETED,

                updated_at:

                    now

            })

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "module",

                module

            )

            .neq(

                "last_sync_batch_id",

                sync_batch_id

            );

        if (entity_type) {

            query = query.eq(

                "entity_type",

                entity_type

            );

        }


       
/*
fs.writeFileSync(

    `./logs/REMOVE_${entity_type}.json`,

    JSON.stringify({

        sync_batch_id,

        company_code,

        tally_owner,

        module,

        entity_type

    }, null, 2)

);
*/

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                "Failed to cleanup snapshot : " +

                error.message

            );

        }

        const { data: activeRows } = await supabase

    .from(TABLES.SNAPSHOT)

    .select("guid,last_sync_batch_id,is_deleted")

    .eq("company_code", company_code)

    .eq("tally_owner", tally_owner)

    .eq("module", module)

    .eq("entity_type", entity_type);

    /*
fs.writeFileSync(

    `./logs/REMOVE_AFTER_${entity_type}.json`,

    JSON.stringify(activeRows, null, 2)

);

*/

        return true;

    }


}

module.exports =
    new SnapshotManager();