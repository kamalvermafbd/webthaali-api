const { createClient } =
    require("@supabase/supabase-js");

const {

    TABLES,

    SNAPSHOT_STATUS

} = require("./constants");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

const SNAPSHOT_CHUNK_SIZE = 500;

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

        i += SNAPSHOT_CHUNK_SIZE

    ) {

        const chunk =

            rows.slice(

                i,

                i + SNAPSHOT_CHUNK_SIZE

            );

        await callback(

            chunk

        );

    }

}

    // ----------------------------------
    // Save Snapshot Rows
    // ----------------------------------

    async saveSnapshotRows({

        rows = [],

        onConflict =

            "company_code,tally_owner,module,entity_type,guid"

    }) {

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

        const {

            data,

            error

        } = await query;

        if (error) {

            throw new Error(

                "Failed to load snapshot : " +

                error.message

            );

        }

        return data || [];

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

                    new Date().toISOString()

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

                    new Date().toISOString()

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

                    new Date().toISOString()

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

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                "Failed to cleanup snapshot : " +

                error.message

            );

        }

        return true;

    }


}

module.exports =
    new SnapshotManager();