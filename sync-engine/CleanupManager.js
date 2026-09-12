const { createClient } =
require("@supabase/supabase-js");

const {
    TABLES
} = require("./constants");

const BatchStatusManager =
require("./BatchStatusManager");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

// ======================================================
// TABLES
// ======================================================

const CLEANUP_TABLES = [

    TABLES.GROUPS,

    TABLES.STOCK_GROUPS,

    TABLES.LEDGERS,

    TABLES.STOCKS,

    TABLES.UNITS,

    TABLES.GODOWNS,

    TABLES.COST_CENTRES,

    TABLES.VOUCHERS,

    TABLES.VOUCHER_LEDGERS,

    TABLES.VOUCHER_INVENTORY,

    TABLES.STOCK_VOUCHERS,

    TABLES.COST_CENTRE_ALLOCATIONS,

    TABLES.BILL_ALLOCATIONS

];

class CleanupManager {

    // ======================================================
    // HARD DELETE
    // ======================================================

    async cleanup({

        company_code,

        tally_owner,

        sync_batch_id

    }) {

        if (

            !company_code ||

            !tally_owner ||

            !sync_batch_id

        ) {

            throw new Error(
                "Cleanup parameters missing"
            );

        }

        //--------------------------------------------------
        // Verify Batch
        //--------------------------------------------------

        const batch =
            await BatchStatusManager.loadBatch({

                batch_id:
                    sync_batch_id

            });

        if (

            batch.batch_status !== "CLOSED" ||

            batch.current_action !== "COMPLETED" ||

            batch.reconciliation_completed !== true

        ) {

            return {

                success: false,

                skipped: true,

                reason:
                    "Sync batch reconciliation not completed"

            };

        }

        //--------------------------------------------------
        // Cleanup
        //--------------------------------------------------

        const cleanedTables = [];

        for (const table of CLEANUP_TABLES) {

            const {

                error,

                count

            } = await supabase

                .from(table)

                .delete({

                    count: "exact"

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

                    "is_deleted",

                    true

                );

            if (error) {

                throw new Error(

                    `Cleanup failed for ${table}: ${error.message}`

                );

            }

            cleanedTables.push({

                table,

                deleted:

                    count || 0

            });

            

        }

             //--------------------------------------------------
        // Opening Balance Allocation Cleanup
        //--------------------------------------------------

        const {
            error: openingCleanupError,
            count: openingDeletedCount
        } = await supabase
            .from(TABLES.OPENING_BALANCE_ALLOCATIONS)
            .delete({
                count: "exact"
            })
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .eq("source_type", "TALLY")
            .eq("is_active", false);

        if (openingCleanupError) {
            throw new Error(
                `Opening balance cleanup failed: ${openingCleanupError.message}`
            );
        }

        cleanedTables.push({
            table: TABLES.OPENING_BALANCE_ALLOCATIONS,
            deleted: openingDeletedCount || 0
        });

        // --------------------------------------------------
        // Stock Opening Balance Cleanup
        // --------------------------------------------------

        const {
            error: stockOpeningCleanupError,
            count: stockOpeningDeletedCount
        } = await supabase
            .from(TABLES.STOCK_OPENING_BALANCES)
            .delete({
                count: "exact"
            })
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .eq("source_type", "TALLY")
            .eq("is_active", false);

        if (stockOpeningCleanupError) {
            throw new Error(
                `Stock opening balance cleanup failed: ${stockOpeningCleanupError.message}`
            );
        }

        cleanedTables.push({
            table: TABLES.STOCK_OPENING_BALANCES,
            deleted: stockOpeningDeletedCount || 0
        });

        //--------------------------------------------------
        // Result
        //--------------------------------------------------

        return {

            success: true,

            sync_batch_id,

            cleanedTables

        };

    }

}

module.exports =
new CleanupManager();