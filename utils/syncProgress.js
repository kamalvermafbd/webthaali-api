const fs = require("fs");
const path = require("path");


// ============================================================
// JSON TRACKER FILE
// ============================================================

const LOG_DIR =
    "C:\\Users\\15FC0704AU\\Downloads\\webthaali-api\\logs";

const LOG_FILE =
    path.join(
        LOG_DIR,
        "sync-tracking.json"
    );

fs.mkdirSync(
    LOG_DIR,
    {
        recursive: true
    }
);


// ============================================================
// TRACK SYNC EVENT IN JSON FILE
// ============================================================

function trackSyncEvent({
    batchId = null,
    stage = null,
    progress = null,
    event = null,
    details = null
} = {}) {

    try {

        const entry = {

            timestamp:
                new Date().toISOString(),

            batchId,

            event,

            stage,

            progress,

            details
        };


        fs.appendFileSync(

            LOG_FILE,

            JSON.stringify(entry) +
                "\n",

            "utf8"
        );

    } catch (err) {

        console.error(
            "❌ SYNC TRACKER ERROR:",
            err.message
        );
    }
}


// ============================================================
// CALCULATE OVERALL SYNC PROGRESS
// ============================================================
//
// Overall flow:
//
// 0  - 30   MASTERS
// 30 - 35   VOUCHER GUID DISCOVERY
// 35 - 75   VOUCHER IMPORT
// 75 - 90   CHILD RECONCILIATION
// 90 - 95   LEDGER RECONCILIATION
// 95 - 100  STOCK RECONCILIATION
// 100      COMPLETED
//
// ============================================================

function calculateOverallProgress(
    stage,
    progress
) {

    const p =
        Number.isFinite(
            Number(progress)
        )
            ? Number(progress)
            : null;


    // --------------------------------------------------------
    // MASTERS
    // --------------------------------------------------------

    if (

        stage === "MASTERS" ||

        stage === "COMPANY" ||

        stage === "GROUPS" ||

        stage === "UNITS" ||

        stage === "LEDGERS" ||

        stage === "STOCK_GROUPS" ||

        stage === "STOCKS" ||

        stage === "GODOWNS" ||

        stage === "COST_CENTRES"

    ) {

        if (p === null) {

            return 0;
        }


        return Math.round(

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                30
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // VOUCHER GUID DISCOVERY
    // --------------------------------------------------------

    if (

        stage ===
        "VOUCHER_GUID_DISCOVERY"

    ) {

        if (p === null) {

            return 30;
        }


        return Math.round(

            30 +

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                5
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // VOUCHER GUIDS RECEIVED
    // --------------------------------------------------------

    if (

        stage ===
        "VOUCHER_GUIDS_RECEIVED"

    ) {

        return 35;
    }


    // --------------------------------------------------------
    // VOUCHER IMPORT
    // --------------------------------------------------------

    if (

        stage ===
            "VOUCHER_IMPORT" ||

        stage ===
            "VOUCHER_CHUNK_SEND" ||

        stage ===
            "VOUCHER_CHUNK_RECEIVE"

    ) {

        if (p === null) {

            return 35;
        }


        return Math.round(

            35 +

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                40
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // VOUCHER COMPLETE
    // --------------------------------------------------------

    if (

        stage ===
            "VOUCHER_COMPLETE" ||

        stage ===
            "VOUCHER_COMPLETED"

    ) {

        return 75;
    }


    // --------------------------------------------------------
    // CHILD RECONCILIATION
    // --------------------------------------------------------

    if (

        stage ===
            "CHILD_RECONCILIATION" ||

        stage ===
            "CHILD_RECON"

    ) {

        if (p === null) {

            return 75;
        }


        return Math.round(

            75 +

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                15
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // LEDGER RECONCILIATION
    // --------------------------------------------------------

    if (

        stage ===
            "LEDGER_RECONCILIATION" ||

        stage ===
            "LEDGER_RECON"

    ) {

        if (p === null) {

            return 90;
        }


        return Math.round(

            90 +

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                5
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // STOCK RECONCILIATION
    // --------------------------------------------------------

    if (

        stage ===
            "STOCK_RECONCILIATION" ||

        stage ===
            "STOCK_RECON"

    ) {

        if (p === null) {

            return 95;
        }


        return Math.round(

            95 +

            (
                Math.max(
                    0,
                    Math.min(
                        100,
                        p
                    )
                ) *
                5
            ) /
            100

        );
    }


    // --------------------------------------------------------
    // COMPLETED
    // --------------------------------------------------------

    if (

        stage ===
            "COMPLETED" ||

        stage ===
            "SYNC_COMPLETED"

    ) {

        return 100;
    }


    // --------------------------------------------------------
    // UNKNOWN STAGE
    // --------------------------------------------------------

    return null;
}


// ============================================================
// UPDATE SUPABASE SYNC PROGRESS
// ============================================================

async function updateSyncProgress({

    supabase,

    batchId,

    stage,

    progress,

    action = null

}) {

    // --------------------------------------------------------
    // VALIDATE SUPABASE
    // --------------------------------------------------------

    if (!supabase) {

        console.warn(
            "⚠️ SYNC PROGRESS: supabase missing"
        );

        return;
    }


    // --------------------------------------------------------
    // VALIDATE BATCH ID
    // --------------------------------------------------------

    if (!batchId) {

        console.warn(
            "⚠️ SYNC PROGRESS: batchId missing"
        );

        return;
    }


    try {

        // ----------------------------------------------------
        // CALCULATE OVERALL PROGRESS
        // ----------------------------------------------------

        const overallProgress =
            calculateOverallProgress(

                stage,

                progress
            );


        // ----------------------------------------------------
        // UNKNOWN STAGE
        // ----------------------------------------------------

        if (
            overallProgress ===
            null
        ) {

            console.warn(

                "⚠️ SYNC PROGRESS: unknown stage",

                {
                    batchId,
                    stage,
                    progress
                }

            );

            return;
        }


        // ----------------------------------------------------
        // DATABASE UPDATE
        // ----------------------------------------------------

        const updateData = {

            sync_progress: {

                stage,

                phase_progress:

                    progress === null ||
                    progress === undefined

                        ? null

                        : Number(
                            progress
                        ),

                progress:
                    overallProgress,

                updated_at:
                    new Date().toISOString()
            }
        };


        // ----------------------------------------------------
        // CURRENT ACTION
        // ----------------------------------------------------

        if (action) {

            updateData.current_action =
                action;
        }


        // ----------------------------------------------------
        // SUPABASE UPDATE
        // ----------------------------------------------------

        const {
            error
        } = await supabase

            .from(
                "sync_batches"
            )

            .update(
                updateData
            )

            .eq(
                "batch_id",
                batchId
            );


        // ----------------------------------------------------
        // ERROR
        // ----------------------------------------------------

        if (error) {

            console.error(

                "❌ SYNC PROGRESS UPDATE ERROR:",

                error.message

            );

            return;
        }


        // ----------------------------------------------------
        // JSON TRACKER
        // ----------------------------------------------------

        trackSyncEvent({

            batchId,

            stage,

            progress:

                overallProgress,

            event:
                "SERVER_SYNC_PROGRESS",

            details: {

                phaseProgress:
                    progress,

                action
            }

        });


        // ----------------------------------------------------
        // CONSOLE
        // ----------------------------------------------------

        console.log(

            "📊 SYNC PROGRESS:",

            {

                batchId,

                stage,

                phaseProgress:
                    progress,

                overallProgress

            }

        );


    } catch (err) {

        console.error(

            "❌ SYNC PROGRESS HELPER ERROR:",

            err.message

        );
    }
}


// ============================================================
// EXPORTS
// ============================================================

module.exports = {

    trackSyncEvent,

    LOG_FILE,

    calculateOverallProgress,

    updateSyncProgress

};