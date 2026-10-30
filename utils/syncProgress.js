const fs = require("fs");
const path = require("path");


// ============================================================
// JSON TRACKER FILE
// ============================================================

const LOG_DIR = path.join(__dirname, "..", "logs");

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

    // Overall flow:
    //
    // 0  - 35   MASTERS (includes initial GUID discovery)
    // 35 - 75   VOUCHER IMPORT
    // 75 - 90   CHILD RECONCILIATION
    // 90 - 95   LEDGER RECONCILIATION
    // 95 - 100  STOCK RECONCILIATION
    // 100       COMPLETED

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
    // Master progress markers in /getMasters range from 0 to 35.
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
            Math.max(0, Math.min(35, p))
        );
    }


   
    // --------------------------------------------------------
    // VOUCHER GUID DISCOVERY
    // This step runs before master processing in /getMasters.
    // It occupies the 0-5% range.
    // --------------------------------------------------------

    if (stage === "VOUCHER_GUID_DISCOVERY") {
        if (p === null) {
            return 0;
        }

        return Math.round(
            (
                Math.max(0, Math.min(100, p)) * 5
            ) / 100
        );
    }



    // --------------------------------------------------------
    // VOUCHER GUIDS RECEIVED
    // --------------------------------------------------------

    if (

        stage ===
        "VOUCHER_GUIDS_RECEIVED"

    ) {

        return 5;
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

    execution_claim_token,

    stage,

    progress,

    action = null

}) {

    // --------------------------------------------------------
    // VALIDATE SUPABASE
    // --------------------------------------------------------

  if (!supabase) {
    const error = new Error(
        "SYNC_PROGRESS_SUPABASE_REQUIRED"
    );
    error.code = "SYNC_PROGRESS_INVALID_INPUT";
    throw error;
}


    // --------------------------------------------------------
    // VALIDATE BATCH ID
    // --------------------------------------------------------

 if (!batchId) {
    const error = new Error(
        "SYNC_PROGRESS_BATCH_ID_REQUIRED"
    );
    error.code = "SYNC_PROGRESS_INVALID_INPUT";
    throw error;
}

    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        const error = new Error(
            `SYNC_PROGRESS_EXECUTION_CLAIM_REQUIRED: batchId=${batchId}`
        );

        error.code = "EXECUTION_CLAIM_LOST";
        throw error;
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

      if (overallProgress === null) {
    const error = new Error(
        `SYNC_PROGRESS_UNKNOWN_STAGE: batchId=${batchId}, stage=${stage}`
    );
    error.code = "SYNC_PROGRESS_INVALID_STAGE";
    throw error;
}


        // ----------------------------------------------------
        // DATABASE UPDATE
        // ----------------------------------------------------
/* 191026
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

*/

     
        
const updateData = {
    current_stage: String(stage).trim().toUpperCase(),

    sync_progress: {
        stage: String(stage).trim().toUpperCase(),
        phase_progress:
            progress === null || progress === undefined
                ? null
                : Number(progress),
        progress: overallProgress,
        updated_at: new Date().toISOString()
    }
};

if (action) {
    updateData.current_action = action;
}


        // ----------------------------------------------------
        // SUPABASE UPDATE
        // ----------------------------------------------------

   const reconciliationStage =
    stage === "LEDGER_RECONCILIATION" ||
    stage === "STOCK_RECONCILIATION";

const { data: updatedBatch, error } = await supabase
    .from("sync_batches")
    .update(updateData)
    .eq("batch_id", batchId)
    .eq("execution_claim_token", execution_claim_token)
    .eq(
        "batch_status",
        reconciliationStage ? "COMPLETED" : "PROCESSING"
    )
    .eq(
        "worker_status",
        reconciliationStage ? "COMPLETED" : "RUNNING"
    )
    .eq("batch_closed", false)
    .or(
        "error_message.is.null,and(error_message.not.match.^HTTP_OUTCOME_UNKNOWN:,error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:)"
    )
    .select("batch_id")
    .maybeSingle();
    
if (error) {
    throw error;
}

if (!updatedBatch) {
    const claimError = new Error(
        `SYNC_PROGRESS_EXECUTION_CLAIM_LOST: batchId=${batchId}`
    );

    claimError.code = "EXECUTION_CLAIM_LOST";
    throw claimError;
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

        throw err;
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