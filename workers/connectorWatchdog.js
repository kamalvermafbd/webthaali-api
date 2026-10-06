// ============================================================
// CONNECTOR WATCHDOG
// Watches RUNNING + WAITING_CONNECTOR sync batches
// ============================================================

const WATCH_INTERVAL_MS = 15 * 1000;       // 15 seconds
const HEARTBEAT_TIMEOUT_MS = 90 * 1000;     // 90 seconds

const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");

const {
    dispatchBatch
} = require("./queueExistingBatch");

let watchdogTimer = null;
let watchdogRunning = false;
const waitingConnectorSince = new Map();

// ------------------------------------------------------------
// CHECK CONNECTOR WATCHDOG
// ------------------------------------------------------------

async function checkConnectorWatchdog({
    supabase,
    registry
}) {

    if (watchdogRunning) {
        return;
    }

    watchdogRunning = true;

    try {

        // ----------------------------------------------------
        // 1. Get batches that need connector monitoring
        //
        // A) PROCESSING + RUNNING
        // B) PENDING + PENDING + WAITING_CONNECTOR
        // ----------------------------------------------------

        const { data: batches, error: batchError } =
            await supabase
                .from("sync_batches")
                .select(`
                    id,
                    batch_id,
                    company_code,
                    tally_owner,
                    batch_status,
                    worker_status,
                    worker_id,
                    started_at,
                    heartbeat_at,
                    last_activity_at,
                    current_stage,
                    current_module,
                    current_action,
                    sync_mode,
                    sync_period,
                    job_type,
                    worker_type
                `)
                .or(
                    "and(batch_status.eq.PROCESSING,worker_status.eq.RUNNING),and(batch_status.eq.PENDING,worker_status.eq.PENDING,current_stage.eq.WAITING_CONNECTOR)"
                );

        if (batchError) {

            console.error(
                "❌ CONNECTOR WATCHDOG BATCH QUERY ERROR:",
                batchError.message
            );

            return;
        }

        if (!Array.isArray(batches) || batches.length === 0) {
            return;
        }

        // ----------------------------------------------------
        // 2. Check every batch
        // ----------------------------------------------------

        for (const batch of batches) {

            try {

                const companyCode =
                    String(
                        batch.company_code || ""
                    ).trim();

                const tallyOwner =
                    String(
                        batch.tally_owner || ""
                    )
                        .trim()
                        .toUpperCase();

                if (!companyCode) {
                    continue;
                }

                // ------------------------------------------------
                // 3. Find connector belonging to this batch
                // ------------------------------------------------

                const connectorField =
                    tallyOwner === "CA"
                        ? "ca_connector_id"
                        : "client_connector_id";

                const { data: company, error: companyError } =
                    await supabase
                        .from("company")
                        .select(`
                            company_code,
                            ${connectorField}
                        `)
                        .eq("company_code", companyCode)
                        .maybeSingle();

                if (companyError) {

                    console.error(
                        "❌ CONNECTOR WATCHDOG COMPANY ERROR:",
                        companyCode,
                        companyError.message
                    );

                    continue;
                }

                if (!company) {

                    console.log(
                        "⚠️ CONNECTOR WATCHDOG: COMPANY NOT FOUND",
                        companyCode
                    );

                    continue;
                }

                const connectorId =
                    String(
                        company[connectorField] || ""
                    ).trim();

                if (!connectorId) {

                    console.log(
                        "⚠️ CONNECTOR WATCHDOG: CONNECTOR NOT PAIRED",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            tally_owner: tallyOwner
                        }
                    );

                    continue;
                }

                // ------------------------------------------------
                // 4. Get connector socket
                // ------------------------------------------------

                const socket =
                    registry.get(connectorId);

                // =================================================
                // CASE A
                // Connector is OFFLINE
                // =================================================

                if (!socket) {

                    const now = Date.now();

                    if (
                        batch.current_stage ===
                        "WAITING_CONNECTOR"
                    ) {

                        if (
                            !waitingConnectorSince.has(
                                batch.batch_id
                            )
                        ) {
                            waitingConnectorSince.set(
                                batch.batch_id,
                                now
                            );
                        }

                        const waitingSince =
                            waitingConnectorSince.get(
                                batch.batch_id
                            );

                        const waitingAge =
                            now - waitingSince;

                        const waitingAgeSeconds =
                            Math.floor(
                                waitingAge / 1000
                            );

                        console.log(
                            "⚠️ CONNECTOR WATCHDOG: CONNECTOR OFFLINE",
                            {
                                batch_id: batch.batch_id,
                                company_code: companyCode,
                                connector_id: connectorId,
                                stage: batch.current_stage,
                                waiting_age_seconds:
                                    waitingAgeSeconds
                            }
                        );

                        if (
                            waitingAge >=
                            HEARTBEAT_TIMEOUT_MS
                        ) {

                            console.log(
                                "🚨 CONNECTOR WATCHDOG: FAILING WAITING BATCH",
                                {
                                    batch_id: batch.batch_id,
                                    company_code: companyCode,
                                    connector_id: connectorId,
                                    reason:
                                        "Connector offline for 90 seconds"
                                }
                            );

                            waitingConnectorSince.delete(
                                batch.batch_id
                            );

                            await BatchStatusManager.markFailed({
                                batch_id: batch.batch_id,
                                error:
                                    "Connector offline / heartbeat timeout"
                            });
                        }

                        continue;
                    }

                    // Connector is offline, but this batch is not
                    // WAITING_CONNECTOR. Do not force-fail it here.
                    continue;
                }

                // =================================================
                // CASE B
                // Connector is ONLINE
                // =================================================

                // ------------------------------------------------
                // WAITING_CONNECTOR + connector back
                // ------------------------------------------------

                if (
                    batch.current_stage ===
                    "WAITING_CONNECTOR"
                ) {

                    waitingConnectorSince.delete(
                        batch.batch_id
                    );

                    console.log(
                        "🔄 CONNECTOR BACK: RESTARTING BATCH",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            connector_id: connectorId
                        }
                    );

                    // ------------------------------------------------
                    // Reset only this waiting batch.
                    //
                    // Atomic condition prevents duplicate dispatch.
                    // ------------------------------------------------

                    const {
                        data: resetBatch,
                        error: resetError
                    } = await supabase
                        .from("sync_batches")
                        .update({
                            batch_status: "PENDING",
                            worker_status: "PENDING",
                            worker_id: null,
                            locked_at: null,
                            heartbeat_at: null,
                            completed_at: null,
                            error_message: null,

                            current_stage: "PENDING",
                            current_module: "MASTERS",
                            current_action: "PENDING"
                        })
                        .eq(
                            "id",
                            batch.id
                        )
                        .eq(
                            "batch_status",
                            "PENDING"
                        )
                        .eq(
                            "worker_status",
                            "PENDING"
                        )
                        .eq(
                            "current_stage",
                            "WAITING_CONNECTOR"
                        )
                        .select()
                        .maybeSingle();

                    if (resetError) {

                        console.error(
                            "❌ CONNECTOR WATCHDOG RESET ERROR:",
                            batch.batch_id,
                            resetError.message
                        );

                        continue;
                    }

                    // Another watchdog cycle may already have
                    // handled this batch.
                    if (!resetBatch) {

                        console.log(
                            "⚠️ CONNECTOR WATCHDOG: BATCH ALREADY HANDLED",
                            batch.batch_id
                        );

                        continue;
                    }

                    // ------------------------------------------------
                    // Re-dispatch through existing queue system.
                    //
                    // This starts the batch from the beginning.
                    // ------------------------------------------------

                    try {

                        await dispatchBatch(
                            resetBatch
                        );

                        console.log(
                            "✅ CONNECTOR WATCHDOG: BATCH RESTARTED",
                            {
                                batch_id:
                                    batch.batch_id,
                                company_code:
                                    companyCode,
                                connector_id:
                                    connectorId
                            }
                        );

                    } catch (dispatchError) {

                        console.error(
                            "❌ CONNECTOR WATCHDOG: RESTART DISPATCH FAILED",
                            {
                                batch_id:
                                    batch.batch_id,
                                error:
                                    dispatchError.message
                            }
                        );

                    }

                    continue;
                }

               // =================================================
                // CASE C
                // Normal PROCESSING + RUNNING batch
                // =================================================

                // -------------------------------------------------
                // 1. Actual Tally request is running
                // -------------------------------------------------

                if (socket.tallyRequestActive) {

                    const requestStartedAt =
                        Number(socket.tallyRequestStartedAt || 0);

                    const requestAge =
                        requestStartedAt
                            ? Date.now() - requestStartedAt
                            : 0;

                    const requestAgeSeconds =
                        Math.floor(requestAge / 1000);

                    // Tally request is still within safe timeout
                    if (requestAge < HEARTBEAT_TIMEOUT_MS) {

                        console.log(
                            "🟢 CONNECTOR WATCHDOG: TALLY REQUEST ACTIVE",
                            {
                                batch_id: batch.batch_id,
                                request_age_seconds:
                                    requestAgeSeconds
                            }
                        );

                        continue;
                    }

                    // Tally request stuck for 90+ seconds
                    console.log(
                        "🚨 CONNECTOR WATCHDOG: TALLY REQUEST STUCK",
                        {
                            batch_id: batch.batch_id,
                            request_age_seconds:
                                requestAgeSeconds
                        }
                    );

                    await BatchStatusManager.markFailed({
                        batch_id: batch.batch_id,
                        error:
                            "Tally request timeout"
                    });

                    continue;
                }


                // -------------------------------------------------
                // 2. No active Tally request
                //    Check normal connector heartbeat
                // -------------------------------------------------

                const lastHeartbeat =
                    Number(
                        socket.lastHeartbeat || 0
                    );

                if (!lastHeartbeat) {

                    console.log(
                        "⚠️ CONNECTOR WATCHDOG: NO HEARTBEAT YET",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            connector_id: connectorId
                        }
                    );

                    continue;
                }

                const heartbeatAge =
                    Date.now() - lastHeartbeat;

                const heartbeatAgeSeconds =
                    Math.floor(
                        heartbeatAge / 1000
                    );


                // -------------------------------------------------
                // Healthy connector
                // -------------------------------------------------

                if (
                    heartbeatAge <
                    HEARTBEAT_TIMEOUT_MS
                ) {

                    console.log(
                        "💓 CONNECTOR WATCHDOG: ALIVE",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            connector_id: connectorId,
                            heartbeat_age_seconds:
                                heartbeatAgeSeconds,
                            stage:
                                batch.current_stage,
                            action:
                                batch.current_action
                        }
                    );

                    continue;
                }


                // -------------------------------------------------
                // Heartbeat stale
                // -------------------------------------------------

                console.log(
                    "🚨 CONNECTOR WATCHDOG: HEARTBEAT STALE",
                    {
                        batch_id: batch.batch_id,
                        company_code: companyCode,
                        connector_id: connectorId,
                        heartbeat_age_seconds:
                            heartbeatAgeSeconds,
                        timeout_seconds:
                            HEARTBEAT_TIMEOUT_MS / 1000
                    }
                );

                await BatchStatusManager.markFailed({
                    batch_id: batch.batch_id,
                    error:
                        "Connector heartbeat timeout"
                });

            } catch (batchError) {

                console.error(
                    "❌ CONNECTOR WATCHDOG BATCH ERROR:",
                    batch?.batch_id,
                    batchError.message
                );

            }

        }

    } catch (error) {

        console.error(
            "❌ CONNECTOR WATCHDOG ERROR:",
            error
        );

    } finally {

        watchdogRunning = false;

    }

}

// ------------------------------------------------------------
// START WATCHDOG
// ------------------------------------------------------------

function startConnectorWatchdog({
    supabase,
    registry
}) {

    if (watchdogTimer) {

        console.log(
            "⚠️ CONNECTOR WATCHDOG ALREADY RUNNING"
        );

        return;
    }

    console.log(
        "🚀 CONNECTOR WATCHDOG STARTED",
        {
            interval_seconds:
                WATCH_INTERVAL_MS / 1000,

            timeout_seconds:
                HEARTBEAT_TIMEOUT_MS / 1000
        }
    );

    // First check
    checkConnectorWatchdog({
        supabase,
        registry
    });

    // Continuous checks
    watchdogTimer =
        setInterval(() => {

            checkConnectorWatchdog({
                supabase,
                registry
            });

        }, WATCH_INTERVAL_MS);

}

// ------------------------------------------------------------
// STOP WATCHDOG
// ------------------------------------------------------------

function stopConnectorWatchdog() {

    if (!watchdogTimer) {
        return;
    }

    clearInterval(
        watchdogTimer
    );

    watchdogTimer = null;

    console.log(
        "🛑 CONNECTOR WATCHDOG STOPPED"
    );

}

// ------------------------------------------------------------
// EXPORT
// ------------------------------------------------------------

module.exports = {
    startConnectorWatchdog,
    stopConnectorWatchdog,
    checkConnectorWatchdog
};