// ============================================================
// CONNECTOR WATCHDOG
// Watches RUNNING sync batches against their connector heartbeat
// ============================================================

const WATCH_INTERVAL_MS = 15 * 1000;   // 15 seconds
const HEARTBEAT_TIMEOUT_MS = 90 * 1000; // 90 seconds

let watchdogTimer = null;
let watchdogRunning = false;


// ------------------------------------------------------------
// CHECK RUNNING BATCHES
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
        // 1. Get only currently running batches
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
                    current_action
                `)
                .eq("batch_status", "PROCESSING")
                .eq("worker_status", "RUNNING");

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
        // 2. Check every running batch
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


                if (!socket) {

                    console.log(
                        "⚠️ CONNECTOR WATCHDOG: CONNECTOR OFFLINE",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            connector_id: connectorId
                        }
                    );

                    continue;
                }


                // ------------------------------------------------
                // 5. Read connector heartbeat
                // ------------------------------------------------

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


                // ------------------------------------------------
                // 6. Healthy connector
                // ------------------------------------------------

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


                // ------------------------------------------------
                // 7. Heartbeat is stale
                // ------------------------------------------------

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