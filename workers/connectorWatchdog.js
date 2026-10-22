// ============================================================
// CONNECTOR WATCHDOG
// Watches RUNNING + WAITING_CONNECTOR sync batches
// ============================================================

const WATCH_INTERVAL_MS = 15 * 1000;       // 15 seconds
const HEARTBEAT_TIMEOUT_MS = 90 * 1000;     // 90 seconds
const CONNECTOR_RECOVERY_TIMEOUT_MS = 30 * 1000; // 30 seconds

const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");

const {
    dispatchBatch
} = require("./secondQueueExistingBatch");

let watchdogTimer = null;
let watchdogRunning = false;
const waitingConnectorSince = new Map();


/* 211026
async function startWaitingConnectorTimer(supabase, batch) {
    const { error } = await supabase
        .from("sync_batches")
        .update({ waiting_connector_since: new Date().toISOString() })
        .eq("id", batch.id)
        .is("waiting_connector_since", null);

    if (error) throw error;
}
*/

async function startWaitingConnectorTimer(supabase, batch) {
    const { data, error } = await supabase
        .from("sync_batches")
        .update({
            waiting_connector_since: new Date().toISOString()
        })
        .eq("id", batch.id)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "WAITING_CONNECTOR")
        .is("waiting_connector_since", null)
        .select("waiting_connector_since")
        .maybeSingle();

    if (error) throw error;

    return data?.waiting_connector_since || null;
}

async function ensureWaitingConnectorSince(supabase, batch) {
    if (batch.waiting_connector_since) {
        return new Date(batch.waiting_connector_since).getTime();
    }

    const now = new Date().toISOString();

    const { data, error } = await supabase
        .from("sync_batches")
        .update({ waiting_connector_since: now })
        .eq("id", batch.id)
        .is("waiting_connector_since", null)
        .select("waiting_connector_since")
        .maybeSingle();

    if (error) {
        throw error;
    }

    if (data?.waiting_connector_since) {
        return new Date(data.waiting_connector_since).getTime();
    }

    const { data: existing, error: readError } = await supabase
        .from("sync_batches")
        .select("waiting_connector_since")
        .eq("id", batch.id)
        .maybeSingle();

    if (readError) {
        throw readError;
    }

    return existing?.waiting_connector_since
        ? new Date(existing.waiting_connector_since).getTime()
        : Date.now();
}

async function failWaitingConnectorBatch(supabase, batch, reason) {
    const { data, error } = await supabase
        .from("sync_batches")
       .update({
            batch_status: "FAILED",
            worker_status: "FAILED",
            current_stage: "FAILED",
            completed_at: new Date().toISOString(),
            error_message: reason,
            worker_id: null,
            locked_at: null,
            heartbeat_at: null
        })
        .eq("id", batch.id)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "WAITING_CONNECTOR")
        .select("batch_id")
        .maybeSingle();

    if (error) {
        throw new Error(
            `Connector timeout failure update failed: ${error.message}`
        );
    }

    if (!data) {
        console.log(
            "WATCHDOG FAILURE SKIPPED — BATCH STATE CHANGED:",
            batch.batch_id
        );
        return false;
    }

    console.log(
        "WATCHDOG BATCH FAILED AFTER CONNECTOR TIMEOUT:",
        batch.batch_id,
        reason
    );

    return true;
}
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
                    waiting_connector_since,
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
                )
                .not("batch_status", "in", '("FAILED","CLOSED","COMPLETED")');

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

                const tallyGuidField =
                    tallyOwner === "CA"
                        ? "ca_tally_company_guid"
                        : "client_tally_company_guid";

                const { data: company, error: companyError } =
                    await supabase
                        .from("company")
                        .select(`
                            company_code,
                            ${connectorField},
                            ${tallyGuidField}
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
/* 211026
                if (
                    batch.batch_status === "PROCESSING" &&
                    batch.worker_status === "RUNNING"
                ) {
                    await BatchStatusManager.markWaitingConnector({
                        batch_id: batch.batch_id
                    });
                }

                */

                if (
                    batch.batch_status === "PROCESSING" &&
                    batch.worker_status === "RUNNING"
                ) {
                    await BatchStatusManager.markWaitingConnector({
                        batch_id: batch.batch_id
                    });

                    await startWaitingConnectorTimer(supabase, batch);
                }

/* 211026
if (batch.current_stage === "WAITING_CONNECTOR") {
    const { error } = await supabase
        .from("sync_batches")
        .update({ waiting_connector_since: null })
        .eq("id", batch.id)
        .eq("current_stage", "WAITING_CONNECTOR");

    if (error) {
        console.error(
            "CONNECTOR WAIT TIMER RESET FAILED:",
            batch.batch_id,
            error.message
        );
    }
}
*/

if (
    batch.batch_status === "PENDING" &&
    batch.worker_status === "PENDING" &&
    batch.current_stage === "WAITING_CONNECTOR"
) {
    const waitingSince = await ensureWaitingConnectorSince(
        supabase,
        batch
    );

    const waitingAge = Date.now() - waitingSince;

    if (waitingAge >= CONNECTOR_RECOVERY_TIMEOUT_MS) {
        await failWaitingConnectorBatch(
            supabase,
            batch,
            "CONNECTOR_LOSS: Connector not paired within 30 seconds"
        );
    }
}
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

    // =================================================
    // RUNNING BATCH + CONNECTOR LOST
    // Give connector 30 seconds to reconnect.
    // =================================================
    if (
        batch.batch_status === "PROCESSING" &&
        batch.worker_status === "RUNNING"
    ) {

        console.log(
            "⏳ CONNECTOR WATCHDOG: CONNECTOR LOST, WAITING FOR RECOVERY",
            {
                batch_id: batch.batch_id,
                company_code: companyCode,
                connector_id: connectorId
            }
        );
/* 181026
        await BatchStatusManager.markWaitingConnector({
            batch_id: batch.batch_id
        });

        waitingConnectorSince.set(
            batch.batch_id,
            now
        );
*/

await BatchStatusManager.markWaitingConnector({
    batch_id: batch.batch_id
});

await startWaitingConnectorTimer(supabase, batch);

continue;
    }

    // =================================================
    // ALREADY WAITING FOR CONNECTOR
    // =================================================
    if (
        batch.current_stage ===
        "WAITING_CONNECTOR"
    ) {
/* 181026
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
*/

const waitingSince =
    await ensureWaitingConnectorSince(
        supabase,
        batch
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
    CONNECTOR_RECOVERY_TIMEOUT_MS
) {

            console.log(
                "🚨 CONNECTOR WATCHDOG: FAILING WAITING BATCH",
                {
                    batch_id: batch.batch_id,
                    company_code: companyCode,
                    connector_id: connectorId,
                    reason:
                        "Connector offline for 30 seconds"
                }
            );

            waitingConnectorSince.delete(
                batch.batch_id
            );
/*
            await BatchStatusManager.markFailed({
                batch_id: batch.batch_id,
                error:
                    "Connector offline / heartbeat timeout"
            });
            */

            await failWaitingConnectorBatch(
                supabase,
                batch,
                "CONNECTOR_LOSS: Connector offline / heartbeat timeout"
            );
        }

        continue;
    }

    // Connector offline, but batch is not
    // in a state that should be handled here.
    continue;
}

                // =================================================
                // TALLY OFFLINE / CONNECTION RECOVERY
                // =================================================

                if (
                    batch.batch_status === "PROCESSING" &&
                    batch.worker_status === "RUNNING" &&
                    socket.tallyStatus === "OFFLINE"
                ) {

                    console.log(
                        "⏳ CONNECTOR WATCHDOG: TALLY OFFLINE, WAITING FOR RECOVERY",
                        {
                            batch_id: batch.batch_id,
                            company_code: companyCode,
                            connector_id: connectorId
                        }
                    );

                  await BatchStatusManager.markWaitingConnector({
                        batch_id: batch.batch_id
                    });

                    await startWaitingConnectorTimer(supabase, batch);
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

    // ------------------------------------------------
    // Connector socket is back, but Tally must also
    // be ONLINE before restarting the batch.
    // ------------------------------------------------

    if (
        socket.tallyStatus !==
        "ONLINE"
    ) {

        console.log(
            "⏳ CONNECTOR WATCHDOG: CONNECTOR BACK BUT TALLY STILL OFFLINE",
            {
                batch_id:
                    batch.batch_id,

                company_code:
                    companyCode,

                connector_id:
                    connectorId,

                tally_status:
                    socket.tallyStatus
            }
        );

        // Make sure the 90-second timer exists.
        /* 181026
        if (
            !waitingConnectorSince.has(
                batch.batch_id
            )
        ) {

            waitingConnectorSince.set(
                batch.batch_id,
                Date.now()
            );
        }
*/
const waitingSince =
    await ensureWaitingConnectorSince(
        supabase,
        batch
    );

const waitingAge =
    Date.now() - waitingSince;
        // ------------------------------------------------
        // Tally did not recover within 30 seconds.
        // ------------------------------------------------

       if (
    waitingAge >=
    CONNECTOR_RECOVERY_TIMEOUT_MS
) {

            console.log(
                "🚨 CONNECTOR WATCHDOG: TALLY OFFLINE FOR 30 SECONDS",
                {
                    batch_id:
                        batch.batch_id,

                    company_code:
                        companyCode,

                    connector_id:
                        connectorId,

                    tally_status:
                        socket.tallyStatus
                }
            );

            
            waitingConnectorSince.delete(
                batch.batch_id
            );
/* 201026
            await BatchStatusManager.markFailed({
                batch_id:
                    batch.batch_id,

                error:
                    "Tally offline / heartbeat timeout"
            });
*/
            await failWaitingConnectorBatch(
                supabase,
                batch,
                "CONNECTOR_LOSS: Tally offline / heartbeat timeout"
            );


        }

        continue;
    }

    // ------------------------------------------------
    // Connector is back AND Tally is ONLINE.
    // Now restart the same batch from beginning.
    // ------------------------------------------------

        // Do not restart until connector heartbeat is fresh
    // and the previous Tally request is no longer active.
// added 181026
    const recoveryHeartbeat =
        Number(socket.lastHeartbeat || 0);

    const recoveryHeartbeatAge =
        recoveryHeartbeat > 0
            ? Date.now() - recoveryHeartbeat
            : Infinity;

    const recoveryHeartbeatFresh =
        socket.connected === true &&
        recoveryHeartbeatAge >= 0 &&
        recoveryHeartbeatAge < HEARTBEAT_TIMEOUT_MS;
/* 181026
    if (
        !recoveryHeartbeatFresh ||
        socket.tallyRequestActive === true
    ) {
        console.log(
            "⏳ CONNECTOR WATCHDOG: RECOVERY NOT SAFE TO RESTART",
            {
                batch_id: batch.batch_id,
                heartbeat_age_seconds:
                    Number.isFinite(recoveryHeartbeatAge)
                        ? Math.floor(recoveryHeartbeatAge / 1000)
                        : null,
                tally_request_active:
                    socket.tallyRequestActive === true
            }
        );

        continue;
    }
*/
    
    if (!recoveryHeartbeatFresh) {

        const now = Date.now();

        const waitingSince =
            await ensureWaitingConnectorSince(
                supabase,
                batch
            );

        const waitingAge =
            now - waitingSince;

        console.log(
            "⏳ CONNECTOR WATCHDOG: RECOVERY HEARTBEAT STALE",
            {
                batch_id: batch.batch_id,
                heartbeat_age_seconds:
                    Number.isFinite(recoveryHeartbeatAge)
                        ? Math.floor(recoveryHeartbeatAge / 1000)
                        : null,
                waiting_age_seconds:
                    Math.floor(waitingAge / 1000),
                tally_request_active:
                    socket.tallyRequestActive === true
            }
        );

        if (waitingAge >= CONNECTOR_RECOVERY_TIMEOUT_MS) {
            waitingConnectorSince.delete(batch.batch_id);
/* 201026 
            await BatchStatusManager.markFailed({
                batch_id: batch.batch_id,
                error:
                    "Connector heartbeat stale; recovery timed out"
            });
            */

            await failWaitingConnectorBatch(
                supabase,
                batch,
                "CONNECTOR_LOSS: Connector heartbeat stale; recovery timed out"
            );

        }

        continue;
    }

    // Heartbeat is fresh, but the previous Tally request
    // is still active. Do not restart it.
    if (socket.tallyRequestActive === true) {
        console.log(
            "⏳ CONNECTOR WATCHDOG: TALLY REQUEST STILL ACTIVE",
            {
                batch_id: batch.batch_id
            }
        );

        continue;
    }


    waitingConnectorSince.delete(
        batch.batch_id
    );

    console.log(
        "🔄 CONNECTOR + TALLY BACK: RESTARTING BATCH",
        {
            batch_id:
                batch.batch_id,

            company_code:
                companyCode,

            connector_id:
                connectorId,

            tally_status:
                socket.tallyStatus
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
            waiting_connector_since: null,
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
            batch_id: batch.batch_id,
            error: dispatchError.message
        }
    );

    const { error: recoveryError } = await supabase
    .from("sync_batches")
    .update({
        current_stage: "WAITING_CONNECTOR",
        worker_id: null,
        locked_at: null
    })
    .eq("id", batch.id)
    .eq("batch_status", "PENDING")
    .eq("worker_status", "PENDING")
    .eq("current_stage", "PENDING")
    .is("worker_id", null);

if (recoveryError) {
    console.error(
        "CONNECTOR WATCHDOG: RECOVERY FAILED",
        batch.batch_id,
        recoveryError.message
    );
} else {
    await startWaitingConnectorTimer(supabase, {
        id: batch.id
    });
}




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
/* 181026
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

    // -------------------------------------------------
    // Tally request stuck for 90+ seconds.
    //
    // Do NOT fail immediately.
    // Put batch into WAITING_CONNECTOR so that:
    //
    // Tally / Connector recovers within 90 sec
    //       ↓
    // Same batch restarts from beginning
    //
    // No recovery within 90 sec
    //       ↓
    // Batch FAILED
    // -------------------------------------------------

    console.log(
        "⏳ CONNECTOR WATCHDOG: TALLY REQUEST STUCK, WAITING FOR RECOVERY",
        {
            batch_id:
                batch.batch_id,

            request_age_seconds:
                requestAgeSeconds
        }
    );

    await BatchStatusManager.markWaitingConnector({
        batch_id:
            batch.batch_id
    });

    waitingConnectorSince.set(
        batch.batch_id,
        Date.now()
    );

    continue;
}
*/

// -------------------------------------------------
// 1. Actual Tally request is running
// -------------------------------------------------

if (socket.tallyRequestActive === true) {

    const requestStartedAt =
        Number(socket.tallyRequestStartedAt || 0);

    const requestAge =
        requestStartedAt
            ? Date.now() - requestStartedAt
            : 0;

    const lastHeartbeat =
        Number(socket.lastHeartbeat || 0);

    const heartbeatAge =
        lastHeartbeat > 0
            ? Date.now() - lastHeartbeat
            : Infinity;

    const heartbeatFresh =
        socket.connected === true &&
        lastHeartbeat > 0 &&
        heartbeatAge >= 0 &&
        heartbeatAge < HEARTBEAT_TIMEOUT_MS;

    // Fresh heartbeat + active request:
    // wait even if request takes longer than 90 seconds.
    if (heartbeatFresh) {

        console.log(
            "🟢 CONNECTOR WATCHDOG: ACTIVE REQUEST + FRESH HEARTBEAT",
            {
                batch_id: batch.batch_id,
                request_age_seconds:
                    Math.floor(requestAge / 1000),
                heartbeat_age_seconds:
                    Math.floor(heartbeatAge / 1000)
            }
        );

        continue;
    }

    // Active request but heartbeat stale:
    // use existing connector recovery flow.
    console.log(
        "⏳ CONNECTOR WATCHDOG: ACTIVE REQUEST + STALE HEARTBEAT — WAITING FOR RECOVERY",
        {
            batch_id: batch.batch_id,
            request_age_seconds:
                Math.floor(requestAge / 1000),
            heartbeat_age_seconds:
                Number.isFinite(heartbeatAge)
                    ? Math.floor(heartbeatAge / 1000)
                    : null
        }
    );

await BatchStatusManager.markWaitingConnector({
    batch_id: batch.batch_id
});

await startWaitingConnectorTimer(supabase, batch);

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
/*211026
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
                    */

                
             
if (!lastHeartbeat) {
    console.log(
        "⚠️ CONNECTOR WATCHDOG: NO HEARTBEAT — WAITING FOR RECOVERY",
        {
            batch_id: batch.batch_id,
            company_code: companyCode,
            connector_id: connectorId,
            tally_request_active:
                socket.tallyRequestActive === true
        }
    );

    await BatchStatusManager.markWaitingConnector({
        batch_id: batch.batch_id
    });

    // Initialize the timer only if it is missing.
    await startWaitingConnectorTimer(supabase, batch);

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

              // -------------------------------------------------
// Heartbeat stale
// -------------------------------------------------

console.log(
    "⏳ CONNECTOR WATCHDOG: HEARTBEAT STALE, WAITING FOR RECOVERY",
    {
        batch_id:
            batch.batch_id,

        company_code:
            companyCode,

        connector_id:
            connectorId,

        heartbeat_age_seconds:
            heartbeatAgeSeconds,

        timeout_seconds:
            HEARTBEAT_TIMEOUT_MS / 1000
    }
);

// -------------------------------------------------
// Do NOT fail immediately.
//
// Give connector/network 90 seconds to recover.
// If connector comes back + Tally is ONLINE,
// existing WAITING_CONNECTOR recovery logic
// will restart the same batch from beginning.
//
// If recovery does not happen within 90 seconds,
// WAITING_CONNECTOR logic will mark the batch FAILED.
// ------------------------------------------------

                await BatchStatusManager.markWaitingConnector({
                    batch_id: batch.batch_id
                });

                await startWaitingConnectorTimer(supabase, batch);

                continue;
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


const batchStatusSnapshots = new Map();
let batchStatusMonitorTimer = null;
let batchStatusMonitorRunning = false;

async function monitorSyncBatchStatus({ supabase }) {
    if (batchStatusMonitorRunning) return;

    batchStatusMonitorRunning = true;

    try {
        const { data, error } = await supabase
            .from("sync_batches")
            .select(`
                batch_id,
                company_code,
                batch_status,
                worker_status,
                sync_progress,
                error_message,
                last_activity_at,
                tally_owner,
                worker_id,
                current_stage,
                current_module,
                current_action,
                heartbeat_at,
                started_at
            `)
            .order("last_activity_at", { ascending: false })
            .limit(100);

        if (error) {
            console.error(
                "[BATCH STATUS MONITOR] Query failed:",
                error.message
            );
            return;
        }

        
console.log("[BATCH STATUS MONITOR POLL]", {
    checked_at: new Date().toISOString(),
    fetched_batches: data?.length || 0
});


        for (const batch of data || []) {
            if (!batch.batch_id) continue;

            
const snapshot = JSON.stringify({
    company_code: batch.company_code,
    tally_owner: batch.tally_owner,
    batch_status: batch.batch_status,
    worker_status: batch.worker_status,
    worker_id: batch.worker_id,
    progress: batch.sync_progress?.progress ?? null,
    stage: batch.current_stage ?? null,
    module: batch.current_module ?? null,
    action: batch.current_action ?? null,
    heartbeat_at: batch.heartbeat_at ?? null,
    error_message: batch.error_message ?? null
});


            const previous =
                batchStatusSnapshots.get(batch.batch_id);

            
            if (previous !== snapshot) {
                console.log("[BATCH LIVE STATUS]", {
                    batch_id: batch.batch_id,
                    company_code: batch.company_code,
                    tally_owner: batch.tally_owner,
                    batch_status: batch.batch_status,
                    worker_status: batch.worker_status,
                    worker_id: batch.worker_id,
                    progress: batch.sync_progress?.progress ?? null,
                    stage: batch.current_stage ?? null,
                    module: batch.current_module ?? null,
                    action: batch.current_action ?? null,
                    heartbeat_at: batch.heartbeat_at ?? null,
                    started_at: batch.started_at ?? null,
                    last_activity_at: batch.last_activity_at,
                    error_message: batch.error_message ?? null,
                    observed_at: new Date().toISOString()
                });

                batchStatusSnapshots.set(
                    batch.batch_id,
                    snapshot
                );
            }
        }
    } catch (error) {
        console.error(
            "[BATCH STATUS MONITOR] Error:",
            error.message
        );
    } finally {
        batchStatusMonitorRunning = false;
    }
}

function startSyncBatchStatusMonitor({ supabase }) {
    if (batchStatusMonitorTimer) return;

    console.log(
        "🔎 READ-ONLY BATCH STATUS MONITOR STARTED"
    );

    monitorSyncBatchStatus({ supabase });

    batchStatusMonitorTimer = setInterval(() => {
        monitorSyncBatchStatus({ supabase });
    }, 15000);
}


// ------------------------------------------------------------
// EXPORT
// ------------------------------------------------------------


module.exports = {
    startConnectorWatchdog,
    stopConnectorWatchdog,
    checkConnectorWatchdog,
    monitorSyncBatchStatus,
    startSyncBatchStatusMonitor
};
