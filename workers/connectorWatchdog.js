// ============================================================
// CONNECTOR WATCHDOG
// Watches RUNNING + WAITING_CONNECTOR sync batches
// ============================================================

const WATCH_INTERVAL_MS = 15 * 1000;       // 15 seconds
const HEARTBEAT_TIMEOUT_MS = 90 * 1000;     // 90 seconds
const CONNECTOR_RECOVERY_TIMEOUT_MS = 30 * 1000; // 30 seconds
const connectorRegistry = require("../socketio/connectorRegistry");

const connectorHealthStore =
    require("../socketio/connectorHealthStore");

const { getSyncQueue } =
    require("./secondSyncQueue");

let watchdogTimer = null;
let watchdogRunning = false;


// ============================================================
// CONNECTOR HEALTH REGISTRY — CONNECTOR ID WISE
// ============================================================

const connectorHealth = new Map();



// ============================================================
// CONNECTOR HEALTH RECORD
// ============================================================

function createConnectorHealthRecord(connectorId) {
    return {
        connector_id: String(connectorId),

        connection_status: "UNKNOWN",
        responsiveness: "UNKNOWN",
        operation_status: "UNKNOWN",
        tally_status: "UNKNOWN",
        heartbeat_status: "UNKNOWN",
        company_status: "UNKNOWN",
        health_status: "UNKNOWN",

        active_operation: null,

        last_response_at: null,
        last_success_at: null,
        last_error: null,

        reasons: [
            "Health record created; connector status not verified yet"
        ],

        updated_at: new Date().toISOString()
    };
}

// ============================================================
// CONNECTOR HEALTH REGISTRY HELPERS
// ============================================================

function getOrCreateConnectorHealth(connectorId) {
    const id = String(connectorId || "").trim();

    if (!id) {
        return null;
    }

    let health = connectorHealth.get(id);

    if (!health) {
        health = createConnectorHealthRecord(id);
        connectorHealth.set(id, health);
    }

    return health;
}



// ============================================================
// CONNECTOR HEALTH STATUS VALIDATION
// ============================================================

const CONNECTOR_HEALTH_STATUSES = {
    connection_status: [
        "UNKNOWN",
        "CONNECTED",
        "DISCONNECTED"
    ],

    responsiveness: [
        "UNKNOWN",
        "RESPONSIVE",
        "UNRESPONSIVE"
    ],

    operation_status: [
        "UNKNOWN",
        "FREE",
        "BUSY",
        "RECOVERING"
    ],

    tally_status: [
        "UNKNOWN",
        "ONLINE",
        "OFFLINE"
    ],

    heartbeat_status: [
        "UNKNOWN",
        "FRESH",
        "STALE",
        "MISSING"
    ],

    company_status: [
        "UNKNOWN",
        "READY",
        "NOT_FOUND",
        "NOT_READY"
    ],

    health_status: [
        "UNKNOWN",
        "HEALTHY",
        "DEGRADED",
        "UNHEALTHY"
    ]
};

// ============================================================
// UPDATE CONNECTOR HEALTH
// ============================================================

function updateConnectorHealth(
    connectorId,
    updates = {},
    registry = connectorRegistry
) {
    const health = getOrCreateConnectorHealth(connectorId);

    if (!health) {
        return null;
    }

    for (const [key, value] of Object.entries(updates)) {
        if (
            Object.prototype.hasOwnProperty.call(
                CONNECTOR_HEALTH_STATUSES,
                key
            )
        ) {
            if (!CONNECTOR_HEALTH_STATUSES[key].includes(value)) {
                console.warn(
                    `[ConnectorWatchdog] Invalid ${key} for connector ${health.connector_id}:`,
                    value
                );
                continue;
            }
        }

        health[key] = value;
    }

    health.updated_at = new Date().toISOString();

connectorHealth.set(health.connector_id, health);

publishConnectorHealthToSharedStore(
    {
        ...health,
        reasons: Array.isArray(health.reasons)
            ? [...health.reasons]
            : []
    },
    registry
);

return health;
}


// ============================================================
// PUBLISH CONNECTOR HEALTH TO SHARED REDIS STORE
// ============================================================


async function publishConnectorHealthToSharedStore(
    health,
    registry = connectorRegistry
) {
    if (!health || !health.connector_id) {
        return;
    }

    try {
        const sharedHealth =
            await connectorHealthStore.getConnectorHealth(
                health.connector_id
            );

        const localSocket =
    registry?.get?.(health.connector_id);

        if (!localSocket || !localSocket.id) {
            console.warn(
                "[ConnectorWatchdog] Shared health publish skipped: no local socket",
                health.connector_id
            );
            return;
        }

        const ownerSocketId = sharedHealth?.socket_id;

        if (!ownerSocketId || ownerSocketId !== localSocket.id) {
            console.warn(
                "[ConnectorWatchdog] Shared health publish skipped: local socket is not owner",
                {
                    connector_id: health.connector_id,
                    local_socket_id: localSocket.id,
                    owner_socket_id: ownerSocketId || null
                }
            );
            return;
        }

        const result =
            await connectorHealthStore.updateConnectorHealthIfSocketOwner(
                health.connector_id,
                localSocket.id,
                {
                    responsiveness: health.responsiveness,
                    tally_status: health.tally_status,
                    heartbeat_status: health.heartbeat_status,
                    health_status: health.health_status,
                    active_operation: health.active_operation,
                    last_response_at: health.last_response_at,
                    last_success_at: health.last_success_at,
                    last_error: health.last_error,
                    reasons: health.reasons,
                    updated_at: health.updated_at
                }
            );

        if (!result.updated) {
            console.warn(
                "[ConnectorWatchdog] Shared health update rejected: owner changed",
                health.connector_id
            );
        }
    } catch (error) {
        console.error(
            "[ConnectorWatchdog] Shared health publish failed:",
            health.connector_id,
            error.message
        );
    }
}



// ============================================================
// GET CONNECTOR HEALTH REPORT
// ============================================================

function getConnectorHealthReport(connectorId) {
    const id = String(connectorId || "").trim();

    if (!id) {
        return null;
    }

    const health = connectorHealth.get(id);

    if (!health) {
        return createConnectorHealthRecord(id);
    }

    return {
        ...health,
        reasons: [...health.reasons]
    };
}

// ============================================================
// GET ALL CONNECTOR HEALTH REPORTS
// ============================================================

function getAllConnectorHealthReports() {
    return Array.from(connectorHealth.values()).map((health) => ({
        ...health,
        reasons: [...health.reasons]
    }));
}

// ============================================================
// DISCOVER REGISTERED CONNECTORS
// ============================================================

function discoverRegisteredConnectors(registry = connectorRegistry) {
    if (!registry || typeof registry.list !== "function") {
        return [];
    }

    const connectorIds = registry.list();

    for (const connectorId of connectorIds) {
        getOrCreateConnectorHealth(connectorId);
    }

    return connectorIds;
}

function refreshRegisteredConnectorHealth(registry = connectorRegistry) {
    if (
        !registry ||
        typeof registry.list !== "function" ||
        typeof registry.get !== "function"
    ) {
        return;
    }

    const connectorIds = registry.list();

    for (const connectorId of connectorIds) {
        const socket = registry.get(connectorId);

        if (!socket) {
            updateConnectorHealth(connectorId, {
                connection_status: "DISCONNECTED",
                responsiveness: "UNKNOWN",
                operation_status: "UNKNOWN",
                tally_status: "UNKNOWN",
                heartbeat_status: "MISSING",
                health_status: "UNHEALTHY",
                last_error: "Connector socket not found",
                reasons: ["Connector socket not found in registry"]
            }, registry);

            continue;
        }

        const lastHeartbeat = Number(socket.lastHeartbeat || 0);
        const heartbeatAge = lastHeartbeat > 0
            ? Date.now() - lastHeartbeat
            : Infinity;

        const heartbeatFresh =
            socket.connected === true &&
            heartbeatAge >= 0 &&
            heartbeatAge < HEARTBEAT_TIMEOUT_MS;

        const reasons = [];

        if (socket.connected !== true) {
            reasons.push("Connector socket is not connected");
        }

        if (!lastHeartbeat) {
            reasons.push("Heartbeat is missing");
        } else if (!heartbeatFresh) {
            reasons.push("Heartbeat is stale");
        }

        if (socket.tallyStatus !== "ONLINE") {
            reasons.push("Tally is not confirmed online");
        }

        updateConnectorHealth(connectorId, {
            connection_status:
                socket.connected === true ? "CONNECTED" : "DISCONNECTED",

            responsiveness: "UNKNOWN",

            operation_status:
                socket.tallyRequestActive === true ? "BUSY" : "UNKNOWN",

            tally_status:
                socket.tallyStatus === "ONLINE"
                    ? "ONLINE"
                    : socket.tallyStatus === "OFFLINE"
                        ? "OFFLINE"
                        : "UNKNOWN",

            heartbeat_status:
                !lastHeartbeat
                    ? "MISSING"
                    : heartbeatFresh
                        ? "FRESH"
                        : "STALE",

            health_status:
                heartbeatFresh && socket.tallyStatus === "ONLINE"
                    ? "HEALTHY"
                    : "DEGRADED",

            last_error: reasons.length
                ? reasons.join("; ")
                : null,

            reasons
        }, registry);
    }
}



function ensureWaitingConnectorSince(supabase, batch) {
    const timestamp = batch.waiting_connector_since;

    if (!timestamp) {
        return Date.now();
    }

    const waitingSince = new Date(timestamp).getTime();

    return Number.isFinite(waitingSince)
        ? waitingSince
        : Date.now();
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
discoverRegisteredConnectors(registry || connectorRegistry);

refreshRegisteredConnectorHealth(registry || connectorRegistry);

// Detect expired connector-loss retry reservations.
// Read-only: do not reset or requeue here.
const { data: expiredRetryReservations, error: expiredRetryError } =
    await supabase
        .from("sync_batches")
        .select(`
            id,
            batch_id,
            company_code,
            tally_owner,
            worker_id,
            retry_claim_token,
            retry_lease_until
        `)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .not("retry_claim_token", "is", null)
        .lt("retry_lease_until", new Date().toISOString())
        .order("retry_lease_until", { ascending: true })
        .limit(25);

if (expiredRetryError) {
    console.error(
        "[CONNECTOR WATCHDOG] Expired retry lookup failed:",
        expiredRetryError.message
    );
} else {
for (const reservation of expiredRetryReservations || []) {
    try {

// Expired retry reservation inspection only.
// Do not invoke recovery RPC until BullMQ state and
// previous execution settlement are independently verified.
   console.warn(
    "[CONNECTOR WATCHDOG] EXPIRED RETRY RECOVERY DEFERRED",
    {
        batch_id: reservation.batch_id,
        retry_claim_token: reservation.retry_claim_token,
        reason: "BullMQ job state and previous execution settlement not verified"
    }
);
        
        const { data: assignedWorker, error: workerError } =
            await supabase
                .from("workers")
                .select("id, worker_name, queue_name, server_id")
                .eq("worker_name", reservation.worker_id)
                .maybeSingle();

        if (workerError) {
            throw workerError;
        }

        if (!assignedWorker?.queue_name) {
            console.warn(
                "[CONNECTOR WATCHDOG] EXPIRED RETRY — WORKER QUEUE NOT FOUND:",
                reservation.batch_id,
                reservation.worker_id
            );
            continue;
        }

        const queue = getSyncQueue(assignedWorker.queue_name);

       try {
    const retryToken = reservation.retry_claim_token;

    const bullJobId = retryToken
        ? `${reservation.batch_id}-retry-${retryToken}`
        : reservation.batch_id;

    const job = await queue.getJob(bullJobId);
    const jobState = job ? await job.getState() : "NOT_FOUND";
            const tokenMatches =
                job !== undefined &&
                job !== null &&
                String(job.data?.batch_db_id) === String(reservation.id) &&
                job.data?.worker_name === assignedWorker.worker_name &&
                String(job.data?.worker_id) === String(assignedWorker.id) &&
                String(job.data?.server_id) === String(assignedWorker.server_id) &&
                (job.data?.retry_claim_token ?? null) ===
                    reservation.retry_claim_token;

                    // Read-only recovery decision.
// Do not modify database or BullMQ jobs here.
let recoveryDecision = "MANUAL_REVIEW";

if (!job) {
    recoveryDecision = "JOB_NOT_FOUND";
} else if (!tokenMatches) {
    recoveryDecision = "TOKEN_MISMATCH";
} else if (jobState === "active") {
    recoveryDecision = "ACTIVE_JOB_DO_NOT_TOUCH";
} else if (
    jobState === "waiting" ||
    jobState === "delayed" ||
    jobState === "waiting-children" ||
    jobState === "prioritized"
) {
    recoveryDecision = "EXPIRED_QUEUED_JOB";
    // Expired reservation: candidate for token-fenced recovery.
    // Do not remove the BullMQ job here.
} else if (jobState === "failed") {
    recoveryDecision = "FAILED_JOB";
} else if (jobState === "completed") {
    recoveryDecision = "COMPLETED_JOB";
}

            console.warn(
                "[CONNECTOR WATCHDOG] EXPIRED RETRY BULLMQ INSPECTION:",
                {
                    batch_id: reservation.batch_id,
                    queue_name: assignedWorker.queue_name,
                    job_found: Boolean(job),
                  job_state: jobState,
                    token_matches: tokenMatches,
                    recovery_decision: recoveryDecision,
                    retry_claim_token: reservation.retry_claim_token,
                    retry_lease_until: reservation.retry_lease_until
                }
            );
        } finally {
            await queue.close();
        }
    } catch (inspectionError) {
        console.error(
            "[CONNECTOR WATCHDOG] EXPIRED RETRY INSPECTION FAILED:",
            reservation.batch_id,
            inspectionError.message
        );
    }
}
}
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
                    error_message,
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
                    worker_type,
                    retry_count
                `)
            .or(
    "and(batch_status.eq.PROCESSING,worker_status.eq.RUNNING),and(batch_status.eq.PENDING,worker_status.eq.PENDING,current_stage.eq.WAITING_CONNECTOR),and(batch_status.eq.FAILED,worker_status.eq.FAILED)"
)
.order("created_at", { ascending: true })
.order("id", { ascending: true });

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

        // Never automatically process a batch with uncertain Tally outcome.
        // Applies to FAILED, WAITING_CONNECTOR and RUNNING paths.
       if (
    (
        String(batch.error_message || "")
            .startsWith("CONNECTOR_OUTCOME_UNKNOWN:")
    ) ||
    (
        String(batch.error_message || "")
            .startsWith("HTTP_OUTCOME_UNKNOWN:")
    )
) {
            console.warn(
                "[CONNECTOR WATCHDOG] UNCERTAIN OUTCOME — MANUAL REVIEW:",
                batch.batch_id
            );

            continue;
        }

        // Inspect explicitly classified CONNECTOR_LOSS failures.
// Automatic retry remains disabled pending execution verification.
// Other FAILED outcomes require manual review.
if (
    batch.batch_status === "FAILED" &&
    batch.worker_status === "FAILED"
) {
    if (
        !String(batch.error_message || "")
            .startsWith("CONNECTOR_LOSS:")
    ) {
        console.warn(
            "[CONNECTOR WATCHDOG] FAILED AUTO-RETRY DEFERRED:",
            batch.batch_id,
            batch.error_message
        );
        continue;
    }
    // Never automatically retry uncertain Tally outcomes.
    if (
        String(batch.error_message || "")
            .startsWith("CONNECTOR_OUTCOME_UNKNOWN:")
    ) {
        continue;
    }

    // Maximum 3 automatic retries.
    // NULL retry_count is treated as 0.
    if (Number(batch.retry_count ?? 0) >= 3) {
        continue;
    }
    const companyCode = String(batch.company_code || "").trim();
    const tallyOwner = String(batch.tally_owner || "").trim().toUpperCase();

    if (!companyCode || !["CA", "USER"].includes(tallyOwner)) {
        continue;
    }

    const connectorField =
        tallyOwner === "CA" ? "ca_connector_id" : "client_connector_id";

    const { data: company, error: companyError } = await supabase
        .from("company")
        .select(connectorField)
        .eq("company_code", companyCode)
        .maybeSingle();

    if (companyError || !company) {
        if (companyError) throw companyError;
        continue;
    }

    const connectorId = String(company[connectorField] || "").trim();
    if (!connectorId) continue;

    const socket = registry.get(connectorId);
    const lastHeartbeat = Number(socket?.lastHeartbeat || 0);
    const heartbeatAge = Date.now() - lastHeartbeat;

    if (
    !socket ||
    socket.connected !== true ||
    socket.tallyStatus !== "ONLINE" ||
    !lastHeartbeat ||
    heartbeatAge < 0 ||
    heartbeatAge >= HEARTBEAT_TIMEOUT_MS
) {
   console.warn(
    "[CONNECTOR WATCHDOG] RETRY READINESS RESET DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Connector unavailable; previous Tally execution settlement unverified",
        action: "OBSERVE_ONLY"
    }
);

        continue;
    }

  
    // Connector and Tally appear healthy.
// Only CONNECTOR_LOSS failures reach this retry path.
// Maximum 3 automatic retries.
// BUSY blocks dispatch; execution settlement still requires verification.

console.warn(
    "[CONNECTOR WATCHDOG] RETRY READINESS ACTIVATION DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Previous Tally execution settlement not independently verified"
    }
);
// BUSY prevents dispatch, but does not remove retry eligibility.
const globalWatchdog =
    await connectorHealthStore.getConnectorWatchdogStatus(connectorId);

if (
    globalWatchdog.operation_status !== "FREE" ||
    globalWatchdog.available !== true ||
    socket.tallyRequestActive === true
) {
    continue;
}

console.warn(
    "[CONNECTOR WATCHDOG] CONNECTOR_LOSS RETRY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Previous Tally execution settlement not independently verified"
    }
);
continue;
}

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


                if (
                    batch.batch_status === "PROCESSING" &&
                    batch.worker_status === "RUNNING"
                ) {
                    console.warn(
    "[CONNECTOR WATCHDOG] RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Connector mapping missing; previous execution settlement unverified"
    }
);
                }



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
    console.warn(
        "[CONNECTOR WATCHDOG] CONNECTOR NOT PAIRED TIMEOUT OBSERVED",
        {
            batch_id: batch.batch_id,
            company_code: companyCode,
            waiting_age_seconds: Math.floor(waitingAge / 1000),
            reason: "Connector not paired; previous Tally execution settlement unverified",
            action: "OBSERVE_ONLY"
        }
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


                    // Update global connector health from socket state
if (!socket) {
    updateConnectorHealth(connectorId, {
        connection_status: "DISCONNECTED",
        responsiveness: "UNKNOWN",
        operation_status: "UNKNOWN",
        tally_status: "UNKNOWN",
        heartbeat_status: "MISSING",
        health_status: "UNHEALTHY",
        last_error: "Connector socket not found",
        reasons: ["Connector socket not found in registry"]
        }, registry);
} else {
    const lastHeartbeat = Number(socket.lastHeartbeat || 0);
    const heartbeatAge = lastHeartbeat > 0
        ? Date.now() - lastHeartbeat
        : Infinity;

    const heartbeatFresh =
        socket.connected === true &&
        heartbeatAge >= 0 &&
        heartbeatAge < HEARTBEAT_TIMEOUT_MS;

    updateConnectorHealth(connectorId, {
        connection_status: socket.connected === true
            ? "CONNECTED"
            : "DISCONNECTED",
        responsiveness: "UNKNOWN",
        operation_status: socket.tallyRequestActive === true
            ? "BUSY"
            : "UNKNOWN",
        tally_status: socket.tallyStatus === "ONLINE"
            ? "ONLINE"
            : socket.tallyStatus === "OFFLINE"
                ? "OFFLINE"
                : "UNKNOWN",
        heartbeat_status: !lastHeartbeat
            ? "MISSING"
            : heartbeatFresh
                ? "FRESH"
                : "STALE",
        health_status: heartbeatFresh &&
            socket.tallyStatus === "ONLINE"
                ? "HEALTHY"
                : "DEGRADED",
        reasons: [
            !socket.connected
                ? "Connector socket is not connected"
                : null,
            !lastHeartbeat
                ? "Heartbeat is missing"
                : !heartbeatFresh
                    ? "Heartbeat is stale"
                    : null,
            socket.tallyStatus !== "ONLINE"
                ? "Tally is not confirmed online"
                : null
        ].filter(Boolean)
       }, registry);
}

                // =================================================
                // CASE A
                // Connector is OFFLINE
                // =================================================

            if (!socket) {

    const now = Date.now();

   // =================================================
// RUNNING BATCH + CONNECTOR LOST
// Observe connector loss; do not automatically
// restart or fail an unsettled Tally execution.
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


console.warn(
    "[CONNECTOR WATCHDOG] RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Connector socket missing; previous execution settlement unverified"
    }
);

continue;
    }

    // =================================================
    // ALREADY WAITING FOR CONNECTOR
    // =================================================
    if (
        batch.current_stage ===
        "WAITING_CONNECTOR"
    ) {


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

          console.warn(
    "[CONNECTOR WATCHDOG] WAITING CONNECTOR TIMEOUT OBSERVED",
    {
        batch_id: batch.batch_id,
        company_code: companyCode,
        connector_id: connectorId,
        waiting_age_seconds: waitingAgeSeconds,
        reason: "Connector still offline; Tally execution outcome unverified",
        action: "OBSERVE_ONLY"
    }
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

              console.warn(
    "[CONNECTOR WATCHDOG] RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Tally offline; previous execution settlement unverified"
    }
);

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
    // Never restart a batch with an uncertain Tally outcome.
    if (
        String(batch.error_message || "")
            .startsWith("CONNECTOR_OUTCOME_UNKNOWN:")
    ) {
        continue;
    }

    // ------------------------------------------------
// Connector socket is back.
// Verify Tally ONLINE before checking readiness.
// Automatic restart remains disabled.
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

            console.warn(
    "[CONNECTOR WATCHDOG] TALLY OFFLINE TIMEOUT OBSERVED",
    {
        batch_id: batch.batch_id,
        company_code: companyCode,
        connector_id: connectorId,
        tally_status: socket.tallyStatus,
        waiting_age_seconds: Math.floor(waitingAge / 1000),
        reason: "Tally still offline; previous execution settlement unverified",
        action: "OBSERVE_ONLY"
    }
);
     

        }

        continue;
    }

   // ------------------------------------------------
// Connector and Tally are ONLINE.
// Verify recovery readiness; restart remains deferred
// until previous Tally execution settlement is confirmed.
// ------------------------------------------------
        // Inspect connector heartbeat and active-request state.
// Neither condition independently proves previous execution settlement.
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
    console.warn(
        "[CONNECTOR WATCHDOG] STALE HEARTBEAT TIMEOUT OBSERVED",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            heartbeat_age_seconds: Number.isFinite(recoveryHeartbeatAge)
                ? Math.floor(recoveryHeartbeatAge / 1000)
                : null,
            waiting_age_seconds: Math.floor(waitingAge / 1000),
            reason: "Heartbeat stale; previous Tally execution settlement unverified",
            action: "OBSERVE_ONLY"
        }
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

// Inspect global Redis connector state; batch restart remains deferred.
const globalWatchdog =
    await connectorHealthStore.getConnectorWatchdogStatus(
        connectorId
    );

if (
    globalWatchdog.operation_status !== "FREE" ||
    !globalWatchdog.available
) {
    console.log(
        "⏳ CONNECTOR WATCHDOG: GLOBAL CONNECTOR NOT FREE",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            operation_status: globalWatchdog.operation_status,
            lock_ttl_ms: globalWatchdog.lock_ttl_ms
        }
    );

    continue;
}

console.warn(
    "[CONNECTOR WATCHDOG] WAITING_CONNECTOR RESTART DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Previous Tally execution settlement not independently verified"
    }
);
continue;
  
}

               // =================================================
                // CASE C
                // Normal PROCESSING + RUNNING batch
                // =================================================

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

  // Active Tally request with stale heartbeat.
// Do not recover or restart: execution settlement is unverified.
    console.log(
        "⏳ CONNECTOR WATCHDOG: ACTIVE REQUEST + STALE HEARTBEAT — RECOVERY DEFERRED",
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

console.warn(
    "[CONNECTOR WATCHDOG] ACTIVE REQUEST: RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Heartbeat loss does not prove execution stopped"
    }
);

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
        "⚠️ CONNECTOR WATCHDOG: NO HEARTBEAT — RECOVERY DEFERRED",
        {
            batch_id: batch.batch_id,
            company_code: companyCode,
            connector_id: connectorId,
            tally_request_active:
                socket.tallyRequestActive === true
        }
    );

    console.warn(
    "[CONNECTOR WATCHDOG] RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Heartbeat missing; previous execution settlement unverified"
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

             
console.log(
    "⏳ CONNECTOR WATCHDOG: HEARTBEAT STALE — RECOVERY DEFERRED",
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
// Heartbeat is stale, but previous execution settlement
// has not been independently verified.
// Keep the RUNNING batch unchanged.
// Do not transition to WAITING_CONNECTOR, restart,
// or fail the batch based on heartbeat alone.
// -------------------------------------------------
           console.warn(
    "[CONNECTOR WATCHDOG] RECOVERY DEFERRED",
    {
        batch_id: batch.batch_id,
        reason: "Heartbeat stale; previous execution settlement unverified"
    }
);

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
    startSyncBatchStatusMonitor,

    getConnectorHealthReport,
    getAllConnectorHealthReports
};