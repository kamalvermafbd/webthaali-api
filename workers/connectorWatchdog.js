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

const { dispatchUniversalFailedRetry } =
    require("./secondQueueExistingBatch");

let watchdogTimer = null;
let watchdogRunning = false;

// Serialize shared health publications per connector.
// A failed publication must not block later publications.
const connectorHealthPublishTails = new Map();


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



const snapshotSocket = registry?.get?.(health.connector_id);

const snapshot = {
    ...health,
    originating_socket_id:
        snapshotSocket?.connected === true && snapshotSocket?.id
            ? snapshotSocket.id
            : null,
    reasons: Array.isArray(health.reasons)
        ? [...health.reasons]
        : []
};

const previousTail =
    connectorHealthPublishTails.get(health.connector_id) ||
    Promise.resolve();
const publishTask = previousTail
    .catch(() => undefined)
    .then(() =>
        publishConnectorHealthToSharedStore(snapshot, registry)
    );

const nextTail = publishTask.then(
    () => undefined,
    () => undefined
);

connectorHealthPublishTails.set(health.connector_id, nextTail);

void nextTail.then(() => {
    if (
        connectorHealthPublishTails.get(health.connector_id) === nextTail
    ) {
        connectorHealthPublishTails.delete(health.connector_id);
    }
});

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

    if (
    !health.originating_socket_id ||
    !localSocket ||
    !localSocket.id ||
    localSocket.connected !== true ||
    localSocket.id !== health.originating_socket_id
) {
 console.warn(
    "[ConnectorWatchdog] Shared health publish skipped: originating socket missing, disconnected, or changed",
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

        if (
    localSocket.connected !== true ||
            registry?.get?.(health.connector_id) !== localSocket
        ) {
            console.warn(
                "[ConnectorWatchdog] Shared health publish skipped: socket disconnected or registry owner changed",
                health.connector_id
            );
            return;
        }
        const result =
            await connectorHealthStore.updateConnectorHealthIfSocketOwner(
                health.connector_id,
                localSocket.id,
                {
                    responsiveness: health.responsiveness,
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



async function ensureWaitingConnectorSince(supabase, batch) {
    const timestamp = batch.waiting_connector_since;

    if (timestamp) {
        const waitingSince = new Date(timestamp).getTime();

        if (Number.isFinite(waitingSince)) {
            return waitingSince;
        }
    }

    if (
        batch.batch_status !== "PROCESSING" ||
        batch.worker_status !== "RUNNING" ||
        !batch.execution_claim_token
    ) {
        return Date.now();
    }

    const now = new Date().toISOString();

    const { data, error } = await supabase
        .from("sync_batches")
        .update({ waiting_connector_since: now })
        .eq("batch_id", batch.batch_id)
        .eq("batch_status", "PROCESSING")
        .eq("worker_status", "RUNNING")
        .eq("execution_claim_token", batch.execution_claim_token)
        .is("waiting_connector_since", null)
        .select("waiting_connector_since")
        .maybeSingle();

    if (error) {
        throw error;
    }

    if (data?.waiting_connector_since) {
        return new Date(data.waiting_connector_since).getTime();
    }

    const { data: current, error: readError } = await supabase
        .from("sync_batches")
        .select("waiting_connector_since")
        .eq("batch_id", batch.batch_id)
        .eq("batch_status", "PROCESSING")
        .eq("worker_status", "RUNNING")
        .eq("execution_claim_token", batch.execution_claim_token)
        .maybeSingle();

    if (readError) {
        throw readError;
    }

    const persisted = current?.waiting_connector_since
        ? new Date(current.waiting_connector_since).getTime()
        : NaN;

    if (!Number.isFinite(persisted)) {
        throw new Error("OFFLINE_TIMER_NOT_VERIFIED");
    }

    return persisted;
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

// Detect expired CONNECTOR_OFFLINE_TIMEOUT retry reservations.
// Recovery is limited to DB-eligible, token-matched, non-active BullMQ jobs.
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
        .eq("retry_reservation_origin", "CONNECTOR_OFFLINE_TIMEOUT")
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
    job != null &&
    job.data?.batch_id === reservation.batch_id &&
    String(job.data?.batch_db_id) === String(reservation.id) &&
    job.data?.worker_name === assignedWorker.worker_name &&
    String(job.data?.worker_id) === String(assignedWorker.id) &&
    String(job.data?.server_id) === String(assignedWorker.server_id) &&
    (job.data?.retry_claim_token ?? null) ===
        reservation.retry_claim_token;

// Assess database eligibility before any recovery attempt.
// BullMQ jobs are inspected only; never modified here.
let recoveryDecision = "MANUAL_REVIEW";

const { data: assessmentData, error: assessmentError } =
    await supabase.rpc(
        "assess_expired_connector_retry_reservation",
        {
            p_batch_db_id: reservation.id,
            p_retry_claim_token: reservation.retry_claim_token
        }
    );

if (assessmentError) {
    throw new Error(
        `RETRY_ASSESSMENT_FAILED: ${assessmentError.message}`
    );
}

const assessment = Array.isArray(assessmentData)
    ? assessmentData[0]
    : assessmentData;

const databaseEligible =
    assessment?.eligible === true &&
    assessment?.reason === "DB_ELIGIBLE_BULLMQ_UNVERIFIED" &&
    assessment?.batch_id === reservation.batch_id;

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

// Recover only DB-eligible reservations with a matching
// terminal BullMQ job. Queued and active jobs remain untouched.
const recoveryCandidate =
    databaseEligible === true &&
    job != null &&
    tokenMatches === true &&
    (
        recoveryDecision === "FAILED_JOB" ||
        recoveryDecision === "COMPLETED_JOB"
    );

if (recoveryCandidate) {
    const { data: recoveryData, error: recoveryError } =
        await supabase.rpc(
            "recover_expired_connector_retry_reservation",
            {
                p_batch_db_id: reservation.id,
                p_retry_claim_token: reservation.retry_claim_token
            }
        );

 if (recoveryError) {
    const { data: current, error: readError } =
        await supabase
            .from("sync_batches")
            .select(`
                batch_id,
                batch_status,
                worker_status,
                current_stage,
                error_message,
                retry_claim_token,
                retry_lease_until,
                retry_reservation_origin
            `)
            .eq("id", reservation.id)
            .maybeSingle();

    if (readError || !current) {
        throw new Error(
            "EXPIRED_RETRY_RECOVERY_OUTCOME_UNKNOWN"
        );
    }

    const recoveryCommitted =
        current.batch_id === reservation.batch_id &&
        current.batch_status === "FAILED" &&
        current.worker_status === "FAILED" &&
        current.current_stage === "FAILED" &&
        current.error_message === "CONNECTOR_OFFLINE_TIMEOUT" &&
        current.retry_claim_token === null &&
        current.retry_lease_until === null &&
        current.retry_reservation_origin === null;

    if (recoveryCommitted) {
        console.warn(
            "[CONNECTOR WATCHDOG] RECOVERY COMMIT OBSERVED AFTER RPC ERROR",
            {
                batch_id: reservation.batch_id,
                action: "READBACK_CONFIRMED",
                automatic_retry: false
            }
        );
    }

    throw new Error(
        recoveryCommitted
            ? "EXPIRED_RETRY_RECOVERY_COMMIT_OBSERVED"
            : "EXPIRED_RETRY_RECOVERY_OUTCOME_UNKNOWN"
    );
}

    const recovery = Array.isArray(recoveryData)
        ? recoveryData[0]
        : recoveryData;

    if (
        recovery?.recovered === true &&
        recovery?.reason === "RECOVERED" &&
        recovery?.recovered_batch_id === reservation.batch_id
    ) {
        recoveryDecision = "RECOVERED";
    } else {
        recoveryDecision =
            `RECOVERY_REJECTED:${recovery?.reason || "INVALID_RESPONSE"}`;
    }
}

            console.warn(
    "[CONNECTOR WATCHDOG] EXPIRED RETRY ASSESSMENT:",
    {
        batch_id: reservation.batch_id,
        queue_name: assignedWorker.queue_name,
        job_found: Boolean(job),
        job_state: jobState,
        token_matches: tokenMatches,
        database_eligible: databaseEligible,
        database_reason: assessment?.reason ?? "NO_ASSESSMENT",
        recovery_decision: recoveryDecision,
        retry_claim_token: reservation.retry_claim_token,
        retry_lease_until: reservation.retry_lease_until,
       action:
    recoveryDecision === "RECOVERED"
        ? "RECOVERED"
        : recoveryDecision.startsWith("RECOVERY_REJECTED:")
            ? "RECOVERY_REJECTED"
            : "OBSERVE_ONLY"
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

// =====================================================
// UNIVERSAL FAILED RETRY - EXPIRED RESERVATION RECOVERY
// =====================================================
// Only terminal, token-matched BullMQ jobs are eligible.
// Never modify or remove BullMQ jobs here.

const { data: expiredUniversalReservations, error: universalLookupError } =
    await supabase
        .from("sync_batches")
        .select(`
            id,
            batch_id,
            company_code,
            tally_owner,
            worker_id,
            retry_claim_token,
            retry_lease_until,
            retry_original_error_message
        `)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "PENDING")
        .eq("retry_reservation_origin", "UNIVERSAL_FAILED_RETRY")
        .not("retry_claim_token", "is", null)
        .lt("retry_lease_until", new Date().toISOString())
        .order("retry_lease_until", { ascending: true })
        .limit(25);

if (universalLookupError) {
    console.error(
        "[CONNECTOR WATCHDOG] UNIVERSAL RETRY LOOKUP FAILED:",
        universalLookupError.message
    );
} else {
    for (const reservation of expiredUniversalReservations || []) {
        try {
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
                    "[CONNECTOR WATCHDOG] UNIVERSAL RETRY WORKER QUEUE NOT FOUND:",
                    reservation.batch_id
                );
                continue;
            }

            const queue = getSyncQueue(assignedWorker.queue_name);

            try {
                const retryToken = reservation.retry_claim_token;

                const bullJobId =
                    `${reservation.batch_id}-retry-${retryToken}`;

                const job = await queue.getJob(bullJobId);
                const jobState = job
                    ? await job.getState()
                    : "NOT_FOUND";

                const tokenMatches =
                    job != null &&
                    job.data?.batch_id === reservation.batch_id &&
                    String(job.data?.batch_db_id) ===
                        String(reservation.id) &&
                    job.data?.worker_name === assignedWorker.worker_name &&
                    String(job.data?.worker_id) ===
                        String(assignedWorker.id) &&
                    String(job.data?.server_id) ===
                        String(assignedWorker.server_id) &&
                    job.data?.retry_claim_token === retryToken;

                // Database assessment is read-only.
                const { data: assessmentData, error: assessmentError } =
                    await supabase.rpc(
                        "assess_expired_universal_retry_reservation",
                        {
                            p_batch_db_id: reservation.id,
                            p_retry_claim_token: retryToken
                        }
                    );

                if (assessmentError) {
                    throw assessmentError;
                }

                const assessment = Array.isArray(assessmentData)
                    ? assessmentData[0]
                    : assessmentData;

                const databaseEligible =
                    assessment?.eligible === true &&
                    assessment?.reason ===
                        "DB_ELIGIBLE_BULLMQ_UNVERIFIED" &&
                    assessment?.batch_id === reservation.batch_id;

                const terminalJob =
                    jobState === "failed" ||
                    jobState === "completed";

                let recoveryDecision = "OBSERVE_ONLY";

                if (!job) {
                    recoveryDecision = "JOB_NOT_FOUND";
                } else if (!tokenMatches) {
                    recoveryDecision = "TOKEN_MISMATCH";
                } else if (!terminalJob) {
                    recoveryDecision =
                        `NON_TERMINAL_JOB:${jobState}`;
                } else if (!databaseEligible) {
                    recoveryDecision =
                        `DATABASE_NOT_ELIGIBLE:${assessment?.reason || "UNKNOWN"}`;
                } else {
                    const { data: recoveryData, error: recoveryError } =
                        await supabase.rpc(
                            "recover_expired_universal_retry_reservation",
                            {
                                p_batch_db_id: reservation.id,
                                p_retry_claim_token: retryToken
                            }
                        );

              if (recoveryError) {
    // RPC response lost ho sakta hai even after DB commit.
    // Verify actual database state before reporting outcome.
    const { data: current, error: readError } =
        await supabase
            .from("sync_batches")
            .select(`
                batch_id,
                batch_status,
                worker_status,
                current_stage,
                error_message,
                retry_claim_token,
                retry_lease_until,
                retry_reservation_origin
            `)
            .eq("id", reservation.id)
            .maybeSingle();

    if (readError || !current) {
        throw new Error(
            "UNIVERSAL_RECOVERY_OUTCOME_UNKNOWN"
        );
    }

    const recoveryCommitted =
        current.batch_id === reservation.batch_id &&
        current.batch_status === "FAILED" &&
        current.worker_status === "FAILED" &&
        current.current_stage === "FAILED" &&
        current.error_message ===
            reservation.retry_original_error_message &&
        current.retry_claim_token === null &&
        current.retry_lease_until === null &&
        current.retry_reservation_origin === null;

    if (recoveryCommitted) {
        console.warn(
            "[CONNECTOR WATCHDOG] UNIVERSAL RECOVERY COMMIT OBSERVED AFTER RPC ERROR",
            {
                batch_id: reservation.batch_id,
                action: "READBACK_CONFIRMED",
                automatic_retry: false
            }
        );
    }

    throw new Error(
        recoveryCommitted
            ? "UNIVERSAL_RECOVERY_COMMIT_OBSERVED"
            : "UNIVERSAL_RECOVERY_OUTCOME_UNKNOWN"
    );
}

                    const recovery = Array.isArray(recoveryData)
                        ? recoveryData[0]
                        : recoveryData;

                    recoveryDecision =
                        recovery?.recovered === true &&
                        recovery?.reason === "RECOVERED" &&
                        recovery?.recovered_batch_id === reservation.batch_id
                            ? "RECOVERED"
                            : `RECOVERY_REJECTED:${recovery?.reason || "INVALID_RESPONSE"}`;
                }

                console.warn(
                    "[CONNECTOR WATCHDOG] UNIVERSAL EXPIRED RETRY ASSESSMENT:",
                    {
                        batch_id: reservation.batch_id,
                        queue_name: assignedWorker.queue_name,
                        job_state: jobState,
                        token_matches: tokenMatches,
                        database_eligible: databaseEligible,
                        database_reason: assessment?.reason ?? "UNKNOWN",
                        recovery_decision: recoveryDecision,
                        retry_claim_token: retryToken,
                        automatic_retry: false
                    }
                );
            } finally {
                await queue.close();
            }
        } catch (error) {
            console.error(
                "[CONNECTOR WATCHDOG] UNIVERSAL EXPIRED RETRY INSPECTION FAILED:",
                reservation.batch_id,
                error.message
            );
        }
    }
}

// =====================================================
// END UNIVERSAL FAILED RETRY RECOVERY
// =====================================================

      // ----------------------------------------------------
// 1. Get batches requiring connector monitoring
//
// A) PROCESSING + RUNNING
// B) PENDING + PENDING + WAITING_CONNECTOR
// C) FAILED + FAILED (universal recovery evaluation)
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
                    execution_claim_token,
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

    // ----------------------------------------------------
// UNCERTAIN OUTCOME SAFETY CLASSIFICATION
// Evaluate FAILED batches regardless of error reason.
// Never permit uncertain outcomes to be replayed.
// ----------------------------------------------------
const uncertainOutcome =
    String(batch.error_message || "")
        .startsWith("CONNECTOR_OUTCOME_UNKNOWN:") ||
    String(batch.error_message || "")
        .startsWith("HTTP_OUTCOME_UNKNOWN:");

if (uncertainOutcome) {
    console.warn(
        "[CONNECTOR WATCHDOG] UNCERTAIN OUTCOME — REPLAY BLOCKED",
        {
            batch_id: batch.batch_id,
            batch_status: batch.batch_status,
            worker_status: batch.worker_status,
            original_error: batch.error_message || null,
            recovery_candidate:
                batch.batch_status === "FAILED" &&
                batch.worker_status === "FAILED",
            action: "MANUAL_REVIEW_REQUIRED"
        }
    );

    continue;
}

      // ----------------------------------------------------
// UNIVERSAL FAILED BATCH RECOVERY EVALUATION
// Every FAILED batch is evaluated regardless of error.
// Actual retry remains blocked until execution settlement
// is independently verified.
// ----------------------------------------------------
if (
    batch.batch_status === "FAILED" &&
    batch.worker_status === "FAILED"
) {
    console.log(
        "[CONNECTOR WATCHDOG] FAILED RECOVERY EVALUATION",
        {
            batch_id: batch.batch_id,
            original_error: batch.error_message || null,
            retry_count: Number(batch.retry_count ?? 0),
            action: "EVALUATE_ONLY"
        }
    );

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

    
    // =====================================================
    // UNIVERSAL FAILED RETRY — LATEST BATCH ONLY
    // =====================================================
    // Check the latest batch across ALL statuses.
    // Older FAILED batches must never be retried.
    // The SQL claim RPC independently rechecks this.

    const { data: latestBatches, error: latestBatchError } =
        await supabase
            .from("sync_batches")
            .select("id, batch_id, batch_status")
            .eq("company_code", companyCode)
            .eq("tally_owner", tallyOwner)
            .order("created_at", { ascending: false })
            .order("id", { ascending: false })
            .limit(1);

    if (latestBatchError) {
        throw latestBatchError;
    }

    const latestBatch = latestBatches?.[0];

    if (!latestBatch || String(latestBatch.id) !== String(batch.id)) {
        console.log(
            "[CONNECTOR WATCHDOG] UNIVERSAL RETRY SKIPPED — NOT LATEST BATCH",
            {
                batch_id: batch.batch_id,
                latest_batch_id: latestBatch?.batch_id || null,
                latest_batch_status: latestBatch?.batch_status || null,
                action: "SKIP_OLDER_FAILED_BATCH"
            }
        );

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
    "[CONNECTOR WATCHDOG] UNIVERSAL FAILED RETRY DEFERRED",
    {
        batch_id: batch.batch_id,
        connector_id: connectorId,
        reason: "Connector socket unavailable, Tally offline, or heartbeat not fresh",
        action: "DEFER_RETRY"
    }
);

        continue;
    }

  // ----------------------------------------------------
// FAILED RECOVERY — READ-ONLY SETTLEMENT ASSESSMENT
// ----------------------------------------------------

let httpSettled = false;
let connectorSettled = false;
let settlementReason = "UNVERIFIED";

if (!batch.execution_claim_token) {
    settlementReason = "EXECUTION_CLAIM_TOKEN_MISSING";
} else {
    try {
        const [httpResponse, connectorResponse] =
            await Promise.all([
                supabase.rpc("assess_sync_http_settlement", {
                    p_batch_id: batch.batch_id,
                    p_execution_claim_token: batch.execution_claim_token
                }),
                supabase.rpc("assess_sync_connector_settlement", {
                    p_batch_id: batch.batch_id,
                    p_execution_claim_token: batch.execution_claim_token
                })
            ]);

        if (httpResponse.error || connectorResponse.error) {
            settlementReason = "SETTLEMENT_RPC_FAILED";
        } else {
            const http = Array.isArray(httpResponse.data)
                ? httpResponse.data[0]
                : httpResponse.data;

            const connector = Array.isArray(connectorResponse.data)
                ? connectorResponse.data[0]
                : connectorResponse.data;
/* 041126
           httpSettled =
                http?.decision === "TERMINATED" &&
                http?.execution_state === "TERMINATED" &&
                Boolean(http?.terminated_at) &&
                [
                    "HANDLER_COMPLETED_SUCCESS",
                    "HANDLER_EXITED"
                ].includes(http?.terminal_outcome);
*/

            // A batch may safely have no HTTP execution record
            // when the current execution never reached HTTP start.
            // This is valid only when connector operations are absent.

            const httpNotStarted =
                http?.decision === "NOT_STARTED" &&
                connector?.decision === "NO_OPERATIONS" &&
                Number(connector?.operation_count) === 0;

            const httpTerminated =
                http?.decision === "TERMINATED" &&
                http?.execution_state === "TERMINATED" &&
                Boolean(http?.terminated_at) &&
                [
                    "HANDLER_COMPLETED_SUCCESS",
                    "HANDLER_EXITED"
                ].includes(http?.terminal_outcome);

            httpSettled =
                httpNotStarted === true ||
                httpTerminated === true;
            const total = Number(connector?.operation_count);
            const prepared = Number(connector?.prepared_count);
            const dispatchPossible =
                Number(connector?.dispatch_possible_count);
            const verified = Number(connector?.verified_count);
            const unknown = Number(connector?.unknown_count);

            connectorSettled =
                [
                    total,
                    prepared,
                    dispatchPossible,
                    verified,
                    unknown
                ].every(Number.isSafeInteger) &&
                total >= 0 &&
                prepared === 0 &&
                dispatchPossible === 0 &&
                unknown === 0 &&
                verified === total &&
                (
                    connector?.decision === "HAS_VERIFIED_OPERATIONS" ||
                    (
                        connector?.decision === "NO_OPERATIONS" &&
                        total === 0
                    )
                );

            settlementReason =
                httpSettled && connectorSettled
                    ? "SETTLEMENT_ASSESSMENT_PASSED"
                    : "EXECUTION_SETTLEMENT_UNVERIFIED";
        }
    } catch (error) {
        settlementReason = "SETTLEMENT_ASSESSMENT_ERROR";

        console.error(
            "[CONNECTOR WATCHDOG] SETTLEMENT ASSESSMENT ERROR:",
            batch.batch_id,
            error
        );
    }
}

const executionSettled =
    httpSettled === true &&
    connectorSettled === true;

console.log(
    "[CONNECTOR WATCHDOG] FAILED SETTLEMENT ASSESSMENT",
    {
        batch_id: batch.batch_id,
        http_settled: httpSettled,
        connector_settled: connectorSettled,
        execution_settled: executionSettled,
        settlement_reason: settlementReason,
        action: executionSettled
            ? "CHECK_CONNECTOR_AVAILABILITY"
            : "DEFER_RETRY"
    }
);
  // ----------------------------------------------------
// UNIVERSAL FAILED RECOVERY — CONNECTOR HEALTH VERIFIED
// Connector is healthy, but retry eligibility depends
// on verified settlement and global connector availability.
// ----------------------------------------------------

console.log(
    "[CONNECTOR WATCHDOG] FAILED RECOVERY READINESS",
    {
        batch_id: batch.batch_id,
        original_error: batch.error_message || null,
        connector_id: connectorId,
        connector_healthy: true,
        execution_settled: executionSettled,
        action: executionSettled
            ? "CHECK_GLOBAL_CONNECTOR_AVAILABILITY"
            : "DEFER_RETRY"
    }
);
// ----------------------------------------------------
// GLOBAL CONNECTOR AVAILABILITY EVALUATION
// FREE is necessary for future dispatch, but not
// sufficient to establish previous execution settlement.
// ----------------------------------------------------
const globalWatchdog =
    await connectorHealthStore.getConnectorWatchdogStatus(connectorId);

const connectorFree =
    globalWatchdog?.operation_status === "FREE" &&
    globalWatchdog?.available === true &&
    String(globalWatchdog?.socket_id || "").trim() ===
        String(socket.id || "").trim() &&
    socket.connected === true &&
    socket.tallyRequestActive !== true;

console.log(
    "[CONNECTOR WATCHDOG] UNIVERSAL FAILED RECOVERY DECISION",
    {
        batch_id: batch.batch_id,
        original_error: batch.error_message || null,
        connector_id: connectorId,
        connector_free: connectorFree,
        global_operation_status:
            globalWatchdog?.operation_status || "UNKNOWN",
        retry_count: Number(batch.retry_count ?? 0),
        execution_settled: executionSettled,
        recovery_allowed:
            executionSettled === true &&
            connectorFree === true,
        reason: !executionSettled
            ? "Previous execution settlement unverified"
            : !connectorFree
                ? "Global connector unavailable or busy"
                : "Ready for atomic retry claim",
        action:
            executionSettled === true && connectorFree === true
                ? "ATTEMPT_ATOMIC_RETRY_CLAIM"
                : "DEFER_RETRY"
    }
);

if (
    httpSettled !== true ||
    connectorSettled !== true ||
    connectorFree !== true
) {
    continue;
}
// Dispatch only after settlement and connector availability
// checks pass. The dispatch function performs the final
// atomic database claim and original-worker validation.
const retryDispatchResult =
    await dispatchUniversalFailedRetry(batch);

console.log(
    "[CONNECTOR WATCHDOG] UNIVERSAL FAILED RETRY DISPATCH",
    {
        batch_id: batch.batch_id,
        result: retryDispatchResult
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
 const globalHealth =
    await connectorHealthStore.getConnectorHealth(
        connectorId
    );

const globallyOffline =
    globalHealth?.connection_status === "DISCONNECTED" &&
    Boolean(globalHealth?.socket_id);

if (!globallyOffline) {
    if (
        globalHealth?.connection_status === "CONNECTED" &&
        globalHealth?.socket_id &&
        batch.waiting_connector_since &&
        batch.execution_claim_token
    ) {
        const { error: resetError } = await supabase
            .from("sync_batches")
            .update({ waiting_connector_since: null })
            .eq("batch_id", batch.batch_id)
            .eq("batch_status", "PROCESSING")
            .eq("worker_status", "RUNNING")
            .eq("execution_claim_token", batch.execution_claim_token)
            .eq("waiting_connector_since", batch.waiting_connector_since);

        if (resetError) throw resetError;
    }

    console.log(
        "[CONNECTOR WATCHDOG] OFFLINE TIMER DEFERRED",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            global_connection_status:
                globalHealth?.connection_status ?? "UNKNOWN",
            global_tally_status:
                globalHealth?.tally_status ?? "UNKNOWN",
            reason: "Global offline state not confirmed"
        }
    );

    continue;
}
    const waitingSince = await ensureWaitingConnectorSince(
        supabase,
        batch
    );

    console.log(
        "[CONNECTOR WATCHDOG] OFFLINE TIMER",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            offline_age_seconds: Math.floor(
                (Date.now() - waitingSince) / 1000
            )
        }
    );

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
    const globalHealth =
    await connectorHealthStore.getConnectorHealth(
        connectorId
    );

if (
    globalHealth?.connection_status !== "CONNECTED" ||
    globalHealth?.tally_status !== "OFFLINE" ||
    globalHealth?.socket_id !== socket.id
) {
    console.log(
        "[CONNECTOR WATCHDOG] TALLY OFFLINE TIMER DEFERRED",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            reason: "Global Tally OFFLINE not verified for socket owner"
        }
    );

    continue;
}
    const waitingSince = await ensureWaitingConnectorSince(
        supabase,
        batch
    );

    console.log(
        "[CONNECTOR WATCHDOG] TALLY OFFLINE TIMER",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            offline_age_seconds: Math.floor(
                (Date.now() - waitingSince) / 1000
            )
        }
    );

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
    !globalWatchdog ||
    globalWatchdog.operation_status !== "FREE" ||
    globalWatchdog.available !== true
) {
    console.log(
        "⏳ CONNECTOR WATCHDOG: GLOBAL CONNECTOR NOT FREE",
        {
            batch_id: batch.batch_id,
            connector_id: connectorId,
            operation_status: globalWatchdog?.operation_status ?? "UNKNOWN",
lock_ttl_ms: globalWatchdog?.lock_ttl_ms ?? null
        }
    );

    continue;
}

// Verify previous HTTP execution settlement before recovery.
if (!batch.execution_claim_token) {
    console.warn(
        "[CONNECTOR WATCHDOG] RECOVERY DEFERRED: CLAIM MISSING",
        { batch_id: batch.batch_id }
    );
    continue;
}

const { data: settlement, error: settlementError } =
    await supabase.rpc("assess_sync_http_settlement", {
        p_batch_id: batch.batch_id,
        p_execution_claim_token: batch.execution_claim_token
    });

if (settlementError || !settlement) {
    console.warn(
        "[CONNECTOR WATCHDOG] RECOVERY DEFERRED: SETTLEMENT CHECK FAILED",
        {
            batch_id: batch.batch_id,
            error: settlementError?.message || "No settlement result"
        }
    );
    continue;
}

const httpNotStarted = settlement.decision === "NOT_STARTED";

console.log(
    "[CONNECTOR WATCHDOG] HTTP SETTLEMENT ASSESSED",
    {
        batch_id: batch.batch_id,
        decision: settlement.decision,
        http_not_started: httpNotStarted,
        recovery_allowed: false
    }
);
if (!httpNotStarted) {
    console.warn(
        "[CONNECTOR WATCHDOG] RECOVERY BLOCKED: HTTP NOT PROVEN UNSTARTED",
        {
            batch_id: batch.batch_id,
            settlement_decision: settlement.decision,
            action: "MANUAL_REVIEW_REQUIRED"
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
    heartbeatAge >= 0 &&
    heartbeatAge < HEARTBEAT_TIMEOUT_MS &&
    socket.connected === true &&
    socket.tallyStatus === "ONLINE"
) {
    const globalWatchdog =
        await connectorHealthStore.getConnectorWatchdogStatus(
            connectorId
        );

    const globallyHealthy =
    globalWatchdog?.connection_status === "CONNECTED" &&
    globalWatchdog?.socket_id === socket.id;

    if (
        globallyHealthy &&
        batch.batch_status === "PROCESSING" &&
        batch.worker_status === "RUNNING" &&
        batch.execution_claim_token &&
        batch.waiting_connector_since
    ) {
        const { error: resetError } = await supabase
            .from("sync_batches")
            .update({ waiting_connector_since: null })
            .eq("batch_id", batch.batch_id)
            .eq("batch_status", "PROCESSING")
            .eq("worker_status", "RUNNING")
            .eq(
                "execution_claim_token",
                batch.execution_claim_token
            )
            .eq(
                "waiting_connector_since",
                batch.waiting_connector_since
            );

        if (resetError) {
            throw resetError;
        }
    }

    console.log(
        "💓 CONNECTOR WATCHDOG: ALIVE",
        {
            batch_id: batch.batch_id,
            company_code: companyCode,
            connector_id: connectorId,
            heartbeat_age_seconds: heartbeatAgeSeconds,
            stage: batch.current_stage,
            action: batch.current_action
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