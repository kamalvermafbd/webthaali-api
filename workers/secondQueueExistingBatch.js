require("dotenv").config();
const { randomUUID } = require("crypto");
const { createClient } =
    require("@supabase/supabase-js");

const {
    addBatchToQueue
} = require("./secondAddBatchToQueue");

const { resolveWorkerForBatch } = require("./secondResolveWorkerForBatch");



const DISPATCH_INTERVAL_MS =
    Number(
        process.env.SYNC_DISPATCH_INTERVAL_MS || 5000
    );

const DISPATCH_BATCH_SIZE =
    Number(
        process.env.SYNC_DISPATCH_BATCH_SIZE || 25
    );


let dispatcherRunning = false;

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

/*211026
async function rollbackConnectorLossRetry(batch, workerName, originalError) {
    const { error: rollbackError } = await supabase
        .from("sync_batches")
        .update({
            batch_status: "FAILED",
            worker_status: "FAILED",
            worker_id: null,
            locked_at: null,
            heartbeat_at: null,
            error_message: originalError
        })
        .eq("id", batch.id)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("worker_id", workerName);

    if (rollbackError) {
        console.error(
            "CONNECTOR RETRY ROLLBACK FAILED:",
            batch.batch_id,
            rollbackError.message
        );
        return false;
    }

    return true;
}
*/

async function rollbackConnectorLossRetry(
    batch,
    workerName,
    originalError,
    retryClaimToken
) {
    console.error(
        "[CONNECTOR RETRY] AUTOMATIC ROLLBACK DEFERRED",
        {
            batch_id: batch.batch_id,
            worker_name: workerName,
            retry_claim_token: retryClaimToken,
            original_error: originalError,
            reason: "ATOMIC_QUEUE_RECONCILIATION_REQUIRED"
        }
    );

    // Never convert a potentially claimed/queued retry
    // back to FAILED using a non-atomic database update.
    return false;
}


async function inspectConnectorRetryJob(
    queueName,
    batch,
    workerConfig
) {
    const { getSyncQueue } = require("./secondSyncQueue");
    const syncQueue = getSyncQueue(queueName);

  try {
    const retryToken = batch.retry_claim_token ?? null;

    const bullJobId = retryToken
        ? `${batch.batch_id}-retry-${retryToken}`
        : batch.batch_id;

    const job = await syncQueue.getJob(bullJobId);

    if (!job) {
            return {
                state: "NOT_FOUND",
                matches: false
            };
        }

        const state = await job.getState();
        const data = job.data || {};

        const matches =
    data.batch_id === batch.batch_id &&
    String(data.batch_db_id) === String(batch.id) &&
    data.worker_name === workerConfig.worker_name &&
    String(data.worker_id) === String(workerConfig.id) &&
    data.server_id === workerConfig.server_id &&
    (data.retry_claim_token ?? null) ===
        (batch.retry_claim_token ?? null);

        return {
            state,
            matches
        };
    } finally {
        await syncQueue.close();
    }
}


async function dispatchConnectorLossRetry(batch) {
    if (!batch?.id || !batch?.batch_id) {
        throw new Error("Retry batch id missing");
    }

     // Only the exact offline-timeout failure may enter
// the dedicated atomic recovery path.
const isOfflineTimeout =
    batch.batch_status === "FAILED" &&
    batch.worker_status === "FAILED" &&
    batch.current_stage === "FAILED" &&
    batch.error_message === "CONNECTOR_OFFLINE_TIMEOUT";

if (!isOfflineTimeout) {
    console.warn(
        "[CONNECTOR RETRY] DISPATCH DEFERRED",
        {
            batch_id: batch.batch_id,
            reason: "EXECUTION_SETTLEMENT_UNVERIFIED"
        }
    );

    return {
        success: false,
        queued: false,
        reason: "EXECUTION_SETTLEMENT_UNVERIFIED"
    };
}
   
    const originalError = String(batch.error_message || "");

    if (
        batch.batch_status !== "FAILED" ||
        batch.worker_status !== "FAILED"
    ) {
        console.log(
            "FAILED RETRY SKIPPED: NOT ELIGIBLE",
            batch.batch_id,
            batch.batch_status,
            batch.worker_status
        );

        return {
            success: false,
            reason: "NOT_ELIGIBLE"
        };
    }

    // Resolve worker using the existing routing configuration.
    // Do not retry while another batch of this company
// and Tally owner is actively processing.
const { data: runningBatch, error: runningError } = await supabase
    .from("sync_batches")
    .select("id, batch_id")
    .eq("company_code", batch.company_code)
    .eq("tally_owner", batch.tally_owner)
    .eq("batch_status", "PROCESSING")
    .eq("worker_status", "RUNNING")
    .neq("id", batch.id)
    .limit(1)
    .maybeSingle();

if (runningError) {
    throw new Error(
        `Retry running-batch check failed: ${runningError.message}`
    );
}

if (runningBatch) {
    console.log(
        "CONNECTOR LOSS RETRY DEFERRED: ANOTHER BATCH RUNNING",
        {
            retry_batch_id: batch.batch_id,
            running_batch_id: runningBatch.batch_id
        }
    );

    return {
        success: false,
        reason: "ANOTHER_BATCH_RUNNING"
    };
}
   const workerConfig = await resolveWorkerForBatch(batch, {
    connectorLossRetry: true
});

    if (!workerConfig?.worker_name || !workerConfig?.queue_name) {
        throw new Error(
            `No valid retry worker for batch ${batch.batch_id}`
        );
    }

    // Claim only if this exact batch is still connector-loss FAILED.
  /* 211026
    const { data: claimed, error: claimError } = await supabase
        .from("sync_batches")
        .update({
            batch_status: "PENDING",
            worker_status: "PENDING",
            worker_id: workerConfig.worker_name,
            locked_at: new Date(),
            heartbeat_at: null,
            completed_at: null,
            error_message: null,
            current_stage: "PENDING",
            current_module: "MASTERS",
            current_action: "PENDING"
        })
        .eq("id", batch.id)
        .eq("batch_status", "FAILED")
        .eq("worker_status", "FAILED")
        .like("error_message", "CONNECTOR_LOSS:%")
        .select()
        .maybeSingle();

    if (claimError) {
        throw new Error(
            `Retry claim failed for ${batch.batch_id}: ${claimError.message}`
        );
    }

    if (!claimed) {
        console.log(
            "CONNECTOR RETRY SKIPPED: ALREADY CLAIMED OR CHANGED",
            batch.batch_id
        );

        return {
            success: false,
            reason: "ALREADY_CLAIMED"
        };
    }
*/


   // Unique ownership token for this retry attempt.
const retryClaimToken = randomUUID();

// Atomically claim the batch with this ownership token.
const { data: claimResult, error: claimError } = await supabase
    .rpc("claim_connector_offline_timeout_retry", {
        p_company_code: batch.company_code,
        p_tally_owner: batch.tally_owner,
        p_expected_batch_id: batch.batch_id,
        p_worker_name: workerConfig.worker_name,
        p_retry_claim_token: retryClaimToken
    });

    if (claimError) {
        throw new Error(
            `Retry claim failed for ${batch.batch_id}: ${claimError.message}`
        );
    }

    const retryClaim = Array.isArray(claimResult)
        ? claimResult[0]
        : claimResult;

    if (
        !retryClaim?.claimed ||
        retryClaim.claimed_batch_id !== batch.batch_id
    ) {
        console.log(
            "CONNECTOR RETRY SKIPPED:",
            retryClaim?.reason || "CLAIM_REJECTED",
            batch.batch_id,
            "LATEST:",
            retryClaim?.claimed_batch_id || null
        );

        return {
            success: false,
            reason: retryClaim?.reason || "CLAIM_REJECTED"
        };
    }

    // Reload the claimed row so queue payload uses current database state.
 const { data: claimed, error: loadClaimedError } = await supabase
    .from("sync_batches")
    .select("*")
    .eq("id", batch.id)
    .eq("batch_id", batch.batch_id)
    .eq("worker_id", workerConfig.worker_name)
    .eq("batch_status", "PENDING")
    .eq("worker_status", "PENDING")
    .eq("current_stage", "PENDING")
    .eq("retry_claim_token", retryClaimToken)
    .gt("retry_lease_until", new Date().toISOString())
    .maybeSingle();


  if (loadClaimedError || !claimed) {
    const uncertainError = new Error(
        `CONNECTOR_RETRY_CLAIM_RELOAD_UNCERTAIN: ${batch.batch_id}: ${
            loadClaimedError?.message || "Claimed batch not found"
        }`
    );

    uncertainError.code = "CONNECTOR_RETRY_CLAIM_RELOAD_UNCERTAIN";

    // Preserve reservation for independent reconciliation.
    // Do not rollback when ownership/read outcome is uncertain.
    throw uncertainError;
}


    try {
    const { data: currentClaim, error: ownershipError } =
        await supabase
            .from("sync_batches")
            .select("id")
            .eq("id", batch.id)
            .eq("batch_status", "PENDING")
            .eq("worker_status", "PENDING")
            .eq("worker_id", workerConfig.worker_name)
            .eq("retry_claim_token", retryClaimToken)
            .gt("retry_lease_until", new Date().toISOString())
            .maybeSingle();

    if (ownershipError || !currentClaim) {
        throw new Error(
            `CONNECTOR_RETRY_OWNERSHIP_LOST: ${batch.batch_id}`
        );
    }

    await addBatchToQueue(claimed, workerConfig);

        console.log(
            "CONNECTOR LOSS RETRY QUEUED:",
            batch.batch_id,
            "QUEUE:",
            workerConfig.queue_name,
            "WORKER:",
            workerConfig.worker_name
        );

       return {
    success: true,
    queued: true,
    batch: claimed,
    worker: workerConfig
};
    
    
    } catch (queueError) {

    if (
        String(queueError.message || "").startsWith(
            "CONNECTOR_RETRY_OWNERSHIP_LOST:"
        )
    ) {
        console.error(
            "CONNECTOR RETRY ABORTED: OWNERSHIP LOST",
            batch.batch_id
        );

        throw queueError;
    }

    let queueCheck;

        try {
            queueCheck = await inspectConnectorRetryJob(
                workerConfig.queue_name,
                claimed,
                workerConfig
            );
        } catch (stateError) {
            console.error(
                "CONNECTOR RETRY QUEUE CHECK FAILED:",
                batch.batch_id,
                stateError.message
            );

            // Preserve the reservation until queue state can be verified.
            throw queueError;
        }

        console.error(
            "CONNECTOR RETRY QUEUE ADD ERROR:",
            batch.batch_id,
            queueError.message,
            "QUEUE STATE:",
            queueCheck.state,
            "ASSIGNMENT MATCH:",
            queueCheck.matches
        );

        const activeStates = [
            "waiting",
            "active",
            "delayed",
            "prioritized",
            "waiting-children"
        ];

        if (activeStates.includes(queueCheck.state)) {
            if (queueCheck.matches) {
                return {
                    success: true,
                    queued: true,
                    reason: "JOB_ALREADY_EXISTS",
                    batch: claimed,
                    worker: workerConfig
                };
            }

            // Do not rollback while a conflicting job exists.
            throw queueError;
        }

        if (queueCheck.state !== "NOT_FOUND") {
    console.error(
        "CONNECTOR RETRY QUEUE STATE UNCERTAIN",
        {
            batch_id: batch.batch_id,
            queue_state: queueCheck.state,
            assignment_matches: queueCheck.matches
        }
    );

    // Never rollback when a BullMQ job exists
    // or its state cannot be conclusively reconciled.
    throw queueError;
}

      const rolledBack = await rollbackConnectorLossRetry(
    batch,
    workerConfig.worker_name,
    originalError,
    retryClaimToken
);

if (!rolledBack) {
    const uncertainError = new Error(
        `CONNECTOR_RETRY_RESERVATION_UNCERTAIN: ${batch.batch_id}`
    );

    uncertainError.code = "CONNECTOR_RETRY_RESERVATION_UNCERTAIN";
    uncertainError.cause = queueError;

    throw uncertainError;
}

throw queueError;
    }


}

async function dispatchUniversalFailedRetry(batch) {
    if (!batch?.id || !batch?.batch_id) {
        throw new Error("Universal retry batch id missing");
    }

    const batchError = String(batch.error_message || "");

    if (
        batch.batch_status !== "FAILED" ||
        batch.worker_status !== "FAILED" ||
        batch.current_stage !== "FAILED" ||
        !batch.execution_claim_token ||
        !batchError ||
        batchError.startsWith("HTTP_OUTCOME_UNKNOWN:") ||
        batchError.startsWith("CONNECTOR_OUTCOME_UNKNOWN:") ||
        Number(batch.retry_count || 0) >= 3
    ) {
        return {
            success: false,
            queued: false,
            reason: "NOT_ELIGIBLE"
        };
    }

   // Resolve an eligible worker for the universal FAILED retry.
// The original worker may be unavailable or NULL.
const workerConfig = await resolveWorkerForBatch(batch, {
    connectorLossRetry: true,
    universalFailedRetry: true
});

if (
    !workerConfig?.worker_name ||
    !workerConfig?.queue_name ||
    !workerConfig?.id ||
    !workerConfig?.server_id
) {
    return {
        success: false,
        queued: false,
        reason: "RETRY_WORKER_UNAVAILABLE"
    };
}

    const retryClaimToken = randomUUID();

    // SQL performs atomic eligibility, FIFO and capacity checks.
    const { data: claimResult, error: claimError } =
        await supabase.rpc(
            "claim_universal_failed_sync_retry",
            {
                p_company_code: batch.company_code,
                p_tally_owner: batch.tally_owner,
                p_expected_batch_id: batch.batch_id,
                p_worker_name: workerConfig.worker_name,
                p_retry_claim_token: retryClaimToken
            }
        );

    if (claimError) {
        throw new Error(
            `UNIVERSAL_RETRY_CLAIM_UNCERTAIN: ${batch.batch_id}: ${claimError.message}`
        );
    }

    const retryClaim = Array.isArray(claimResult)
        ? claimResult[0]
        : claimResult;

    if (
        retryClaim?.claimed !== true ||
        retryClaim.claimed_batch_id !== batch.batch_id
    ) {
        return {
            success: false,
            queued: false,
            reason: retryClaim?.reason || "CLAIM_REJECTED"
        };
    }

    // Reload the exact reservation owned by this dispatcher.
    const { data: reserved, error: reloadError } =
        await supabase
            .from("sync_batches")
            .select("*")
            .eq("id", batch.id)
            .eq("batch_id", batch.batch_id)
            .eq("worker_id", workerConfig.worker_name)
            .eq("batch_status", "PENDING")
            .eq("worker_status", "PENDING")
            .eq("current_stage", "PENDING")
            .eq("retry_reservation_origin", "UNIVERSAL_FAILED_RETRY")
            .eq("retry_claim_token", retryClaimToken)
            .gt("retry_lease_until", new Date().toISOString())
            .maybeSingle();

    if (reloadError || !reserved) {
        throw new Error(
            `UNIVERSAL_RETRY_RESERVATION_UNCERTAIN: ${batch.batch_id}`
        );
    }

    try {
        await addBatchToQueue(reserved, workerConfig);

        console.log(
            "[UNIVERSAL RETRY] QUEUED",
            {
                batch_id: batch.batch_id,
                worker_name: workerConfig.worker_name,
                queue_name: workerConfig.queue_name
            }
        );

        return {
            success: true,
            queued: true,
            batch: reserved,
            worker: workerConfig
        };
    } catch (queueError) {
        // Queue add may have succeeded even if its response was lost.
        let queueCheck;

        try {
            queueCheck = await inspectConnectorRetryJob(
                workerConfig.queue_name,
                reserved,
                workerConfig
            );
        } catch (inspectError) {
            console.error(
                "[UNIVERSAL RETRY] QUEUE INSPECTION FAILED",
                batch.batch_id,
                inspectError.message
            );

            throw queueError;
        }

        const activeStates = [
            "waiting",
            "active",
            "delayed",
            "prioritized",
            "waiting-children"
        ];

        if (
            queueCheck.matches === true &&
            activeStates.includes(queueCheck.state)
        ) {
            return {
                success: true,
                queued: true,
                reason: "JOB_ALREADY_EXISTS",
                batch: reserved,
                worker: workerConfig
            };
        }

        // Never rollback an uncertain reservation.
        // Recovery must independently verify queue and execution state.
        console.error(
            "[UNIVERSAL RETRY] QUEUE HANDOFF UNCERTAIN",
            {
                batch_id: batch.batch_id,
                queue_state: queueCheck.state,
                assignment_matches: queueCheck.matches,
                error: queueError.message
            }
        );

        throw queueError;
    }
}

async function inspectNormalBatchJob(
    batch,
    workerConfig
) {
    const { getSyncQueue } = require("./secondSyncQueue");

    const syncQueue = getSyncQueue(workerConfig.queue_name);

    const retryToken = batch.retry_claim_token ?? null;

    const bullJobId = retryToken
        ? `${batch.batch_id}-retry-${retryToken}`
        : batch.batch_id;

    try {
        const job = await syncQueue.getJob(bullJobId);

        if (!job) {
            return {
                state: "NOT_FOUND",
                matches: false
            };
        }

        const state = await job.getState();
        const data = job.data || {};

        const matches =
            data.batch_id === batch.batch_id &&
            String(data.batch_db_id) === String(batch.id) &&
            data.worker_name === workerConfig.worker_name &&
            String(data.worker_id) === String(workerConfig.id) &&
            data.server_id === workerConfig.server_id &&
            (data.retry_claim_token ?? null) === retryToken;

        return {
            state,
            matches
        };
    } finally {
        await syncQueue.close();
    }
}


async function dispatchBatch(batch) {

    if (!batch) {
        throw new Error("Batch is required");
    }

    // Never dispatch a batch with uncertain Tally or HTTP outcome.
const batchError = String(batch.error_message || "");

if (
    batchError.startsWith("CONNECTOR_OUTCOME_UNKNOWN:") ||
    batchError.startsWith("HTTP_OUTCOME_UNKNOWN:")
) {
    console.error(
        "[BATCH DISPATCH] UNCERTAIN OUTCOME — MANUAL REVIEW:",
        batch.batch_id
    );

    return {
        success: false,
        queued: false,
        reason: "OUTCOME_UNKNOWN"
    };
}

    const workerConfig =
        await resolveWorkerForBatch(batch);

    if (
        !workerConfig?.worker_name ||
        !workerConfig?.queue_name
    ) {
        throw new Error(
            `No valid worker configuration for batch ${batch.batch_id}`
        );
    }

   // Atomically reserve worker capacity in PostgreSQL.
const {
    data: reservationResult,
    error: reserveError
} = await supabase.rpc(
    "reserve_normal_sync_batch",
    {
        p_batch_db_id: batch.id,
        p_worker_name: workerConfig.worker_name,
        p_sync_mode: batch.sync_mode ?? null,
        p_sync_period: batch.sync_period ?? null,
        p_job_type: batch.job_type ?? null,
        p_worker_type: batch.worker_type ?? null,
        p_retry_claim_token: batch.retry_claim_token ?? null
    }
);

if (reserveError) {
    throw new Error(
        "BATCH RESERVATION RPC FAILED: " +
        reserveError.message
    );
}

const reservation = Array.isArray(reservationResult)
    ? reservationResult[0]
    : reservationResult;

if (
    reservation?.reserved !== true ||
    reservation?.reserved_batch_id == null ||
    String(reservation.reserved_batch_id) !== String(batch.id)
) {
    throw new Error(
        `BATCH RESERVATION REJECTED: ${
            reservation?.reason || "UNKNOWN"
        }`
    );
}

// Reload the reserved batch for existing queue logic.

    // Reload the exact reserved batch for existing queue logic.
    let reloadQuery = supabase
        .from("sync_batches")
        .select("*")
        .eq("id", batch.id)
        .eq("worker_id", workerConfig.worker_name)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "PENDING");

    if (batch.retry_claim_token == null) {
        reloadQuery = reloadQuery.is(
            "retry_claim_token",
            null
        );
    } else {
        reloadQuery = reloadQuery.eq(
            "retry_claim_token",
            batch.retry_claim_token
        );
    }

    const {
        data: reserved,
        error: reloadError
    } = await reloadQuery.maybeSingle();

    if (reloadError || !reserved) {
        const uncertainError = new Error(
            `BATCH_RESERVATION_STATE_UNCERTAIN: ${batch.batch_id}`
        );

        uncertainError.code =
            "BATCH_RESERVATION_STATE_UNCERTAIN";

        throw uncertainError;
    }


    try {

        await addBatchToQueue(
            reserved,
            workerConfig
        );

        console.log(
            "BATCH QUEUED:",
            batch.batch_id,
            "→",
            workerConfig.worker_name
        );

       return {
    success: true,
    queued: true,
    batch: reserved,
    worker: workerConfig
};

   } catch (queueError) {

    let queueCheck;

    try {
        queueCheck = await inspectNormalBatchJob(
            reserved,
            workerConfig
        );
    } catch (inspectError) {
        console.error(
            "❌ NORMAL BATCH QUEUE INSPECTION FAILED:",
            {
                batch_id: batch.batch_id,
                queue_error: queueError.message,
                inspect_error: inspectError.message
            }
        );

        // Queue state unknown: do not release reservation.
        const uncertainError = new Error(
    `BATCH_QUEUE_STATE_UNCERTAIN: ${batch.batch_id}: ${queueError.message}`
);

uncertainError.code = "BATCH_QUEUE_STATE_UNCERTAIN";
uncertainError.cause = queueError;

throw uncertainError;
    }

    const activeStates = [
        "waiting",
        "active",
        "delayed",
        "prioritized",
        "waiting-children"
    ];

    if (activeStates.includes(queueCheck.state)) {
        if (queueCheck.matches) {
            console.warn(
                "⚠️ NORMAL BATCH JOB ALREADY QUEUED:",
                {
                    batch_id: batch.batch_id,
                    queue_state: queueCheck.state
                }
            );

            return {
                success: true,
                queued: true,
                reason: "JOB_ALREADY_EXISTS",
                batch: reserved,
                worker: workerConfig
            };
        }

        // Conflicting job exists: never rollback.
const uncertainError = new Error(
    `BATCH_QUEUE_STATE_UNCERTAIN: ${batch.batch_id}: conflicting BullMQ job (${queueCheck.state})`
);

uncertainError.code = "BATCH_QUEUE_STATE_UNCERTAIN";
uncertainError.cause = queueError;

throw uncertainError;
}

if (queueCheck.state !== "NOT_FOUND") {
    // Existing job state needs separate handling.
    const uncertainError = new Error(
        `BATCH_QUEUE_STATE_UNCERTAIN: ${batch.batch_id}: BullMQ state=${queueCheck.state}`
    );

    uncertainError.code = "BATCH_QUEUE_STATE_UNCERTAIN";
    uncertainError.cause = queueError;

    throw uncertainError;
}

   
let rollbackQuery = supabase
    .from("sync_batches")
    .update({
        worker_id: null,
        locked_at: null
    })
    .eq("id", reserved.id)
    .eq("batch_status", "PENDING")
    .eq("worker_status", "PENDING")
    .eq("current_stage", "PENDING")
    .eq("worker_id", workerConfig.worker_name)
    .eq("locked_at", reserved.locked_at)
    .or(
        "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
    );

if (reserved.retry_claim_token == null) {
    rollbackQuery = rollbackQuery.is(
        "retry_claim_token",
        null
    );
} else {
    rollbackQuery = rollbackQuery.eq(
        "retry_claim_token",
        reserved.retry_claim_token
    );
}

const {
    data: rolledBack,
    error: rollbackError
} = await rollbackQuery
    .select("id")
    .maybeSingle();


if (rollbackError || !rolledBack) {
    console.error(
        "❌ NORMAL BATCH RESERVATION ROLLBACK NOT CONFIRMED:",
        {
            batch_id: batch.batch_id,
            error: rollbackError?.message || "Reservation changed"
        }
    );

    const uncertainError = new Error(
        `BATCH_QUEUE_STATE_UNCERTAIN: ${batch.batch_id}: rollback not confirmed`
    );

    uncertainError.code = "BATCH_QUEUE_STATE_UNCERTAIN";
    uncertainError.cause = queueError;

    throw uncertainError;
}

const confirmedError = new Error(
    `BATCH_QUEUE_CONFIRMED_NOT_FOUND: ${batch.batch_id}: ${queueError.message}`
);

confirmedError.code = "BATCH_QUEUE_CONFIRMED_NOT_FOUND";
confirmedError.cause = queueError;

throw confirmedError;
}
}
/* 261026
async function dispatchPendingBatches() {

    if (dispatcherRunning) {
        return;
    }

    dispatcherRunning = true;

    try {

        const {
            data: batches,
            error
        } = await supabase
            .from("sync_batches")
            .select("*")
            .eq("batch_status", "PENDING")
            .eq("worker_status", "PENDING")
                .neq(
                    "current_stage",
                    "WAITING_CONNECTOR"
                )
            .not("worker_type", "is", null)
            .not("job_type", "is", null)
            .order("priority", {
                ascending: true
            })
            .order("created_at", {
                ascending: true
            })
            .limit(DISPATCH_BATCH_SIZE);

        if (error) {

            console.error(
                "BATCH DISPATCH LOOKUP FAILED:",
                error.message
            );

            return;
        }

        if (!batches || batches.length === 0) {
            return;
        }

        

        for (const batch of batches) {

            try {

                const workerConfig =
                    await resolveWorkerForBatch(
                        batch
                    );

                if (
                    !workerConfig?.worker_name ||
                    !workerConfig?.queue_name
                ) {
                    continue;
                }

              

                const {
                    data: reserved,
                    error: reserveError
                } = await supabase
                    .from("sync_batches")
                   .update({
                            worker_id: workerConfig.worker_name,
                            locked_at: new Date(),
                            sync_mode: batch.sync_mode,
                            sync_period: batch.sync_period,
                            job_type: batch.job_type,
                            worker_type: batch.worker_type
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
                    .is(
                        "worker_id",
                        null
                    )
                    .select()
                    .maybeSingle();

                if (reserveError) {

                    console.error(
                        "BATCH RESERVATION FAILED:",
                        batch.batch_id,
                        reserveError.message
                    );

                    continue;
                }

                if (!reserved) {
                    continue;
                }

                try {

                    await addBatchToQueue(
                        reserved,
                        workerConfig
                    );

                    console.log(
                        "AUTO QUEUED:",
                        batch.batch_id,
                        "→",
                        workerConfig.worker_name
                    );

                } catch (queueError) {

                    await supabase
                        .from("sync_batches")
                        .update({
                            worker_id: null,
                            locked_at: null
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
                            "worker_id",
                            workerConfig.worker_name
                        );

                    throw queueError;
                }

            } catch (error) {

                console.error(
                    "BATCH DISPATCH FAILED:",
                    batch.batch_id,
                    error.message
                );
            }
        }

    } finally {

        dispatcherRunning = false;
    }
}
*/

/* 02092026
function startBatchQueueDispatcher() {

    console.log(
        "AUTO BATCH QUEUE DISPATCHER STARTED"
    );

    const run = async () => {

        try {

            await dispatchPendingBatches();

        } catch (error) {

            console.error(
                "AUTO DISPATCHER ERROR:",
                error
            );

        }
    };

    run();

    return setInterval(
        run,
        DISPATCH_INTERVAL_MS
    );
}
    */


module.exports = {
    //startBatchQueueDispatcher
    dispatchBatch,
    dispatchConnectorLossRetry,
    dispatchUniversalFailedRetry
};
