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
    const { data: rolledBack, error: rollbackError } = await supabase
        .from("sync_batches")
      .update({
    batch_status: "FAILED",
    worker_status: "FAILED",
    worker_id: null,
    locked_at: null,
    heartbeat_at: null,
    retry_claim_token: null,
    retry_lease_until: null,
    error_message: originalError
})
.eq("id", batch.id)
.eq("batch_status", "PENDING")
.eq("worker_status", "PENDING")
.eq("worker_id", workerName)
.eq("retry_claim_token", retryClaimToken)
.select("id")
        .maybeSingle();

    if (rollbackError) {
        console.error(
            "CONNECTOR RETRY ROLLBACK FAILED:",
            batch.batch_id,
            rollbackError.message
        );
        return false;
    }

    if (!rolledBack) {
        console.error(
            "CONNECTOR RETRY ROLLBACK SKIPPED: RESERVATION NOT OWNED",
            batch.batch_id,
            workerName
        );
        return false;
    }

    return true;
}


async function inspectConnectorRetryJob(
    queueName,
    batch,
    workerConfig
) {
    const { getSyncQueue } = require("./secondSyncQueue");
    const syncQueue = getSyncQueue(queueName);

    try {
        const job = await syncQueue.getJob(batch.batch_id);

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
    .rpc("claim_latest_connector_loss_retry", {
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
    .eq("batch_id", batch.batch_id)
    .eq("worker_id", workerConfig.worker_name)
    .eq("batch_status", "PENDING")
    .eq("worker_status", "PENDING")
    .eq("retry_claim_token", retryClaimToken)
    .gt("retry_lease_until", new Date().toISOString())
    .maybeSingle();


  if (loadClaimedError || !claimed) {
    await rollbackConnectorLossRetry(
        batch,
        workerConfig.worker_name,
        originalError,
        retryClaimToken
    );

        throw new Error(
            `Retry claimed but batch reload failed for ${batch.batch_id}: ${
                loadClaimedError?.message || "Claimed batch not found"
            }`
        );
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

        if (queueCheck.state === "UNKNOWN") {
            throw queueError;
        }

      const rolledBack = await rollbackConnectorLossRetry(
    batch,
    workerConfig.worker_name,
    originalError,
    retryClaimToken
);

        if (!rolledBack) {
            console.error(
                "CONNECTOR RETRY RESERVATION NEEDS REVIEW:",
                batch.batch_id
            );
        }

        throw queueError;
    }


}


async function dispatchBatch(batch) {

    if (!batch) {
        throw new Error("Batch is required");
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
        throw new Error(
            "BATCH RESERVATION FAILED: "
            + reserveError.message
        );
    }

    if (!reserved) {
        throw new Error(
            `Batch ${batch.batch_id} is already assigned or processed`
        );
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
            batch: reserved,
            worker: workerConfig
        };

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
}

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

                /*
                 * Atomic reservation.
                 *
                 * worker_status stays PENDING because
                 * syncBullWorker -> lockJob() expects PENDING.
                 */

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
    dispatchConnectorLossRetry
};
