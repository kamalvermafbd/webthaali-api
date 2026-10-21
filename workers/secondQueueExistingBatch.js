require("dotenv").config();

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


async function dispatchConnectorLossRetry(batch) {
    if (!batch?.id || !batch?.batch_id) {
        throw new Error("Retry batch id missing");
    }

    const originalError = String(batch.error_message || "");

    if (
        batch.batch_status !== "FAILED" ||
        batch.worker_status !== "FAILED" ||
        !originalError.startsWith("CONNECTOR_LOSS:")
    ) {
        console.log(
            "CONNECTOR RETRY SKIPPED: NOT ELIGIBLE",
            batch.batch_id
        );

        return {
            success: false,
            reason: "NOT_ELIGIBLE"
        };
    }

    // Resolve worker using the existing routing configuration.
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


    // Atomically verify latest batch and claim connector-loss retry.
    const { data: claimResult, error: claimError } = await supabase
        .rpc("claim_latest_connector_loss_retry", {
            p_company_code: batch.company_code,
            p_tally_owner: batch.tally_owner,
            p_expected_batch_id: batch.batch_id,
            p_worker_name: workerConfig.worker_name
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
        .maybeSingle();

    if (loadClaimedError || !claimed) {
        throw new Error(
            `Retry claimed but batch reload failed for ${batch.batch_id}: ${
                loadClaimedError?.message || "Claimed batch not found"
            }`
        );
    }


    try {
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
        // Roll back only if this retry still owns the reservation.
        const { error: rollbackError } = await supabase
            .from("sync_batches")
            .update({
                batch_status: "FAILED",
                worker_status: "FAILED",
                worker_id: null,
                locked_at: null,
                error_message: originalError
            })
            .eq("id", batch.id)
            .eq("batch_status", "PENDING")
            .eq("worker_status", "PENDING")
            .eq("worker_id", workerConfig.worker_name);

        if (rollbackError) {
            console.error(
                "CONNECTOR RETRY ROLLBACK FAILED:",
                batch.batch_id,
                rollbackError.message
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
