require("dotenv").config();

const { createClient } =
    require("@supabase/supabase-js");

const {
    addBatchToQueue
} = require("./addBatchToQueue");

const {
    resolveWorkerForBatch
} = require("./resolveWorkerForBatch");



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
                        worker_id:
                            workerConfig.worker_name,

                        locked_at:
                            new Date()
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

module.exports = {
    startBatchQueueDispatcher
};