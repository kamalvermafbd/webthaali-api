require("dotenv").config();

const {
    createClient
} = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function lockJob(job, workerConfig) {

    const { data, error } =
        await supabase
            .from("sync_batches")
            .update({

                batch_status:
                    "PROCESSING",

                worker_status:
                    "RUNNING",

                worker_id:
                    workerConfig.worker_name,

                locked_at:
                    new Date(),

                started_at:
                    new Date(),

                heartbeat_at:
                    new Date()

            })
            .eq(
                "id",
                job.data.batch_db_id
            )
            .eq(
                "worker_status",
                "PENDING"
            )

            .eq(
                "worker_id",
                workerConfig.worker_name
            )
            .select()
            .single();

    if (error) {

        console.error(
            "Lock failed:",
            error
        );

        return null;
    }

    return data;
}

async function startHeartbeat(
    jobId,
    workerConfig
) {

    console.log(
        "HEARTBEAT STARTED:",
        jobId
    );

    const heartbeat =
        setInterval(
            async () => {

                const now =
                    new Date();

                const { error } =
                    await supabase
                        .from("sync_batches")
                        .update({
                            heartbeat_at:
                                now,

                            last_activity_at:
                                now
                        })
                       .eq(
                            "id",
                            jobId
                        )
                        .eq(
                            "worker_status",
                            "RUNNING"
                        )
                        .eq(
                            "worker_id",
                            workerConfig.worker_name
                        );

                if (error) {

                    console.error(
                        "Heartbeat update error:",
                        error
                    );
                }

                const { error: workerHeartbeatError } =
                    await supabase
                        .from("workers")
                        .update({
                            runtime_heartbeat_at:
                                now
                        })
                        .eq(
                            "runtime_id",
                            workerConfig.runtime_id
                        )
                        .eq(
                            "is_active",
                            true
                        );

                if (workerHeartbeatError) {

                    console.error(
                        "Worker runtime heartbeat update error:",
                        workerHeartbeatError
                    );
                }

            },
            30 * 1000
        );

    return heartbeat;
}

async function recoverStaleBatches(workerConfig) {

    const heartbeatTimeout =
        workerConfig.heartbeat_timeout_seconds || 120;

    const staleBefore =
        new Date(
            Date.now() -
            heartbeatTimeout * 1000
        ).toISOString();

    /*
     * ---------------------------------------------------------
     * 1. RECOVER GENUINELY STALE RUNNING BATCH
     *
     * PROCESSING + RUNNING + stale heartbeat
     * means worker execution was abandoned.
     * ---------------------------------------------------------
     */

    const {
        data: staleRunning,
        error: staleRunningError
    } = await supabase
        .from("sync_batches")
        .update({
            batch_status: "PENDING",
            worker_status: "PENDING",
            worker_id: null,
            locked_at: null,
            started_at: null,
            heartbeat_at: null
        })
        .eq(
            "worker_id",
            workerConfig.worker_name
        )
        .eq(
            "batch_status",
            "PROCESSING"
        )
        .eq(
            "worker_status",
            "RUNNING"
        )
        .lt(
            "heartbeat_at",
            staleBefore
        )
        .select(
            "id, batch_id"
        );

    if (staleRunningError) {

        throw new Error(
            "Stale running batch recovery failed: " +
            staleRunningError.message
        );
    }

    /*
     * ---------------------------------------------------------
     * 2. CLEAN TERMINAL BATCH STATE
     *
     * FAILED/COMPLETED batch must NEVER occupy worker slot.
     * Preserve terminal batch status.
     * Only release worker execution fields.
     * ---------------------------------------------------------
     */

    const {
        data: terminalBatches,
        error: terminalError
    } = await supabase
        .from("sync_batches")
        .update({
            worker_status: "FAILED",
            worker_id: null,
            locked_at: null,
            heartbeat_at: null
        })
        .eq(
            "worker_id",
            workerConfig.worker_name
        )
        .eq(
            "batch_status",
            "FAILED"
        )
        .eq(
            "worker_status",
            "RUNNING"
        )
        .select(
            "id, batch_id"
        );

    if (terminalError) {

        throw new Error(
            "Terminal batch cleanup failed: " +
            terminalError.message
        );
    }

    if (
        staleRunning?.length ||
        terminalBatches?.length
    ) {

        console.log(
            "WORKER STALE STATE RECOVERED:",
            JSON.stringify(
                {
                    staleRunning,
                    terminalBatches
                },
                null,
                2
            )
        );
    }

    return {
        staleRunning: staleRunning || [],
        terminalBatches: terminalBatches || []
    };
}


module.exports = {
    lockJob,
    startHeartbeat,
    recoverStaleBatches
};