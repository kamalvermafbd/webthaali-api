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

module.exports = {
    lockJob,
    startHeartbeat
};