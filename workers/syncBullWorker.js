require("dotenv").config();

const {
    Worker
} = require("bullmq");

const {
    createClient
} = require("@supabase/supabase-js");

const crypto = require("crypto");

const {
    runJob
} = require("../jobs/jobRunner");

const {
    lockJob,
    startHeartbeat
} = require("./workerLockService");

const supabase =
    createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_SERVICE_KEY
    );



const connection = {
    host: "127.0.0.1",
    port: 6379
};

async function startWorker() {

    const workerId =
        process.argv[2];

    if (!workerId) {

        throw new Error(
            "Worker ID required"
        );
    }

    const {
        data: worker,
        error
    } =
        await supabase
            .from("workers")
            .select(`
                id,
                worker_name,
                worker_type,
                queue_name,
                server_id,
                is_active,
                concurrency,
                heartbeat_timeout_seconds
            `)
            .eq(
                "id",
                workerId
            )
            .eq(
                "is_active",
                true
            )
            .single();

    if (error || !worker) {

        throw new Error(
            "Active worker not found: "
            + (error?.message || workerId)
        );
    }

    const runtimeId = crypto.randomUUID();

const { error: runtimeError } =
    await supabase
        .from("workers")
        .update({
            runtime_id: runtimeId,
            runtime_started_at: new Date(),
            runtime_heartbeat_at: new Date(),
            updated_at: new Date()
        })
        .eq("id", worker.id)
        .eq("is_active", true);

if (runtimeError) {
    throw new Error(
        "Worker runtime registration failed: "
        + runtimeError.message
    );
}

worker.runtime_id = runtimeId;

const runtimeHeartbeat = setInterval(
    async () => {

        const now = new Date();

        const { error } =
            await supabase
                .from("workers")
                .update({
                    runtime_heartbeat_at: now,
                    updated_at: now
                })
                .eq("id", worker.id)
                .eq("runtime_id", worker.runtime_id)
                .eq("is_active", true);

        if (error) {
            console.error(
                "Worker runtime heartbeat error:",
                error
            );
        }
    },
    30 * 1000
);

    if (!worker.queue_name) {

        throw new Error(
            `queue_name missing for worker ${worker.worker_name}`
        );
    }

    const syncBullWorker =
        new Worker(

            worker.queue_name,

            async job => {

                console.log(
                    "BULLMQ SYNC JOB RECEIVED:",
                    job.id
                );

                console.log(
                    "BATCH ID:",
                    job.data.batch_id
                );

                console.log(
                    "WORKER:",
                    worker.worker_name
                );

                console.log(
                    "SERVER:",
                    worker.server_id
                );

                if (
                    job.data.worker_name !==
                    worker.worker_name
                ) {

                    throw new Error(
                        `Job assigned to ${job.data.worker_name}, ` +
                        `but this worker is ${worker.worker_name}`
                    );
                }

    const lockInput = {
    ...job,
    data: {
        ...job.data,
        batch_db_id: job.data.batch_db_id
    }
};

const lockedJob =
    await lockJob(
        lockInput,
        worker
    );
    console.log("LOCKED JOB:", JSON.stringify(lockedJob, null, 2));

if (!lockedJob) {

    throw new Error(
        `Unable to lock batch ${job.data.batch_id}`
    );
}

const heartbeat =
    await startHeartbeat(
        lockedJob.id,
        worker
    );

                try {

                                
                    await runJob(
                    lockedJob,
                    job.data.http_url
                );

                } finally {

        clearInterval(heartbeat);
    }

                return {
                    success: true
                };
            },

            {
                connection,

                concurrency:
                    worker.concurrency || 1
            }
        );

        const cleanupRuntime = async () => {

        clearInterval(runtimeHeartbeat);

        await supabase
            .from("workers")
            .update({
                runtime_id: null,
                runtime_started_at: null,
                runtime_heartbeat_at: null,
                updated_at: new Date()
            })
            .eq("id", worker.id)
            .eq("runtime_id", worker.runtime_id);

    };

    process.once(
        "SIGINT",
        async () => {

            await cleanupRuntime();

            await syncBullWorker.close();

            process.exit(0);
        }
    );

    process.once(
        "SIGTERM",
        async () => {

            await cleanupRuntime();

            await syncBullWorker.close();

            process.exit(0);
        }
    );
    
    syncBullWorker.on(
        "completed",
        job => {

            console.log(
                "BULLMQ SYNC JOB COMPLETED:",
                job.id
            );
        }
    );

    syncBullWorker.on(
        "failed",
        (job, error) => {

            console.error(
                "BULLMQ SYNC JOB FAILED:",
                job?.id,
                error
            );
        }
    );

    syncBullWorker.on(
        "error",
        error => {

            console.error(
                "BULLMQ WORKER ERROR:",
                error
            );
        }
    );

    console.log(
        "BULLMQ SYNC WORKER STARTED"
    );

    console.log(
        "WORKER NAME:",
        worker.worker_name
    );

    console.log(
        "QUEUE:",
        worker.queue_name
    );

    console.log(
        "WORKER TYPE:",
        worker.worker_type
    );

    console.log(
        "SERVER:",
        worker.server_id
    );
}

startWorker()
    .catch(error => {

        console.error(
            "BULLMQ WORKER START FAILED:",
            error
        );

        process.exit(1);
    });