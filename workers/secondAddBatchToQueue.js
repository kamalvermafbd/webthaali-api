const {
    getSyncQueue
} = require("./secondSyncQueue");

/*211026
async function removeFailedJobIfSafe(syncQueue, batchId) {
    const existingJob = await syncQueue.getJob(batchId);

    if (!existingJob) {
        return true;
    }

    const state = await existingJob.getState();

    console.log(
        "EXISTING BULLMQ JOB CHECK:",
        batchId,
        state
    );

    if (state === "failed") {
        await existingJob.remove();

        console.log(
            "OLD FAILED BULLMQ JOB REMOVED:",
            batchId
        );

        return true;
    }

    // Never remove an active or waiting job.
    return false;
}
*/


async function removeFailedJobIfSafe(
    syncQueue,
    batch,
    workerConfig
) {
    const existingJob = await syncQueue.getJob(
        batch.batch_id
    );

    if (!existingJob) {
        return true;
    }

    const state = await existingJob.getState();

    console.log(
        "EXISTING BULLMQ JOB CHECK:",
        batch.batch_id,
        state
    );

    if (state === "failed") {
        await existingJob.remove();

        console.log(
            "OLD FAILED BULLMQ JOB REMOVED:",
            batch.batch_id
        );

        return true;
    }

    const jobData = existingJob.data || {};

    const sameAssignment =
        jobData.batch_id === batch.batch_id &&
        String(jobData.batch_db_id) === String(batch.id) &&
        jobData.worker_name === workerConfig.worker_name &&
        String(jobData.worker_id) === String(workerConfig.id) &&
        jobData.server_id === workerConfig.server_id;

    if (
        [
            "waiting",
            "active",
            "delayed",
            "prioritized",
            "waiting-children"
        ].includes(state) &&
        sameAssignment
    ) {
        console.log(
            "EXISTING BULLMQ JOB MATCHES RETRY:",
            batch.batch_id,
            workerConfig.worker_name,
            state
        );

        return false;
    }

    throw new Error(
        `Existing BullMQ job conflicts with retry assignment: ${batch.batch_id}; state=${state}; worker=${jobData.worker_name || "UNKNOWN"}`
    );
}

/* 211026
async function addBatchToQueue(
    batch,
    workerConfig
) {

    if (!batch?.batch_id) {

        throw new Error(
            "batch_id missing"
        );
    }

    if (!workerConfig?.worker_name) {

        throw new Error(
            "worker_name missing"
        );
    }

    if (!workerConfig?.queue_name) {

        throw new Error(
            "queue_name missing"
        );
    }

    if (!workerConfig?.server_id) {

        throw new Error(
            "server_id missing"
        );
    }

    const syncQueue =
        getSyncQueue(
            workerConfig.queue_name
        );

      

    const canEnqueue = await removeFailedJobIfSafe(
            syncQueue,
            batch,
            workerConfig
        );

    if (!canEnqueue) {
        throw new Error(
            `Existing BullMQ job is not failed; retry skipped for ${batch.batch_id}`
        );
    }
        console.log(
    "ABOUT TO ADD BULLMQ JOB:",
    batch.batch_id,
    workerConfig.queue_name,
    workerConfig.worker_name
);

    const job =
        await syncQueue.add(

            "TALLY_SYNC",

            {
                batch_id:
                    batch.batch_id,

                batch_db_id:
                  batch.id,

                company_code:
                    batch.company_code,

                tally_owner:
                    batch.tally_owner,

                job_type:
                    batch.job_type,

                worker_type:
                    batch.worker_type,

                sync_mode:
                    batch.sync_mode,

                sync_period:
                    batch.sync_period,

                worker_name:
                    workerConfig.worker_name,

                worker_id:
                    workerConfig.id,

                server_id:
                    workerConfig.server_id,

                server_category:
                    workerConfig.server_category,

                http_url:
                    workerConfig.http_url
            },

            {
                jobId:
                    batch.batch_id
            }
        );

        console.log(
    "BULLMQ ADD RETURNED:",
    job?.id,
    batch.batch_id
);

    console.log(
        "BULLMQ BATCH QUEUED:",
        batch.batch_id
    );

    console.log(
        "QUEUE:",
        workerConfig.queue_name
    );

    console.log(
        "WORKER:",
        workerConfig.worker_name
    );

    return job;
}
*/


async function addBatchToQueue(
    batch,
    workerConfig
) {
    if (!batch?.batch_id) {
        throw new Error("batch_id missing");
    }

    if (!workerConfig?.worker_name) {
        throw new Error("worker_name missing");
    }

    if (!workerConfig?.queue_name) {
        throw new Error("queue_name missing");
    }

    if (!workerConfig?.server_id) {
        throw new Error("server_id missing");
    }

    const syncQueue = getSyncQueue(
        workerConfig.queue_name
    );

    try {
        const canEnqueue = await removeFailedJobIfSafe(
            syncQueue,
            batch,
            workerConfig
        );

        if (!canEnqueue) {
            throw new Error(
                `Existing BullMQ job is not failed; retry skipped for ${batch.batch_id}`
            );
        }

        console.log(
            "ABOUT TO ADD BULLMQ JOB:",
            batch.batch_id,
            workerConfig.queue_name,
            workerConfig.worker_name
        );

        const job = await syncQueue.add(
            "TALLY_SYNC",
            {
                batch_id: batch.batch_id,
                batch_db_id: batch.id,
                company_code: batch.company_code,
                tally_owner: batch.tally_owner,
                job_type: batch.job_type,
                worker_type: batch.worker_type,
                sync_mode: batch.sync_mode,
                sync_period: batch.sync_period,
                worker_name: workerConfig.worker_name,
                worker_id: workerConfig.id,
                server_id: workerConfig.server_id,
                server_category: workerConfig.server_category,
                http_url: workerConfig.http_url
            },
            {
                jobId: batch.batch_id
            }
        );

        console.log(
            "BULLMQ ADD RETURNED:",
            job?.id,
            batch.batch_id
        );

        console.log(
            "BULLMQ BATCH QUEUED:",
            batch.batch_id
        );

        console.log("QUEUE:", workerConfig.queue_name);
        console.log("WORKER:", workerConfig.worker_name);

        return job;
    } finally {
        await syncQueue.close();
    }
}




module.exports = {
    addBatchToQueue
};