const {
    getSyncQueue
} = require("./syncQueue");

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

module.exports = {
    addBatchToQueue
};