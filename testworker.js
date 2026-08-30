const { Worker } = require("bullmq");

const connection = {
    host: "127.0.0.1",
    port: 6379
};

const worker = new Worker(
    "billey-test-queue",
    async job => {

        console.log(
            "TEST JOB RECEIVED:",
            job.id,
            job.data
        );

        return {
            success: true
        };
    },
    {
        connection
    }
);

worker.on("completed", job => {
    console.log(
        "TEST JOB COMPLETED:",
        job.id
    );
});

worker.on("failed", (job, error) => {
    console.error(
        "TEST JOB FAILED:",
        job?.id,
        error
    );
});

console.log("BULLMQ TEST WORKER STARTED");