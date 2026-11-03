const { Queue } = require("bullmq");

const connection = {
    host:
        process.env.REDIS_HOST ||
        "127.0.0.1",

    port:
        Number(
            process.env.REDIS_PORT ||
            6379
        )
};

function getSyncQueue(queueName) {

    if (!queueName) {

        throw new Error(
            "queueName is required"
        );
    }

    return new Queue(
        queueName,
        {
            connection,

            defaultJobOptions: {
    removeOnComplete: false,
    removeOnFail: false,
    attempts: 1
}
        }
    );
}

module.exports = {
    getSyncQueue
};