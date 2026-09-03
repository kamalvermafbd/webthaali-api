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
                removeOnComplete: true,
                removeOnFail: 500
            }
        }
    );
}

module.exports = {
    getSyncQueue
};