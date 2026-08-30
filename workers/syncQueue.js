const { Queue } = require("bullmq");

const connection = {
    host: "127.0.0.1",
    port: 6379
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