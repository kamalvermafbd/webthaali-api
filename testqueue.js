const { Queue } = require("bullmq");

const connection = {
    host: "127.0.0.1",
    port: 6379
};

const testQueue = new Queue("billey-test-queue", {
    connection
});

module.exports = {
    testQueue
};