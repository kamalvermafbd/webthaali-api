const { testQueue } = require("./testQueue");

async function main() {

    const job = await testQueue.add(
        "test-sync",
        {
            message: "Billey BullMQ test"
        }
    );

    console.log(
        "TEST JOB ADDED:",
        job.id
    );

    await testQueue.close();
}

main().catch(console.error);