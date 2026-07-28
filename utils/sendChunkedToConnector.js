const crypto = require("crypto");

async function sendChunkedToConnector(
    socket,
    event,
    payload,
    timeout = 30000
) {

    return new Promise((resolve, reject) => {

        const requestId = crypto.randomUUID();

        let masterResult = null;
        let voucherList = [];

        let expectedChunks = null;
        let expectedItems = null;

        let completed = false;

        const resultEvent = `${event}Result`;
        const chunkEvent = `${event}Chunk`;
        const ackEvent = `${event}ChunkAck`;
        const completeEvent = `${event}Complete`;

        const timer = setTimeout(() => {

            cleanup();

            reject(
                new Error(
                    `${event} timeout`
                )
            );

        }, timeout);

        function cleanup() {

            clearTimeout(timer);

            socket.off(resultEvent, onResult);
            socket.off(chunkEvent, onChunk);
            socket.off(completeEvent, onComplete);

        }

        function onResult(data) {

            if (!data) {
                return;
            }

            if (data.success === false) {

                cleanup();

                return reject(
                    new Error(
                        data.error || "Unknown error"
                    )
                );

            }

            masterResult = data;

            console.log(
                "Master payload received"
            );

        }

        function onChunk(data) {

            if (!data) {
                return;
            }

            if (!Array.isArray(data.data)) {
                console.error(
    `Invalid chunk received: ${data.chunkIndex}`
);
                socket.emit(
                    ackEvent,
                    {
                        batchId: data.batchId,
                        chunkIndex: data.chunkIndex,
                        success: false,
                        error: "Invalid chunk"
                    }
                );

                return;
            }

     expectedChunks = data.totalChunks;

voucherList.push(
    ...data.data
);

console.log(
    `Received chunk ${data.chunkIndex}/${data.totalChunks} (${data.data.length} vouchers)`
);

console.log(
    `Total vouchers received so far: ${voucherList.length}`
);

socket.emit(
    ackEvent,
    {
        batchId: data.batchId,
        chunkIndex: data.chunkIndex,
        success: true
    }
);

console.log(
    `ACK sent for chunk ${data.chunkIndex}/${data.totalChunks}`
);
        }

        function onComplete(data) {

            if (completed) {
                return;
            }

            completed = true;

            if (!masterResult) {

    cleanup();

    return reject(
        new Error("Master payload not received")
    );

}

            expectedItems =
                data.totalItems;

            if (
                expectedChunks !== null &&
                data.totalChunks !== expectedChunks
            ) {

                cleanup();

                return reject(
                    new Error(
                        "Chunk count mismatch"
                    )
                );

            }

            if (
                expectedItems !== null &&
                voucherList.length !== expectedItems
            ) {

                cleanup();

                return reject(
                    new Error(
                        `Voucher count mismatch. Expected ${expectedItems}, Received ${voucherList.length}`
                    )
                );

            }

            cleanup();

            console.log("=================================");
console.log("Chunk transfer completed");
console.log(`Total chunks : ${expectedChunks}`);
console.log(`Total vouchers : ${voucherList.length}`);
console.log("=================================");

            resolve({

                ...masterResult,

                vouchers: voucherList

            });

        }

        socket.on(
            resultEvent,
            onResult
        );

        socket.on(
            chunkEvent,
            onChunk
        );

        socket.on(
            completeEvent,
            onComplete
        );

        socket.emit(
            event,
            {
                requestId,
                ...payload
            }
        );

    });

}

module.exports = {
    sendChunkedToConnector
};