const ACK_TIMEOUT = 30000;


function prepareCollection(
    socket,
    baseEvent
) {

    const chunkEvent =
        `${baseEvent}Chunk`;

    const completeEvent =
        `${baseEvent}Complete`;

    return {

        socket,

        baseEvent,

        chunkEvent,

        completeEvent

    };

}










async function receiveCollection(
    socket,
    baseEvent,
    timeout = ACK_TIMEOUT
) {

    return new Promise((resolve, reject) => {

        const chunkEvent =
            `${baseEvent}Chunk`;

        const ackEvent =
            `${baseEvent}ChunkAck`;

        const completeEvent =
            `${baseEvent}Complete`;

        const completeAckEvent =
             `${baseEvent}CompleteAck`;

        let timer;

        let expectedChunks = 0;
        let expectedItems = 0;

        let receivedItems = 0;

        const rows = [];

        function resetTimeout() {

            clearTimeout(timer);

            timer = setTimeout(() => {

                cleanup();

                reject(
                    new Error(
                        `${baseEvent} timeout`
                    )
                );

            }, timeout);

        }

        function cleanup() {

            clearTimeout(timer);

            socket.off(
                chunkEvent,
                onChunk
            );

            socket.off(
                completeEvent,
                onComplete
            );

            socket.off(
                "disconnect",
                onDisconnect
            );

        }

        async function onChunk(data) {

            resetTimeout();

            if (
                !data ||
                !Array.isArray(data.data)
            ) {

                socket.emit(
                    ackEvent,
                    {
                        batchId: data?.batchId,
                        chunkIndex: data?.chunkIndex,
                        success: false,
                        error: "Invalid chunk"
                    }
                );

                return;
            }

            expectedChunks =
                data.totalChunks;

            rows.push(
                ...data.data
            );

            receivedItems +=
                data.data.length;

            socket.emit(
                ackEvent,
                {
                    batchId: data.batchId,
                    chunkIndex: data.chunkIndex,
                    success: true
                }
            );

            console.log(
                `${baseEvent} : chunk ${data.chunkIndex}/${data.totalChunks}`
            );

         
        }

        function onComplete(data) {

              console.log("######## COMPLETE EVENT RECEIVED ########");
    console.log(baseEvent);
    console.log(data);

            resetTimeout();

            expectedItems =
                data.totalItems;

            if (
                receivedItems !==
                expectedItems
            ) {

                cleanup();

                return reject(
                    new Error(
                        `${baseEvent} record mismatch`
                    )
                );

            }

            console.log(
                `${baseEvent} completed (${receivedItems} records)`
            );

            socket.emit(
                completeAckEvent,
                {
                    batchId: data.batchId,
                    success: true
                }
            );

            cleanup();

            resolve(rows);

        }

        function onDisconnect(reason) {

            cleanup();

            reject(
                new Error(
                    `Connector disconnected: ${reason}`
                )
            );

        }

        socket.on(
            chunkEvent,
            onChunk
        );

        socket.once(
            completeEvent,
            onComplete
        );


console.log(
    "Listening for Complete:",
    completeEvent
);

        socket.once(
            "disconnect",
            onDisconnect
        );

        resetTimeout();

    });

}

module.exports = {

    prepareCollection,

    receiveCollection

};