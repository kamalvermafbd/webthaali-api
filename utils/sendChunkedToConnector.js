const crypto = require("crypto");

const {
    saveSyncSnapshotChunk,
    clearSyncSnapshot
} = require("../services/sync/SyncSnapshotService");

async function sendChunkedToConnector(
    socket,
    event,
    payload,
    timeout = 30000
) {

    return new Promise((resolve, reject) => {

        const requestId = crypto.randomUUID();

        let masterResult = null;
        let collectionList = [];

        let expectedChunks = null;
        let expectedItems = null;
        let receivedItems = 0;

        let completed = false;
        let snapshotCleared = false;

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

        async function onChunk(data) {

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


if (
    payload.snapshot === true &&
    snapshotCleared === false
) {

    await clearSyncSnapshot({

        company_code:
            payload.company_code,

        tally_owner:
            payload.tally_owner,

        module:
            payload.module,

        entity_type:
            payload.entity_type

    });


    snapshotCleared = true;

}


if (payload.snapshot === true) {

    await saveSyncSnapshotChunk({

        sync_batch_id:
            payload.sync_batch_id,

        company_code:
            payload.company_code,

        tally_owner:
            payload.tally_owner,

        module:
            payload.module,

        entity_type:
            payload.entity_type,

        rows:
            data.data

    });


    receivedItems += data.data.length;

}
else {

    collectionList.push(
        ...data.data
    );


    receivedItems += data.data.length;

}

console.log(
    `Received chunk ${data.chunkIndex}/${data.totalChunks} (${data.data.length} records)`
);

console.log(
    `Total records received so far: ${receivedItems}`
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

          const actualReceived =
    payload.snapshot === true
    ? receivedItems
    : collectionList.length;


if (
    expectedItems !== null &&
    actualReceived !== expectedItems
) {

    cleanup();

    return reject(
        new Error(
            `Record count mismatch. Expected ${expectedItems}, Received ${actualReceived}`
        )
    );

}

            cleanup();

            console.log("=================================");
console.log("Chunk transfer completed");
console.log(`Total chunks : ${expectedChunks}`);
console.log(
    `Total records : ${actualReceived}`
);
console.log("=================================");

           const collectionName =
    masterResult.collectionName || "vouchers";


if (payload.snapshot === true) {

    resolve({

        ...masterResult,

        snapshotSaved: true,

        totalItems: receivedItems

    });

}
else {

    resolve({

        ...masterResult,

        [collectionName]: collectionList

    });

}
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