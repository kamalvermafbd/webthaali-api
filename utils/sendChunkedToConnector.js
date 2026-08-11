const crypto = require("crypto");

const fs = require("fs");

const { buildChunks } = require("./ChunkBuilder");

/*
const ServerProtocolReceiver =
    require("./protocol/ServerProtocolReceiver");
*/

const ServerProtocolController =
    require("./protocol/ServerProtocolController");

const {
    saveSyncSnapshotChunk,
    removeMissingSnapshotGuids
} = require("../services/sync/SyncSnapshotService");

const ACK_TIMEOUT = 15 * 60 * 1000;

async function sendChunkedToConnector(
    socket,
    event,
    payload
) {

    return new Promise((resolve, reject) => {

        const requestId = crypto.randomUUID();

        let masterResult = null;
        let collectionList = [];

        let expectedChunks = null;
        let expectedItems = null;
        let receivedItems = 0;

        let completed = false;


        let responseBatchId = null;
        const receivedChunkIndexes = new Set();

        const resultEvent = `${event}Result`;
        const chunkEvent = `${event}Chunk`;
        const ackEvent = `${event}ChunkAck`;
        const completeEvent = `${event}Complete`;
    /*
        let timer;

    function resetTimeout() {
        }
    */
/*
        const timer = setTimeout(() => {

            cleanup();

            reject(
                new Error(
                    `${event} timeout`
                )
            );

        }, timeout);
*/
        function cleanup() {

            //clearTimeout(timer);

            socket.off(resultEvent, onResult);
            socket.off(chunkEvent, onChunk);
            socket.off(completeEvent, onComplete);
            socket.off(
                    "disconnect",
                    onDisconnect
            );

        }

        function waitForAck(
        batchId,
        chunkIndex
    ) {

    return new Promise((resolve, reject) => {

        const ackTimeout = setTimeout(() => {

            socket.off(
                ackEvent,
                onAck
            );

            socket.off(
                "disconnect",
                onDisconnect
            );

            reject(
                new Error(
                    `ACK timeout for chunk ${chunkIndex}`
                )
            );

        }, ACK_TIMEOUT);

        function onAck(data) {

            if (
                data.batchId !== batchId ||
                data.chunkIndex !== chunkIndex
            ) {
                return;
            }

            clearTimeout(
                ackTimeout
            );

            socket.off(
                ackEvent,
                onAck
            );

             
            socket.off(
                "disconnect",
                onDisconnect
            );


            if (data.success === false) {
                return reject(
                    new Error(data.error || "Chunk rejected")
                );
            }

            resolve();

        }

    function onDisconnect(reason) {

        clearTimeout(ackTimeout);

        socket.off(
            ackEvent,
            onAck
        );

        socket.off(
            "disconnect",
            onDisconnect
        );

        reject(
            new Error(
                `Connector disconnected: ${reason}`
            )
        );

    }

    socket.on(
        ackEvent,
        onAck
    );

    socket.once(
        "disconnect",
        onDisconnect
    );

    });

}


function waitForCompleteAck(batchId) {

    return new Promise((resolve, reject) => {

        const completeAckEvent =
            `${event}CompleteAck`;

        const timeout = setTimeout(() => {

            socket.off(
                completeAckEvent,
                onCompleteAck
            );

            socket.off(
                "disconnect",
                onDisconnect
            );

            reject(
                new Error(
                    "Complete ACK timeout"
                )
            );

        }, ACK_TIMEOUT);

        function onCompleteAck(data) {

            if (
                !data ||
                data.batchId !== batchId
            ) {
                return;
            }

            clearTimeout(timeout);

            socket.off(
                completeAckEvent,
                onCompleteAck
            );

            socket.off(
                "disconnect",
                onDisconnect
            );

            if (data.success === false) {

                return reject(
                    new Error(
                        data.error ||
                        "Complete rejected"
                    )
                );

            }

            resolve();

        }

        function onDisconnect(reason) {

            clearTimeout(timeout);

            socket.off(
                completeAckEvent,
                onCompleteAck
            );

            socket.off(
                "disconnect",
                onDisconnect
            );

            reject(
                new Error(
                    `Connector disconnected: ${reason}`
                )
            );

        }

        socket.on(
            completeAckEvent,
            onCompleteAck
        );

        socket.once(
            "disconnect",
            onDisconnect
        );

    });

}

    async function sendRequestChunks(items) {

    const batchId = crypto.randomUUID();

    const chunks = buildChunks(items);

    console.log(
        `Sending ${chunks.length} request chunks`
    );

    for (const chunk of chunks) {

        console.log(
            `Sending chunk ${chunk.chunkIndex}/${chunk.totalChunks}`
        );

       socket.emit(
    `${event}Chunk`,
            {

                batchId,

                company: payload.company,

                sync_batch_id: payload.sync_batch_id,

                tally_owner: payload.tally_owner,

                module: payload.module,

                entity_type: payload.entity_type,

                fromDate: payload.fromDate,
                toDate: payload.toDate,

                chunkIndex: chunk.chunkIndex,

                totalChunks: chunk.totalChunks,

                payloadSize: chunk.payloadSize,

                data: chunk.data

            }
        );

        await waitForAck(
            batchId,
            chunk.chunkIndex
        );

        console.log(
            `ACK received for chunk ${chunk.chunkIndex}/${chunk.totalChunks}`
        );

    }

    const completeAckPromise =
        waitForCompleteAck(batchId);

    socket.emit(
        `${event}Complete`,
            {

            batchId,

            company: payload.company,

            sync_batch_id: payload.sync_batch_id,

            tally_owner: payload.tally_owner,

            module: payload.module,

            entity_type: payload.entity_type,

            totalChunks: chunks.length,

            totalItems: items.length,

            completedAt: new Date().toISOString()

        }

        
    );


    await completeAckPromise;

        console.log(
            "Request chunk transfer completed"
        );

}

const protocolController =
    new ServerProtocolController(
        socket.protocolReceiver
    );

async function onResult(data) {

    try {

        if (!data) {
            return;
        }

        //resetTimeout();

        console.log(
            "RESULT DEBUG :",
            JSON.stringify(data, null, 2)
        );

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

        // Sirf getMasters ke liye
        if (event !== "getMasters") {
            return;
        }

        console.log(
            "Receiving master collections..."
        );

        const collections =
         await protocolController.receiveAll();

        Object.assign(
            masterResult,
            collections
        );

        console.log(
            "All master collections received"
        );

        cleanup();

        resolve(masterResult);

        return;

    }
    catch (error) {

        cleanup();

        reject(error);

    }

}

async function onChunk(data) {

    console.log("SNAPSHOT FLAG:", payload.snapshot);
console.log("MODULE:", payload.module);
console.log("ENTITY:", payload.entity_type);

    if (!data) {
     return;
    }

            if (
            event === "getMasters"
        ) {
            return;
        }

            console.log("🔥 SERVER RECEIVED CHUNK");
            console.log(
                data.chunkIndex,
                "/",
                data.totalChunks
            );

            //resetTimeout();

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
if (!responseBatchId) {
    responseBatchId = data.batchId;
}

if (data.batchId !== responseBatchId) {
    console.error(
        "Ignoring chunk from unexpected batch:",
        data.batchId
    );
    return;
}

if (receivedChunkIndexes.has(data.chunkIndex)) {
    console.log(
        `Duplicate chunk ignored: ${data.chunkIndex}`
    );

    socket.emit(
        ackEvent,
        {
            batchId: data.batchId,
            chunkIndex: data.chunkIndex,
            success: true
        }
    );

    return;
}

if (
    !Number.isInteger(data.totalChunks) ||
    data.totalChunks <= 0 ||
    !Number.isInteger(data.chunkIndex) ||
    data.chunkIndex < 1 ||
    data.chunkIndex > data.totalChunks
) {
    socket.emit(
        ackEvent,
        {
            batchId: data.batchId,
            chunkIndex: data.chunkIndex,
            success: false,
            error: "Invalid chunk metadata"
        }
    );

    return;
}



if (expectedChunks === null) {
    expectedChunks = data.totalChunks;
}

if (data.totalChunks !== expectedChunks) {
    socket.emit(
        ackEvent,
        {
            batchId: data.batchId,
            chunkIndex: data.chunkIndex,
            success: false,
            error: "Inconsistent totalChunks"
        }
    );

    return;
}    

fs.appendFileSync(
    "./logs/snapshot-flag-debug.jsonl",
    JSON.stringify({
        entity: payload.entity_type,
        module: payload.module,
        snapshot: payload.snapshot,
        chunk: data.chunkIndex,
        rows: data.data.length
    }) + "\n"
);

if (payload.snapshot === true) {

    fs.writeFileSync(

    `./logs/PROTO_${payload.entity_type}_${data.chunkIndex}.json`,

    JSON.stringify(

        {
            entity: payload.entity_type,
            module: payload.module,
            chunkIndex: data.chunkIndex,
            totalChunks: data.totalChunks,
            rows: data.data.length,
            guids: data.data.map(r => ({
                guid: r.guid,
                alterId: r.alterId ?? r.alter_id,
                name: r.name
            }))
        },

        null,

        2

    )

);
console.log("EVENT :", event);
console.log("SNAPSHOT :", payload.snapshot);
console.log("ENTITY :", payload.entity_type);
console.log("ROWS :", data.data.length);
console.log("========================");

collectionList.push(
    ...data.data
);

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

        receivedChunkIndexes.add(data.chunkIndex);
        
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

async function onComplete(data) {

    //resetTimeout();

    if (
            event === "getMasters"
        ) {
            return;
        }

        if (!data?.batchId) {
    cleanup();

    return reject(
        new Error(
            "Complete event missing batchId"
        )
    );
}

if (
    responseBatchId &&
    data.batchId !== responseBatchId
) {
    cleanup();
    return reject(
        new Error(
            `Complete batch mismatch. Expected ${responseBatchId}, Received ${data.batchId}`
        )
    );
}

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

            if (
    !Number.isInteger(data.totalItems) ||
    data.totalItems < 0
) {
    cleanup();

    return reject(
        new Error(
            "Invalid totalItems in Complete event"
        )
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
            fs.writeFileSync(

                `./logs/PROTO_FINAL_${payload.entity_type}.json`,

                JSON.stringify(

                {
                    entity: payload.entity_type,
                    expectedItems,
                    actualReceived,
                    expectedChunks,
                    snapshot: payload.snapshot
                },

        null,

        2

    )

);

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

try{
    await removeMissingSnapshotGuids({

        sync_batch_id:
            payload.sync_batch_id,

        company_code:
            payload.company_code,

        tally_owner:
            payload.tally_owner,

        module:
            payload.module,

        entity_type:
            payload.entity_type

                });
                }
                catch(error) {

                    cleanup();

                    return reject(error);

                }

            }

            if (payload.snapshot === true) {

    socket.emit(
        `${event}CompleteAck`,
        {
            batchId: data.batchId,
            success: true
        }
    );

    cleanup();

    resolve({

        ...masterResult,

        snapshotSaved: true,

        totalItems: receivedItems,

        items: collectionList

    });

}
            else {

    socket.emit(
            `${event}CompleteAck`,
            {
                batchId: data.batchId,
                success: true
            }
        );

        cleanup();

        resolve({

            ...masterResult,

            [collectionName]: collectionList

        });

    }
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
            resultEvent,
            onResult
        );

       if (event !== "getMasters") {

    socket.on(
        chunkEvent,
        onChunk
    );

    console.log(
        "Listening for:",
        chunkEvent
    );

    socket.on(
        completeEvent,
        onComplete
    );

}
        socket.once(
            "disconnect",
            onDisconnect
        );

    if (Array.isArray(payload.requestItems)) {

    return sendRequestChunks(
        payload.requestItems
    ).catch(error => {

        cleanup();

        reject(error);

    });

}

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