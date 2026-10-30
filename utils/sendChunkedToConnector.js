const crypto = require("crypto");

const fs = require("fs");

const { buildChunks } = require("./ChunkBuilder");
const { withGlobalConnectorLock } = require("../socketio/connectorOperationCoordinator");

/*
const ServerProtocolReceiver =
    require("./protocol/ServerProtocolReceiver");
*/

const ServerProtocolController =
    require("./protocol/ServerProtocolController");

/*241026
const {
    acquireConnectorOperationLock,
    renewConnectorOperationLock,
    releaseConnectorOperationLock
} = require("../socketio/connectorHealthStore");
*/

const {
    saveSyncSnapshotChunk,
    removeMissingSnapshotGuids
} = require("../services/sync/SyncSnapshotService");

const ACK_TIMEOUT = 15 * 60 * 1000;
async function sendChunkedToConnector(
    socket,
    event,
    payload,
    connectorIdInput,
    executionContext = {}
) {
    const connectorId = String(
        connectorIdInput || ""
    ).trim();

    if (!connectorId) {
        throw new Error(
            "CONNECTOR_ID_MISSING: Cannot start Tally operation"
        );
    }

    const dispatchState = {
        dispatched: false
    };

    try {
        return await withGlobalConnectorLock(
            connectorId,
            async (lockedSocket, signal) => {

                if (lockedSocket !== socket || signal.aborted) {
                    throw new Error(
                        `CONNECTOR_SOCKET_CHANGED_OR_ABORTED: ${connectorId}`
                    );
                }

                // Durable dispatch authorization must happen
                // inside the global connector lock.
                if (executionContext?.request_id) {

                    if (
                        typeof executionContext.before_dispatch !== "function"
                    ) {
                        throw new Error(
                            "DURABLE_CONNECTOR_DISPATCH_HOOK_MISSING"
                        );
                    }

                    if (signal.aborted || !lockedSocket.connected) {
                        throw new Error(
                            "CONNECTOR_UNAVAILABLE_BEFORE_DISPATCH"
                        );
                    }

                    // Do not mark the request as emitted here.
                    // The database dispatch mark is only authorization.
                    await executionContext.before_dispatch();

                    if (signal.aborted || !lockedSocket.connected) {
                        throw new Error(
                            "CONNECTOR_UNAVAILABLE_AFTER_DISPATCH_MARK"
                        );
                    }
                }

                const transferResult = await executeChunkedTransfer(
    lockedSocket,
    event,
    payload,
    signal,
    connectorId,
    dispatchState,
    executionContext
);

if (signal.aborted || !lockedSocket.connected) {
    throw new Error(
        `CONNECTOR_LOCK_LOST_AFTER_TRANSFER: ${connectorId}`
    );
}

if (typeof executionContext.after_transfer === "function") {
    if (!executionContext.request_id) {
        throw new Error(
            "POST_TRANSFER_HOOK_REQUIRES_DURABLE_REQUEST"
        );
    }

    return await executionContext.after_transfer(
        transferResult,
        signal
    );
}

return transferResult;
            }
        );

    } catch (error) {
        if (error && typeof error === "object") {
            // Preserve the actual dispatch status.
            // A failed before_dispatch hook must not be
            // converted into a dispatched request.
            error.requestDispatched =
                error.requestDispatched === true ||
                dispatchState.dispatched === true;
        }

        throw error;
    }
}

async function executeChunkedTransfer(
    socket,
    event,
    payload,
    signal,
    connectorId,
    dispatchState,
    executionContext
) {
    /* 241026
   const ownerToken = crypto.randomUUID();
const activeChunkTasks = new Set();
let activeCompletionTask = null;
let lockAcquired = false;

while (!lockAcquired) {

    if (!socket.connected) {
        throw new Error(
            `CONNECTOR_DISCONNECTED_WHILE_WAITING: ${connectorId}`
        );
    }

    lockAcquired = await acquireConnectorOperationLock(
        connectorId,
        ownerToken,
        { ttlMs: 120000 }
    );

    if (lockAcquired) {
        console.log(
            "🔓 CONNECTOR LOCK ACQUIRED:",
            connectorId,
            event
        );
        break;
    }

    console.log(
        "⏳ CONNECTOR BUSY — WAITING:",
        connectorId,
        event
    );

    await new Promise(resolve =>
        setTimeout(resolve, 2000)
    );
}
    */
const activeChunkTasks = new Set();
let activeCompletionTask = null;
let activeProtocolTask = null;
let activeRequestTransferTask = null;
/* 241026
let lockLost = false;
let renewing = false;
let rejectActiveOperation = null;

const renewalTimer = setInterval(async () => {
    if (renewing || lockLost) return;

    renewing = true;

    try {
        const renewed = await renewConnectorOperationLock(
            connectorId,
            ownerToken,
            120000
        );

        if (!renewed) {
    lockLost = true;

    console.error(
        "CONNECTOR LOCK LOST:",
        connectorId
    );

    rejectActiveOperation?.();
}
   } catch (error) {
    lockLost = true;

    console.error(
        "CONNECTOR LOCK RENEW FAILED:",
        connectorId,
        error
    );

    rejectActiveOperation?.();

} finally {
    renewing = false;
}
}, 30000);

renewalTimer.unref?.();
*/

let lockLost = signal.aborted;
let rejectActiveOperation = null;

// Tracks whether any part of this request was emitted to the connector.
// Once true, a transport failure may leave the Tally outcome unknown.
let requestDispatched = false;

function onGlobalLockAbort() {
    lockLost = true;

    console.error(
        "GLOBAL CONNECTOR LOCK ABORTED:",
        connectorId,
        event
    );

    if (event === "getMasters") {
        socket.protocolReceiver?.cancelCollection(
            `Global connector lock aborted: ${connectorId}`
        );
    }

    rejectActiveOperation?.();
}
signal.addEventListener(
    "abort",
    onGlobalLockAbort,
    { once: true }
);

try {
    return await new Promise((resolve, reject) => {

        let operationSettled = false;
const pendingAckCleanups = new Set();

        function rejectOnLockLost() {
    if (operationSettled) return;

    operationSettled = true;

    // cleanup() cancels all pending ACK waits
cleanup();

    reject(
        new Error(
            `CONNECTOR_LOCK_LOST: ${connectorId}`
        )
    );
}

        

if (lockLost || signal.aborted) {
    operationSettled = true;

    reject(
        new Error(
            `CONNECTOR_LOCK_LOST: ${connectorId}`
        )
    );

    return;
}

        const durableRequestId =
    executionContext?.request_id;

if (
    durableRequestId != null &&
    (
        typeof durableRequestId !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            durableRequestId
        )
    )
) {
    throw new Error("INVALID_DURABLE_CONNECTOR_REQUEST_ID");
}

// Durable requests must have a dispatch authorization callback.
if (
    durableRequestId &&
    typeof executionContext?.before_dispatch !== "function"
) {
    throw new Error(
        "DURABLE_CONNECTOR_DISPATCH_HOOK_MISSING"
    );
}

const requestId =
    durableRequestId || crypto.randomUUID();

        let masterResult = null;

// getMasters requires both protocol receipt and connector transfer completion.
let protocolCollectionsComplete = false;
let transferCompleteReceived = false;
let transferCompleteTimer = null;

const GET_MASTERS_FINAL_TIMEOUT_MS = 30000;

let collectionList = [];

        let expectedChunks = null;
        let expectedItems = null;
        let receivedItems = 0;

        let completed = false;
let completionStarted = false;

let responseBatchId = null;
const receivedChunkIndexes = new Set();
const processingChunkIndexes = new Set();

let chunkProcessingError = null;

const resultEvent = `${event}Result`;
const chunkEvent = `${event}Chunk`;
const ackEvent = `${event}ChunkAck`;
const completeEvent = `${event}Complete`;

function makeGetMastersOutcomeUnknown(message) {
    const error = new Error(message);

    error.code = "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN";
    error.outcomeUnknown = true;
    error.requestDispatched = true;

    return error;
}

function tryCompleteGetMasters() {
    if (event !== "getMasters") return;

    if (
        operationSettled ||
        lockLost ||
        signal.aborted ||
        masterResult?.success !== true ||
        !protocolCollectionsComplete ||
        !transferCompleteReceived
    ) {
        return;
    }

    operationSettled = true;
    cleanup();
    resolve(masterResult);
}

function startGetMastersFinalTimer() {
    if (
        event !== "getMasters" ||
        operationSettled ||
        lockLost ||
        signal.aborted ||
        !protocolCollectionsComplete ||
        transferCompleteReceived ||
        transferCompleteTimer !== null
    ) {
        return;
    }

    function checkFinalCompletion() {
        transferCompleteTimer = setTimeout(() => {
            if (operationSettled || transferCompleteReceived) {
                return;
            }

            const lastHeartbeat = Number(socket.lastHeartbeat || 0);

            const heartbeatFresh =
                socket.connected === true &&
                lastHeartbeat > 0 &&
                Date.now() - lastHeartbeat < 90000;

            const connectorActive =
                socket.tallyRequestActive === true;

            if (heartbeatFresh && connectorActive) {
                console.log(
                    "GET_MASTERS_FINAL_WAIT — CONNECTOR STILL ACTIVE",
                    { connectorId, requestId }
                );

                checkFinalCompletion();
                return;
            }

            operationSettled = true;
            cleanup();

            reject(
                makeGetMastersOutcomeUnknown(
                    `GET_MASTERS_FINAL_COMPLETION_TIMEOUT: requestId=${requestId}`
                )
            );
        }, GET_MASTERS_FINAL_TIMEOUT_MS);
    }

    checkFinalCompletion();
}


function onGetMastersTransferComplete(data) {
    if (
        event !== "getMasters" ||
        !data ||
        data.requestId !== requestId
    ) {
        return;
    }

    if (
        operationSettled ||
        lockLost ||
        signal.aborted
    ) {
        return;
    }

    if (transferCompleteReceived) {
        socket.emit("getMastersTransferCompleteAck", {
            requestId,
            success: true
        });
        return;
    }

    if (data.success !== true) {
        operationSettled = true;
        cleanup();

        return reject(
            makeGetMastersOutcomeUnknown(
                `GET_MASTERS_FINAL_COMPLETION_FAILED: requestId=${requestId}; error=${data.error || "Invalid completion status"}`
            )
        );
    }

    transferCompleteReceived = true;

    socket.emit("getMastersTransferCompleteAck", {
        requestId,
        success: true
    });

    tryCompleteGetMasters();
}
function cleanup() {
    if (transferCompleteTimer !== null) {
        clearTimeout(transferCompleteTimer);
        transferCompleteTimer = null;
    }

    socket.off(resultEvent, onResult);
    socket.off(chunkEvent, onChunk);
    socket.off(completeEvent, onComplete);

    if (event === "getMasters") {
        socket.off(
            "getMastersTransferComplete",
            onGetMastersTransferComplete
        );
    }

    socket.off("disconnect", onDisconnect);

    for (const cancelAck of [...pendingAckCleanups]) {
        cancelAck();
    }
}

        function waitForAck(
        batchId,
        chunkIndex
    ) {

    return new Promise((resolve, reject) => {

        let ackTimeout;

function resetAckTimeout() {
    clearTimeout(ackTimeout);

    ackTimeout = setTimeout(() => {

        const lastHeartbeat =
            Number(socket.lastHeartbeat || 0);

        const heartbeatFresh =
            lastHeartbeat > 0 &&
            Date.now() - lastHeartbeat < 90000;

        if (
            socket.connected &&
            heartbeatFresh &&
            socket.tallyRequestActive === true
        ) {
            console.log(
                "⏳ CHUNK ACK WAIT — CONNECTOR STILL ACTIVE",
                {
                    batchId,
                    chunkIndex,
                    heartbeatAgeMs:
                        Date.now() - lastHeartbeat
                }
            );

            resetAckTimeout();
            return;
        }

        console.error("CHUNK ACK TIMEOUT:", {
            event: ackEvent,
            batchId,
            chunkIndex,
            socketConnected: socket.connected,
            heartbeatFresh,
            tallyRequestActive:
                socket.tallyRequestActive
        });

      socket.off(ackEvent, onAck);
socket.off("disconnect", onDisconnect);

pendingAckCleanups.delete(cancelPendingAck);

reject(
            new Error(
                `ACK timeout for chunk ${chunkIndex}`
            )
        );

    }, ACK_TIMEOUT);
}

resetAckTimeout();

function cancelPendingAck() {
    clearTimeout(ackTimeout);

    socket.off(ackEvent, onAck);
    socket.off("disconnect", onDisconnect);

    pendingAckCleanups.delete(cancelPendingAck);

    reject(
        new Error(
            `CONNECTOR_LOCK_LOST: ${connectorId}`
        )
    );
}

pendingAckCleanups.add(cancelPendingAck);

function onAck(data) {

   if (
    !data ||
    data.batchId !== batchId ||
    data.requestId !== requestId ||
    data.chunkIndex !== chunkIndex
) {
        console.error("CHUNK ACK MISMATCH:", {
            event: ackEvent,
            expectedBatchId: batchId,
            receivedBatchId: data?.batchId,
            expectedChunkIndex: chunkIndex,
            receivedChunkIndex: data?.chunkIndex,
            success: data?.success
        });

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

pendingAckCleanups.delete(cancelPendingAck);

if (data.success === false) {
    const connectorError = new Error(
        data.error || "Chunk rejected"
    );

    connectorError.connectorResponseReceived = true;

    return reject(connectorError);
}

            resolve();

        }
//latest
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

pendingAckCleanups.delete(cancelPendingAck);

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
/* 181026
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
*/

        let timeout;

        function resetCompleteAckTimeout() {
            clearTimeout(timeout);

            timeout = setTimeout(() => {
                const lastHeartbeat =
                    Number(socket.lastHeartbeat || 0);

                const heartbeatAge =
                    lastHeartbeat > 0
                        ? Date.now() - lastHeartbeat
                        : Infinity;

                const heartbeatFresh =
                    socket.connected === true &&
                    heartbeatAge >= 0 &&
                    heartbeatAge < 90000;

                if (
                    heartbeatFresh &&
                    socket.tallyRequestActive === true
                ) {
                    console.log(
                        "⏳ COMPLETE ACK WAIT — CONNECTOR STILL ACTIVE",
                        {
                            batchId,
                            heartbeatAgeMs: heartbeatAge
                        }
                    );

                    resetCompleteAckTimeout();
                    return;
                }

              socket.off(
                            completeAckEvent,
                            onCompleteAck
                        );

                        socket.off(
                            "disconnect",
                            onDisconnect
                        );

                        pendingAckCleanups.delete(cancelPendingCompleteAck);

                        reject(
                            new Error("Complete ACK timeout")
                        );

            }, ACK_TIMEOUT);
        }

       resetCompleteAckTimeout();

function cancelPendingCompleteAck() {
    clearTimeout(timeout);

    socket.off(completeAckEvent, onCompleteAck);
    socket.off("disconnect", onDisconnect);

    pendingAckCleanups.delete(cancelPendingCompleteAck);

    reject(
        new Error(
            `CONNECTOR_LOCK_LOST: ${connectorId}`
        )
    );
}

pendingAckCleanups.add(cancelPendingCompleteAck);

function onCompleteAck(data) {


if (
    !data ||
    data.batchId !== batchId ||
    data.requestId !== requestId
) {
    console.warn("Ignoring mismatched Complete ACK:", {
        event,
        expectedBatchId: batchId,
        receivedBatchId: data?.batchId,
        expectedRequestId: requestId,
        receivedRequestId: data?.requestId
    });

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

pendingAckCleanups.delete(cancelPendingCompleteAck);

if (data.success === false) {
    const connectorError = new Error(
        data.error || "Complete rejected"
    );

    connectorError.connectorResponseReceived = true;

    return reject(connectorError);
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

pendingAckCleanups.delete(cancelPendingCompleteAck);

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

    if (lockLost || signal.aborted) {
        throw new Error(
            `CONNECTOR_LOCK_LOST: ${connectorId}`
        );
    }

    console.log(
        `Sending chunk ${chunk.chunkIndex}/${chunk.totalChunks}`
    );

const ackPromise = waitForAck(batchId, chunk.chunkIndex);

// Request chunk is about to be dispatched to connector.
requestDispatched = true;
dispatchState.dispatched = true;

socket.emit(
    `${event}Chunk`,
    {
        batchId,
        requestId,
        company: payload.company,
        sync_batch_id: payload.sync_batch_id,
        tally_owner: payload.tally_owner,
        module: payload.module,
        entity_type: payload.entity_type,
        fromDate: payload.fromDate,
        toDate: payload.toDate,
        booksBeginningFrom: payload.booksBeginningFrom,
        chunkIndex: chunk.chunkIndex,
        totalChunks: chunk.totalChunks,
        payloadSize: chunk.payloadSize,
        data: chunk.data
    }
);

await ackPromise;

if (lockLost || signal.aborted) {
    throw new Error(
        `CONNECTOR_LOCK_LOST: ${connectorId}`
    );
}
        console.log(
            `ACK received for chunk ${chunk.chunkIndex}/${chunk.totalChunks}`
        );

    }

    if (lockLost || signal.aborted) {
    throw new Error(
        `CONNECTOR_LOCK_LOST: ${connectorId}`
    );
}

const completeAckPromise =
    waitForCompleteAck(batchId);

// Complete event can also be the first emitted request
// when requestItems is empty.
requestDispatched = true;
dispatchState.dispatched = true;

socket.emit(
    `${event}Complete`,
            {

            batchId,

            requestId,

            company: payload.company,

            sync_batch_id: payload.sync_batch_id,

            tally_owner: payload.tally_owner,

            module: payload.module,

            entity_type: payload.entity_type,

            totalChunks: chunks.length,

            booksBeginningFrom:
    payload.booksBeginningFrom, 

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

    if (lockLost || operationSettled) {
        return;
    }

    try {
        console.log("CHUNK RESULT DEBUG:", {
    event,
    requestId,
    receivedRequestId: data?.requestId,
    success: data?.success,
    collectionName: data?.collectionName
});

     if (data?.requestId !== requestId) {

    console.warn("Ignoring mismatched connector result:", {
        event,
        expectedRequestId: requestId,
        receivedRequestId: data?.requestId
    });

    return;
}
        if (!data) {
            return;
        }

        //resetTimeout();

        console.log(
            "RESULT DEBUG :",
            JSON.stringify(data, null, 2)
        );

       if (data.success !== true) {

    operationSettled = true;

    cleanup();

    const connectorError = new Error(
    data.error || "Unknown error"
);

const isExplicitOutcomeUnknown =
    event === "getMasters" &&
    (
        data.error === "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN" ||
        data.outcomeUnknown === true
    );

if (isExplicitOutcomeUnknown) {
    connectorError.code = "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN";
    connectorError.outcomeUnknown = true;

    // An uncertainty report is not a settled connector result.
    connectorError.connectorResponseReceived = false;
} else {
    connectorError.connectorResponseReceived = true;
}

    return reject(connectorError);
}

        if (
    event === "getMasters" &&
    masterResult !== null
) {
    console.warn(
        "DUPLICATE_GET_MASTERS_SUCCESS_IGNORED",
        { requestId }
    );
    return;
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

activeProtocolTask = protocolController.receiveAll(requestId);

const collections = await activeProtocolTask;
        if (lockLost || operationSettled) {
            return;
        }

        Object.assign(
            masterResult,
            collections
        );

        console.log(
            "All master collections received"
        );

   if (operationSettled || lockLost || signal.aborted) {
    return;
}

protocolCollectionsComplete = true;

tryCompleteGetMasters();

if (!operationSettled) {
    startGetMastersFinalTimer();
}

return;
    }
 catch (error) {

    if (operationSettled) {
        return;
    }

    operationSettled = true;

    cleanup();

    reject(error);
}

}

async function onChunk(data) {

    if (lockLost || operationSettled || completionStarted) {
        return;
    }

    let finishChunkTask;

const chunkTask = {
    promise: new Promise(resolve => {
        finishChunkTask = resolve;
    })
};

activeChunkTasks.add(chunkTask);

    try {

    console.log("SNAPSHOT FLAG:", payload.snapshot);
    console.log("MODULE:", payload.module);
    console.log("ENTITY:", payload.entity_type);

    if (!data) {
        return;
    }

       if (data.requestId !== requestId) {

    console.warn("Ignoring mismatched connector chunk:", {
        event,
        expectedRequestId: requestId,
        receivedRequestId: data.requestId,
        batchId: data.batchId,
        chunkIndex: data.chunkIndex
    });

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
                        requestId,
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

if (processingChunkIndexes.has(data.chunkIndex)) {
    console.log(
        `Chunk already processing: ${data.chunkIndex}`
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
        requestId,
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
        requestId,
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
        requestId,
        chunkIndex: data.chunkIndex,
        success: false,
        error: "Inconsistent totalChunks"
    }
);

      return;
}

processingChunkIndexes.add(data.chunkIndex);
chunkTask.reservedIndex = data.chunkIndex;

if (payload.snapshot === true) {
   
console.log("EVENT :", event);
console.log("SNAPSHOT :", payload.snapshot);
console.log("ENTITY :", payload.entity_type);
console.log("ROWS :", data.data.length);
console.log("========================");

    try {
await saveSyncSnapshotChunk({
    batch_id: executionContext.batch_id,
    execution_claim_token: executionContext.execution_claim_token,
    sync_batch_id: payload.sync_batch_id,
    company_code: payload.company_code,
    tally_owner: payload.tally_owner,
    module: payload.module,
    entity_type: payload.entity_type,
    rows: data.data
});

if (lockLost || operationSettled) {
    return;
}

} catch (error) {

    chunkProcessingError = error;

    console.error("SNAPSHOT CHUNK SAVE FAILED:", {
        batchId: data.batchId,
        chunkIndex: data.chunkIndex,
        error: error.message
    });
   socket.emit(ackEvent, {
    batchId: data.batchId,
    requestId,
    chunkIndex: data.chunkIndex,
    success: false,
    error: error.message || "Snapshot chunk save failed"
});

    return;
}

collectionList.push(...data.data);
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
        requestId,
        chunkIndex: data.chunkIndex,
        success: true
    }
);

         console.log(
        `ACK sent for chunk ${data.chunkIndex}/${data.totalChunks}`
    );

   } catch (error) {

    chunkProcessingError = error;

    if (operationSettled) {
        return;
    }

    operationSettled = true;
        console.error(
            "CONNECTOR CHUNK HANDLER FAILED:",
            connectorId,
            event,
            error
        );

               cleanup();

        reject(error);
} finally {

    if (
    data &&
    Number.isInteger(data.chunkIndex) &&
    chunkTask.reservedIndex === data.chunkIndex
) {
    processingChunkIndexes.delete(data.chunkIndex);
}

    activeChunkTasks.delete(chunkTask);

    finishChunkTask();

    }
}

async function onComplete(data) {

    if (lockLost || operationSettled) {
        return;
    }

    try {

    console.log("CHUNK COMPLETE DEBUG:", {
    event,
    requestId,
    receivedBatchId: data?.batchId,
    masterResultReceived: Boolean(masterResult),
    socketConnected: socket.connected
});

    //resetTimeout();

        if (data?.requestId !== requestId) {

    console.warn("Ignoring mismatched connector Complete:", {
        event,
        expectedRequestId: requestId,
        receivedRequestId: data?.requestId,
        batchId: data?.batchId
    });

    return;
}
    if (
            event === "getMasters"
        ) {
            return;
        }

        if (!data?.batchId) {
    operationSettled = true;

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
    operationSettled = true;

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
completionStarted = true;

// Wait for all in-flight chunk processing
// Wait until ALL in-flight chunk handlers settle,
// even if one or more handlers fail.
while (activeChunkTasks.size > 0) {
    const pendingTasks = [...activeChunkTasks];

    await Promise.allSettled(
        pendingTasks.map(task => task.promise)
    );
}

// Lock/disconnect may have settled the operation while waiting
if (lockLost || operationSettled) {
    return;
}
if (chunkProcessingError) {
    operationSettled = true;
    cleanup();
    return reject(chunkProcessingError);
}

// Verify all expected chunks were received
if (
    !Number.isInteger(data.totalChunks) ||
    data.totalChunks < 0 ||
    receivedChunkIndexes.size !== data.totalChunks ||
    (
        expectedChunks !== null &&
        expectedChunks !== data.totalChunks
    )
) {
    operationSettled = true;

    cleanup();

    return reject(
        new Error(
            `INCOMPLETE_CHUNK_TRANSFER: Expected ${data.totalChunks}, Received ${receivedChunkIndexes.size}`
        )
    );
}



if (!masterResult) {
    operationSettled = true;

    cleanup();

    return reject(
                    new Error(
    `${event}: Master payload not received; ` +
    `requestId=${requestId}, ` +
    `batchId=${payload.sync_batch_id || payload.batchId || "unknown"}`
)
                );

            }

           if (
    !Number.isInteger(data.totalItems) ||
    data.totalItems < 0
) {
    operationSettled = true;

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

    operationSettled = true;

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

    operationSettled = true;

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

        

if (
    payload.snapshot === true &&
    payload.syncMode === "FULL"
) {

    try {

        activeCompletionTask = removeMissingSnapshotGuids({
    batch_id: executionContext.batch_id,
    execution_claim_token: executionContext.execution_claim_token,
    sync_batch_id: payload.sync_batch_id,
    company_code: payload.company_code,
    tally_owner: payload.tally_owner,
    module: payload.module,
    entity_type: payload.entity_type
});

await activeCompletionTask;

        if (lockLost || operationSettled) {
    return;
}
    }
    
    catch(error) {

    operationSettled = true;

    cleanup();

    return reject(error);

}

}

            if (payload.snapshot === true) {

    socket.emit(
        `${event}CompleteAck`,
        {
            batchId: data.batchId,
            requestId,
            success: true
        }
    );

   operationSettled = true;

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
                requestId,
                success: true
            }
        );

        operationSettled = true;

cleanup();

resolve({

            ...masterResult,

            [collectionName]: collectionList

        });

       }

    } catch (error) {

    if (operationSettled) {
        return;
    }

    operationSettled = true;

        console.error(
            "CONNECTOR COMPLETE HANDLER FAILED:",
            connectorId,
            event,
            error
        );

        cleanup();

        reject(error);

    }

}

function onDisconnect(reason) {
    if (operationSettled) {
        return;
    }

    operationSettled = true;

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

if (event === "getMasters") {
    socket.on(
        "getMastersTransferComplete",
        onGetMastersTransferComplete
    );
}

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

// All event names and handlers are initialized.
// Abort can now safely trigger cleanup().
rejectActiveOperation = rejectOnLockLost;

if (lockLost || signal.aborted) {
    rejectOnLockLost();
    return;
}

    if (Array.isArray(payload.requestItems)) {

    activeRequestTransferTask = sendRequestChunks(
        payload.requestItems
    );

    activeRequestTransferTask.catch(error => {

        if (operationSettled) {
            return;
        }

        operationSettled = true;

        cleanup();

        reject(error);

    });

    return;
}
if (lockLost || signal.aborted) {
    rejectOnLockLost();
    return;
}

// Normal request is about to be dispatched to connector.
requestDispatched = true;
dispatchState.dispatched = true;

socket.emit(
    event,
    {
        ...payload,
        requestId,

        ...(durableRequestId
            ? {
                identityScope: executionContext.identity_scope,
                companyIdentity: executionContext.company_identity,
                payloadFingerprint: executionContext.payload_fingerprint,

                ...(event === "getMasters"
                    ? {
                        tallyInputFingerprint:
                            "sha256:" +
                            crypto.createHash("sha256")
                                .update(
                                    JSON.stringify([
                                        "getMasters-input-v1",
                                        payload.company,
                                        payload.lastAlterId ?? null,
                                        payload.lastStockAlterId ?? null,
                                        payload.lastLedgerAlterId ?? null
                                    ]),
                                    "utf8"
                                )
                                .digest("hex")
                    }
                    : {})
            }
            : {})
    }
);

    });
} catch (error) {
    if (error && typeof error === "object") {
        // Only actual socket emission counts as dispatch.
        // Database dispatch authorization alone does not.
        const wasDispatched =
            requestDispatched === true ||
            dispatchState.dispatched === true;

        error.requestDispatched =
            error.requestDispatched === true ||
            wasDispatched;
    }

    throw error;
} finally {

    // Wait for outgoing request transfer before releasing global lock
    if (activeRequestTransferTask) {
        try {
            await activeRequestTransferTask;
        } catch (error) {
            console.error(
                "REQUEST TRANSFER TASK FAILED:",
                connectorId,
                error.message
            );
        }
    }

    // Wait for all in-flight chunk handlers
  while (activeChunkTasks.size > 0) {
    const pendingTasks = [...activeChunkTasks];

    await Promise.allSettled(
        pendingTasks.map(task => task.promise)
    );
}
// Wait for FULL snapshot cleanup before releasing connector lock
if (activeCompletionTask) {
    try {
        await activeCompletionTask;
    } catch (error) {
        console.error(
            "SNAPSHOT COMPLETION TASK FAILED:",
            connectorId,
            error.message
        );
    }
}

// Wait for getMasters protocol receiving to finish
// before the global coordinator releases the lock.
if (activeProtocolTask) {
    try {
        await activeProtocolTask;
    } catch (error) {
        console.error(
            "PROTOCOL RECEIVE TASK FAILED:",
            connectorId,
            error.message
        );
    }
}

    // Stop renewal only after chunk processing finishes
    /* 241026
    clearInterval(renewalTimer);

    // Wait for any already-running Redis lock renewal
    while (renewing) {
        await new Promise(resolve => setTimeout(resolve, 50));
    }

    try {
        await releaseConnectorOperationLock(
            connectorId,
            ownerToken
        );
    } catch (error) {
        console.error(
            "CONNECTOR LOCK RELEASE FAILED:",
            connectorId,
            error.message
        );
    }
*/
    // Global coordinator owns lock renewal and release.
    // Remove this operation's abort listener after tasks finish.
    signal.removeEventListener(
        "abort",
        onGlobalLockAbort
    );

    rejectActiveOperation = null;

}

}

module.exports = {
    sendChunkedToConnector
};