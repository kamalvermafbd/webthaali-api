const EVENTS =
    require("./ProtocolEvents");

const ServerProtocolSession =
    require("./ServerProtocolSession");

class ServerProtocolReceiver {

    constructor(socket) {

        this.socket = socket;

        this.collectionCancelled = false;

this.inactivityTimeoutMs = 120 * 1000;
this.inactivityTimer = null;
this.expectedCollection = null;
this.expectedRequestId = null;

this.session =
    new ServerProtocolSession(socket);

    }

    resetInactivityTimer() {

    clearTimeout(this.inactivityTimer);

    if (this.collectionCancelled) {
        return;
    }

    this.inactivityTimer = setTimeout(() => {

    this.inactivityTimer = null;

if (
    this.collectionCancelled ||
    !this.expectedCollection ||
    !this.expectedRequestId
) {
    return;
}

this.socket.emit(EVENTS.ERROR, {
        collection: this.expectedCollection,
        batchId: this.session.batchId,
        requestId: this.expectedRequestId,
        error: "Protocol inactivity timeout: no collection activity for 120 seconds"
    });

    this.cancelCollection(
        "Protocol inactivity timeout: no collection activity for 120 seconds"
    );

}, this.inactivityTimeoutMs);
}
  sendReady(collection, requestId) {

    if (this.collectionCancelled) {
        throw new Error(
            "Protocol receiver cancelled; connector must reconnect"
        );
    }

    if (!requestId) {
        throw new Error(
            "Protocol READY requires requestId"
        );
    }

    this.expectedCollection = collection;
    this.expectedRequestId = requestId;

    this.socket.emit(
        EVENTS.READY,
        {
            collection,
            requestId
        }
    );
}

    sendReceived(
    collection,
    batchId,
    chunkIndex,
    requestId
) {

    this.socket.emit(
        EVENTS.RECEIVED,
        {
            collection,
            batchId,
            chunkIndex,
            requestId
        }
    );

}

sendCompleted(
    collection,
    batchId,
    success = true,
    error = null,
    requestId = this.expectedRequestId
) {

    // Collection finished — stop accepting its chunks
    this.expectedCollection = null;
    this.expectedRequestId = null;

    this.socket.emit(
        EVENTS.COMPLETED,
        {
            collection,
            batchId,
            requestId,
            success,
            error
        }
    );

}

registerChunkListener() {

    this.socket.on(

        EVENTS.CHUNK,

       (data) => {

    if (!data || this.collectionCancelled) {
    return;
}

// Accept chunks only for the collection currently requested
if (
    !this.expectedCollection ||
    data.collection !== this.expectedCollection
) {
    console.warn(
        "Ignoring protocol chunk from unexpected collection:",
        data.collection,
        "EXPECTED:",
        this.expectedCollection
    );

    return;
}

// Accept chunks only from the active request
if (
    !this.expectedRequestId ||
    data.requestId !== this.expectedRequestId
) {
    console.warn(
        "Ignoring protocol chunk from unexpected request:",
        data.requestId,
        "EXPECTED:",
        this.expectedRequestId
    );

    return;
}

if (!this.session.batchId) {

    if (
        typeof data.batchId !== "string" ||
        !data.batchId.trim() ||
        !Number.isSafeInteger(data.totalChunks) ||
        data.totalChunks < 1
    ) {

        console.error(
            "Invalid protocol first chunk metadata:",
            data.collection,
            data.batchId,
            data.totalChunks
        );

      this.socket.emit(EVENTS.ERROR, {
    collection: data.collection,
    batchId: data.batchId,
    requestId: data.requestId,
    error: "Invalid protocol first chunk metadata"
});

this.cancelCollection(
    "Invalid protocol first chunk metadata"
);

return;
    }
    this.session.start({

        collection: data.collection,

        batchId: data.batchId,

        requestId: data.requestId,

        totalChunks: data.totalChunks,

        totalItems: 0

    });

} else {
   if (
    data.collection !== this.session.collection ||
    data.batchId !== this.session.batchId ||
    data.requestId !== this.session.requestId ||
    data.totalChunks !== this.session.totalChunks
) {

        console.error(
            "Ignoring chunk from unexpected batch:",
            data.collection,
            data.batchId,
            "EXPECTED:",
            this.session.collection,
            this.session.batchId
        );

        return;

    }

}

                   console.log(
                "🔥 SERVER RECEIVED CHUNK:",
                data.collection,
                data.batchId,
                data.chunkIndex
            );

            try {

    const isNewChunk = this.session.addChunk(
        data.data,
        data.chunkIndex
    );

    // Only new chunks count as protocol progress
    if (isNewChunk) {
        this.resetInactivityTimer();
    }

} catch (error) {

    console.error(
        "Invalid protocol chunk:",
        data.collection,
        data.batchId,
        data.chunkIndex,
        error.message
    );

   this.socket.emit(EVENTS.ERROR, {
    collection: data.collection,
    batchId: data.batchId,
    requestId: data.requestId,
    error: `Invalid protocol chunk: ${error.message}`
});

this.cancelCollection(
    `Invalid protocol chunk: ${error.message}`
);

return;
}
            console.log(
    "🔥 SERVER SENDING ACK:",
    data.collection,
    data.batchId,
    data.chunkIndex
);

            this.sendReceived(
    data.collection,
    data.batchId,
    data.chunkIndex,
    data.requestId
);

        }

    );

}

registerCompleteListener() {

    this.socket.on(

        EVENTS.COMPLETE,

      (data) => {

    if (!data || this.collectionCancelled) {
    return;
}

// Accept COMPLETE only for the currently requested collection
if (
    !this.expectedCollection ||
    data.collection !== this.expectedCollection
) {
    console.warn(
        "Ignoring protocol COMPLETE from unexpected collection:",
        data.collection,
        "EXPECTED:",
        this.expectedCollection
    );

    return;
}

// Accept COMPLETE only from the active request
if (
    !this.expectedRequestId ||
    data.requestId !== this.expectedRequestId
) {
    console.warn(
        "Ignoring protocol COMPLETE from unexpected request:",
        data.requestId,
        "EXPECTED:",
        this.expectedRequestId
    );

    return;
}

// No chunks received: only a genuinely empty collection is valid
if (
    !this.session.batchId &&
    (
        data.totalChunks !== 0 ||
        data.totalItems !== 0 ||
        typeof data.batchId !== "string" ||
        !data.batchId.trim()
    )
) {
   console.error(
    "Invalid empty protocol COMPLETE:",
    data.collection,
    data.batchId,
    data.totalChunks,
    data.totalItems
);

this.sendCompleted(
    data.collection,
    data.batchId,
    false,
    "Invalid empty protocol COMPLETE metadata",
    data.requestId
);

this.cancelCollection(
    "Invalid empty protocol COMPLETE metadata"
);

return;
}


if (!this.session.batchId) {

            const resolve =
                this.session.resolve;

            this.sendCompleted(
    data.collection,
    data.batchId,
    true,
    null,
    data.requestId
);

            clearTimeout(this.inactivityTimer);
this.inactivityTimer = null;

this.session.finish();

            if (resolve) {

                resolve([]);

            }

            return;

        }

            if (
                data.collection !==
                this.session.collection
            ) {
                return;
            }

          if (
    data.batchId !== this.session.batchId ||
    data.requestId !== this.session.requestId
) {
    console.warn(
        "Ignoring protocol COMPLETE from unexpected session:",
        data.batchId,
        data.requestId,
        "EXPECTED:",
        this.session.batchId,
        this.session.requestId
    );

    return;
}


if (
    this.session.receivedChunks !== this.session.totalChunks ||
    data.totalChunks !== this.session.totalChunks ||
    !Number.isSafeInteger(data.totalItems) ||
    data.totalItems < 0 ||
    this.session.rows.length !== data.totalItems
) {

   const error = new Error(
    `Protocol collection mismatch: ` +
    `expectedChunks=${this.session.totalChunks}, ` +
    `receivedChunks=${this.session.receivedChunks}, ` +
    `declaredChunks=${data.totalChunks}, ` +
    `expectedItems=${data.totalItems}, ` +
    `receivedItems=${this.session.rows.length}`
);

    const reject =
        this.session.reject;

    this.sendCompleted(
    data.collection,
    data.batchId,
    false,
    error.message,
    data.requestId
);
    clearTimeout(this.inactivityTimer);
this.inactivityTimer = null;

this.session.finish();

// Protocol mismatch invalidates this receiver
this.collectionCancelled = true;

if (reject) {
    reject(error);
}

return;

}

const rows = [
    ...this.session.rows
];

const resolve =
    this.session.resolve;

this.sendCompleted(
    data.collection,
    data.batchId,
    true,
    null,
    data.requestId
);

clearTimeout(this.inactivityTimer);
this.inactivityTimer = null;

this.session.finish();

if (resolve) {

    resolve(rows);

}

        }

    );

}

waitForCollection() {

    if (this.collectionCancelled) {
        return Promise.reject(
            new Error(
                "Protocol receiver cancelled; connector must reconnect"
            )
        );
    }

    const collectionPromise = new Promise((resolve, reject) => {

    this.session.resolve = resolve;

    this.session.reject = reject;

});

collectionPromise.catch(() => {});

// Start inactivity monitoring for this collection
this.resetInactivityTimer();

return collectionPromise;

}

cancelCollection(reason = "Protocol collection cancelled") {

    if (this.collectionCancelled) {
        return;
    }

     this.collectionCancelled = true;

    // Stop accepting chunks for the cancelled collection
    this.expectedCollection = null;
    this.expectedRequestId = null;

    // Stop inactivity monitoring after cancellation
    clearTimeout(this.inactivityTimer);
    this.inactivityTimer = null;
    const reject = this.session.reject;
    this.session.finish();

    if (reject) {
        reject(new Error(reason));
    }
}

start() {

    this.registerChunkListener();

    this.registerCompleteListener();

    this.socket.once("disconnect", () => {

        this.cancelCollection(
            "Socket disconnected during protocol collection"
        );

    });

}

}

module.exports =
    ServerProtocolReceiver;