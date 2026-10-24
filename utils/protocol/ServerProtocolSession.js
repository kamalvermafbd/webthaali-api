
class ServerProtocolSession {

    constructor(socket) {
        this.socket = socket;
        this.collection = null;
        this.batchId = null;
        this.requestId = null;
        this.totalChunks = 0;
        this.receivedChunks = 0;
        this.receivedChunkIndexes = new Set();
        this.totalItems = 0;
        this.rows = [];
        this.completed = false;
        this.resolve = null;
        this.reject = null;
    }

    start({
        collection,
        batchId,
        requestId,
        totalChunks,
        totalItems
    }) {
        this.collection = collection;
        this.batchId = batchId;
        this.requestId = requestId;
        this.totalChunks = totalChunks;
        this.totalItems = totalItems;
        this.receivedChunks = 0;
        this.receivedChunkIndexes.clear();
        this.rows = [];
        this.completed = false;
    }

    addChunk(rows, chunkIndex) {
      if (
    !Number.isSafeInteger(chunkIndex) ||
    chunkIndex < 1 ||
    chunkIndex > this.totalChunks
) {
            throw new Error("Invalid protocol chunk index");
        }

        if (!Array.isArray(rows)) {
    throw new Error("Invalid protocol chunk data");
}

if (this.receivedChunkIndexes.has(chunkIndex)) {
    return false;
}

        this.receivedChunkIndexes.add(chunkIndex);
        this.rows.push(...rows);
        this.receivedChunks++;

        return true;
    }

    finish() {
        this.completed = true;
        this.collection = null;
        this.batchId = null;
        this.requestId = null;
        this.totalChunks = 0;
        this.receivedChunks = 0;
        this.receivedChunkIndexes.clear();
        this.totalItems = 0;
        this.rows = [];
        this.resolve = null;
        this.reject = null;
    }
}

module.exports = ServerProtocolSession;
