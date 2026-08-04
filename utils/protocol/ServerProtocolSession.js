class ServerProtocolSession {

    constructor(socket) {

        this.socket = socket;

        this.collection = null;

        this.batchId = null;

        this.totalChunks = 0;

        this.receivedChunks = 0;

        this.totalItems = 0;

        this.rows = [];

        this.completed = false;

        this.resolve = null;

        this.reject = null;

    }

    start({

        collection,

        batchId,

        totalChunks,

        totalItems

    }) {

        this.collection = collection;

        this.batchId = batchId;

        this.totalChunks = totalChunks;

        this.totalItems = totalItems;

        this.receivedChunks = 0;

        this.rows = [];

        this.completed = false;

    }

    addChunk(rows) {

        this.rows.push(...rows);

        this.receivedChunks++;

    }

   finish() {

    this.completed = true;

    this.collection = null;

    this.batchId = null;

    this.totalChunks = 0;

    this.receivedChunks = 0;

    this.totalItems = 0;

    this.rows = [];

    this.resolve = null;

    this.reject = null;

}

}

module.exports =
    ServerProtocolSession;