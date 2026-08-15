const EVENTS =
    require("./ProtocolEvents");

const ServerProtocolSession =
    require("./ServerProtocolSession");

class ServerProtocolReceiver {

    constructor(socket) {

        this.socket = socket;

        this.session =
            new ServerProtocolSession(socket);

    }

    sendReady(collection) {

    this.socket.emit(

        EVENTS.READY,

        {
            collection
        }

    );
}

    sendReceived(
    collection,
    batchId,
    chunkIndex
) {

    this.socket.emit(

        EVENTS.RECEIVED,

        {

            collection,

            batchId,

            chunkIndex

        }

    );

}

sendCompleted(
    collection,
    batchId,
    success = true,
    error = null
) {

    this.socket.emit(

        EVENTS.COMPLETED,

        {

            collection,

            batchId,

            success,

            error

        }

    );

}

registerChunkListener() {

    this.socket.on(

        EVENTS.CHUNK,

        (data) => {

            if (!data) {
                return;
            }

            if (!this.session.batchId) {

                this.session.start({

                    collection:
                        data.collection,

                    batchId:
                        data.batchId,

                    totalChunks:
                        data.totalChunks,

                    totalItems: 0

                });

            }

            console.log(
                "🔥 SERVER RECEIVED CHUNK:",
                data.collection,
                data.batchId,
                data.chunkIndex
            );

            this.session.addChunk(
                data.data || []
            );

            console.log(
    "🔥 SERVER SENDING ACK:",
    data.collection,
    data.batchId,
    data.chunkIndex
);

            this.sendReceived(

                data.collection,

                data.batchId,

                data.chunkIndex

            );

        }

    );

}

registerCompleteListener() {

    this.socket.on(

        EVENTS.COMPLETE,

        (data) => {

            if (!data) {
                return;
            }
        if (!this.session.batchId) {

            const resolve =
                this.session.resolve;

            this.sendCompleted(
                data.collection,
                data.batchId
            );

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
                data.batchId !==
                this.session.batchId
            ) {
                return;
            }

            if (
                this.session.rows.length !==
                data.totalItems
            ) {
this.sendCompleted(

    data.collection,

    data.batchId,

    false,

    "Record count mismatch"

);

return;

            }

        const rows = [
    ...this.session.rows
];

const resolve =
    this.session.resolve;

this.sendCompleted(

    data.collection,

    data.batchId

);

this.session.finish();

if (resolve) {

    resolve(rows);

}

        }

    );

}

waitForCollection() {

    return new Promise((resolve, reject) => {

        this.session.resolve = resolve;

        this.session.reject = reject;

    });

}


start() {

    this.registerChunkListener();

    this.registerCompleteListener();

}





}



module.exports =
    ServerProtocolReceiver;