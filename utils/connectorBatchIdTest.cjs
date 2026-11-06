
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto");
const { EventEmitter } = require("node:events");

const source = fs.readFileSync(
    path.join(__dirname, "sendChunkedToConnector.js"),
    "utf8"
);

const EVENT = "getMastersVoucherGuids";
const REQUEST_ID =
    "12345678-1234-1234-1234-123456789abc";

const tick = () =>
    new Promise(resolve => setImmediate(resolve));

async function setup(snapshot = true, options = {}) {
    const socket = new EventEmitter();

    socket.connected = true;
    socket.id = "test-socket";
    socket.connectorIds = new Set(["test-connector"]);
    socket.protocolReceiver = {
        collectionCancelled: false
    };

    const controller = new AbortController();
    const acknowledgements = [];
    const savedChunks = [];
    const removedSnapshots = [];

    const originalEmit = socket.emit.bind(socket);

    socket.emit = (name, data) => {
        if (name === `${EVENT}ChunkAck` ||
            name === `${EVENT}CompleteAck`) {
            acknowledgements.push({ name, data });
            return true;
        }

        return originalEmit(name, data);
    };

    const dependencies = {
        crypto,
        fs: {},
        "./ChunkBuilder": {
            buildChunks() {
                throw new Error("Unexpected request upload");
            }
        },
      "../socketio/connectorOperationCoordinator": {
    async withGlobalConnectorLock(id, operation) {
        assert.equal(id, "test-connector");

        options.onLockAcquired?.();

        try {
            return await operation(socket, controller.signal);
        } finally {
            options.onLockReleased?.();
        }
    }
},
        "../socketio/connectorRegistry": {
            get() {
                return socket;
            }
        },
        "./masterFinalAckReceipts": {
            begin() {
                return {};
            },
            publish() {
                return true;
            }
        },
        "./protocol/ServerProtocolController": class {
            receiveAll() {
                throw new Error("Unexpected protocol read");
            }
        },
       "../services/sync/SyncSnapshotService": {
   async saveSyncSnapshotChunk(data) {
    if (options.failSnapshotSave === true) {
        throw new Error("SIMULATED_SNAPSHOT_DB_FAILURE");
    }

    if (typeof options.waitForSnapshotSave === "function") {
        await options.waitForSnapshotSave();
    }

    savedChunks.push(data);
},
            async removeMissingSnapshotGuids(data) {
                removedSnapshots.push(data);
            }
        }
    };

    const moduleObject = { exports: {} };

    const loader = vm.runInNewContext(
        `(function(require, module, exports) {
            ${source}
        })`,
        {
            console: {
                log() {},
                warn() {},
                error() {}
            },
            setTimeout,
            clearTimeout,
            setInterval,
            clearInterval
        },
        {
            filename: "sendChunkedToConnector.js"
        }
    );

    loader(
        name => {
            assert.ok(
                Object.hasOwn(dependencies, name),
                `Unexpected dependency: ${name}`
            );
            return dependencies[name];
        },
        moduleObject,
        moduleObject.exports
    );

    let dispatched = false;

    socket.on(EVENT, request => {
        dispatched = true;
        assert.equal(request.requestId, REQUEST_ID);

        queueMicrotask(() => {
            socket.emit(`${EVENT}Result`, {
                requestId: REQUEST_ID,
                success: true,
                collectionName: "vouchers"
            });
        });
    });

    const operation =
        moduleObject.exports.sendChunkedToConnector(
            socket,
            EVENT,
            {
                company: "Test Company",
                company_code: "TEST",
                tally_owner: "USER",
                sync_batch_id: "sync-test",
                module: "masters",
                entity_type: "voucher",
                snapshot,
                syncMode: "INCREMENTAL"
            },
            "test-connector",
            {
                request_id: REQUEST_ID,
                batch_id: "database-batch",
                execution_claim_token: "test-token",
                async before_dispatch() {}
            }
        );

    // Attach a rejection observer immediately so Node
    // does not report an unhandled rejection.
    const observed = operation.then(
        value => ({ success: true, value }),
        error => ({ success: false, error })
    );

    await tick();
    await tick();

    assert.equal(dispatched, true);

  function chunk(batchId, index = 1, overrides = {}) {
    socket.emit(`${EVENT}Chunk`, {
        requestId: REQUEST_ID,
        batchId,
        chunkIndex: index,
        totalChunks: 1,
        data: [{ guid: "test-guid" }],
        ...overrides
    });
}

    function complete(batchId, totalChunks = 0,
                      totalItems = 0) {
        socket.emit(`${EVENT}Complete`, {
            requestId: REQUEST_ID,
            batchId,
            totalChunks,
            totalItems
        });
    }

    async function settle() {
        const result = await observed;

        for (const name of [
            `${EVENT}Result`,
            `${EVENT}Chunk`,
            `${EVENT}Complete`,
            "disconnect"
        ]) {
            assert.equal(
                socket.listenerCount(name),
                0,
                `Listener leaked: ${name}`
            );
        }

        return result;
    }

 return {
    chunk,
    complete,
    settle,
    disconnect() {
        socket.connected = false;
        socket.emit("disconnect", "transport close");
    },
    acknowledgements,
    savedChunks,
    removedSnapshots
};
}

const invalidIds = [
    { name: "missing", value: undefined },
    { name: "null", value: null },
    { name: "empty", value: "" },
    { name: "spaces", value: "   " },
    { name: "number", value: 123 },
    { name: "boolean", value: true },
    { name: "object", value: {} },
    { name: "array", value: [] }
];

for (const item of invalidIds) {
    test(`invalid chunk batchId: ${item.name}`,
        async () => {
            const h = await setup(true);

            h.chunk(item.value);
            await tick();

            assert.equal(h.savedChunks.length, 0);

            const ack = h.acknowledgements.find(
                x => x.name === `${EVENT}ChunkAck`
            );

            assert.ok(ack);
            assert.equal(ack.data.success, false);
            assert.equal(
                ack.data.error,
                "Invalid batchId"
            );

            // Finish operation with invalid completion.
            h.complete(null);

            const result = await h.settle();
            assert.equal(result.success, false);
            assert.match(
                result.error.message,
                /missing or invalid batchId/
            );

            assert.equal(h.savedChunks.length, 0);
        }
    );
}

for (const item of invalidIds) {
    test(`invalid completion batchId: ${item.name}`,
        async () => {
            const h = await setup(true);

            h.complete(item.value);

            const result = await h.settle();

            assert.equal(result.success, false);
            assert.match(
                result.error.message,
                /missing or invalid batchId/
            );
            assert.equal(h.savedChunks.length, 0);
            assert.equal(
                h.removedSnapshots.length,
                0
            );
        }
    );
}

test("valid snapshot chunk persists once",
    async () => {
        const h = await setup(true);

        h.chunk("batch-A");
        await tick();
        await tick();

        assert.equal(h.savedChunks.length, 1);

        const ack = h.acknowledgements.find(
            x => x.name === `${EVENT}ChunkAck`
        );

        assert.ok(ack);
        assert.equal(ack.data.success, true);

        h.complete("batch-A", 1, 1);

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(result.value.snapshotSaved, true);
        assert.equal(h.savedChunks.length, 1);
    }
);

test("different batch chunk never persists",
    async () => {
        const h = await setup(true);

        h.chunk("batch-A");
        await tick();
        await tick();

        assert.equal(h.savedChunks.length, 1);

        h.chunk("batch-B");
        await tick();
        await tick();

        assert.equal(h.savedChunks.length, 1);

        h.complete("batch-A", 1, 1);

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(h.savedChunks.length, 1);
    }
);

test("completion with different batch rejects",
    async () => {
        const h = await setup(true);

        h.chunk("batch-A");
        await tick();
        await tick();

        h.complete("batch-B", 1, 1);

        const result = await h.settle();

        assert.equal(result.success, false);
        assert.match(
            result.error.message,
            /Complete batch mismatch/
        );
        assert.equal(h.savedChunks.length, 1);
    }
);

test("valid zero-chunk transfer completes",
    async () => {
        const h = await setup(true);

        h.complete("batch-empty", 0, 0);

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(result.value.snapshotSaved, true);
        assert.equal(h.savedChunks.length, 0);
    }
);

test("valid non-snapshot transfer completes",
    async () => {
        const h = await setup(false);

        h.chunk("batch-normal");
        await tick();
        await tick();

        h.complete("batch-normal", 1, 1);

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(h.savedChunks.length, 0);
        assert.equal(result.value.vouchers.length, 1);
    }
);
test("invalid first chunk metadata must not reserve batchId",
    async () => {
        const h = await setup(true);

        // First chunk has a valid batchId but invalid metadata.
        h.chunk("invalid-first-batch", 0);

        await tick();
        await tick();

        assert.equal(h.savedChunks.length, 0);

        const invalidAck = h.acknowledgements.find(
            x => x.name === `${EVENT}ChunkAck`
        );

        assert.ok(invalidAck);
        assert.equal(invalidAck.data.success, false);
        assert.equal(
            invalidAck.data.error,
            "Invalid chunk metadata"
        );

        // A valid chunk from another batch must be accepted.
        h.chunk("valid-batch", 1);

        await tick();
        await tick();

        assert.equal(h.savedChunks.length, 1);

        h.complete("valid-batch", 1, 1);

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(result.value.snapshotSaved, true);
        assert.equal(h.savedChunks.length, 1);
    }
);

test("snapshot DB save failure must reject transfer",
    async () => {
        const h = await setup(true, {
            failSnapshotSave: true
        });

        h.chunk("batch-db-failure");

        await tick();
        await tick();

        // Failed DB save must not count as persisted.
        assert.equal(h.savedChunks.length, 0);

        const chunkAck = h.acknowledgements.find(
            x => x.name === `${EVENT}ChunkAck`
        );

        assert.ok(chunkAck);
        assert.equal(chunkAck.data.success, false);
        assert.equal(
            chunkAck.data.error,
            "SIMULATED_SNAPSHOT_DB_FAILURE"
        );

        // Connector sends completion despite failed chunk.
        h.complete("batch-db-failure", 1, 1);

        const result = await h.settle();

        // Transfer must never report success.
        assert.equal(result.success, false);
        assert.match(
            result.error.message,
            /SIMULATED_SNAPSHOT_DB_FAILURE/
        );

        // Failed transfer must not run snapshot cleanup.
        assert.equal(h.removedSnapshots.length, 0);

        const successCompleteAck = h.acknowledgements.find(
            x =>
                x.name === `${EVENT}CompleteAck` &&
                x.data.success === true
        );

        assert.equal(successCompleteAck, undefined);
    }
);

test("global lock stays held until pending snapshot save finishes",
    async () => {
        let releaseSnapshotSave;
        let saveStarted = false;
        let lockAcquired = false;
        let lockReleased = false;

        const saveGate = new Promise(resolve => {
            releaseSnapshotSave = resolve;
        });

        const h = await setup(true, {
            waitForSnapshotSave: async () => {
                saveStarted = true;
                await saveGate;
            },
            onLockAcquired: () => {
                lockAcquired = true;
            },
            onLockReleased: () => {
                lockReleased = true;
            }
        });

        assert.equal(lockAcquired, true);

        h.chunk("batch-pending-save");

        await tick();
        await tick();

        assert.equal(saveStarted, true);
        assert.equal(h.savedChunks.length, 0);
        assert.equal(lockReleased, false);

        // Completion arrives while DB save is pending.
        h.complete("batch-pending-save", 1, 1);

        await tick();
        await tick();

        // Neither completion nor lock release is allowed yet.
        assert.equal(lockReleased, false);
        assert.equal(h.savedChunks.length, 0);

        // Allow DB persistence to finish.
        releaseSnapshotSave();

        const result = await h.settle();

        assert.equal(result.success, true);
        assert.equal(result.value.snapshotSaved, true);
        assert.equal(h.savedChunks.length, 1);
        assert.equal(lockReleased, true);
    }
);

test("disconnect during pending snapshot save must retain lock",
    async () => {
        let releaseSnapshotSave;
        let saveStarted = false;
        let lockReleased = false;

        const saveGate = new Promise(resolve => {
            releaseSnapshotSave = resolve;
        });

        const h = await setup(true, {
            waitForSnapshotSave: async () => {
                saveStarted = true;
                await saveGate;
            },
            onLockReleased: () => {
                lockReleased = true;
            }
        });

        h.chunk("batch-disconnect");

        await tick();
        await tick();

        assert.equal(saveStarted, true);
        assert.equal(lockReleased, false);

        // Disconnect while database write is pending.
        h.disconnect();

        await tick();
        await tick();

        // Lock must remain held until the DB write settles.
        assert.equal(lockReleased, false);
        assert.equal(h.savedChunks.length, 0);

        // Allow the in-flight database write to finish.
        releaseSnapshotSave();

        const result = await h.settle();

        // Disconnect must not produce a successful transfer.
        assert.equal(result.success, false);

        // No snapshot cleanup after disconnect.
        assert.equal(h.removedSnapshots.length, 0);

        // The lock may release only after the DB write settles.
        assert.equal(lockReleased, true);
    }
);