
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto");
const { EventEmitter } = require("node:events");

const sourcePath = path.join(
    __dirname,
    "sendChunkedToConnector.js"
);

const source = fs.readFileSync(sourcePath, "utf8");

const REQUEST_ID = "12345678-1234-1234-1234-123456789abc";

const SCOPED_EVENTS = [
    "getMastersVoucherGuids",
    "getMastersGroupGuids",
    "getMastersLedgerGuids",
    "getMastersStockGroupGuids",
    "getMastersStockGuids",
    "getMastersUnitGuids",
    "getMastersGodownGuids",
    "getMastersCostCentreGuids",
    "ledgerByGuid",
    "stockByGuid",
    "voucherByGuid"
];

async function runFailure(event, response, options = {}) {
    const socket = new EventEmitter();

    Object.assign(socket, {
        connected: true,
        id: "isolated-socket",
        connectorIds: new Set(["isolated-connector"]),
        protocolReceiver: {
            collectionCancelled: false
        }
    });

    const controller = new AbortController();

    const calls = {
        authorization: 0,
        dispatch: 0,
        verification: 0,
        publication: 0,
        protocolReception: 0,
        persistence: 0
    };

    let beforeDispatchError = null;

    const forbiddenPersistence = () => {
        calls.persistence++;
        throw new Error("Unexpected persistence call");
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
            async withGlobalConnectorLock(connectorId, operation) {
                assert.equal(connectorId, "isolated-connector");

                return await operation(
                    socket,
                    controller.signal
                );
            }
        },
        "../socketio/connectorRegistry": {
            get(connectorId) {
                return connectorId === "isolated-connector"
                    ? socket
                    : null;
            }
        },
        "./masterFinalAckReceipts": {
            begin() {
                return Object.freeze({});
            },
            publish() {
                calls.publication++;
                return true;
            }
        },
        "./protocol/ServerProtocolController": class {
            receiveAll() {
                calls.protocolReception++;
                throw new Error("Unexpected protocol reception");
            }
        },
        "../services/sync/SyncSnapshotService": {
            saveSyncSnapshotChunk: forbiddenPersistence,
            removeMissingSnapshotGuids: forbiddenPersistence
        }
    };

    function mockRequire(name) {
        assert.ok(
            Object.prototype.hasOwnProperty.call(
                dependencies,
                name
            ),
            `Unexpected dependency: ${name}`
        );

        return dependencies[name];
    }

    const moduleObject = { exports: {} };

    const load = vm.runInNewContext(
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
        { filename: sourcePath }
    );

    load(
        mockRequire,
        moduleObject,
        moduleObject.exports
    );

    socket.on(event, (request) => {
        calls.dispatch++;

        assert.equal(request.requestId, REQUEST_ID);

        queueMicrotask(() => {
            socket.emit(`${event}Result`, {
                ...response,
                requestId: request.requestId,
                success: false
            });
        });
    });

    const operation = moduleObject.exports.sendChunkedToConnector(
        socket,
        event,
        { company: "Isolated company" },
        "isolated-connector",
      {
    request_id: REQUEST_ID,
    identity_scope: "test:isolated-connector",
    company_identity: "guid:isolated-company",
    payload_fingerprint: "sha256:" + "a".repeat(64),

    async before_dispatch() {
        calls.authorization++;

        if (options.failBeforeDispatch) {
            beforeDispatchError =
                new Error("Authorization rejected");

            throw beforeDispatchError;
        }
    },

    async after_transfer() {
        calls.verification++;
        throw new Error("Unexpected verification");
    }
}
    );

    let rejectedError;

    await assert.rejects(operation, (error) => {
        rejectedError = error;
        return true;
    });

    assert.equal(calls.authorization, 1);
    assert.equal(calls.verification, 0);
    assert.equal(calls.publication, 0);
    assert.equal(calls.protocolReception, 0);
    assert.equal(calls.persistence, 0);

    for (const eventName of [
        `${event}Result`,
        `${event}Chunk`,
        `${event}Complete`,
        "getMastersTransferComplete",
        "disconnect"
    ]) {
        assert.equal(
            socket.listenerCount(eventName),
            0,
            `Operation listener leaked: ${eventName}`
        );
    }

    if (options.failBeforeDispatch) {
        assert.equal(calls.dispatch, 0);
        assert.equal(rejectedError, beforeDispatchError);
        assert.equal(rejectedError.requestDispatched, false);
    } else {
        assert.equal(calls.dispatch, 1);
        assert.equal(rejectedError.requestDispatched, true);
    }

    return rejectedError;
}

for (const event of SCOPED_EVENTS) {
    test(`${event}: explicit uncertainty is preserved`, async () => {
        const message = `Unconfirmed result for ${event}`;

        const error = await runFailure(event, {
            error: message,
            outcomeUnknown: true
        });

        assert.equal(error.message, message);
        assert.equal(error.outcomeUnknown, true);
        assert.equal(error.connectorResponseReceived, false);
        assert.equal(error.code, undefined);
    });

    for (const variant of [
        { name: "missing flag", fields: {} },
        {
            name: "false flag",
            fields: { outcomeUnknown: false }
        },
        {
            name: 'string "true"',
            fields: { outcomeUnknown: "true" }
        }
    ]) {
        test(`${event}: ${variant.name} stays authoritative`, async () => {
            const message = `Rejected result for ${event}`;

            const error = await runFailure(event, {
                error: message,
                ...variant.fields
            });

            assert.equal(error.message, message);
            assert.equal(error.outcomeUnknown, undefined);
            assert.equal(error.connectorResponseReceived, true);
            assert.equal(error.code, undefined);
        });
    }
}

for (const variant of [
    {
        name: "explicit boolean uncertainty",
        response: {
            error: "Master transfer unconfirmed",
            outcomeUnknown: true
        },
        uncertain: true
    },
    {
        name: "existing master error string",
        response: {
            error: "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN"
        },
        uncertain: true
    },
    {
        name: "authoritative failure",
        response: {
            error: "Master request rejected"
        },
        uncertain: false
    },
    {
        name: "string uncertainty flag",
        response: {
            error: "Master request rejected",
            outcomeUnknown: "true"
        },
        uncertain: false
    }
]) {
    test(`getMasters: ${variant.name}`, async () => {
        const error = await runFailure(
            "getMasters",
            variant.response
        );

        assert.equal(error.message, variant.response.error);
        assert.equal(
            error.connectorResponseReceived,
            !variant.uncertain
        );
        assert.equal(
            error.outcomeUnknown,
            variant.uncertain ? true : undefined
        );
        assert.equal(
            error.code,
            variant.uncertain
                ? "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN"
                : undefined
        );
    });
}

test("unrelated event retains existing classification", async () => {
    const error = await runFailure(
        "isolatedUnrelatedRead",
        {
            error: "Unrelated failure",
            outcomeUnknown: true
        }
    );

    assert.equal(error.message, "Unrelated failure");
    assert.equal(error.outcomeUnknown, undefined);
    assert.equal(error.connectorResponseReceived, true);
    assert.equal(error.code, undefined);
});

test("authorization failure remains undispatched", async () => {
    const error = await runFailure(
        "getMastersVoucherGuids",
        { error: "Must not be delivered" },
        { failBeforeDispatch: true }
    );

    assert.equal(error.message, "Authorization rejected");
    assert.equal(error.requestDispatched, false);
    assert.equal(error.outcomeUnknown, undefined);
    assert.equal(error.connectorResponseReceived, undefined);
});
