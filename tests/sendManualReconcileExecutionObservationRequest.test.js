
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { EventEmitter } = require("node:events");
const Module = require("node:module");

const CONNECTOR_ID = "CON-TEST";
const SOCKET_ID = "original-socket";

const REQUEST_EVENT = "reconcile:observeExecution";
const RESULT_EVENT = "reconcile:observeExecutionResult";

const helperPath = require.resolve(
    "../socketio/sendManualReconcileExecutionObservationRequest"
);

const registryPath = require.resolve(
    "../socketio/connectorRegistry"
);

function harness(options = {}) {
    const attemptId = randomUUID();
    const executionId = randomUUID();
    const processEpoch = randomUUID();
    const fenceId = randomUUID();

    const registryMap = new Map();
    const socket = new EventEmitter();

    socket.id = SOCKET_ID;
    socket.connected = true;
    socket.connectorIds = new Set([CONNECTOR_ID]);

    registryMap.set(CONNECTOR_ID, socket);

    const registry = {
        get(connectorId) {
            return registryMap.get(connectorId) || null;
        }
    };

    const requests = [];
    const originalEmit = socket.emit.bind(socket);

    socket.emit = function (event, data) {
        if (event === REQUEST_EVENT) {
            requests.push(data);

            if (typeof options.onRequest === "function") {
                options.onRequest({
                    socket,
                    request: data,
                    originalEmit,
                    registryMap,
                    attemptId,
                    executionId,
                    processEpoch,
                    fenceId
                });
            }

            return true;
        }

        return originalEmit(event, data);
    };

    const originalLoad = Module._load;

    delete require.cache[helperPath];

    Module._load = function (request, parent, isMain) {
        const resolved = Module._resolveFilename(
            request,
            parent,
            isMain
        );

        if (resolved === registryPath) {
            return registry;
        }

        return originalLoad.apply(this, arguments);
    };

    let send;

    try {
        ({
            sendManualReconcileExecutionObservationRequest: send
        } = require(helperPath));
    } finally {
        Module._load = originalLoad;
        delete require.cache[helperPath];
    }

    function request(overrides = {}) {
        return send({
            connectorId: CONNECTOR_ID,
            expectedSocketId: SOCKET_ID,
            attemptId,
            executionId,
            expectedProcessEpoch: processEpoch,
            expectedFenceId: fenceId,
            timeoutMs: 30,
            ...overrides
        });
    }

    function reply(requestId, overrides = {}) {
        const observation = {
            attemptId,
            executionId,
            processEpoch,
            generation: 2,
            fenced: true,
            fenceId,
            executionKnown: true,
            pending: 0,
            running: 0,
            settled: 1,
            locallyDrained: true,
            settlementEligible: false,
            ...(overrides.observation || {})
        };

        originalEmit(RESULT_EVENT, {
            requestId,
            success: true,
            settlementEligible: false,
            observation,
            ...overrides,
            observation
        });
    }

    return {
        socket,
        registryMap,
        requests,
        request,
        reply,
        attemptId,
        executionId,
        processEpoch,
        fenceId
    };
}

test("valid observation is non-settling", async () => {
    let h;

    h = harness({
        onRequest({ request }) {
            h.reply(request.requestId);
        }
    });

    const result = await h.request();

    assert.equal(result.settlementEligible, false);
    assert.equal(result.observation.executionKnown, true);
    assert.equal(result.observation.locallyDrained, true);
    assert.equal(result.observation.settled, 1);
    assert.equal(result.observation.pending, 0);
    assert.equal(result.observation.running, 0);

    assert.equal(h.requests.length, 1);
    assert.equal(h.requests[0].attemptId, h.attemptId);
    assert.equal(h.requests[0].executionId, h.executionId);

    assert.equal(h.socket.listenerCount(RESULT_EVENT), 0);
    assert.equal(h.socket.listenerCount("disconnect"), 0);
});

test("wrong original socket is rejected before emit", async () => {
    const h = harness();

    await assert.rejects(
        h.request({
            expectedSocketId: "replacement-socket"
        }),
        /RECONCILE_OBSERVATION_SOCKET_NOT_OWNED/
    );

    assert.equal(h.requests.length, 0);
});

test("disconnect rejects pending observation", async () => {
    const h = harness({
        onRequest({ socket, originalEmit }) {
            socket.connected = false;
            originalEmit("disconnect");
        }
    });

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_SOCKET_DISCONNECTED/
    );

    assert.equal(h.socket.listenerCount(RESULT_EVENT), 0);
    assert.equal(h.socket.listenerCount("disconnect"), 0);
});

test("process epoch mismatch is rejected", async () => {
    let h;

    h = harness({
        onRequest({ request }) {
            h.reply(request.requestId, {
                observation: {
                    processEpoch: randomUUID()
                }
            });
        }
    });

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_NOT_VERIFIED/
    );
});

test("unknown execution is rejected", async () => {
    let h;

    h = harness({
        onRequest({ request }) {
            h.reply(request.requestId, {
                observation: {
                    executionKnown: false,
                    settled: 0,
                    locallyDrained: false
                }
            });
        }
    });

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_NOT_VERIFIED/
    );
});

test("observation timeout rejects and cleans listeners", async () => {
    const h = harness();

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_ACK_TIMEOUT/
    );

    assert.equal(h.requests.length, 1);
    assert.equal(h.socket.listenerCount(RESULT_EVENT), 0);
    assert.equal(h.socket.listenerCount("disconnect"), 0);
});

test("registry ownership loss rejects response", async () => {
    let h;

    h = harness({
        onRequest({ request, registryMap }) {
            registryMap.delete(CONNECTOR_ID);
            h.reply(request.requestId);
        }
    });

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_SOCKET_OWNERSHIP_LOST/
    );
});

test("running execution is rejected", async () => {
    let h;

    h = harness({
        onRequest({ request }) {
            h.reply(request.requestId, {
                observation: {
                    running: 1,
                    settled: 0,
                    locallyDrained: false
                }
            });
        }
    });

    await assert.rejects(
        h.request(),
        /RECONCILE_OBSERVATION_NOT_VERIFIED/
    );
});
