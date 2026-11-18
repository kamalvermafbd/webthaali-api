
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { randomUUID } = require("node:crypto");

const registry = require("../socketio/connectorRegistry");
const {
    sendManualReconcileFenceRequest
} = require("../socketio/sendManualReconcileFenceRequest");

const connectorId = "CON-TEST-FENCE";

function setup(t, onRequest) {
    const socket = new EventEmitter();

    socket.id = `test-${randomUUID()}`;
    socket.connected = true;

    const originalEmit = socket.emit.bind(socket);

    socket.emit = (event, data) => {
        if (event === "reconcile:installFence") {
            onRequest?.(data, socket, originalEmit);
            return true;
        }

        return originalEmit(event, data);
    };

    assert.equal(registry.register(connectorId, socket), true);

    t.after(() => {
        socket.connected = false;
        registry.remove(connectorId, socket);
    });

    return socket;
}

function args(socket) {
    const a = {
        connectorId,
        expectedSocketId: socket.id,
        attemptId: randomUUID(),
        executionId: randomUUID(),
        fenceId: randomUUID(),
        timeoutMs: 35
    };

    console.log("TEST ARGUMENTS:", {
        connectorId: a.connectorId,
        expectedSocketId: a.expectedSocketId,
        attemptId: a.attemptId,
        executionId: a.executionId,
        fenceId: a.fenceId,
        timeoutMs: a.timeoutMs
    });

    return a;
}

function ack(request, a, overrides = {}) {
    return {
        requestId: request.requestId,
        attemptId: a.attemptId,
        executionId: a.executionId,
        fenceId: a.fenceId,
        success: true,
        settlementEligible: false,
       snapshot: {
            processEpoch: randomUUID(),
            generation: 1,
            fenced: true,
            fenceId: a.fenceId,

            executionKnown: true,
            identityConflict: false,
            settlementEligible: false,

            pending: 0,
            running: 0,
            drained: true
        },
        ...overrides
    };
}

test("valid ACK is transport-only, never settlement evidence", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit("reconcile:installFenceResult", ack(request, a));
        });
    });

    a = args(socket);

    const result = await sendManualReconcileFenceRequest(a);

    assert.equal(result.settlementEligible, false);
    assert.equal(result.snapshot.drained, true);
    assert.equal(
        socket.listenerCount("reconcile:installFenceResult"),
        0
    );
});

test("wrong original socket ID rejects before dispatch", async t => {
    let dispatched = false;

    const socket = setup(t, () => {
        dispatched = true;
    });

    await assert.rejects(
        sendManualReconcileFenceRequest({
            ...args(socket),
            expectedSocketId: "old-socket"
        }),
        /SOCKET_NOT_OWNED/
    );

    assert.equal(dispatched, false);
});

test("missing registry ownership rejects before dispatch", async t => {
    let dispatched = false;

    const socket = setup(t, () => {
        dispatched = true;
    });

    const a = args(socket);

    registry.remove(connectorId, socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /SOCKET_NOT_OWNED/
    );

    assert.equal(dispatched, false);
});

test("invalid ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    fenceId: randomUUID()
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /ACK_INVALID/
    );
});

test("disconnect fails closed", async t => {
    const socket = setup(t, (_request, s, emit) => {
        queueMicrotask(() => {
            s.connected = false;
            emit("disconnect", "test");
        });
    });

    await assert.rejects(
        sendManualReconcileFenceRequest(args(socket)),
        /SOCKET_DISCONNECTED/
    );
});

test("no ACK times out and removes listeners", async t => {
    const socket = setup(t, () => {});

    await assert.rejects(
        sendManualReconcileFenceRequest(args(socket)),
        /ACK_TIMEOUT/
    );

    assert.equal(
        socket.listenerCount("reconcile:installFenceResult"),
        0
    );
});

test("wrong attemptId ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    attemptId: randomUUID()
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /RECONCILE_FENCE_ACK_INVALID/
    );
});

test("wrong executionId ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    executionId: randomUUID()
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /RECONCILE_FENCE_ACK_INVALID/
    );
});

test("missing attemptId ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    attemptId: undefined
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /RECONCILE_FENCE_ACK_INVALID/
    );
});

test("settlementEligible true ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    settlementEligible: true
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /RECONCILE_FENCE_ACK_INVALID/
    );
});

test("missing executionId ACK fails closed", async t => {
    let a;

    const socket = setup(t, (request, _socket, emit) => {
        queueMicrotask(() => {
            emit(
                "reconcile:installFenceResult",
                ack(request, a, {
                    executionId: undefined
                })
            );
        });
    });

    a = args(socket);

    await assert.rejects(
        sendManualReconcileFenceRequest(a),
        /RECONCILE_FENCE_ACK_INVALID/
    );
});
