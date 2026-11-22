
"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { EventEmitter } = require("node:events");

const ServerProtocolReceiver = require(
    path.join(
        __dirname,
        "..",
        "utils",
        "protocol",
        "ServerProtocolReceiver.js"
    )
);

class TestSocket extends EventEmitter {
    constructor() {
        super();

        this.id = "test-partial-protocol-socket";
        this.connected = true;

        this.registrationCount = 0;
        this.injectFailure = true;
    }

    on(event, listener) {
        this.registrationCount++;

        // Fail on the second listener registration.
        // The first listener has already been attached.
        if (
            this.injectFailure &&
            this.registrationCount === 3
        ) {
            throw new Error(
                "INJECTED_SECOND_LISTENER_FAILURE"
            );
        }

        return super.on(event, listener);
    }
}

function snapshotListeners(socket) {
    const snapshot = new Map();

    for (const event of socket.eventNames()) {
        snapshot.set(
            event,
            new Set(socket.rawListeners(event))
        );
    }

    return snapshot;
}

function findLeakedListeners(socket, before) {
    const leaked = [];

    for (const event of socket.eventNames()) {
        const previous = before.get(event) || new Set();

        for (const listener of socket.rawListeners(event)) {
            if (!previous.has(listener)) {
                leaked.push(String(event));
            }
        }
    }

    return leaked;
}

function run() {
    const socket = new TestSocket();

    // Existing unrelated listener must remain untouched.
    let unrelatedCalls = 0;

    socket.on("unrelated:event", () => {
        unrelatedCalls++;
    });

    const baseline = snapshotListeners(socket);

    const failedReceiver = new ServerProtocolReceiver(
        socket
    );

    assert.throws(
        () => failedReceiver.start(),
        /INJECTED_SECOND_LISTENER_FAILURE/
    );

    console.log(
        "PASS: Partial start exception reproduced"
    );
console.log("BASELINE EVENTS:", [
    ...baseline.keys()
]);

console.log("CURRENT EVENTS:", socket.eventNames());

console.log(
    "LISTENER COUNTS:",
    socket.eventNames().map(event => ({
        event,
        count: socket.listenerCount(event)
    }))
);
    const leaked = findLeakedListeners(
        socket,
        baseline
    );

    console.log(
        "Listeners remaining after failed start:",
        leaked
    );

    // Do not silently accept leaked protocol listeners.
    assert.deepEqual(
        leaked,
        [],
        "BUG: Failed receiver left socket listeners attached"
    );

    console.log(
        "PASS: Failed start cleaned up its listeners"
    );

    // Retry without injection.
    socket.injectFailure = false;

    const retryReceiver = new ServerProtocolReceiver(
        socket
    );

    retryReceiver.start();

    const retryListeners = findLeakedListeners(
        socket,
        baseline
    );

    assert.equal(
        retryListeners.length,
        3,
        "Expected exactly two protocol listeners and one disconnect listener"
    );

    console.log(
        "PASS: Retry registered exactly three listeners"
    );

    socket.emit("unrelated:event");

    assert.equal(
        unrelatedCalls,
        1,
        "Unrelated socket listener was damaged"
    );

    console.log(
        "PASS: Unrelated listener preserved"
    );

    console.log(
        "\nPASS: PROTOCOL PARTIAL START CLEANUP"
    );
}

try {
    run();
} catch (error) {
    console.error(
        "\nFAIL: PROTOCOL PARTIAL START CLEANUP"
    );

    console.error(error.stack || error.message);

    process.exitCode = 1;
}
