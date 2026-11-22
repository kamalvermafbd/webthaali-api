
"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { EventEmitter } = require("node:events");

const EVENTS = require(
    path.join(
        __dirname,
        "..",
        "utils",
        "protocol",
        "ProtocolEvents.js"
    )
);

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

        this.id = "test-listener-ownership";
        this.connected = true;
        this.injectFailure = true;

        this.externalListener = () => {};
        this.externalListenerAdded = false;
    }

    on(event, listener) {

        if (
            this.injectFailure &&
            event === EVENTS.CHUNK &&
            !this.externalListenerAdded
        ) {
            this.externalListenerAdded = true;

            // Simulate another component registering
            // its own listener during receiver activation.
            super.on(
                EVENTS.CHUNK,
                this.externalListener
            );
        }

        if (
            this.injectFailure &&
            event === EVENTS.COMPLETE
        ) {
            throw new Error(
                "INJECTED_COMPLETE_LISTENER_FAILURE"
            );
        }

        return super.on(event, listener);
    }
}

function run() {
    const socket = new TestSocket();

    const receiver = new ServerProtocolReceiver(socket);

    assert.throws(
        () => receiver.start(),
        /INJECTED_COMPLETE_LISTENER_FAILURE/
    );

    console.log(
        "PASS: Partial activation failure reproduced"
    );

    const chunkListeners = socket.rawListeners(
        EVENTS.CHUNK
    );

    assert.equal(
        chunkListeners.includes(socket.externalListener),
        true,
        "BUG: Receiver cleanup removed another component's listener"
    );

    console.log(
        "PASS: External component listener preserved"
    );

    assert.equal(
        chunkListeners.length,
        1,
        "BUG: Failed receiver listener was not removed"
    );

    console.log(
        "PASS: Failed receiver listener removed"
    );

    socket.injectFailure = false;

    const retryReceiver = new ServerProtocolReceiver(
        socket
    );

    retryReceiver.start();

    assert.equal(
        socket.listenerCount(EVENTS.CHUNK),
        2,
        "Retry produced unexpected chunk listener count"
    );

    assert.equal(
        socket.listenerCount(EVENTS.COMPLETE),
        1
    );

    console.log(
        "PASS: Retry listener counts correct"
    );

    console.log(
        "\nPASS: PROTOCOL LISTENER OWNERSHIP SAFETY"
    );
}

try {
    run();
} catch (error) {
    console.error(
        "\nFAIL: PROTOCOL LISTENER OWNERSHIP SAFETY"
    );

    console.error(error.stack || error.message);

    process.exitCode = 1;
}
