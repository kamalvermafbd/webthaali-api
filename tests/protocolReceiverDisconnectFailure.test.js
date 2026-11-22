
"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");

const EVENTS = require("../utils/protocol/ProtocolEvents");
const ServerProtocolReceiver = require(
    "../utils/protocol/ServerProtocolReceiver"
);

class TestSocket extends EventEmitter {
    constructor() {
        super();
        this.id = "disconnect-failure-test";
        this.connected = true;
        this.injectFailure = true;
    }

    once(event, listener) {
        const result = super.once(event, listener);

        if (this.injectFailure && event === "disconnect") {
            throw new Error("INJECTED_DISCONNECT_REGISTRATION_FAILURE");
        }

        return result;
    }
}

const socket = new TestSocket();

const unrelatedListener = () => {};
socket.on("disconnect", unrelatedListener);

const receiver = new ServerProtocolReceiver(socket);

assert.throws(
    () => receiver.start(),
    /INJECTED_DISCONNECT_REGISTRATION_FAILURE/
);

assert.equal(
    socket.listenerCount(EVENTS.CHUNK),
    0,
    "Chunk listener leaked"
);

assert.equal(
    socket.listenerCount(EVENTS.COMPLETE),
    0,
    "Complete listener leaked"
);

assert.deepEqual(
    socket.listeners("disconnect"),
    [unrelatedListener],
    "Disconnect cleanup removed unrelated listener or leaked receiver listener"
);

console.log("PASS: Failed start cleaned up all receiver listeners");
console.log("PASS: Unrelated disconnect listener preserved");

socket.injectFailure = false;

const retryReceiver = new ServerProtocolReceiver(socket);
retryReceiver.start();

assert.equal(socket.listenerCount(EVENTS.CHUNK), 1);
assert.equal(socket.listenerCount(EVENTS.COMPLETE), 1);
assert.equal(socket.listenerCount("disconnect"), 2);

console.log("PASS: Retry registered exactly three receiver listeners");
console.log("\nPASS: DISCONNECT REGISTRATION FAILURE SAFETY");
