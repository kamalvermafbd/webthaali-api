
"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const source = fs.readFileSync(
    path.join(__dirname, "secondConnectorEvents.js"),
    "utf8"
);

const startMarker = "let heartbeatProcessing = false;";
const endMarker = 'socket.on("tally:queue:status"';

const start = source.indexOf(startMarker);
const end = source.indexOf(endMarker, start);

assert.ok(start >= 0, "Heartbeat guard not found");
assert.ok(end > start, "Heartbeat end not found");

const section = source.slice(start, end);
const handlerEnd = section.lastIndexOf("});");

assert.ok(
    handlerEnd >= 0,
    "Heartbeat handler closing not found"
);

const heartbeatCode = section.slice(
    0,
    handlerEnd + 3
);

function deferred() {
    let resolve;
    let reject;

    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });

    return { promise, resolve, reject };
}

function flush() {
    return new Promise(resolve => setImmediate(resolve));
}

function createHarness(options = {}) {
    const connectorId = "test-connector";
    const handlers = {};
    const emitted = [];
    const logs = [];
    const healthCalls = [];
    const leaseCalls = [];
    const removals = [];

    let registered = true;
    let registrationGeneration = 1;

    const socket = {
        id: "test-socket",
        connected: true,
        connectorId,
        connectorIds: new Set([connectorId]),
        tallyStatus: "OFFLINE",
        tallyRequestActive: false,
        companyGuids: [],

        on(event, handler) {
            handlers[event] = handler;
        },

        emit(event, payload) {
            emitted.push({ event, payload });
        }
    };

    function reRegister() {
        registered = false;
        socket.connectorIds.delete(connectorId);

        registrationGeneration += 1;

        registered = true;
        socket.connectorIds.add(connectorId);
    }

    function unregister() {
        registered = false;
        socket.connectorIds.delete(connectorId);
    }

    const registry = {
        get(id) {
            return registered && id === connectorId
                ? socket
                : null;
        },

        getRegistrationGeneration(id, owner) {
            if (
                !socket.connected ||
                !registered ||
                id !== connectorId ||
                owner !== socket ||
                !socket.connectorIds.has(id)
            ) {
                return null;
            }

            return registrationGeneration;
        },

        remove(id, owner) {
            removals.push({ id, owner });

            if (
                id === connectorId &&
                owner === socket &&
                registered
            ) {
                unregister();
            }
        }
    };

    const context = {
        socket,
        registry,
        Set,
        Date,

        console: {
            warn(...args) {
                logs.push(["warn", ...args]);
            },

            error(...args) {
                logs.push(["error", ...args]);
            }
        },

        async updateOwnedSocketHealth(patch) {
            healthCalls.push(patch);

            if (options.healthGate) {
                await options.healthGate.promise;
            }

            if (options.healthError) {
                throw options.healthError;
            }
        },

        async renewConnectorSocketLease(id, socketId) {
            leaseCalls.push({ id, socketId });

            if (options.leaseGate) {
                await options.leaseGate.promise;
            }

            if (options.leaseError) {
                throw options.leaseError;
            }

            return {
                renewed: options.renewed !== false
            };
        },

        async recoverWaitingBatchesForCompanies() {
            throw new Error(
                "Unexpected recovery invocation"
            );
        }
    };

    vm.runInNewContext(
        heartbeatCode,
        context,
        { filename: "isolated-heartbeat.js" }
    );

    assert.equal(
        typeof handlers["protocol:heartbeat"],
        "function",
        "Heartbeat handler not registered"
    );

    return {
        socket,
        emitted,
        logs,
        healthCalls,
        leaseCalls,
        removals,
        heartbeat: handlers["protocol:heartbeat"],
        isRegistered: () => registered,
        getRegistrationGeneration: () =>
            registrationGeneration,
        reRegister,
        unregister
    };
}

function registrationEvents(h) {
    return h.emitted.filter(event =>
        event.event === "connector:registration:status"
    );
}

function assertNoRevocation(h) {
    assert.equal(
        h.isRegistered(),
        true,
        "Current registration was revoked"
    );

    assert.equal(
        h.removals.length,
        0,
        "Unexpected registry.remove call"
    );

    assert.equal(
        registrationEvents(h).length,
        0,
        "Unexpected registration status notification"
    );
}

// ============================================================
// EXISTING HEARTBEAT TESTS
// ============================================================

test("1. Normal heartbeat renews lease", async () => {
    const h = createHarness();

    await h.heartbeat();

    assert.equal(h.healthCalls.length, 1);
    assert.equal(h.leaseCalls.length, 1);
    assert.equal(h.isRegistered(), true);
});

test("2. Overlapping heartbeat is skipped", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });

    const first = h.heartbeat();

    await flush();

    const second = h.heartbeat();
    await second;

    assert.equal(h.healthCalls.length, 1);

    gate.resolve();
    await first;

    assert.equal(h.leaseCalls.length, 1);
});

test("3. Lease rejection revokes registration", async () => {
    const h = createHarness({ renewed: false });

    await h.heartbeat();

    assert.equal(h.isRegistered(), false);

    assert.ok(
        registrationEvents(h).some(event =>
            event.payload.reason === "REDIS_LEASE_LOST"
        )
    );
});

test("4. Redis exception revokes registration", async () => {
    const h = createHarness({
        leaseError: new Error("MOCK_REDIS_FAILURE")
    });

    await h.heartbeat();

    assert.equal(h.isRegistered(), false);

    assert.ok(
        registrationEvents(h).some(event =>
            event.payload.reason ===
                "REDIS_LEASE_UNVERIFIED"
        )
    );
});

test("5. Heartbeat guard resets after completion", async () => {
    const h = createHarness();

    await h.heartbeat();
    await h.heartbeat();

    assert.equal(h.healthCalls.length, 2);
    assert.equal(h.leaseCalls.length, 2);
});

test("6. Disconnected socket cannot renew lease", async () => {
    const h = createHarness();

    h.socket.connected = false;

    await h.heartbeat();

    assert.equal(h.healthCalls.length, 0);
    assert.equal(h.leaseCalls.length, 0);
});

test("7. Guard resets after health exception", async () => {
    const h = createHarness({
        healthError: new Error("MOCK_HEALTH_FAILURE")
    });

    await assert.rejects(
        h.heartbeat(),
        /MOCK_HEALTH_FAILURE/
    );

    await assert.rejects(
        h.heartbeat(),
        /MOCK_HEALTH_FAILURE/
    );

    assert.equal(h.healthCalls.length, 2);
});

test("8. Old lease rejection cannot revoke re-registration", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        renewed: false
    });

    const oldHeartbeat = h.heartbeat();

    await flush();

    assert.equal(h.leaseCalls.length, 1);

    const oldGeneration =
        h.getRegistrationGeneration();

    h.reRegister();

    assert.equal(
        h.getRegistrationGeneration(),
        oldGeneration + 1
    );

    gate.resolve();
    await oldHeartbeat;

    assertNoRevocation(h);
});

test("9. Old Redis exception cannot revoke re-registration", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        leaseError: new Error("MOCK_OLD_REDIS_FAILURE")
    });

    const oldHeartbeat = h.heartbeat();

    await flush();

    assert.equal(h.leaseCalls.length, 1);

    const oldGeneration =
        h.getRegistrationGeneration();

    h.reRegister();

    assert.equal(
        h.getRegistrationGeneration(),
        oldGeneration + 1
    );

    gate.resolve();
    await oldHeartbeat;

    assertNoRevocation(h);
});

// ============================================================
// ADDITIONAL REGRESSION TESTS
// ============================================================

test("10. Successful lease renewal preserves registration", async () => {
    const h = createHarness({ renewed: true });

    await h.heartbeat();

    assertNoRevocation(h);
});

test("11. Lease rejection emits exactly one failure", async () => {
    const h = createHarness({ renewed: false });

    await h.heartbeat();

    const events = registrationEvents(h);

    assert.equal(events.length, 1);
    assert.equal(events[0].payload.registered, false);
    assert.equal(
        events[0].payload.reason,
        "REDIS_LEASE_LOST"
    );
});

test("12. Redis exception emits exactly one failure", async () => {
    const h = createHarness({
        leaseError: new Error("REDIS_DOWN")
    });

    await h.heartbeat();

    const events = registrationEvents(h);

    assert.equal(events.length, 1);
    assert.equal(
        events[0].payload.reason,
        "REDIS_LEASE_UNVERIFIED"
    );
});

test("13. Unregistered connector cannot renew lease", async () => {
    const h = createHarness();

    h.unregister();

    await h.heartbeat();

    assert.equal(h.leaseCalls.length, 0);
    assert.equal(h.removals.length, 0);
});

test("14. Missing connector alias cannot renew lease", async () => {
    const h = createHarness();

    h.socket.connectorIds.delete("test-connector");

    await h.heartbeat();

    assert.equal(h.leaseCalls.length, 0);
    assert.equal(h.removals.length, 0);
});

test("15. Disconnection during health update skips lease", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });

    const heartbeat = h.heartbeat();

    await flush();

    h.socket.connected = false;

    gate.resolve();
    await heartbeat;

    assert.equal(h.leaseCalls.length, 0);
});

test("16. Unregistration during health update skips lease", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });

    const heartbeat = h.heartbeat();

    await flush();

    h.unregister();

    gate.resolve();
    await heartbeat;

    assert.equal(h.leaseCalls.length, 0);
});

test("17. Re-registration during health update uses current generation", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });

    const heartbeat = h.heartbeat();

    await flush();

    const oldGeneration =
        h.getRegistrationGeneration();

    h.reRegister();

    gate.resolve();
    await heartbeat;

    assert.equal(
        h.getRegistrationGeneration(),
        oldGeneration + 1
    );

    assert.equal(h.leaseCalls.length, 1);
    assertNoRevocation(h);
});

test("18. Old rejected lease cannot emit stale failure", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        renewed: false
    });

    const heartbeat = h.heartbeat();

    await flush();

    h.reRegister();

    gate.resolve();
    await heartbeat;

    assert.equal(registrationEvents(h).length, 0);
    assert.equal(h.removals.length, 0);
});

test("19. Old Redis exception cannot emit stale failure", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        leaseError: new Error("STALE_REDIS_ERROR")
    });

    const heartbeat = h.heartbeat();

    await flush();

    h.reRegister();

    gate.resolve();
    await heartbeat;

    assert.equal(registrationEvents(h).length, 0);
    assert.equal(h.removals.length, 0);
});

test("20. Multiple re-registrations survive stale rejection", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        renewed: false
    });

    const heartbeat = h.heartbeat();

    await flush();

    const initialGeneration =
        h.getRegistrationGeneration();

    h.reRegister();
    h.reRegister();
    h.reRegister();

    assert.equal(
        h.getRegistrationGeneration(),
        initialGeneration + 3
    );

    gate.resolve();
    await heartbeat;

    assertNoRevocation(h);
});

test("21. Multiple re-registrations survive stale exception", async () => {
    const gate = deferred();

    const h = createHarness({
        leaseGate: gate,
        leaseError: new Error("OLD_FAILURE")
    });

    const heartbeat = h.heartbeat();

    await flush();

    h.reRegister();
    h.reRegister();

    gate.resolve();
    await heartbeat;

    assertNoRevocation(h);
});

test("22. Heartbeat guard resets after rejected lease", async () => {
    const h = createHarness({ renewed: false });

    await h.heartbeat();

    h.reRegister();

    await h.heartbeat();

    assert.equal(h.healthCalls.length, 2);
    assert.equal(h.leaseCalls.length, 2);
});

test("23. Heartbeat guard resets after Redis lease exception", async () => {
    const h = createHarness({
        leaseError: new Error("REDIS_FAILURE")
    });

    await h.heartbeat();

    h.reRegister();

    await h.heartbeat();

    assert.equal(h.healthCalls.length, 2);
    assert.equal(h.leaseCalls.length, 2);
});
