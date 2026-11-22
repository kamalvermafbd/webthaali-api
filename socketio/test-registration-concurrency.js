
"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = __dirname;
const eventsSource = fs.readFileSync(
    path.join(root, "secondConnectorEvents.js"), "utf8"
);
const registrySource = fs.readFileSync(
    path.join(root, "connectorRegistry.js"), "utf8"
);

function deferred() {
    let resolve, reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}

function createHarness(options = {}) {
    const handlers = new Map();
    const emitted = [];
    const calls = { health: [], claim: [], queue: [] };
    const connectorId =
        options.connectorId || "registration-test-connector";
    const companyGuid = "registration-test-guid";
    const companyCode = "registration-test-company";

    const socket = {
        id: "registration-test-socket",
        connected: true,
        tallyStatus: "OFFLINE",
        on(event, handler) {
            handlers.set(event, handler);
        },
        emit(event, payload) {
            emitted.push({ event, payload });
        }
    };

    const quiet = {
        log() {},
        warn() {},
        error() {}
    };

    const registryContext = {
        module: { exports: {} },
        exports: {},
        require,
        console: quiet,
        structuredClone,
        Set, Map, Promise, JSON, Error, String, Array
    };

    vm.runInNewContext(
        registrySource,
        registryContext,
        { filename: "connectorRegistry.js" }
    );

    const registry = registryContext.module.exports;

    const supabase = {
        from(table) {
            assert.equal(table, "company");
            const query = {
                select() { return query; },
                in(field) {
                    return Promise.resolve({
                        data: field ===
                            "client_tally_company_guid"
                            ? [{
                                company_code: companyCode,
                                client_tally_company_guid: companyGuid,
                                client_connector_id: connectorId
                            }]
                            : [],
                        error: null
                    });
                },
                eq(field) {
                    return Promise.resolve({
                        data: field === "client_connector_id"
                            ? [{
                                company_code: companyCode,
                                client_connector_id: connectorId,
                                client_tally_company_guid: companyGuid
                            }]
                            : [],
                        error: null
                    });
                }
            };
            return query;
        }
    };

    let receiverStarts = 0;

    class Receiver {
        constructor(owner) {
            this.socket = owner;
        }
        start() {
            receiverStarts++;
            if (options.protocolError) {
                throw new Error("MOCK_PROTOCOL_FAILURE");
            }
        }
        cancelCollection() {}
    }

    const healthStore = {
        async inspectConnectorRecoveryState() {
            return { quarantined: false };
        },
        async claimConnectorSocketOwnership(id, socketId) {
            calls.claim.push({ id, socketId });
            return { claimed: true };
        },
        async updateConnectorHealthIfSocketOwner(
            id, socketId, patch
        ) {
            calls.health.push({ id, socketId, patch });

            if (
                options.healthGate &&
                patch.connection_status === "CONNECTED"
            ) {
                await options.healthGate.promise;
            }

            if (
                options.healthError &&
                patch.connection_status === "CONNECTED"
            ) {
                throw options.healthError;
            }

            return {
                updated:
                    typeof options.healthUpdated === "function"
                        ? options.healthUpdated()
                        : options.healthUpdated !== false
            };
        },
        async updateConnectorTallyQueueStatus(...args) {
    calls.queue.push(args);

    if (options.queueGate) {
        await options.queueGate.promise;
    }

    return { updated: true };
},
        async renewConnectorSocketLease() {
            return { renewed: true };
        },
        async getConnectorWatchdogStatus() {
            return { available: false };
        }
    };

    const mocks = {
        "./connectorRegistry": registry,
        "./connectorHealth": {
            publishConnectorHealth() {},
            updateConnectorHealth() {}
        },
        "./connectorHealthStore": healthStore,
        "@supabase/supabase-js": {
            createClient: () => supabase
        },
        "../utils/protocol/ServerProtocolReceiver": Receiver,
        "../utils/masterFinalAckReceipts": {
            install() {}
        },
        "../utils/syncProgress": {
            updateSyncProgress() {}
        },
        "../utils/getMastersProgressAuthorization": {
            verifyGetMastersProgress() {
                return null;
            },
            revokeGetMastersProgressForSocket() {}
        },
        "../workers/secondQueueExistingBatch": {
            dispatchRecoveredWaitingBatch() {}
        }
    };

    const context = {
        module: { exports: {} },
        exports: {},
        require(name) {
            if (Object.hasOwn(mocks, name)) {
                return mocks[name];
            }
            if (name === "crypto") {
                return require("node:crypto");
            }
            throw new Error(`Unexpected dependency: ${name}`);
        },
        console: quiet,
        process: {
            env: {
                SUPABASE_URL: "mock",
                SUPABASE_SERVICE_KEY: "mock"
            }
        },
        Date, Set, Map, Promise, String, Array,
        JSON, Error, Boolean, Number
    };

    vm.runInNewContext(
        eventsSource,
        context,
        { filename: "secondConnectorEvents.js" }
    );

    context.module.exports.registerEvents({
        on(event, cb) {
            assert.equal(event, "connection");
            cb(socket);
        }
    });

    async function dispatch(event, data) {
        const handler = handlers.get(event);
        assert.equal(
            typeof handler,
            "function",
            `Missing ${event} handler`
        );
        handler(data);
        await socket.registrationQueue.tail;
    }

    function statuses() {
        return emitted.filter(
            x => x.event === "connector:registration:status"
        );
    }

    function directData() {
        return {
            connector_id: connectorId,
            company_code: companyCode,
            company_guid: companyGuid
        };
    }

    return {
        socket,
        registry,
        emitted,
        calls,
        statuses,
        dispatch,
        directData,
        connectorId,
        companyCode,
        companyGuid,
        get receiverStarts() {
            return receiverStarts;
        }
    };
}

test("1. Direct registration succeeds", async () => {
    const h = createHarness();
    await h.dispatch("register", h.directData());

    assert.equal(h.registry.get(h.connectorId), h.socket);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        1
    );
    assert.equal(h.socket.companyCode, h.companyCode);
    assert.equal(h.socket.companyGuid, h.companyGuid);
    assert.equal(h.receiverStarts, 1);
    assert.ok(
        h.statuses().some(x => x.payload.registered === true)
    );
});

test("2. Direct health rejection removes new alias", async () => {
    const h = createHarness({ healthUpdated: false });
    await h.dispatch("register", h.directData());

    assert.equal(h.registry.get(h.connectorId), null);
    assert.equal(h.socket.connectorId, undefined);
    assert.ok(
        h.statuses().some(
            x => x.payload.reason === "HEALTH_UPDATE_UNVERIFIED"
        )
    );
});

test("3. Direct health exception removes new alias", async () => {
    const h = createHarness({
        healthError: new Error("MOCK_HEALTH_FAILURE")
    });
    await h.dispatch("register", h.directData());

    assert.equal(h.registry.get(h.connectorId), null);
    assert.ok(
        h.statuses().some(
            x => x.payload.reason === "HEALTH_UPDATE_UNVERIFIED"
        )
    );
});

test("4. Failed repeat preserves existing alias", async () => {
    let acceptHealth = true;
    const h = createHarness({
        healthUpdated: () => acceptHealth
    });

    await h.dispatch("register", h.directData());

    const first =
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        );
    const oldStatusCount = h.statuses().length;

    assert.equal(first, 1);

    acceptHealth = false;
    await h.dispatch("register", h.directData());

    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );
    assert.equal(h.registry.get(h.connectorId), h.socket);
    assert.equal(h.statuses().length, oldStatusCount);
});

test("5. Direct protocol failure removes new alias", async () => {
    const h = createHarness({ protocolError: true });
    await h.dispatch("register", h.directData());

    assert.equal(h.registry.get(h.connectorId), null);
    assert.ok(
        h.statuses().some(
            x => x.payload.reason ===
                "PROTOCOL_ACTIVATION_FAILED"
        )
    );
});

test("6. Auto-identification registers connector", async () => {
    const h = createHarness();

    await h.dispatch("identifyConnector", {
        company_guids: [h.companyGuid]
    });

    assert.equal(h.registry.get(h.connectorId), h.socket);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        1
    );
    assert.ok(
        h.statuses().some(x => x.payload.registered === true)
    );
});

test("7. Auto health rejection revokes alias", async () => {
    const h = createHarness({ healthUpdated: false });

    await h.dispatch("identifyConnector", {
        company_guids: [h.companyGuid]
    });

    assert.equal(h.registry.get(h.connectorId), null);
    assert.ok(
        h.statuses().some(
            x => x.payload.reason === "HEALTH_UPDATE_UNVERIFIED"
        )
    );
});

test("8. Auto protocol failure revokes alias", async () => {
    const h = createHarness({ protocolError: true });

    await h.dispatch("identifyConnector", {
        company_guids: [h.companyGuid]
    });

    assert.equal(h.registry.get(h.connectorId), null);
    assert.ok(
        h.statuses().some(
            x => x.payload.reason ===
                "PROTOCOL_ACTIVATION_FAILED"
        )
    );
});

test("9. Same-socket registrations are serialized", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });
    const register = h.directData();

    const first = h.dispatch("register", register);
    const second = h.dispatch("register", register);

    await new Promise(resolve => setImmediate(resolve));

    assert.equal(h.calls.claim.length, 1);

    gate.resolve();
    await Promise.all([first, second]);

    assert.equal(h.calls.claim.length, 2);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );
    assert.equal(h.registry.get(h.connectorId), h.socket);
});

test("10. Direct and auto share registration queue", async () => {
    const gate = deferred();
    const h = createHarness({ healthGate: gate });

    const first = h.dispatch(
        "register", h.directData()
    );
    const second = h.dispatch(
        "identifyConnector",
        { company_guids: [h.companyGuid] }
    );

    await new Promise(resolve => setImmediate(resolve));

    assert.equal(h.calls.claim.length, 1);

    gate.resolve();
    await Promise.all([first, second]);

    assert.equal(h.calls.claim.length, 2);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );
    assert.equal(h.registry.get(h.connectorId), h.socket);
});

test("11. Stale direct health failure preserves new generation", async () => {
    const gate = deferred();
    const h = createHarness({
        healthGate: gate,
        healthUpdated: false
    });

    const first = h.dispatch(
        "register", h.directData()
    );

    await new Promise(resolve => setImmediate(resolve));

    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        1
    );

    assert.equal(
        h.registry.register(h.connectorId, h.socket),
        true
    );

    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );

    gate.resolve();
    await first;

    assert.equal(h.registry.get(h.connectorId), h.socket);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );
    assert.ok(
        !h.statuses().some(
            x => x.payload.registered === false &&
                x.payload.reason ===
                    "HEALTH_UPDATE_UNVERIFIED"
        )
    );
});

test("12. Stale auto health failure preserves new generation", async () => {
    const gate = deferred();
    const h = createHarness({
        healthGate: gate,
        healthUpdated: false
    });

    const first = h.dispatch(
        "identifyConnector",
        { company_guids: [h.companyGuid] }
    );

    await new Promise(resolve => setImmediate(resolve));

    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        1
    );

    assert.equal(
        h.registry.register(h.connectorId, h.socket),
        true
    );

    gate.resolve();
    await first;

    assert.equal(h.registry.get(h.connectorId), h.socket);
    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId, h.socket
        ),
        2
    );
    assert.ok(
        !h.statuses().some(
            x => x.payload.registered === false &&
                x.payload.reason ===
                    "HEALTH_UPDATE_UNVERIFIED"
        )
    );
});
test("13. Stale auto queue update cannot emit registration success", async () => {
    const gate = deferred();
    const h = createHarness({ queueGate: gate });

    const registration = h.dispatch("identifyConnector", {
        company_guids: [h.companyGuid],
        tally_queue_status: {
            idle: true,
            pendingOperations: 0,
            runningOperations: 0,
            operationGeneration: 1
        }
    });

    for (let i = 0; i < 100 && h.calls.queue.length === 0; i++) {
        await new Promise(resolve => setImmediate(resolve));
    }

    assert.equal(h.calls.queue.length, 1);

    // Simulate a newer registration while the old
    // queue-status persistence is still pending.
    assert.equal(
        h.registry.register(h.connectorId, h.socket),
        true
    );

    gate.resolve();
    await registration;

    assert.equal(
        h.registry.getRegistrationGeneration(
            h.connectorId,
            h.socket
        ),
        2
    );

    assert.ok(
        !h.statuses().some(
            x => x.payload.registered === true
        ),
        "Stale registration must not emit success"
    );
});