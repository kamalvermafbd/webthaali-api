
"use strict";

const assert = require("node:assert/strict");
const Module = require("node:module");
const path = require("node:path");

const registry = require(
    path.join(__dirname, "..", "socketio", "connectorRegistry.js")
);

const originalLoad = Module._load;

const ALIAS_A = "TEST-PROTOCOL-A";
const ALIAS_B = "TEST-PROTOCOL-B";
const ALIAS_C = "TEST-PROTOCOL-C";

const events = [];
const handlers = new Map();

let startCalls = 0;
let healthCalls = 0;

const socket = {
    id: "test-protocol-socket",
    connected: true,
    connectorId: ALIAS_A,
    companyCode: "COMPANY-A",
    companyGuid: "GUID-A",
    companyGuids: ["GUID-A", "GUID-C"],

    on(event, handler) {
        handlers.set(event, handler);
    },

    emit(event, payload) {
        events.push({ event, payload });
    }
};

const healthStore = {
    inspectConnectorRecoveryState: async () => ({
        quarantined: false
    }),

    claimConnectorSocketOwnership: async () => ({
        claimed: true
    }),

    updateConnectorHealthIfSocketOwner: async () => {
        healthCalls++;
        return { updated: true };
    },

    renewConnectorSocketLease: async () => ({
        renewed: true
    }),

    getConnectorWatchdogStatus: async () => ({
        available: false
    }),

    updateConnectorTallyQueueStatus: async () => ({
        updated: true
    })
};

class FailingProtocolReceiver {
    constructor(socket) {
        this.socket = socket;
    }

    start() {
        startCalls++;
        throw new Error("INJECTED_PROTOCOL_START_FAILURE");
    }

    cancelCollection() {}
}

function fakeSupabase() {
    return {
        from() {
            return {
                select() {
                    return {
                        eq() {
                            return Promise.resolve({
                                data: [],
                                error: null
                            });
                        }
                    };
                }
            };
        }
    };
}

function addExistingAlias(connectorId, companyCode, guid) {
    assert.equal(
        registry.register(connectorId, socket),
        true
    );

    socket.availableCompanyGuids.add(guid);

    registry.commitCompanyRegistrations(socket, [{
        company_code: companyCode,
        tally_owner: "USER",
        tally_company_guid: guid,
        connector_id: connectorId
    }]);
}

function hasContext(connectorId) {
    return [...socket.companyRegistrations.values()]
        .some(item => item.connector_id === connectorId);
}

function assertAliasAlive(connectorId) {
    assert.equal(
        registry.get(connectorId),
        socket,
        `${connectorId} mapping lost`
    );

    assert.equal(
        socket.connectorIds.has(connectorId),
        true,
        `${connectorId} missing from connectorIds`
    );

    assert.equal(
        hasContext(connectorId),
        true,
        `${connectorId} company context lost`
    );
}

async function register(connectorId, companyCode, guid) {
    const handler = handlers.get("register");

    assert.equal(typeof handler, "function");

    const previousCount = events.length;

    handler({
        connector_id: connectorId,
        company_code: companyCode,
        company_guid: guid,
        tally_owner: "USER"
    });

    await socket.registrationQueue.tail;

    return events.slice(previousCount);
}

function assertErrorStatus(result, connectorId) {
    assert.ok(
        result.some(item =>
            item.event === "connector:registration:status" &&
            item.payload?.connector_id === connectorId &&
            item.payload?.registered === false &&
                        item.payload?.reason === "PROTOCOL_ACTIVATION_FAILED"
        ),
        `Expected PROTOCOL_ACTIVATION_FAILED for ${connectorId}`
    );

    assert.equal(
        result.some(item =>
            item.event === "connector:registration:status" &&
            item.payload?.connector_id === connectorId &&
            item.payload?.registered === true
        ),
        false,
        `False success for ${connectorId}`
    );
}

async function main() {
    registry.initializeRegistrationState(socket);

    // Two existing companies on the same physical socket.
    addExistingAlias(ALIAS_A, "COMPANY-A", "GUID-A");
    addExistingAlias(ALIAS_C, "COMPANY-C", "GUID-C");

    assertAliasAlive(ALIAS_A);
    assertAliasAlive(ALIAS_C);

    try {
        Module._load = function(request, parent, isMain) {
            if (request === "@supabase/supabase-js") {
                return { createClient: fakeSupabase };
            }

            if (request === "./connectorHealthStore") {
                return healthStore;
            }

            if (request === "./connectorHealth") {
                return {
                    publishConnectorHealth: async () => {},
                    updateConnectorHealth: async () => {}
                };
            }

            if (
                request ===
                "../utils/protocol/ServerProtocolReceiver"
            ) {
                return FailingProtocolReceiver;
            }

            if (request === "../utils/masterFinalAckReceipts") {
                return { install() {} };
            }

            if (request === "../utils/syncProgress") {
                return { updateSyncProgress: async () => {} };
            }

            if (
                request ===
                "../utils/getMastersProgressAuthorization"
            ) {
                return {
                    verifyGetMastersProgress: () => null,
                    revokeGetMastersProgressForSocket() {}
                };
            }

            if (
                request ===
                "../workers/secondQueueExistingBatch"
            ) {
                return {
                    dispatchRecoveredWaitingBatch: async () => {}
                };
            }

            return originalLoad.apply(this, arguments);
        };

        const eventsModule = require(
            path.join(
                __dirname,
                "..",
                "socketio",
                "secondConnectorEvents.js"
            )
        );

        eventsModule.registerEvents({
            on(event, callback) {
                if (event === "connection") {
                    callback(socket);
                }
            }
        });
    } finally {
        Module._load = originalLoad;
    }

    // CASE 1:
    // Existing alias A attempts registration again.
    // Protocol start throws.
    const firstResult = await register(
        ALIAS_A,
        "COMPANY-A",
        "GUID-A"
    );

    assert.equal(startCalls, 1);
    assert.ok(healthCalls >= 1);

    assertErrorStatus(firstResult, ALIAS_A);
    assertAliasAlive(ALIAS_A);
    assertAliasAlive(ALIAS_C);

    assert.equal(
        socket.directRegistrationInProgress,
        false
    );

    console.log(
        "PASS: Existing alias survives protocol exception"
    );

    console.log(
        "PASS: Other company survives protocol exception"
    );

    // start() threw after protocolReceiver was assigned.
    // Reset only the mock receiver to inject a second failure.
    //socket.protocolReceiver = null;

    // CASE 2:
    // New alias B is acquired, then protocol start throws.
    const secondResult = await register(
        ALIAS_B,
        "COMPANY-B",
        "GUID-B"
    );

    assert.equal(startCalls, 2);

    assertErrorStatus(secondResult, ALIAS_B);

    assert.equal(
        registry.get(ALIAS_B),
        null,
        "BUG: Newly acquired alias B was not revoked"
    );

    assert.equal(
        socket.connectorIds.has(ALIAS_B),
        false
    );

    assert.equal(
        hasContext(ALIAS_B),
        false
    );

    console.log(
        "PASS: New alias revoked after protocol exception"
    );

    assertAliasAlive(ALIAS_A);
    assertAliasAlive(ALIAS_C);

    console.log(
        "PASS: Existing companies A and C remain registered"
    );

    assert.equal(
        socket.directRegistrationInProgress,
        false
    );

    console.log(
        "PASS: Registration in-progress flag cleared"
    );

    console.log(
        "\nPASS: PROTOCOL ACTIVATION EXCEPTION SAFETY"
    );
}

main()
    .catch(error => {
        console.error(
            "\nFAIL: PROTOCOL ACTIVATION EXCEPTION SAFETY"
        );
        console.error(error.stack || error.message);
        process.exitCode = 1;
    })
    .finally(() => {
        for (const connectorId of [ALIAS_A, ALIAS_B, ALIAS_C]) {
            registry.remove(connectorId, socket);
        }

        registry.remove(null, socket);
    });
