
"use strict";

const assert = require("node:assert/strict");
const Module = require("node:module");
const path = require("node:path");

const registry = require(
    path.join(__dirname, "..", "socketio", "connectorRegistry.js")
);

const originalLoad = Module._load;
const connectorId = "TEST-HEALTH-FAILURE-001";

const events = [];
let registerHandler;
let healthCalls = 0;

const socket = {
    id: "test-health-socket",
    connected: true,
    connectorId: connectorId,
    companyCode: "TEST-COMPANY",
    companyGuid: "TEST-GUID",
    companyGuids: ["TEST-GUID"],

    on(event, callback) {
        if (event === "register") {
            registerHandler = callback;
        }
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

        // Simulate Redis health verification failure.
        return { updated: false };
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

async function main() {
    registry.initializeRegistrationState(socket);

    // Existing valid local registration.
    assert.equal(
        registry.register(connectorId, socket),
        true
    );

    socket.availableCompanyGuids.add("TEST-GUID");

    registry.commitCompanyRegistrations(socket, [{
        company_code: "TEST-COMPANY",
        tally_owner: "USER",
        tally_company_guid: "TEST-GUID",
        connector_id: connectorId
    }]);

    assert.equal(socket.companyRegistrations.size, 1);

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
                return class FakeProtocolReceiver {
                    start() {}
                    cancelCollection() {}
                };
            }

            if (request === "../utils/masterFinalAckReceipts") {
                return { install() {} };
            }

            if (request === "../utils/syncProgress") {
                return {
                    updateSyncProgress: async () => {}
                };
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

        const io = {
            on(event, callback) {
                if (event === "connection") {
                    callback(socket);
                }
            }
        };

        eventsModule.registerEvents(io);
    } finally {
        Module._load = originalLoad;
    }

    assert.equal(
        typeof registerHandler,
        "function",
        "Register handler not installed"
    );

    // Invoke the actual direct-registration handler.
    registerHandler({
        connector_id: connectorId,
        company_code: "TEST-COMPANY",
        company_guid: "TEST-GUID",
        tally_owner: "USER"
    });

    await socket.registrationQueue.tail;

    assert.ok(
        healthCalls > 0,
        "Health failure injection was not reached"
    );

    console.log(
        "PASS: Health verification failure injected"
    );

    const failures = events.filter(item =>
        item.event === "connector:registration:status" &&
        item.payload?.connector_id === connectorId &&
        item.payload?.registered === false &&
        item.payload?.reason === "HEALTH_UPDATE_UNVERIFIED"
    );

    assert.equal(
        failures.length,
        1,
        "Expected HEALTH_UPDATE_UNVERIFIED rejection"
    );

    console.log(
        "PASS: HEALTH_UPDATE_UNVERIFIED emitted"
    );

    const successEvents = events.filter(item =>
        item.event === "connector:registration:status" &&
        item.payload?.connector_id === connectorId &&
        item.payload?.registered === true
    );

    assert.equal(
        successEvents.length,
        0,
        "BUG: Success emitted despite unverified health"
    );

    console.log(
        "PASS: No false registration success"
    );

    // Fail-closed safety: unverified ownership must
    // not remain usable through the local registry.
    assert.equal(
        registry.get(connectorId),
        null,
        "BUG: Unverified connector remains available"
    );

    assert.equal(
        socket.connectorIds.has(connectorId),
        false,
        "BUG: Unverified alias remains registered"
    );

    assert.equal(
        socket.companyRegistrations.size,
        0,
        "BUG: Unverified company context remains active"
    );

    console.log(
        "PASS: Unverified connector removed locally"
    );

    assert.equal(
        socket.directRegistrationInProgress,
        false,
        "BUG: Registration in-progress flag stuck"
    );

    console.log(
        "PASS: Registration in-progress flag cleared"
    );
}

main()
    .then(() => {
        console.log("\nPASS: HEALTH FAILURE SAFETY");
    })
    .catch(error => {
        console.error("\nFAIL: HEALTH FAILURE SAFETY");
        console.error(error.stack || error.message);
        process.exitCode = 1;
    })
    .finally(() => {
        registry.remove(connectorId, socket);
        registry.remove(null, socket);
    });
