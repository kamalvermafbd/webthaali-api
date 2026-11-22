
"use strict";

const assert = require("node:assert/strict");
const Module = require("node:module");
const path = require("node:path");

const registry = require(
    path.join(__dirname, "..", "socketio", "connectorRegistry.js")
);

const originalLoad = Module._load;
const connectorId = "TEST-REPEAT-FAILURE-001";

let handler;
let injected = false;

const events = [];

const socket = {
    id: "test-repeat-socket",
    connected: true,
    connectorId: connectorId,
    companyCode: "TEST-COMPANY",
    companyGuid: "TEST-GUID",
    companyGuids: ["TEST-GUID"],

    on(event, callback) {
        if (event === "register") {
            handler = callback;
        }
    },

    emit(event, payload) {
        events.push({ event, payload });

        // Inject a one-time unexpected exception AFTER
        // registry.register() has already succeeded.
        if (
            !injected &&
            event === "connector:registration:status" &&
            payload?.connector_id === connectorId &&
            payload?.registered === true &&
            !payload?.company_code
        ) {
            injected = true;
            throw new Error("INJECTED_SUCCESS_EMIT_FAILURE");
        }
    }
};

const healthStore = {
    inspectConnectorRecoveryState: async () => ({
        quarantined: false
    }),

    claimConnectorSocketOwnership: async () => ({
        claimed: true
    }),

    updateConnectorHealthIfSocketOwner: async () => ({
        updated: true
    }),

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

class FakeProtocolReceiver {
    constructor() {}
    start() {}
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

async function main() {
    registry.initializeRegistrationState(socket);

    // Establish a valid registration BEFORE the failure.
    assert.equal(registry.register(connectorId, socket), true);

    socket.availableCompanyGuids.add("TEST-GUID");

    registry.commitCompanyRegistrations(socket, [{
        company_code: "TEST-COMPANY",
        tally_owner: "USER",
        tally_company_guid: "TEST-GUID",
        connector_id: connectorId
    }]);

    assert.equal(socket.companyRegistrations.size, 1);

    try {
        // Replace only external dependencies during module load.
        // No real Redis, database or network calls.
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
                return FakeProtocolReceiver;
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
        typeof handler,
        "function",
        "Direct register handler was not installed"
    );

    // Run the actual registered Socket.IO handler.
    handler({
        connector_id: connectorId,
        company_code: "TEST-COMPANY",
        company_guid: "TEST-GUID",
        tally_owner: "USER"
    });

    // registrationHandler uses an asynchronous queue.
    await socket.registrationQueue.tail;

    assert.equal(
        injected,
        true,
        "Failure injection was not reached"
    );

    assert.ok(
        events.some(item =>
            item.event === "connector:registration:status" &&
            item.payload?.reason === "REGISTRATION_ERROR"
        ),
        "Expected registration error event"
    );

    console.log("PASS: Actual handler exception triggered");

    // These are the required safety invariants.
    assert.equal(
        registry.get(connectorId),
        socket,
        "BUG: Existing connector mapping was deleted"
    );

    assert.equal(
        socket.connectorIds.has(connectorId),
        true,
        "BUG: Existing connector alias was deleted"
    );

    assert.equal(
        socket.companyRegistrations.size,
        1,
        "BUG: Existing company context was deleted"
    );

    console.log(
        "PASS: Existing mapping and context survived"
    );
}

main()
    .then(() => {
        console.log("\nPASS: REPEAT FAILURE SAFETY");
    })
    .catch(error => {
        console.error("\nFAIL: REPEAT FAILURE SAFETY");
        console.error(error.message);
        process.exitCode = 1;
    })
    .finally(() => {
        registry.remove(connectorId, socket);
        registry.remove(null, socket);
    });
