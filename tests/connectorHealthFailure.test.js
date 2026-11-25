
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
        0,
        "BUG: Existing registration incorrectly rejected"
    );

    console.log(
        "PASS: No false rejection of existing registration"
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

      // A failed repeat-registration attempt must not
    // destroy a previously established local mapping.
    assert.equal(
        registry.get(connectorId),
        socket,
        "BUG: Existing connector mapping removed"
    );

    assert.equal(
        socket.connectorIds.has(connectorId),
        true,
        "BUG: Existing connector alias removed"
    );

    assert.equal(
        socket.companyRegistrations.size,
        1,
        "BUG: Existing company context removed"
    );

    console.log(
        "PASS: Existing registration preserved"
    );
    assert.equal(
        socket.directRegistrationInProgress,
        false,
        "BUG: Registration in-progress flag stuck"
    );

        console.log(
        "PASS: Registration in-progress flag cleared"
    );

    // Verify rollback of a newly acquired registration.
    const newConnectorId = "TEST-HEALTH-FAILURE-002";
    const previousHealthCalls = healthCalls;

    events.length = 0;

    try {
        assert.equal(
            registry.get(newConnectorId),
            null
        );

        registerHandler({
            connector_id: newConnectorId,
            company_code: "TEST-COMPANY-NEW",
            company_guid: "TEST-GUID-NEW",
            tally_owner: "USER"
        });

        await socket.registrationQueue.tail;

        assert.ok(
            healthCalls > previousHealthCalls,
            "New registration health failure was not reached"
        );

        const newFailures = events.filter(item =>
            item.event === "connector:registration:status" &&
            item.payload?.connector_id === newConnectorId &&
            item.payload?.registered === false &&
            item.payload?.reason === "HEALTH_UPDATE_UNVERIFIED"
        );

        assert.equal(
            newFailures.length,
            1,
            "Expected new registration rejection"
        );

        assert.equal(
            events.some(item =>
                item.event === "connector:registration:status" &&
                item.payload?.connector_id === newConnectorId &&
                item.payload?.registered === true
            ),
            false,
            "BUG: Unverified new registration succeeded"
        );

        assert.equal(
            registry.get(newConnectorId),
            null,
            "BUG: Unverified new connector remains registered"
        );

        assert.equal(
            socket.connectorIds.has(newConnectorId),
            false,
            "BUG: Unverified new alias remains registered"
        );

        assert.equal(
            registry.get(connectorId),
            socket,
            "BUG: Existing connector mapping damaged"
        );

        assert.equal(
            socket.companyRegistrations.size,
            1,
            "BUG: Existing company context damaged"
        );

        assert.equal(
            socket.directRegistrationInProgress,
            false,
            "BUG: Registration flag stuck"
        );

        console.log(
            "PASS: New unverified registration safely revoked"
        );
       } finally {
        registry.remove(newConnectorId, socket);
    }
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
