
"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");

const registry = require(
    path.join(__dirname, "..", "socketio", "connectorRegistry.js")
);

let passed = 0;
let failed = 0;

async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`PASS: ${name}`);
    } catch (error) {
        failed++;
        console.error(`FAIL: ${name}`);
        console.error(error.stack || error.message);
    }
}

function createSocket(id) {
    const events = [];

    const socket = {
        id,
        connected: true,
        connectorId: null,
        companyCode: null,
        companyGuid: null,
        companyGuids: [],
        connectorIds: new Set(),
        companyRegistrations: new Map(),
        availableCompanyGuids: new Set(),
        emit(event, payload) {
            events.push({ event, payload });
        },
        events
    };

    registry.initializeRegistrationState(socket);
    return socket;
}

function cleanup(connectorId, socket) {
    registry.remove(connectorId, socket);
    registry.remove(null, socket);
}

async function main() {
    const connectorId = "TEST-REGRESSION-001";
    const socketA = createSocket("test-socket-A");
    const socketB = createSocket("test-socket-B");

    try {
        await test("First registration succeeds", () => {
            assert.equal(registry.register(connectorId, socketA), true);
            assert.equal(registry.get(connectorId), socketA);
            assert.equal(socketA.connectorIds.has(connectorId), true);
        });

        await test("Same-socket repeat registration succeeds", () => {
            assert.equal(registry.register(connectorId, socketA), true);
            assert.equal(registry.get(connectorId), socketA);
        });

        await test("Different active socket is rejected", () => {
            assert.equal(registry.register(connectorId, socketB), false);
            assert.equal(registry.get(connectorId), socketA);
            assert.equal(socketB.connectorIds.has(connectorId), false);
        });

        await test("Company context survives repeat registration", () => {
            const context = {
                company_code: "TEST-COMPANY",
                tally_owner: "USER",
                tally_company_guid: "TEST-GUID",
                connector_id: connectorId
            };

            socketA.availableCompanyGuids.add("TEST-GUID");

            registry.commitCompanyRegistrations(socketA, [context]);

            assert.equal(registry.register(connectorId, socketA), true);
            assert.equal(socketA.companyRegistrations.size, 1);
        });

        await test("Current cleanup removes existing same-socket mapping", () => {
            // Simulates the cleanup action currently present
            // in secondConnectorEvents.js unexpected-error catch.
            //
            // This verifies the registry contract, not the
            // complete Socket.IO registration handler.

            assert.equal(registry.get(connectorId), socketA);

            registry.remove(connectorId, socketA);

            assert.equal(registry.get(connectorId), null);
            assert.equal(socketA.connectorIds.has(connectorId), false);
            assert.equal(socketA.companyRegistrations.size, 0);
        });

        await test("Disconnected old socket can be replaced", () => {
            assert.equal(registry.register(connectorId, socketA), true);

            socketA.connected = false;

            assert.equal(registry.register(connectorId, socketB), true);
            assert.equal(registry.get(connectorId), socketB);
            assert.equal(socketA.connectorIds.has(connectorId), false);
        });

        await test("Old socket cannot delete replacement mapping", () => {
            registry.remove(connectorId, socketA);

            assert.equal(registry.get(connectorId), socketB);
            assert.equal(socketB.connectorIds.has(connectorId), true);
        });

    } finally {
        cleanup(connectorId, socketA);
        cleanup(connectorId, socketB);
    }

    console.log("\n==================================");
    console.log("CONNECTOR REGISTRATION TEST SUMMARY");
    console.log("PASS:", passed);
    console.log("FAIL:", failed);
    console.log("==================================");

    if (failed > 0) {
        process.exitCode = 1;
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
