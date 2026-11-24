
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
    withGlobalConnectorLock,
    getManualReconcileCoordinatorLifecycle
} = require("../socketio/connectorOperationCoordinator");

test("Coordinator exports required functions", () => {
    assert.equal(
        typeof withGlobalConnectorLock,
        "function"
    );

    assert.equal(
        typeof getManualReconcileCoordinatorLifecycle,
        "function"
    );
});

test("Unknown attempt has no lifecycle evidence", () => {
    const lifecycle =
        getManualReconcileCoordinatorLifecycle(
            "00000000-0000-4000-8000-000000000001"
        );

    assert.equal(lifecycle, null);
});

test("Manual reconciliation requires durable receipt writer", async () => {
    await assert.rejects(
        withGlobalConnectorLock(
            "TEST-CONNECTOR",
            async () => true,
            {
                releaseEvidence: true
            }
        ),
        /MANUAL_RECONCILE_DURABLE_RECEIPT_WRITER_REQUIRED/
    );
});

test("Manual reconciliation rejects invalid completion identity", async () => {
    await assert.rejects(
        withGlobalConnectorLock(
            "TEST-CONNECTOR",
            async () => true,
            {
                releaseEvidence: true,
                exposeLockIdentity: true,
                manualCoordinatorReceiptWriter: async () => true,
                completionIdentity: {
                    attemptId: "invalid",
                    executionId: "invalid",
                    serverInstanceId: "invalid"
                }
            }
        ),
        /GLOBAL_CONNECTOR_COMPLETION_IDENTITY_INVALID/
    );
});

test("Coordinator rejects non-function operation", async () => {
    await assert.rejects(
        withGlobalConnectorLock(
            "TEST-CONNECTOR",
            null
        ),
        /operation must be a function/
    );
});

test("Failed operation retries durable coordinator receipt", async () => {
    const Module = require("node:module");
    const EventEmitter = require("node:events");
    const crypto = require("node:crypto");
    const path = require("node:path");

    const originalLoad = Module._load;
    const coordinatorPath = path.resolve(
        __dirname,
        "../socketio/connectorOperationCoordinator.js"
    );

    const connectorId = "TEST-RECEIPT-RETRY";
    const socket = new EventEmitter();

    socket.id = "test-retry-socket";
    socket.connected = true;
    socket.protocolReceiver = {
        cancelCollection() {}
    };
    socket.disconnect = function () {
        this.connected = false;
        this.emit("disconnect");
    };

    let receiptCalls = 0;
    let quarantineCalls = 0;

    const registry = {
        get(id) {
            return id === connectorId ? socket : null;
        }
    };

    const healthStore = {
        acquireConnectorAndSocketLock: async () => true,
        renewConnectorAndSocketLock: async () => true,
        releaseConnectorAndSocketLock: async () => true,
        releaseManualReconcileLockWithReceipt: async () => {
            throw new Error("Unexpected successful release");
        },
        quarantineConnectorAndSocket: async () => {
            quarantineCalls++;
            return { quarantined: true };
        },
        getConnectorWatchdogStatus: async () => ({
            operation_status: "BUSY"
        })
    };

    let coordinator;

    try {
        Module._load = function (request, parent, isMain) {
            if (
                parent?.filename === coordinatorPath &&
                request === "./connectorRegistry"
            ) {
                return registry;
            }

            if (
                parent?.filename === coordinatorPath &&
                request === "./connectorHealthStore"
            ) {
                return healthStore;
            }

            return originalLoad.apply(this, arguments);
        };

        delete require.cache[coordinatorPath];
        coordinator = require(coordinatorPath);
    } finally {
        Module._load = originalLoad;
    }

    const attemptId = crypto.randomUUID();

    try {
        await assert.rejects(
            coordinator.withGlobalConnectorLock(
                connectorId,
                async () => {
                    throw new Error("SIMULATED_TALLY_FAILURE");
                },
                {
                    releaseEvidence: true,
                    exposeLockIdentity: true,
                    prepareManualAcquisition: async () => {},
                    completionIdentity: {
                        attemptId,
                        executionId: crypto.randomUUID(),
                        serverInstanceId: crypto.randomUUID()
                    },
                    manualCoordinatorReceiptWriter: async receipt => {
                        receiptCalls++;

                        assert.equal(
                            receipt.attemptId,
                            attemptId
                        );

                        assert.equal(
                            receipt.outcome,
                            "LOCALLY_SETTLED_UNVERIFIED"
                        );

                        if (receiptCalls === 1) {
                            throw new Error(
                                "SIMULATED_RECEIPT_WRITE_FAILURE"
                            );
                        }

                        return true;
                    }
                }
            ),
            /SIMULATED_TALLY_FAILURE/
        );

        assert.equal(receiptCalls, 2);
        assert.ok(quarantineCalls >= 1);

        const lifecycle =
            coordinator.getManualReconcileCoordinatorLifecycle(
                attemptId
            );

        assert.equal(lifecycle.coordinatorExited, true);
        assert.equal(lifecycle.locallySettled, true);
        assert.equal(lifecycle.recoveryAuthorized, false);
    } finally {
        delete require.cache[coordinatorPath];
    }
});
