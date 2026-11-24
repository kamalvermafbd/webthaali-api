
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
        assert.equal(
    socket.connected,
    true,
    "Confirmed quarantine must not disconnect a healthy socket"
);

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
test(
    "Ownership failure aborts hung callback without disconnecting quarantined socket",
    { timeout: 30000 },
    async () => {
        const Module = require("node:module");
        const EventEmitter = require("node:events");
        const path = require("node:path");

        const originalLoad = Module._load;

        const coordinatorPath = path.resolve(
            __dirname,
            "../socketio/connectorOperationCoordinator.js"
        );

        const connectorId = "TEST-OWNERSHIP-FAILURE";

        const socket = new EventEmitter();

        socket.id = "test-ownership-socket";
        socket.connected = true;

        let disconnectCalls = 0;
        let quarantineCalls = 0;
        let releaseCalls = 0;
        let renewalCalls = 0;
        let observedSignal;

        socket.protocolReceiver = {
            cancelCollection() {}
        };

        socket.disconnect = function () {
            disconnectCalls++;
            this.connected = false;
            this.emit("disconnect");
        };

        const registry = {
            get(id) {
                return id === connectorId
                    ? socket
                    : null;
            }
        };

        const healthStore = {
            acquireConnectorAndSocketLock: async () => true,

            renewConnectorAndSocketLock: async () => {
                renewalCalls++;
                return false;
            },

            releaseConnectorAndSocketLock: async () => {
                releaseCalls++;
                return true;
            },

            quarantineConnectorAndSocket: async () => {
                quarantineCalls++;

                return {
                    connector_id: connectorId,
                    socket_id: socket.id,
                    quarantined: true
                };
            },

            getConnectorWatchdogStatus: async () => ({
                operation_status: "BUSY"
            })
        };

        let coordinator;

        try {
            Module._load = function (
                request,
                parent,
                isMain
            ) {
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

                return originalLoad.apply(
                    this,
                    arguments
                );
            };

            delete require.cache[coordinatorPath];

            coordinator = require(coordinatorPath);

        } finally {
            Module._load = originalLoad;
        }

        try {
            await assert.rejects(
                coordinator.withGlobalConnectorLock(
                    connectorId,

                    async (lockedSocket, signal, identity) => {
                        observedSignal = signal;

                        try {
                            await identity.assertOwnership();
                        } catch (_) {
                            // Simulate a callback that fails
                            // to settle after ownership loss.
                        }

                        await new Promise(() => {});
                    },

                    {
                        exposeLockIdentity: true
                    }
                ),

                /Global lock ownership lost/
            );

            assert.equal(renewalCalls, 1);
            assert.equal(observedSignal?.aborted, true);

            assert.ok(quarantineCalls >= 1);

            assert.equal(releaseCalls, 0);
            assert.equal(disconnectCalls, 0);
            assert.equal(socket.connected, true);

            assert.equal(
                socket.listenerCount("disconnect"),
                0
            );

        } finally {
            delete require.cache[coordinatorPath];
        }
    }
);

test(
    "Late acquisition after timeout retains socket and never dispatches operation",
    { timeout: 30000 },
    async () => {
        const Module = require("node:module");
        const EventEmitter = require("node:events");
        const path = require("node:path");

        const originalLoad = Module._load;

        const coordinatorPath = path.resolve(
            __dirname,
            "../socketio/connectorOperationCoordinator.js"
        );

        const connectorId = "TEST-LATE-ACQUISITION";

        const socket = new EventEmitter();

        socket.id = "test-late-acquisition-socket";
        socket.connected = true;

        let disconnectCalls = 0;
        let operationCalls = 0;
        let releaseCalls = 0;
        let renewalCalls = 0;
        let acquisitionCalls = 0;
        let resolveAcquisition;

        socket.protocolReceiver = {
            cancelCollection() {}
        };

        socket.disconnect = function () {
            disconnectCalls++;
            this.connected = false;
            this.emit("disconnect");
        };

        const registry = {
            get(id) {
                return id === connectorId
                    ? socket
                    : null;
            }
        };

        const acquisitionPromise = new Promise(resolve => {
            resolveAcquisition = resolve;
        });

        const healthStore = {
            acquireConnectorAndSocketLock: async () => {
                acquisitionCalls++;
                return acquisitionPromise;
            },

            renewConnectorAndSocketLock: async () => {
                renewalCalls++;
                return true;
            },

            releaseConnectorAndSocketLock: async () => {
                releaseCalls++;
                return true;
            },

            quarantineConnectorAndSocket: async () => ({
                quarantined: true
            }),

            getConnectorWatchdogStatus: async () => ({
                operation_status: "BUSY"
            })
        };

        let coordinator;

        try {
            Module._load = function (
                request,
                parent,
                isMain
            ) {
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

                return originalLoad.apply(
                    this,
                    arguments
                );
            };

            delete require.cache[coordinatorPath];

            coordinator = require(coordinatorPath);

        } finally {
            Module._load = originalLoad;
        }

        try {
            await assert.rejects(
                coordinator.withGlobalConnectorLock(
                    connectorId,
                    async () => {
                        operationCalls++;
                        return true;
                    }
                ),
                /GLOBAL_CONNECTOR_ACQUISITION_TIMEOUT/
            );

            // Verify state immediately after timeout.
            assert.equal(acquisitionCalls, 1);
            assert.equal(operationCalls, 0);
            assert.equal(disconnectCalls, 0);
            assert.equal(socket.connected, true);
            assert.equal(releaseCalls, 0);

            // Simulate Redis acquiring the lock
            // after the coordinator has already exited.
            resolveAcquisition(true);

            await new Promise(resolve =>
                setImmediate(resolve)
            );

            // Late acquisition must not dispatch work
            // or release uncertain ownership.
            assert.equal(acquisitionCalls, 1);
            assert.equal(operationCalls, 0);
            assert.equal(renewalCalls, 0);
            assert.equal(releaseCalls, 0);
            assert.equal(disconnectCalls, 0);
            assert.equal(socket.connected, true);

            assert.equal(
                socket.listenerCount("disconnect"),
                0
            );

        } finally {
            resolveAcquisition(false);
            delete require.cache[coordinatorPath];
        }
    }
);