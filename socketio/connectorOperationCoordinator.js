"use strict";

const crypto = require("crypto");

const registry = require("./connectorRegistry");

const {
    acquireConnectorAndSocketLock,
    releaseConnectorAndSocketLock,
    renewConnectorAndSocketLock,
    quarantineConnectorAndSocket,
    getConnectorWatchdogStatus
} = require("./connectorHealthStore");

const LOCK_TTL_MS = 120000;
const LOCK_RENEW_MS = 30000;
const LOCK_WAIT_MS = 2000;

const OPERATION_SETTLE_TIMEOUT_MS = 15 * 1000;
// ==========================================
// WAIT AND ACQUIRE GLOBAL CONNECTOR LOCK
// ==========================================

async function waitForConnectorLock(connectorId) {
    const id = String(connectorId || "").trim();

    if (!id) {
        throw new Error("connectorId is required");
    }

  const ownerToken = crypto.randomUUID();

   while (true) {
        const socket = registry.get(id);

        if (!socket?.connected) {
            throw new Error(
                `Connector disconnected: ${id}`
            );
        }

        if (!socket.id) {
            throw new Error(
                `Socket ID missing: ${id}`
            );
        }

        if (!socket.protocolReceiver) {
            throw new Error(
                `Protocol receiver missing: ${id}`
            );
        }

        const acquired =
            await acquireConnectorAndSocketLock(
                id,
                socket.id,
                ownerToken,
                { ttlMs: LOCK_TTL_MS }
            );

        if (acquired) {
            // Socket must still be the registered connection.
if (
    !socket.connected ||
    !socket.protocolReceiver ||
    registry.get(id) !== socket
) {
  await quarantineConnectorAndSocket(
    id,
    socket.id,
    "CONNECTOR_CHANGED_DURING_LOCK_ACQUISITION",
    ownerToken
);

    // Keep persistent markers for verified recovery.
    // Do not release lock after uncertain socket transition.
    throw new Error(
        `Connector changed during lock acquisition: ${id}`
    );
}

            return {
                connectorId: id,
                socketId: socket.id,
                socket,
                ownerToken
            };
        }

        const watchdog = await getConnectorWatchdogStatus(id);

if (watchdog.operation_status === "QUARANTINED") {
    throw new Error(
        `Connector recovery required (QUARANTINED): ${id}`
    );
}

        await new Promise(resolve =>
            setTimeout(resolve, LOCK_WAIT_MS)
        );
    }
}

// ==========================================
// EXECUTE OPERATION UNDER GLOBAL LOCK
// ==========================================

async function withGlobalConnectorLock(connectorId, operation) {
    if (typeof operation !== "function") {
        throw new Error("operation must be a function");
    }

    const lock = await waitForConnectorLock(connectorId);

    let renewalTimer = null;
    let renewing = false;
    let lockLost = false;
    let operationPromise = null;
    let operationCompletedSafely = false;
    

    const operationAbortController = new AbortController();

operationAbortController.signal.addEventListener(
    "abort",
    () => {
        try {
            const receiver = lock.socket.protocolReceiver;

            if (typeof receiver?.cancelCollection === "function") {
                const cancelResult = receiver.cancelCollection(
                    "Global connector operation aborted"
                );

                if (cancelResult?.catch) {
                    cancelResult.catch(error => {
                        console.error(
                            `[GLOBAL LOCK] Collection cancellation failed: ${lock.connectorId}`,
                            error
                        );
                    });
                }
            }
        } catch (error) {
            console.error(
                `[GLOBAL LOCK] Collection cancellation failed: ${lock.connectorId}`,
                error
            );
        }
    },
    { once: true }
);   
 try {
        renewalTimer = setInterval(async () => {
            if (renewing || lockLost) return;

            renewing = true;

            try {
                const renewed = await renewConnectorAndSocketLock(
                    lock.connectorId,
                    lock.socketId,
                    lock.ownerToken,
                    LOCK_TTL_MS
                );

                if (!renewed) {
    lockLost = true;

    const error = new Error(
        `Global lock ownership lost: ${lock.connectorId}`
    );

    console.error(
        `[GLOBAL LOCK] Ownership lost: ${lock.connectorId}`
    );

    operationAbortController.abort(error);

    try {
       await quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "GLOBAL_LOCK_OWNERSHIP_LOST",
    lock.ownerToken
);
    } catch (quarantineError) {
        console.error(
            `[GLOBAL LOCK] Quarantine failed: ${lock.connectorId}`,
            quarantineError
        );
    }

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }
}
   } catch (error) {
    lockLost = true;

    console.error(
        `[GLOBAL LOCK] Renewal failed: ${lock.connectorId}`,
        error
    );

    operationAbortController.abort(error);

    try {
      await quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "GLOBAL_LOCK_RENEWAL_ERROR",
    lock.ownerToken
);
    } catch (quarantineError) {
        console.error(
            `[GLOBAL LOCK] Renewal quarantine failed: ${lock.connectorId}`,
            quarantineError
        );
    }

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }
} finally {
    renewing = false;
}
        }, LOCK_RENEW_MS);

        if (
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    lockLost = true;

   await quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "CONNECTOR_UNAVAILABLE_BEFORE_OPERATION",
    lock.ownerToken
);

    throw new Error(
        `Connector unavailable before operation: ${lock.connectorId}`
    );
}

const result = await new Promise((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
        lock.socket.off("disconnect", onDisconnect);
    };

    const onDisconnect = () => {
    if (settled) return;

    settled = true;
    lockLost = true;
    cleanup();
    const error = new Error(
        `Connector disconnected during operation: ${lock.connectorId}`
    );

    operationAbortController.abort(error);

   quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "CONNECTOR_DISCONNECTED_DURING_OPERATION",
    lock.ownerToken
).then(
        () => reject(error),
        (quarantineError) => {
            console.error(
                `[GLOBAL LOCK] Disconnect quarantine failed: ${lock.connectorId}`,
                quarantineError
            );

            // Preserve original disconnect error.
            if (error && typeof error === "object") {
                error.quarantineError = quarantineError;
            }

            reject(error);
        }
    );
};
    lock.socket.once("disconnect", onDisconnect);

    if (!lock.socket.connected) {
        onDisconnect();
        return;
    }

    Promise.resolve()
        .then(() => {
    if (
    settled ||
    lockLost ||
    operationAbortController.signal.aborted ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    throw new Error(
        `Connector unavailable or lock lost before operation started: ${lock.connectorId}`
    );
}
  operationPromise = Promise.resolve().then(() => {
    if (
        settled ||
        lockLost ||
        operationAbortController.signal.aborted ||
        !lock.socket.connected ||
        !lock.socket.protocolReceiver ||
        registry.get(lock.connectorId) !== lock.socket
    ) {
        throw new Error(
            `Connector unavailable before callback execution: ${lock.connectorId}`
        );
    }

    return operation(
        lock.socket,
        operationAbortController.signal
    );
});
        return operationPromise;
        })
        .then(
            (value) => {
                if (settled) return;

                settled = true;
                cleanup();
                resolve(value);
            },
           (error) => {
    if (settled) return;

    settled = true;
    lockLost = true;
    cleanup();

    quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "OPERATION_FAILED_REMOTE_STATE_UNKNOWN",
    lock.ownerToken
).then(
        () => reject(error),
        (quarantineError) => {
            console.error(
                `[GLOBAL LOCK] Failure quarantine failed: ${lock.connectorId}`,
                quarantineError
            );

            // Preserve the original operation error and its
            // requestDispatched metadata.
            if (error && typeof error === "object") {
                error.quarantineError = quarantineError;
            }

            reject(error);
        }
    );
}
        );
});

        // Keep renewal active until operation cleanup finishes.
while (renewing) {
    await new Promise(resolve => setTimeout(resolve, 50));
}
    if (
    lockLost ||
    operationAbortController.signal.aborted ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    lockLost = true;

    throw new Error(
        `Connector lock or connection lost: ${lock.connectorId}`
    );
}
        let stillOwned = false;

try {
    stillOwned = await renewConnectorAndSocketLock(
        lock.connectorId,
        lock.socketId,
        lock.ownerToken,
        LOCK_TTL_MS
    );
} catch (error) {
    lockLost = true;

    console.error(
        `[GLOBAL LOCK] Final renewal failed: ${lock.connectorId}`,
        error
    );

    operationAbortController.abort(error);

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }

    throw error;
}
     if (!stillOwned) {
    lockLost = true;

    const error = new Error(
        `Global lock ownership lost: ${lock.connectorId}`
    );

    console.error(
        `[GLOBAL LOCK] Final ownership check failed: ${lock.connectorId}`
    );

    operationAbortController.abort(error);

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }

    throw error;
}

      if (renewalTimer) {
    clearInterval(renewalTimer);
    renewalTimer = null;
}

while (renewing) {
    await new Promise(resolve => setTimeout(resolve, 50));
}

if (
    lockLost ||
    operationAbortController.signal.aborted ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    lockLost = true;

    throw new Error(
        `Connector became unavailable before completion: ${lock.connectorId}`
    );
}

operationCompletedSafely = true;
return result;
 } finally {

    if (lockLost) {
    if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = null;
    }
}

    while (renewing) {
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    if (
    lockLost &&
    !operationAbortController.signal.aborted
) {
    operationAbortController.abort(
        new Error(
            `Global connector operation failed: ${lock.connectorId}`
        )
    );
}
if (operationPromise) {
    let settleTimer = null;
    let abortListener = null;

    try {
        const abortPromise = new Promise((_, reject) => {
            const onAbort = () => {
                settleTimer = setTimeout(() => {
                    reject(new Error(
                        `Global operation settle timeout: ${lock.connectorId}`
                    ));
                }, OPERATION_SETTLE_TIMEOUT_MS);
            };

            abortListener = onAbort;

            if (operationAbortController.signal.aborted) {
                onAbort();
            } else {
                operationAbortController.signal.addEventListener(
                    "abort",
                    onAbort,
                    { once: true }
                );
            }
        });

        // No fixed timeout while the operation is healthy.
        await Promise.race([
            operationPromise,
            abortPromise
        ]);

    } catch (error) {
    lockLost = true;

    console.error(
        `[GLOBAL LOCK] Pending operation failed: ${lock.connectorId}`,
        error
    );

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }
      if (error.message?.startsWith(
    "Global operation settle timeout:"
)) {
    lockLost = true;

    if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = null;
    }

    while (renewing) {
        await new Promise(resolve => setTimeout(resolve, 50));
    }

   await quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "OPERATION_SETTLE_TIMEOUT",
    lock.ownerToken
);

    throw error;
}
    } finally {
        if (settleTimer) {
            clearTimeout(settleTimer);
        }

        if (abortListener) {
            operationAbortController.signal.removeEventListener(
                "abort",
                abortListener
            );
        }
    }
}

if (renewalTimer) {
    clearInterval(renewalTimer);
    renewalTimer = null;
}

while (renewing) {
    await new Promise(resolve => setTimeout(resolve, 50));
}

if (
    !operationCompletedSafely ||
    lockLost ||
    operationAbortController.signal.aborted ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    console.error(
        `[GLOBAL LOCK] Unsafe operation retained for recovery: ${lock.connectorId}`
    );

    // Keep persistent markers until verified safe recovery.
} else {
    try {
    const released = await releaseConnectorAndSocketLock(
        lock.connectorId,
        lock.socketId,
        lock.ownerToken
    );

   if (!released) {
    throw new Error(
        `Global lock release ownership mismatch: ${lock.connectorId}`
    );
}
} catch (error) {
    lockLost = true;

    console.error(
        `[GLOBAL LOCK] Release failed: ${lock.connectorId}`,
        error
    );

    try {
       await quarantineConnectorAndSocket(
    lock.connectorId,
    lock.socketId,
    "GLOBAL_LOCK_RELEASE_FAILED",
    lock.ownerToken
);
    } catch (quarantineError) {
        console.error(
            `[GLOBAL LOCK] Release quarantine failed: ${lock.connectorId}`,
            quarantineError
        );
    }

    if (lock.socket.connected) {
        lock.socket.disconnect(true);
    }

    throw error;
}
}
    }
}
module.exports = {
    withGlobalConnectorLock
};