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

        const acquisitionPromise = acquireConnectorAndSocketLock(
    id,
    socket.id,
    ownerToken,
    { ttlMs: LOCK_TTL_MS }
);

let acquisitionTimer;

const acquisitionTimeout = new Promise((_, reject) => {
    acquisitionTimer = setTimeout(() => {
        reject(
            new Error(
                `GLOBAL_CONNECTOR_ACQUISITION_TIMEOUT: ${id}`
            )
        );
    }, OPERATION_SETTLE_TIMEOUT_MS);
});

let acquired;

try {
    acquired = await Promise.race([
        acquisitionPromise,
        acquisitionTimeout
    ]);
} catch (error) {
    if (
        error.message?.startsWith(
            "GLOBAL_CONNECTOR_ACQUISITION_TIMEOUT:"
        )
    ) {
        // The Redis command may still acquire the lock.
        // Observe its eventual result without releasing markers.
        acquisitionPromise.then(
            lateAcquired => {
                if (lateAcquired) {
                    console.error(
                        `[GLOBAL LOCK] Late acquisition requires verified recovery: ${id}`,
                        { socketId: socket.id }
                    );
                }
            },
            lateError => {
                console.error(
                    `[GLOBAL LOCK] Late acquisition failed: ${id}`,
                    lateError
                );
            }
        );

        // Prevent an uncertain acquisition from starting Tally work.
        if (socket.connected) {
            socket.disconnect(true);
        }
    }

    throw error;
} finally {
    clearTimeout(acquisitionTimer);
}

        if (acquired) {
            // Socket must still be the registered connection.
if (
    !socket.connected ||
    !socket.protocolReceiver ||
    registry.get(id) !== socket
) {
const quarantinePromise = quarantineConnectorAndSocket(
    id,
    socket.id,
    "CONNECTOR_CHANGED_DURING_LOCK_ACQUISITION",
    ownerToken
);

let quarantineTimer;

try {
    await Promise.race([
        quarantinePromise,
        new Promise((_, reject) => {
            quarantineTimer = setTimeout(() => {
                reject(
                    new Error(
                        `GLOBAL_CONNECTOR_ACQUISITION_QUARANTINE_TIMEOUT: ${id}`
                    )
                );
            }, OPERATION_SETTLE_TIMEOUT_MS);
        })
    ]);
} finally {
    clearTimeout(quarantineTimer);
}

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

     let watchdogTimer;

const watchdogPromise = Promise.resolve().then(() =>
    getConnectorWatchdogStatus(id)
);

let watchdog;

try {
    watchdog = await Promise.race([
        watchdogPromise,
        new Promise((_, reject) => {
            watchdogTimer = setTimeout(() => {
                reject(
                    new Error(
                        `GLOBAL_CONNECTOR_WATCHDOG_STATUS_TIMEOUT: ${id}`
                    )
                );
            }, OPERATION_SETTLE_TIMEOUT_MS);
        })
    ]);
} finally {
    clearTimeout(watchdogTimer);
}

if (
    !watchdog ||
    typeof watchdog !== "object" ||
    typeof watchdog.operation_status !== "string"
) {
    throw new Error(
        `GLOBAL_CONNECTOR_WATCHDOG_STATUS_INVALID: ${id}`
    );
}

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

async function withGlobalConnectorLock(
    connectorId,
    operation,
    options = undefined
) {
     if (typeof operation !== "function") {
        throw new Error("operation must be a function");
    }

    const UUID =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    const receiptRequested = options?.releaseEvidence === true;
    let completionIdentity = null;

    if (receiptRequested) {
        const identity = options?.completionIdentity;

        if (
            options?.exposeLockIdentity !== true ||
            !identity ||
            typeof identity !== "object" ||
            Array.isArray(identity) ||
            !UUID.test(identity.attemptId || "") ||
            !UUID.test(identity.executionId || "") ||
            !UUID.test(identity.serverInstanceId || "")
        ) {
            throw new Error(
                "GLOBAL_CONNECTOR_COMPLETION_IDENTITY_INVALID"
            );
        }

        completionIdentity = Object.freeze({
            attemptId: identity.attemptId,
            executionId: identity.executionId,
            serverInstanceId: identity.serverInstanceId
        });
    }

    const completion = receiptRequested
        ? { result: null, releaseEvidence: null }
        : null;

    const lock = await waitForConnectorLock(connectorId);

  let renewalTimer = null;
let renewing = false;
let lockLost = false;
let operationPromise = null;
let operationCompletedSafely = false;

// Tracks explicit ownership verification separately from timer renewal.
let ownershipCheckPromise = null;
let ownershipRedisPromise = null;

let callbackActive = false;
let ownershipFailure = null;
let settlementFailure = null;

// Opt-in only: existing sync callers retain their current behavior.
const identityEnabled = options?.exposeLockIdentity === true;
    

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
function observedRejection(error) {
    const promise = Promise.reject(error);
    promise.catch(() => {});
    return promise;
}

function lifecycleUsable() {
    return (
        callbackActive &&
        !lockLost &&
        !ownershipFailure &&
        !operationAbortController.signal.aborted &&
        lock.socket.connected === true &&
        !!lock.socket.protocolReceiver &&
        registry.get(lock.connectorId) === lock.socket
    );
}
function markOwnershipFailure(error) {
    if (ownershipFailure) {
        return ownershipFailure;
    }

    ownershipFailure = error;
    lockLost = true;

    if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = null;
    }

    if (!operationAbortController.signal.aborted) {
        operationAbortController.abort(error);
    }

    if (lock.socket.connected) {
        try {
            lock.socket.disconnect(true);
        } catch (disconnectError) {
            console.error(
                `[GLOBAL LOCK] Ownership-check disconnect failed: ${lock.connectorId}`,
                disconnectError
            );
        }
    }

    return ownershipFailure;
}
function bounded(promise, message) {
    let timer;

    const timeout = new Promise((_, reject) => {
        timer = setTimeout(
            () => reject(new Error(message)),
            OPERATION_SETTLE_TIMEOUT_MS
        );
    });

    return Promise.race([promise, timeout])
        .finally(() => clearTimeout(timer));
}
function runOptInRedisCommand(command, message) {
    if (ownershipRedisPromise) {
        return observedRejection(
            markOwnershipFailure(
                new Error("GLOBAL_CONNECTOR_REDIS_COMMAND_ALREADY_PENDING")
            )
        );
    }

    const pending = Promise.resolve().then(() => {
        if (
            lockLost ||
            ownershipFailure ||
            operationAbortController.signal.aborted ||
            !lock.socket.connected ||
            !lock.socket.protocolReceiver ||
            registry.get(lock.connectorId) !== lock.socket
        ) {
            throw ownershipFailure ||
                new Error("GLOBAL_CONNECTOR_REDIS_COMMAND_NOT_USABLE");
        }

        return command();
    });

    ownershipRedisPromise = pending;

    const clearPending = () => {
        if (ownershipRedisPromise === pending) {
            ownershipRedisPromise = null;
        }
    };

    pending.then(clearPending, clearPending);

    return bounded(pending, message).catch(error => {
        throw markOwnershipFailure(error);
    });
}
function quarantineForCleanup(...args) {
       if (!identityEnabled) {
        const pending = Promise.resolve().then(() =>
            quarantineConnectorAndSocket(...args)
        );

        return bounded(
            pending,
            `GLOBAL_CONNECTOR_DEFAULT_QUARANTINE_TIMEOUT: ${lock.connectorId}`
        );
    }

    const work = Promise.resolve().then(async () => {
        // Wait for a check that may not yet have submitted Redis work.
        const pendingCheck = ownershipCheckPromise;

        if (pendingCheck) {
            try {
                await pendingCheck;
            } catch (_) {
                // Failed checks still require quarantine.
            }
        }

        // Wait for the actual Redis command, including late settlement.
        const pendingRedis = ownershipRedisPromise;

        if (pendingRedis) {
            try {
                await pendingRedis;
            } catch (_) {
                // Uncertain ownership still requires quarantine.
            }
        }

        return quarantineConnectorAndSocket(...args);
    });

    return bounded(
        work,
        `Global quarantine timeout: ${lock.connectorId}`
    );
}
async function drainRenewalAndChecks() {
     if (!identityEnabled) {
        const deadline = Date.now() + OPERATION_SETTLE_TIMEOUT_MS;

        while (renewing || ownershipCheckPromise) {
            if (Date.now() >= deadline) {
                markOwnershipFailure(
                    new Error(
                        `GLOBAL_CONNECTOR_DEFAULT_RENEWAL_DRAIN_TIMEOUT: ${lock.connectorId}`
                    )
                );

                // Redis command may still complete later.
                // Never assume ownership has safely settled.
                return;
            }

            await new Promise(resolve => setTimeout(resolve, 50));
        }

        return;
    }

    const deadline = Date.now() + OPERATION_SETTLE_TIMEOUT_MS;

    while (
        renewing ||
        ownershipCheckPromise ||
        ownershipRedisPromise
    ) {
        if (Date.now() >= deadline) {
    markOwnershipFailure(
        new Error("GLOBAL_CONNECTOR_OWNERSHIP_DRAIN_TIMEOUT")
    );

    // Pending Redis command may still be executing.
    // Never treat timeout as confirmed settlement.
    return;
}

        await new Promise(resolve => setTimeout(resolve, 50));
    }
}
function assertOwnership() {
    if (!lifecycleUsable()) {
        return observedRejection(
            ownershipFailure ||
            new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_NOT_ACTIVE")
        );
    }

   if (ownershipCheckPromise) {
    return observedRejection(
        new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_IN_PROGRESS")
    );
}

    let resolveCheck;
    let rejectCheck;

    const check = new Promise((resolve, reject) => {
        resolveCheck = resolve;
        rejectCheck = reject;
    });

    // Reserve synchronously to prevent overlapping checks.
    ownershipCheckPromise = check;
    check.catch(() => {});

    let finished = false;
    let timeout;

    const cleanup = () => {
        clearTimeout(timeout);
        operationAbortController.signal.removeEventListener(
            "abort",
            onAbort
        );
    };

    function fail(error) {
        if (finished) return;

        finished = true;
        cleanup();

        rejectCheck(markOwnershipFailure(error));
    }

    function onAbort() {
        fail(
            operationAbortController.signal.reason instanceof Error
                ? operationAbortController.signal.reason
                : new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_ABORTED")
        );
    }

    timeout = setTimeout(
        () => fail(
            new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_TIMEOUT")
        ),
        OPERATION_SETTLE_TIMEOUT_MS
    );

    operationAbortController.signal.addEventListener(
        "abort",
        onAbort,
        { once: true }
    );

    if (operationAbortController.signal.aborted) {
        onAbort();
    }

    const work = Promise.resolve().then(async () => {
        while (renewing || ownershipRedisPromise) {
    if (finished || !lifecycleUsable()) {
        throw ownershipFailure ||
            new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_ABORTED");
    }

    await new Promise(resolve => setTimeout(resolve, 50));
}

        if (finished || !lifecycleUsable()) {
            throw ownershipFailure ||
                new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_ABORTED");
        }

        const renewed = await runOptInRedisCommand(
    () => renewConnectorAndSocketLock(
        lock.connectorId,
        lock.socketId,
        lock.ownerToken,
        LOCK_TTL_MS
    ),
    `GLOBAL_CONNECTOR_OWNERSHIP_RENEW_TIMEOUT: ${lock.connectorId}`
);

        if (!renewed) {
            throw new Error(
                `Global lock ownership lost: ${lock.connectorId}`
            );
        }

        if (finished || !lifecycleUsable()) {
            throw ownershipFailure ||
                new Error("GLOBAL_CONNECTOR_OWNERSHIP_CHECK_ABORTED");
        }
    });

    const clearCheck = () => {
        if (ownershipCheckPromise === check) {
            ownershipCheckPromise = null;
        }
    };

    work.then(
        () => {
            clearCheck();

            if (finished) return;

            finished = true;
            cleanup();
            resolveCheck(true);
        },
        error => {
            clearCheck();
            fail(error);
        }
    ).catch(fail);

    return check;
}
 try {
        renewalTimer = setInterval(async () => {
            if (
    renewing ||
    ownershipCheckPromise ||
    ownershipRedisPromise ||
    lockLost
) return;

            renewing = true;

            try {
                const renewed = identityEnabled
    ? await runOptInRedisCommand(
        () => renewConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken,
            LOCK_TTL_MS
        ),
        `GLOBAL_CONNECTOR_TIMER_RENEW_TIMEOUT: ${lock.connectorId}`
    )
       : await bounded(
        renewConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken,
            LOCK_TTL_MS
        ),
        `GLOBAL_CONNECTOR_DEFAULT_TIMER_RENEW_TIMEOUT: ${lock.connectorId}`
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
       await quarantineForCleanup(
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
      await quarantineForCleanup(
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

   await quarantineForCleanup(
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

    const error = ownershipFailure || new Error(
        `Connector disconnected during operation: ${lock.connectorId}`
    );

    operationAbortController.abort(error);

   quarantineForCleanup(
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

   if (identityEnabled) {
    callbackActive = true;

    return Promise.resolve()
        .then(() => operation(
            lock.socket,
            operationAbortController.signal,
            Object.freeze({
                connectorId: lock.connectorId,
                socketId: lock.socketId,
                ownerToken: lock.ownerToken,
                assertOwnership
            })
        ))
        .then(value => {
            if (ownershipFailure) {
                throw ownershipFailure;
            }

         if (ownershipCheckPromise) {
    throw markOwnershipFailure(
        new Error(
            "GLOBAL_CONNECTOR_OWNERSHIP_CHECK_UNSETTLED"
        )
    );
}

            if (receiptRequested) {
                completion.result = value;
            }

            return value;
        })
        .finally(() => {
            callbackActive = false;
        });
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

    quarantineForCleanup(
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

// Stop future timer renewals for every operation
// before draining and final ownership verification.
if (renewalTimer) {
    clearInterval(renewalTimer);
    renewalTimer = null;
}

// Wait for already-running renewals and ownership checks.
await drainRenewalAndChecks();
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
 stillOwned = identityEnabled
    ? await runOptInRedisCommand(
        () => renewConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken,
            LOCK_TTL_MS
        ),
        `GLOBAL_CONNECTOR_FINAL_RENEW_TIMEOUT: ${lock.connectorId}`
    )
      : await bounded(
        renewConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken,
            LOCK_TTL_MS
        ),
        `GLOBAL_CONNECTOR_DEFAULT_FINAL_RENEW_TIMEOUT: ${lock.connectorId}`
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

await drainRenewalAndChecks();

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
return receiptRequested ? completion : result;
 } finally {

    if (lockLost) {
    if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = null;
    }
}

    await drainRenewalAndChecks();
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

    await drainRenewalAndChecks();

    try {
        await quarantineForCleanup(
            lock.connectorId,
            lock.socketId,
            "OPERATION_SETTLE_TIMEOUT",
            lock.ownerToken
        );
    } catch (quarantineError) {
        console.error(
            `[GLOBAL LOCK] Settlement quarantine failed: ${lock.connectorId}`,
            quarantineError
        );

        if (error && typeof error === "object") {
            error.quarantineError = quarantineError;
        }
    }

    if (!identityEnabled) {
        throw error;
    }

    // Preserve the first ownership failure as the primary error.
// Settlement timeout is secondary diagnostic evidence.
if (ownershipFailure) {
    if (ownershipFailure !== error) {
        ownershipFailure.settlementError = error;
    }

    settlementFailure = ownershipFailure;
} else {
    settlementFailure = error;
}
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

await drainRenewalAndChecks();

if (
    !operationCompletedSafely ||
    lockLost ||
    operationAbortController.signal.aborted ||
    (
        identityEnabled &&
        (
            ownershipFailure ||
            ownershipCheckPromise ||
            ownershipRedisPromise
        )
    ) ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    console.error(
        `[GLOBAL LOCK] Unsafe operation retained for recovery: ${lock.connectorId}`
    );

    // Keep persistent markers until verified safe recovery.
    if (!operationAbortController.signal.aborted) {
    operationAbortController.abort(
        new Error(
            `Unsafe connector operation: ${lock.connectorId}`
        )
    );
}

try {
    await quarantineForCleanup(
        lock.connectorId,
        lock.socketId,
        "UNSAFE_OPERATION_FINALIZATION",
        lock.ownerToken
    );
} catch (quarantineError) {
    console.error(
        `[GLOBAL LOCK] Unsafe finalization quarantine failed: ${lock.connectorId}`,
        quarantineError
    );
}

if (lock.socket.connected) {
    lock.socket.disconnect(true);
}

if (settlementFailure) {
    throw settlementFailure;
}

if (operationCompletedSafely) {
    throw new Error(
        `GLOBAL_CONNECTOR_UNSAFE_FINALIZATION: ${lock.connectorId}`
    );
}
} else {
    try {
   const released = identityEnabled
    ? await runOptInRedisCommand(
        () => releaseConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken
        ),
        `GLOBAL_CONNECTOR_RELEASE_TIMEOUT: ${lock.connectorId}`
    )
       : await bounded(
        releaseConnectorAndSocketLock(
            lock.connectorId,
            lock.socketId,
            lock.ownerToken
        ),
        `GLOBAL_CONNECTOR_DEFAULT_RELEASE_TIMEOUT: ${lock.connectorId}`
    );

if (!released) {
    throw new Error(
        `Global lock release ownership mismatch: ${lock.connectorId}`
    );
}
// The release command has resolved successfully.
// Allow its pending-command tracker to clear before
// validating the release evidence.
if (receiptRequested) {
    await drainRenewalAndChecks();

    if (
        !completionIdentity ||
        !operationCompletedSafely ||
        lockLost ||
        ownershipFailure ||
        ownershipCheckPromise ||
        renewing ||
        ownershipRedisPromise ||
        operationAbortController.signal.aborted
    ) {
        throw new Error(
            `GLOBAL_CONNECTOR_RELEASE_EVIDENCE_UNSAFE: ${lock.connectorId}`
        );
    }
}
// Recheck after asynchronous Redis release.
// Socket or registry may have changed while release was pending.
if (
    lockLost ||
    operationAbortController.signal.aborted ||
    !lock.socket.connected ||
    !lock.socket.protocolReceiver ||
    registry.get(lock.connectorId) !== lock.socket
) {
    throw new Error(
        `GLOBAL_CONNECTOR_UNSAFE_AFTER_RELEASE: ${lock.connectorId}`
    );
}

// Only expose release evidence after confirmed Redis release
// and final socket/registry validation.
if (receiptRequested) {
    completion.releaseEvidence = Object.freeze({
        attemptId: completionIdentity.attemptId,
        executionId: completionIdentity.executionId,
        serverInstanceId: completionIdentity.serverInstanceId,
        connectorId: lock.connectorId,
        socketId: lock.socketId,
        ownerToken: lock.ownerToken,
        redisReleaseConfirmed: true
    });
}
} catch (error) {
    lockLost = true;

    operationAbortController.abort();

    console.error(
        `[GLOBAL LOCK] Release failed: ${lock.connectorId}`,
        error
    );

    try {
       await quarantineForCleanup(
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