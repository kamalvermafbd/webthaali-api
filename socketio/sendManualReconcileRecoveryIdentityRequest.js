
"use strict";

const { randomUUID } = require("crypto");
const registry = require("./connectorRegistry");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const REQUEST_EVENT = "reconcile:getRecoveryIdentity";
const RESULT_EVENT = "reconcile:getRecoveryIdentityResult";

// Diagnostic only. Pending candidate is NOT authenticated ownership.
function sendManualReconcileRecoveryIdentityRequest({
    connectorId,
    expectedSocketId,
    timeoutMs = 10000
} = {}) {
    if (
        typeof connectorId !== "string" ||
        !connectorId.trim() ||
        typeof expectedSocketId !== "string" ||
        !expectedSocketId.trim() ||
        !Number.isSafeInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > 60000
    ) {
        return Promise.reject(
            new Error("RECONCILE_RECOVERY_PROBE_ARGUMENTS_INVALID")
        );
    }

    const candidates =
        registry.getPendingSocketsForRecovery(connectorId);

    const matches = candidates.filter(socket =>
        socket.connected === true &&
        socket.id === expectedSocketId
    );

    if (matches.length !== 1) {
        return Promise.reject(
            new Error("RECONCILE_RECOVERY_PROBE_SOCKET_UNAVAILABLE")
        );
    }

    const socket = matches[0];

    const valid = () =>
        socket.connected === true &&
        socket.id === expectedSocketId &&
        socket.reconcileRecoveryCandidateIds?.has(connectorId) === true &&
        registry.getPendingSocketsForRecovery(connectorId)
            .includes(socket);

    if (!valid()) {
        return Promise.reject(
            new Error("RECONCILE_RECOVERY_PROBE_CANDIDATE_CHANGED")
        );
    }

    const requestId = randomUUID();

    return new Promise((resolve, reject) => {
        let finished = false;
        let timer;

        const cleanup = () => {
            clearTimeout(timer);
            socket.off(RESULT_EVENT, onResult);
            socket.off("disconnect", onDisconnect);
        };

        const finish = (error, value) => {
            if (finished) return;

            finished = true;
            cleanup();

            if (error) reject(error);
            else resolve(value);
        };

        const onDisconnect = () =>
            finish(
                new Error("RECONCILE_RECOVERY_PROBE_DISCONNECTED")
            );

        const onResult = data => {
            if (data?.requestId !== requestId) return;

            if (!valid()) {
                return finish(
                    new Error("RECONCILE_RECOVERY_PROBE_CANDIDATE_CHANGED")
                );
            }

            const identity = data?.identity;

            if (
                data.success !== true ||
                data.settlementEligible !== false ||
                !identity ||
                typeof identity !== "object" ||
                !UUID.test(identity.processEpoch) ||
                !Number.isSafeInteger(identity.generation) ||
                identity.generation < 0 ||
                typeof identity.fenced !== "boolean" ||
                (
                    identity.fenceId !== null &&
                    !UUID.test(identity.fenceId)
                ) ||
                !Number.isSafeInteger(identity.pending) ||
                identity.pending < 0 ||
                !Number.isSafeInteger(identity.running) ||
                identity.running < 0 ||
                typeof identity.drained !== "boolean" ||
                typeof identity.transportOutcomeKnown !== "boolean" ||
                identity.durableExecutionEvidence !== false ||
                identity.settlementEligible !== false
            ) {
                return finish(
                    new Error("RECONCILE_RECOVERY_PROBE_RESULT_INVALID")
                );
            }

            finish(null, {
                connectorId,
                socketId: socket.id,
                identity,
                settlementEligible: false
            });
        };

        socket.on(RESULT_EVENT, onResult);
        socket.once("disconnect", onDisconnect);

        timer = setTimeout(
            () => finish(
                new Error("RECONCILE_RECOVERY_PROBE_TIMEOUT")
            ),
            timeoutMs
        );

        if (!valid()) {
            finish(
                new Error("RECONCILE_RECOVERY_PROBE_CANDIDATE_CHANGED")
            );
            return;
        }

        socket.emit(REQUEST_EVENT, { requestId });
    });
}

module.exports = {
    sendManualReconcileRecoveryIdentityRequest
};
