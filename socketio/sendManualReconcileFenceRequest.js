
"use strict";

const { randomUUID } = require("crypto");
const registry = require("./connectorRegistry");

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const RESULT_EVENT = "reconcile:installFenceResult";
const REQUEST_EVENT = "reconcile:installFence";

function isUuid(value) {
    return typeof value === "string" && UUID.test(value);
}

function isOwnedSocket(socket, connectorId, expectedSocketId) {
    return Boolean(
        socket &&
        socket.connected === true &&
        socket.id === expectedSocketId &&
        registry.get(connectorId) === socket &&
        (socket.connectorIds?.has(connectorId) || socket.connectorId === connectorId)
    );
}

/**
 * Transport only. The caller must independently validate the original
 * durable attempt/execution/connector ownership before calling this function.
 * A successful response is NOT durable quiescence or settlement evidence.
 */
function sendManualReconcileFenceRequest({
    connectorId,
    expectedSocketId,
    attemptId,
    executionId,
    fenceId,
    timeoutMs = 10000
} = {}) {
    if (
        typeof connectorId !== "string" || !connectorId.trim() ||
        typeof expectedSocketId !== "string" || !expectedSocketId.trim() ||
        !isUuid(attemptId) || !isUuid(executionId) || !isUuid(fenceId) ||
        !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000
    ) {
        return Promise.reject(new Error("RECONCILE_FENCE_ARGUMENTS_INVALID"));
    }

    const socket = registry.get(connectorId);
    if (!isOwnedSocket(socket, connectorId, expectedSocketId)) {
        return Promise.reject(new Error("RECONCILE_FENCE_SOCKET_NOT_OWNED"));
    }

    const requestId = randomUUID();

    return new Promise((resolve, reject) => {
        let finished = false;
        let timer;

        function cleanup() {
            clearTimeout(timer);
            socket.off(RESULT_EVENT, onResult);
            socket.off("disconnect", onDisconnect);
        }

        function finish(error, result) {
            if (finished) return;
            finished = true;
            cleanup();
            if (error) reject(error);
            else resolve(result);
        }

        function onDisconnect() {
            finish(new Error("RECONCILE_FENCE_SOCKET_DISCONNECTED"));
        }

        function onResult(data) {
            if (data?.requestId !== requestId) return;

            if (!isOwnedSocket(socket, connectorId, expectedSocketId)) {
                finish(new Error("RECONCILE_FENCE_SOCKET_OWNERSHIP_LOST"));
                return;
            }

         if (
    data.success !== true ||
    data.settlementEligible !== false ||
    data.fenceId !== fenceId ||
    !isUuid(data.attemptId) ||
    !isUuid(data.executionId) ||
    data.attemptId.toLowerCase() !== attemptId.toLowerCase() ||
    data.executionId.toLowerCase() !== executionId.toLowerCase()
) {
    finish(new Error("RECONCILE_FENCE_ACK_INVALID"));
    return;
}

          const s = data.snapshot;

if (
    !s ||
    typeof s !== "object" ||
    Array.isArray(s) ||
    !isUuid(s.processEpoch) ||
    !Number.isSafeInteger(s.generation) ||
    s.generation < 0 ||
    s.fenced !== true ||
    !isUuid(s.fenceId) ||
    s.fenceId.toLowerCase() !== fenceId.toLowerCase() ||
    !Number.isSafeInteger(s.pending) ||
    s.pending < 0 ||
    !Number.isSafeInteger(s.running) ||
    s.running < 0 ||
    typeof s.drained !== "boolean" ||
    typeof s.executionKnown !== "boolean" ||
    typeof s.identityConflict !== "boolean" ||
    s.settlementEligible !== false ||
    (s.executionKnown === true && s.identityConflict === true)
) {
    finish(new Error("RECONCILE_FENCE_SNAPSHOT_INVALID"));
    return;
}

            finish(null, {
                requestId,
                fenceId,
                attemptId,
                executionId,
                connectorId,
                socketId: expectedSocketId,
                snapshot: {
                    processEpoch: s.processEpoch,
                    generation: s.generation,
                    fenced: true,
                    fenceId: s.fenceId,
                    pending: s.pending,
                    running: s.running,
                    drained: s.drained,
                    executionKnown: s.executionKnown,
                    identityConflict: s.identityConflict,
                    settlementEligible: false
                },
                // This result is transport evidence only.
                settlementEligible: false
            });
        }

        socket.on(RESULT_EVENT, onResult);
        socket.once("disconnect", onDisconnect);
        timer = setTimeout(
            () => finish(new Error("RECONCILE_FENCE_ACK_TIMEOUT")),
            timeoutMs
        );

        if (!isOwnedSocket(socket, connectorId, expectedSocketId)) {
            finish(new Error("RECONCILE_FENCE_SOCKET_OWNERSHIP_LOST"));
            return;
        }

        try {
            socket.emit(REQUEST_EVENT, {
                requestId,
                fenceId,
                attemptId,
                executionId
            });
        } catch (_error) {
            finish(new Error("RECONCILE_FENCE_EMIT_FAILED"));
        }
    });
}

module.exports = { sendManualReconcileFenceRequest };
