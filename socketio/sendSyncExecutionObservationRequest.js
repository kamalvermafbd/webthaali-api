"use strict";

const { randomUUID } = require("crypto");
const registry = require("./connectorRegistry");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const REQUEST_EVENT = "sync:observeExecution";
const RESULT_EVENT = "sync:observeExecutionResult";

function isUuid(value) {
    return typeof value === "string" &&
        UUID.test(value);
}

function isOwnedSocket(
    socket,
    connectorId,
    expectedSocketId
) {
    return Boolean(
        socket &&
        socket.connected === true &&
        socket.id === expectedSocketId &&
        registry.get(connectorId) === socket &&
        (
            socket.connectorIds?.has(connectorId) ||
            socket.connectorId === connectorId
        )
    );
}

// Read-only observation.
// This function never authorizes Redis cleanup.

async function sendSyncExecutionObservationRequest({
    connectorId,
    expectedSocketId,
    expectedConnectorEpoch,
    requestId,
    identityScope,
    operation,
    companyIdentity,
    payloadFingerprint,
    timeoutMs = 5000
} = {}) {

    if (
        typeof connectorId !== "string" ||
        !connectorId.trim() ||
        typeof expectedSocketId !== "string" ||
        !expectedSocketId.trim() ||
        !isUuid(expectedConnectorEpoch) ||
        !isUuid(requestId) ||
        typeof identityScope !== "string" ||
        !identityScope.trim() ||
        typeof operation !== "string" ||
        !operation.trim() ||
        typeof companyIdentity !== "string" ||
        !companyIdentity.startsWith("guid:") ||
        companyIdentity.length <= 5 ||
        typeof payloadFingerprint !== "string" ||
        !/^sha256:[0-9a-f]{64}$/i.test(
            payloadFingerprint
        ) ||
        !Number.isSafeInteger(timeoutMs) ||
        timeoutMs < 1 ||
        timeoutMs > 60000
    ) {
        throw new Error(
            "SYNC_OBSERVATION_ARGUMENTS_INVALID"
        );
    }

    const socket = registry.get(connectorId);

    if (!isOwnedSocket(
        socket,
        connectorId,
        expectedSocketId
    )) {
        throw new Error(
            "SYNC_OBSERVATION_SOCKET_NOT_OWNED"
        );
    }

    const observationId = randomUUID();

    return new Promise((resolve, reject) => {

        let finished = false;
        let timer;

        function cleanup() {
            clearTimeout(timer);

            socket.off(
                RESULT_EVENT,
                onResult
            );

            socket.off(
                "disconnect",
                onDisconnect
            );
        }

        function finish(error, result) {
            if (finished) return;

            finished = true;
            cleanup();

            if (error) reject(error);
            else resolve(result);
        }

        function onDisconnect() {
            finish(
                new Error(
                    "SYNC_OBSERVATION_SOCKET_DISCONNECTED"
                )
            );
        }

        function onResult(data) {

            if (
                data?.observationId !== observationId
            ) {
                return;
            }

            if (!isOwnedSocket(
                socket,
                connectorId,
                expectedSocketId
            )) {
                finish(
                    new Error(
                        "SYNC_OBSERVATION_SOCKET_OWNERSHIP_LOST"
                    )
                );

                return;
            }

            const execution = data.execution;
            const queue = data.queueStatus;

            if (
                data.requestId !== requestId ||
                data.success !== true ||
                data.settlementEligible !== false ||
                data.executionKnown !== true ||
                data.identityMatches !== true ||
                data.executionFinished !== true ||
                data.locallyDrained !== true ||
                !execution ||
                execution.requestId !== requestId ||
                execution.scope !== identityScope ||
                execution.operation !== operation ||
                execution.companyIdentity !==
                    companyIdentity ||
                execution.payloadFingerprint !==
                    payloadFingerprint ||
                execution.protectedAgainstRetransmission
                    !== true ||
                execution.state !== "CALLBACK_FINISHED" ||
                !queue ||
                queue.connectorEpoch !==
                    expectedConnectorEpoch ||
                queue.idle !== true ||
                queue.pendingOperations !== 0 ||
                queue.runningOperations !== 0 ||
                !Number.isSafeInteger(
                    queue.operationGeneration
                ) ||
                queue.operationGeneration < 0
            ) {
                finish(
                    new Error(
                        "SYNC_OBSERVATION_NOT_VERIFIED"
                    )
                );

                return;
            }

            finish(null, Object.freeze({
                observationId,
                requestId,
                connectorId,
                socketId: expectedSocketId,
                connectorEpoch: expectedConnectorEpoch,
                identityScope,
                operation,
                companyIdentity,
                payloadFingerprint,
                executionKnown: true,
                identityMatches: true,
                executionFinished: true,
                locallyDrained: true,
                operationGeneration:
                    queue.operationGeneration,

                // Observation is not recovery authorization.
                settlementEligible: false
            }));
        }

        socket.on(
            RESULT_EVENT,
            onResult
        );

        socket.once(
            "disconnect",
            onDisconnect
        );

        timer = setTimeout(
            () => finish(
                new Error(
                    "SYNC_OBSERVATION_ACK_TIMEOUT"
                )
            ),
            timeoutMs
        );

        if (!isOwnedSocket(
            socket,
            connectorId,
            expectedSocketId
        )) {
            finish(
                new Error(
                    "SYNC_OBSERVATION_SOCKET_OWNERSHIP_LOST"
                )
            );

            return;
        }

        try {
            socket.emit(REQUEST_EVENT, {
                observationId,
                requestId,
                identityScope,
                operation,
                companyIdentity,
                payloadFingerprint
            });

        } catch {
            finish(
                new Error(
                    "SYNC_OBSERVATION_EMIT_FAILED"
                )
            );
        }
    });
}

module.exports = {
    sendSyncExecutionObservationRequest
};