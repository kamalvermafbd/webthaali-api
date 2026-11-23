
"use strict";

// Recovery-only socket binding. This module never registers connector ownership,
// clears quarantine, or asserts remote execution settlement.


const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const MAX_TTL_MS = 60_000;
const bindings = new Map();

function validUuid(value) {
    return typeof value === "string" && UUID.test(value);
}

function keyOf(connectorId, attemptId) {
    return `${connectorId}\0${attemptId.toLowerCase()}`;
}

function revokeBinding(binding) {
    if (bindings.get(binding.key) !== binding) return false;
    bindings.delete(binding.key);
    binding.socket.off("disconnect", binding.onDisconnect);
    return true;
}


/**
 * Recovery transport authorization is disabled until
 * a protected durable authorization verifier exists.
 *
 * A caller-provided authorization object is not proof.
 * Never bind a quarantined connector using a boolean
 * or a process-local identity observation.
 */
function bindAuthorizedRecoveryTransport() {
    throw new Error(
        "RECONCILE_RECOVERY_DURABLE_AUTHORIZATION_NOT_IMPLEMENTED"
    );
}

function revokeRecoveryTransportsForSocket(socket) {
    if (!socket) return 0;

    let revoked = 0;

    for (const binding of bindings.values()) {
        if (
            binding.socket === socket &&
            revokeBinding(binding)
        ) {
            revoked += 1;
        }
    }

    return revoked;
}
function getAuthorizedRecoveryTransport({
    connectorId, attemptId, executionId, fenceId, authorizationId
} = {}) {
    if (typeof connectorId !== "string" || !validUuid(attemptId)) return null;

    const binding = bindings.get(keyOf(connectorId, attemptId));
    if (!binding) return null;

    if (
        binding.socket.connected !== true ||
        binding.socket.id !== binding.socketId ||
        Date.now() >= binding.expiresAt
    ) {
        revokeBinding(binding);
        return null;
    }

    if (
        executionId &&
        (!validUuid(executionId) ||
            binding.executionId !== executionId.toLowerCase())
    ) return null;

    if (
        fenceId &&
        (!validUuid(fenceId) ||
            binding.fenceId !== fenceId.toLowerCase())
    ) return null;

    if (
        authorizationId &&
        (!validUuid(authorizationId) ||
            binding.authorizationId !== authorizationId.toLowerCase())
    ) return null;

    // Diagnostic only. Consumers must still verify the durable authorization
    // and remote evidence before using any response for settlement.
    return Object.freeze({
        socket: binding.socket,
        socketId: binding.socketId,
        connectorId: binding.connectorId,
        attemptId: binding.attemptId,
        executionId: binding.executionId,
        fenceId: binding.fenceId,
        authorizationId: binding.authorizationId,
        expiresAt: binding.expiresAt,
        settlementEligible: false
    });
}

module.exports = {
    bindAuthorizedRecoveryTransport,
    getAuthorizedRecoveryTransport,
    revokeRecoveryTransportsForSocket
};
