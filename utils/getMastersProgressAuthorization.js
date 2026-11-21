
"use strict";

const authorizations = new Map();

function registerGetMastersProgress({
    requestId,
    batchId,
    connectorId,
    socket,
    executionClaimToken
}) {
    if (
        typeof requestId !== "string" ||
        !requestId ||
        typeof batchId !== "string" ||
        !batchId ||
        connectorId == null ||
        !String(connectorId) ||
        !socket?.id ||
        typeof executionClaimToken !== "string" ||
        !executionClaimToken
    ) {
        throw new Error(
            "INVALID_GET_MASTERS_PROGRESS_AUTHORIZATION"
        );
    }

    if (authorizations.has(requestId)) {
        throw new Error(
            "GET_MASTERS_PROGRESS_AUTHORIZATION_EXISTS"
        );
    }

    const authorization = {
        requestId,
        batchId,
        connectorId: String(connectorId),
        socket,
        socketId: socket.id,
        executionClaimToken,
        active: false
    };

    authorizations.set(requestId, authorization);

    return authorization;
}

function activateGetMastersProgress(authorization) {
    if (
        !authorization ||
        authorizations.get(authorization.requestId) !==
            authorization ||
        authorization.active === true ||
        authorization.socket?.connected !== true ||
        authorization.socket.id !== authorization.socketId
    ) {
        throw new Error(
            "GET_MASTERS_PROGRESS_ACTIVATION_REJECTED"
        );
    }

    authorization.active = true;
}

function verifyGetMastersProgress({
    socket,
    requestId,
    batchId
}) {
    if (
        typeof requestId !== "string" ||
        typeof batchId !== "string"
    ) {
        return null;
    }

    const authorization = authorizations.get(requestId);

    if (
        !authorization ||
        authorization.active !== true ||
        authorization.socket !== socket ||
        authorization.socketId !== socket?.id ||
        socket.connected !== true ||
        authorization.batchId !== batchId
    ) {
        return null;
    }

    return {
        batchId: authorization.batchId,
        connectorId: authorization.connectorId,
        executionClaimToken:
            authorization.executionClaimToken
    };
}

function revokeGetMastersProgress(authorization) {
    if (!authorization) {
        return;
    }

    if (
        authorizations.get(authorization.requestId) ===
        authorization
    ) {
        authorization.active = false;
        authorizations.delete(authorization.requestId);
    }
}

function revokeGetMastersProgressForSocket(socket) {
    for (const authorization of authorizations.values()) {
        if (authorization.socket === socket) {
            revokeGetMastersProgress(authorization);
        }
    }
}

module.exports = {
    registerGetMastersProgress,
    activateGetMastersProgress,
    verifyGetMastersProgress,
    revokeGetMastersProgress,
    revokeGetMastersProgressForSocket
};
