"use strict";

const {
    withGlobalConnectorLock
} = require("./connectorOperationCoordinator");

const {
    sendReconcileRequest
} = require("./sendReconcileRequest");

async function sendReconcileRequestWithGlobalLock(
    connectorId,
    event,
    payload
) {
    const id = String(connectorId || "").trim();

    if (!id) {
        throw new Error(
            "RECONCILE_CONNECTOR_ID_REQUIRED"
        );
    }

    return withGlobalConnectorLock(
        id,
        (lockedSocket, signal) =>
            sendReconcileRequest(
                lockedSocket,
                event,
                payload,
                { signal }
            )
    );
}

module.exports = {
    sendReconcileRequestWithGlobalLock
};