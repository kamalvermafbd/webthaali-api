"use strict";

const sendToConnector = require("./sendToConnector");

const {
    withGlobalConnectorLock
} = require("./connectorOperationCoordinator");

/**
 * Send a Tally operation while holding the global
 * Redis reservation for the specified connector.
 *
 * Do not call this helper from inside an existing
 * withGlobalConnectorLock() callback.
 */
async function sendToConnectorWithGlobalLock(
    connectorId,
    event,
    payload,
    options = {}
) {
    const id = String(connectorId || "").trim();

    if (!id) {
        throw new Error(
            "CONNECTOR_ID_REQUIRED_FOR_GLOBAL_LOCK"
        );
    }

    return withGlobalConnectorLock(
        id,
        (lockedSocket, signal) =>
            sendToConnector(
                lockedSocket,
                event,
                payload,
                {
                    ...options,
                    signal
                }
            )
    );
}

module.exports = sendToConnectorWithGlobalLock;