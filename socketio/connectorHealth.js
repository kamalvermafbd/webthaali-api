
"use strict";

const {
    updateConnectorHealthAtomic,
    getConnectorHealth
} = require("./connectorHealthStore");

async function updateConnectorHealth(connectorId, patch) {
    if (!connectorId) {
        throw new Error("connectorId is required");
    }

    if (
        !patch ||
        typeof patch !== "object" ||
        Array.isArray(patch)
    ) {
        throw new Error("patch must be an object");
    }

    return updateConnectorHealthAtomic(
        connectorId,
        patch
    );
}

async function getSharedConnectorHealth(connectorId) {
    return getConnectorHealth(connectorId);
}

function publishConnectorHealth(connectorId, patch) {
    updateConnectorHealth(connectorId, patch)
        .catch(error => {
            console.error(
                "[CONNECTOR HEALTH] Update failed:",
                {
                    connector_id: connectorId,
                    error: error.message
                }
            );
        });
}

module.exports = {
    updateConnectorHealth,
    getSharedConnectorHealth,
    publishConnectorHealth
};
