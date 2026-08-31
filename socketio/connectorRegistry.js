const connectors = new Map();
const pendingConnectors = new Set();

/**
 * Register Connector
 */
function register(connectorId, socket) {

    pendingConnectors.delete(socket);

    connectors.set(connectorId, socket);

    console.log(
        `✅ Registered Connector : ${connectorId}`
    );

}
/**
 * Get Connector
 */
function get(connectorId) {

    return connectors.get(connectorId);

}

/**
 * Get Any Connector
 */
function getAny() {

    return connectors.values().next().value;

}

function registerPending(socket) {

    pendingConnectors.add(socket);

    console.log(
        `🆕 Pending Connector : ${socket.id}`
    );

}

function getPending() {

    return pendingConnectors.values().next().value;

}

/**
 * Remove Connector
 */
function remove(connectorId, socket) {

    const currentSocket =
        connectors.get(connectorId);

    if (currentSocket === socket) {

        connectors.delete(connectorId);

        console.log(
            `❌ Removed Connector : ${connectorId}`
        );

    }

    pendingConnectors.delete(socket);

}

/**
 * Check Connector
 */
function isOnline(connectorId) {

    return connectors.has(connectorId);

}

/**
 * List Connected Companies
 */
function list() {

    return Array.from(connectors.keys());

}

module.exports = {

    register,
    get,
    getAny,
    registerPending,
    getPending,
    remove,
    isOnline,
    list

};