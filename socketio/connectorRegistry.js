const connectors = new Map();
const pendingConnectors = new Set();

/**
 * Register Connector
 */

/*010926 commented
function register(connectorId, socket) {

    pendingConnectors.delete(socket);

    connectors.set(connectorId, socket);

    console.log(
        `✅ Registered Connector : ${connectorId}`
    );

}

*/
/**
 * Get Connector
 */

/*
function get(connectorId) {

    return connectors.get(connectorId);

}
*/

function register(connectorId, socket) {

    pendingConnectors.delete(socket);

    // Remove any old connector ID belonging to this socket
    for (const [oldConnectorId, oldSocket] of connectors.entries()) {

        if (oldSocket === socket &&
            oldConnectorId !== connectorId) {

            connectors.delete(oldConnectorId);

            console.log(
                `♻️ Removed Old Connector Mapping : ${oldConnectorId}`
            );

        }

    }

    connectors.set(connectorId, socket);

    console.log("REGISTRY SET TRACE:", {
    connector_id: connectorId,
    socket_id: socket.id,
    socket_connector_id: socket.connectorId,
    socket_company_code: socket.companyCode
});

    console.log(
        `✅ Registered Connector : ${connectorId}`
    );

}

function get(connectorId) {

    const socket = connectors.get(connectorId);

    console.log(
        "REGISTRY GET:",
        connectorId,
        "SOCKET:",
        socket?.id,
        "SOCKET CONNECTOR:",
        socket?.connectorId
    );

    return socket;
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