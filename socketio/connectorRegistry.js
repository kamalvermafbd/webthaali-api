const connectors = new Map();
const pendingConnectors = new Set();

/**
 * Register Connector
 */
function register(companyCode, socket) {

    pendingConnectors.delete(socket);

    connectors.set(companyCode, socket);

    console.log(`✅ Registered : ${companyCode}`);

}
/**
 * Get Connector
 */
function get(companyCode) {

    return connectors.get(companyCode);

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
function remove(companyCode) {

    const socket = connectors.get(companyCode);

    connectors.delete(companyCode);

    if (socket) {
        pendingConnectors.delete(socket);
    }

    console.log(`❌ Removed : ${companyCode}`);

}

/**
 * Check Connector
 */
function isOnline(companyCode) {

    return connectors.has(companyCode);

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