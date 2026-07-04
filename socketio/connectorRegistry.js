const connectors = new Map();

/**
 * Register Connector
 */
function register(companyCode, socket) {

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
 * Remove Connector
 */
function remove(companyCode) {

    connectors.delete(companyCode);

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

    remove,

    isOnline,

    list

};