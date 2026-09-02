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

    if (!connectorId) {
        console.error("❌ REGISTER REJECTED: connector_id missing");
        return false;
    }

    if (!socket) {
        console.error("❌ REGISTER REJECTED: socket missing");
        return false;
    }

    // Socket ki identity pehle set honi chahiye
    if (socket.connectorId !== connectorId) {

        console.error("❌ REGISTER REJECTED: connector identity mismatch", {
            connector_id: connectorId,
            socket_connector_id: socket.connectorId,
            socket_id: socket.id
        });

        return false;
    }

    pendingConnectors.delete(socket);

    // Remove old mappings belonging to this socket
    for (const [oldConnectorId, oldSocket] of connectors.entries()) {

        if (
            oldSocket === socket &&
            oldConnectorId !== connectorId
        ) {

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

    return true;
}

function get(connectorId, companyCode) {

    const socket = connectors.get(connectorId);

    if (!socket) {
        console.log(
            "REGISTRY GET: CONNECTOR NOT FOUND",
            connectorId
        );
        return null;
    }

    if (socket.connectorId !== connectorId) {

        console.error(
            "❌ REGISTRY GET REJECTED: CONNECTOR ID MISMATCH",
            {
                connector_id: connectorId,
                socket_connector_id: socket.connectorId,
                socket_id: socket.id
            }
        );

        return null;
    }

    if (
        companyCode &&
        socket.companyCode !== companyCode
    ) {

        console.error(
            "❌ REGISTRY GET REJECTED: COMPANY CODE MISMATCH",
            {
                requested_company_code: companyCode,
                socket_company_code: socket.companyCode,
                connector_id: connectorId,
                socket_id: socket.id
            }
        );

        return null;
    }

    console.log(
        "REGISTRY GET VALID:",
        {
            connector_id: connectorId,
            company_code: socket.companyCode,
            socket_id: socket.id
        }
    );

    return socket;
}

function waitForConnector(
    connectorId,
    companyCode,
    timeout = 10000
) {

    return new Promise((resolve, reject) => {

        const findValidSocket = () => {

            const socket =
                connectors.get(connectorId);

            if (!socket) {
                return null;
            }

            if (socket.connectorId !== connectorId) {

                console.error(
                    "❌ WAIT CONNECTOR REJECTED: CONNECTOR ID MISMATCH",
                    {
                        requested_connector_id: connectorId,
                        socket_connector_id: socket.connectorId,
                        socket_id: socket.id
                    }
                );

                return null;
            }

            if (
                companyCode &&
                socket.companyCode !== companyCode
            ) {

                console.error(
                    "❌ WAIT CONNECTOR REJECTED: COMPANY CODE MISMATCH",
                    {
                        requested_company_code: companyCode,
                        socket_company_code: socket.companyCode,
                        connector_id: connectorId,
                        socket_id: socket.id
                    }
                );

                return null;
            }

            return socket;
        };

        const existing =
            findValidSocket();

        if (existing) {
            return resolve(existing);
        }

        const startedAt = Date.now();

        const timer = setInterval(() => {

            const socket =
                findValidSocket();

            if (socket) {

                clearInterval(timer);

                console.log(
                    "WAIT CONNECTOR FOUND:",
                    {
                        connector_id: connectorId,
                        company_code: companyCode,
                        socket_id: socket.id
                    }
                );

                return resolve(socket);
            }

            if (
                Date.now() - startedAt >=
                timeout
            ) {

                clearInterval(timer);

                reject(
                    new Error(
                        `Connector offline: ${connectorId}`
                    )
                );
            }

        }, 100);

    });

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
    waitForConnector,
    getAny,
    registerPending,
    getPending,
    remove,
    isOnline,
    list

};