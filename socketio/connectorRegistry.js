const connectors = new Map();
const pendingConnectors = new Set();
const pendingConnectorMap = new Map();

let currentPendingConnector = null;


// Logical company registrations are separate from socket aliases and Redis ownership.
function registrationError(code) {
    const error = new Error(code);
    error.code = code;
    return error;
}

function cleanText(value) {
    return typeof value === "string" ? value.trim() : "";
}

function companyRegistrationKey(companyCode, tallyOwner) {
    return JSON.stringify([companyCode, tallyOwner]);
}

function initializeRegistrationState(socket) {
    if (!(socket.connectorIds instanceof Set)) {
        socket.connectorIds = new Set();
    }

    if (!(socket.companyRegistrations instanceof Map)) {
        socket.companyRegistrations = new Map();
    }

    if (!(socket.availableCompanyGuids instanceof Set)) {
        socket.availableCompanyGuids = new Set(
            Array.isArray(socket.companyGuids)
                ? socket.companyGuids
                : []
        );
    }

    if (!socket.registrationQueue) {
        socket.registrationQueue = {
            tail: Promise.resolve(),
            pending: 0
        };
    }
}

function registrationHandler(socket, handler) {
    initializeRegistrationState(socket);

    return function receiveRegistration(data) {
        const queue = socket.registrationQueue;

        if (!socket.connected) return;

        if (queue.pending >= 256) {
            socket.emit("connector:registration:status", {
                connector_id: cleanText(data?.connector_id) || null,
                registered: false,
                reason: "REGISTRATION_QUEUE_FULL",
                socket_id: socket.id
            });
            return;
        }

        let payload;

        try {
            payload = structuredClone(data ?? {});
        } catch {
            socket.emit("connector:registration:status", {
                connector_id: null,
                registered: false,
                reason: "INVALID_REGISTRATION_PAYLOAD",
                socket_id: socket.id
            });
            return;
        }

        queue.pending += 1;

        const run = async () => {
            if (socket.connected) {
                await handler(payload);
            }
        };

        queue.tail = queue.tail
            .then(run)
            .catch(error => {
                if (socket.connected) {
                    socket.emit("connector:registration:status", {
                        connector_id:
                            cleanText(payload?.connector_id) || null,
                        registered: false,
                        reason: error.code || "REGISTRATION_FAILED",
                        socket_id: socket.id
                    });
                }

                console.error("[CONNECTOR REGISTRATION]", {
                    socket_id: socket.id,
                    reason: error.code || "REGISTRATION_FAILED"
                });
            })
            .finally(() => {
                queue.pending -= 1;
            });
    };
}

function applyCompanyAvailability(socket, data) {
    initializeRegistrationState(socket);

    const mode = data?.company_guids_mode ?? "add";

    if (mode !== "snapshot" && mode !== "add") {
        throw registrationError("INVALID_AVAILABILITY_MODE");
    }

    if (
        !Array.isArray(data?.company_guids) ||
        data.company_guids.some(
            guid => typeof guid !== "string"
        )
    ) {
        throw registrationError("INVALID_COMPANY_GUIDS");
    }

    const received = [
        ...new Set(
            data.company_guids
                .map(guid => guid.trim())
                .filter(Boolean)
        )
    ];

    if (mode === "snapshot") {
        socket.availableCompanyGuids = new Set(received);
    } else {
        for (const guid of received) {
            socket.availableCompanyGuids.add(guid);
        }
    }

    socket.companyGuids = [...socket.availableCompanyGuids];

    for (const [key, registration] of socket.companyRegistrations) {
        const available = socket.availableCompanyGuids.has(
            registration.tally_company_guid
        );

        if (registration.available !== available) {
            socket.companyRegistrations.set(
                key,
                Object.freeze({
                    ...registration,
                    available
                })
            );
        }
    }

    return [...socket.companyGuids];
}

function normalizeCompanyRegistration(input) {
    const registration = {
        company_code: cleanText(input?.company_code),
        tally_owner: cleanText(input?.tally_owner).toUpperCase(),
        tally_company_guid: cleanText(
            input?.tally_company_guid ?? input?.company_guid
        ),
        connector_id: cleanText(input?.connector_id)
    };

    if (
        !registration.company_code ||
        !registration.tally_company_guid ||
        !registration.connector_id ||
        !["USER", "CA"].includes(registration.tally_owner)
    ) {
        throw registrationError("INCOMPLETE_COMPANY_REGISTRATION");
    }

    return registration;
}

function assertCompanyRegistration(socket, input) {
    initializeRegistrationState(socket);

    const registration = normalizeCompanyRegistration(input);

    const existing = socket.companyRegistrations.get(
        companyRegistrationKey(
            registration.company_code,
            registration.tally_owner
        )
    );

    if (
        existing &&
        (
            existing.connector_id !== registration.connector_id ||
            existing.tally_company_guid !==
                registration.tally_company_guid
        )
    ) {
        throw registrationError("COMPANY_REGISTRATION_CONFLICT");
    }

    return registration;
}

function prepareCompanyRegistrations(socket, inputs) {
    initializeRegistrationState(socket);

    const prepared = new Map();

    for (const input of inputs) {
        const registration = assertCompanyRegistration(
            socket,
            input
        );

        const key = companyRegistrationKey(
            registration.company_code,
            registration.tally_owner
        );

        const previous = prepared.get(key);

        if (
            previous &&
            (
                previous.connector_id !== registration.connector_id ||
                previous.tally_company_guid !==
                    registration.tally_company_guid
            )
        ) {
            throw registrationError("COMPANY_REGISTRATION_CONFLICT");
        }

        prepared.set(key, registration);
    }

    return [...prepared.values()];
}

function commitCompanyRegistrations(socket, inputs) {
    const registrations = prepareCompanyRegistrations(
        socket,
        inputs
    );

    // Validate every context before modifying local state.
    for (const registration of registrations) {
        if (
            !socket.connected ||
            connectors.get(registration.connector_id) !== socket ||
            !socket.connectorIds.has(registration.connector_id)
        ) {
            throw registrationError(
                "REGISTRATION_SOCKET_NOT_CURRENT"
            );
        }
    }

    // No awaits or external calls during this local commit.
    for (const registration of registrations) {
        const key = companyRegistrationKey(
            registration.company_code,
            registration.tally_owner
        );

        if (!socket.companyRegistrations.has(key)) {
            socket.companyRegistrations.set(
                key,
                Object.freeze({
                    ...registration,
                    available: socket.availableCompanyGuids.has(
                        registration.tally_company_guid
                    )
                })
            );
        }
    }

    return registrations;
}

function emitCompanyRegistration(socket, input, reason) {
    const registration = normalizeCompanyRegistration(input);

    socket.emit("connector:registration:status", {
        ...registration,
        registered: true,
        socket_id: socket.id,
        ...(reason ? { reason } : {})
    });
}

// Legacy alias registration must not be blocked when its
// company-side context cannot be resolved.
async function resolveDirectRegistration(supabase, input) {
    const data = input ?? {};

    try {
        const connectorId = cleanText(data.connector_id);

        if (!connectorId) {
            return { data, contexts: [] };
        }

        const [clientResult, caResult] = await Promise.all([
            supabase
                .from("company")
                .select(
                    "company_code, client_connector_id, client_tally_company_guid"
                )
                .eq("client_connector_id", connectorId),

            supabase
                .from("company")
                .select(
                    "company_code, ca_connector_id, ca_tally_company_guid"
                )
                .eq("ca_connector_id", connectorId)
        ]);

        if (clientResult.error || caResult.error) {
            return { data, contexts: [] };
        }

        const candidates = [
            ...(clientResult.data || []).map(row => ({
                company_code: row.company_code,
                tally_owner: "USER",
                connector_id: row.client_connector_id,
                tally_company_guid: row.client_tally_company_guid
            })),

            ...(caResult.data || []).map(row => ({
                company_code: row.company_code,
                tally_owner: "CA",
                connector_id: row.ca_connector_id,
                tally_company_guid: row.ca_tally_company_guid
            }))
        ].filter(row =>
            (
                !cleanText(data.company_code) ||
                row.company_code === cleanText(data.company_code)
            ) &&
            (
                !cleanText(data.tally_owner) ||
                row.tally_owner ===
                    cleanText(data.tally_owner).toUpperCase()
            )
        );

        if (candidates.length !== 1) {
            return { data, contexts: [] };
        }

        
const databaseGuid = cleanText(
    candidates[0].tally_company_guid
);

const incomingGuid = cleanText(
    data.tally_company_guid ?? data.company_guid
);

if (
    !databaseGuid ||
    (incomingGuid && incomingGuid !== databaseGuid)
) {
    return { data, contexts: [] };
}

const context = normalizeCompanyRegistration({
    ...candidates[0],
    tally_company_guid: databaseGuid
});


        return {
            data,
            contexts: [context]
        };
    } catch {
        return {
            data,
            contexts: []
        };
    }
}


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
    if (
        typeof connectorId !== "string" ||
        !connectorId.trim()
    ) {
        console.error(
            "❌ REGISTER REJECTED: connector_id missing"
        );
        return false;
    }

    if (!socket || socket.connected !== true) {
        console.error(
            "❌ REGISTER REJECTED: socket unavailable",
            {
                connector_id: connectorId,
                socket_id: socket?.id || null
            }
        );
        return false;
    }

    const normalizedConnectorId = connectorId.trim();

    initializeRegistrationState(socket);

    // Check existing alias owner BEFORE modifying
    // the incoming socket's local registration state.
    const existingSocket =
        connectors.get(normalizedConnectorId);

    if (
        existingSocket &&
        existingSocket !== socket &&
        existingSocket.connected === true
    ) {
        console.error(
            "❌ REGISTRY REJECTED: CONNECTOR ALREADY HAS ACTIVE SOCKET",
            {
                connector_id: normalizedConnectorId,
                existing_socket_id: existingSocket.id,
                incoming_socket_id: socket.id
            }
        );

        // Do not delete from socket.connectorIds here.
        // A rejected attempt must not mutate existing state.
        return false;
    }

    // A disconnected previous socket can leave an old
    // alias mapping. Clean only that old socket's
    // local state before replacing the mapping.
    if (
        existingSocket &&
        existingSocket !== socket
    ) {
        existingSocket.connectorIds?.delete(
            normalizedConnectorId
        );

        for (
            const [key, registration] of
            existingSocket.companyRegistrations || []
        ) {
            if (
                registration.connector_id ===
                normalizedConnectorId
            ) {
                existingSocket.companyRegistrations.delete(key);
            }
        }
    }

    // Local registry commit: no await between these writes.
    // Local registry commit: no await between these writes.
connectors.set(normalizedConnectorId, socket);
socket.connectorIds.add(normalizedConnectorId);

// Every successful registration gets a fresh local generation.
// Even when the same physical socket registers again.
if (!(socket.connectorRegistrationGenerations instanceof Map)) {
    socket.connectorRegistrationGenerations = new Map();
}

const previousGeneration =
    socket.connectorRegistrationGenerations.get(normalizedConnectorId) || 0;

socket.connectorRegistrationGenerations.set(
    normalizedConnectorId,
    previousGeneration + 1
);

    // Preserve existing pending-connector behavior.
    if (!socket.pendingConnectorId) {
        pendingConnectors.delete(socket);
    }

    console.log("REGISTRY SET TRACE:", {
        connector_id: normalizedConnectorId,
        socket_id: socket.id,
        socket_connector_id: socket.connectorId,
        socket_company_code: socket.companyCode
    });

    console.log(
        `✅ Registered Connector : ${normalizedConnectorId}`
    );

    return true;
}


function get(connectorId, companyCode) {

    const socket = connectors.get(connectorId);

if (!socket || socket.connected !== true) {
    console.log(
        "REGISTRY GET: CONNECTOR NOT AVAILABLE",
        {
            connector_id: connectorId,
            socket_id: socket?.id || null,
            connected: socket?.connected ?? false
        }
    );

    return null;
}

    if (
    !socket.connectorIds ||
    !socket.connectorIds.has(connectorId)
    ) {

        console.error(
            "❌ REGISTRY GET REJECTED: CONNECTOR ID MISMATCH",
            {
                connector_id: connectorId,
                socket_connector_ids:
                    socket.connectorIds
                        ? [...socket.connectorIds]
                        : [],
                socket_id: socket.id
            }
        );

        return null;
    }

    /*040926
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
        */
/* 030926
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
*/
    console.log(
        "REGISTRY GET VALID:",
        {
            connector_id: connectorId,
            //company_code: socket.companyCode,
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

if (!socket || socket.connected !== true) {
    return null;
}
            if (
                !socket.connectorIds ||
                !socket.connectorIds.has(connectorId)
            ) {

                console.error(
                    "❌ WAIT CONNECTOR REJECTED: CONNECTOR ID MISMATCH",
                    {
                        requested_connector_id: connectorId,
                        socket_connector_ids:
                            socket.connectorIds
                                ? [...socket.connectorIds]
                                : [],
                        socket_id: socket.id
                    }
                );

                return null;
            }
/*040926
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
                */
/* 030926
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
*/
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

/**
 * Get any connected pending connector.
 */
function getPending() {
    for (const socket of pendingConnectors) {
        if (socket?.connected === true) {
            return socket;
        }
    }

    return null;
}
function getAny() {
    for (const [connectorId, socket] of connectors) {
        if (
            socket?.connected === true &&
            socket.connectorIds?.has(connectorId)
        ) {
            return socket;
        }
    }

    return null;
}

/* 060926

function registerPending(socket) {

    pendingConnectors.add(socket);

    console.log(
        `🆕 Pending Connector : ${socket.id}`
    );

}
*/

function registerPending(socket, connectorId) {

    pendingConnectors.add(socket);

     currentPendingConnector = socket;

    if (connectorId) {
//060926 start
            socket.pendingConnectorId =
            connectorId;
// 060926 end

        pendingConnectorMap.set(
            connectorId,
            socket
        );
    }

    console.log(
        `🆕 Pending Connector : ${socket.id}`,
        {
            connector_id: connectorId
        }
    );

}

function getPendingById(connectorId) {
    if (!connectorId) {
        return null;
    }

    const socket = pendingConnectorMap.get(connectorId);

    if (!socket || socket.connected !== true) {
        return null;
    }

    if (!pendingConnectors.has(socket)) {
        return null;
    }

    return socket;
}


function getCurrentPendingConnector() {

    if (!currentPendingConnector) {
        return null;
    }

    if (!pendingConnectors.has(currentPendingConnector)) {
        currentPendingConnector = null;
        return null;
    }

    return currentPendingConnector;
}

// 060926 start
function getPendingConnectors() {

    return Array.from(
        pendingConnectors
    ).map(socket => ({
        pending_connector_id:
            socket.pendingConnectorId,

        socket_id:
            socket.id,

        computer_name:
            socket.computerName || null
    }));

}
// 060926 end
// Recovery-only candidates: never treated as registered owners.
function getPendingSocketsForRecovery(connectorId) {
    if (typeof connectorId !== "string" || !connectorId.trim()) {
        return [];
    }

    return [...pendingConnectors].filter(socket =>
        socket?.connected === true &&
        socket.reconcileRecoveryCandidateIds?.has(connectorId) === true
    );
}
function getPendingByGuid(companyGuid) {

    if (!companyGuid) {
        return null;
    }

     console.log(
        "PENDING CONNECTORS COUNT:",
        pendingConnectors.size
    );

    for (const socket of pendingConnectors) {

          console.log(
            "PENDING SOCKET CHECK:",
            {
                socket_id: socket.id,
                company_guids: socket.companyGuids,
                requested_guid: companyGuid
            }
        );

        if (
            Array.isArray(socket.companyGuids) &&
            socket.companyGuids.includes(companyGuid)
        ) {

            return socket;

        }

    }

    return null;
}

/**
 * Remove Connector
 */
/*040926
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
*/

function remove(connectorId, socket) {
    if (!socket) {
        return;
    }

    const isFullCleanup =
        connectorId === null || connectorId === undefined;

    // ============================================
    // CASE 1: Remove a specific connector alias
    // ============================================
    if (!isFullCleanup) {
        const currentSocket = connectors.get(connectorId);

        // A stale socket must not remove a replacement
        // socket's connector mapping.
        if (currentSocket === socket) {
    connectors.delete(connectorId);

    if (socket.connectorIds) {
        socket.connectorIds.delete(connectorId);
    }

    // Keep the generation counter so a future registration
    // cannot reuse an earlier generation on this socket.

            // Remove only company registrations belonging
            // to this successfully removed alias.
            for (
                const [key, registration] of
                socket.companyRegistrations || []
            ) {
                if (registration.connector_id === connectorId) {
                    socket.companyRegistrations.delete(key);
                }
            }

            console.log(
                `❌ Removed Connector : ${connectorId}`
            );
        }

        // Never clear whole-socket pending state here.
        return;
    }

    // ============================================
    // CASE 2: Full socket disconnect cleanup
    // ============================================

    if (socket === currentPendingConnector) {
        currentPendingConnector = null;
    }

    if (
        socket.pendingConnectorId &&
        pendingConnectorMap.get(socket.pendingConnectorId) === socket
    ) {
        pendingConnectorMap.delete(socket.pendingConnectorId);
    }

    pendingConnectors.delete(socket);

    // Clear only this physical socket's local state.
    // Global alias mappings are removed separately
    // through exact socket-owner checks.
    socket.companyRegistrations?.clear();
    socket.availableCompanyGuids?.clear();
    socket.companyGuids = [];
}



/**
 * Check Connector
 */

function getRegistrationGeneration(connectorId, socket) {
    if (
        !socket ||
        !socket.connected ||
        connectors.get(connectorId) !== socket ||
        !socket.connectorIds?.has(connectorId)
    ) {
        return null;
    }

    return socket.connectorRegistrationGenerations?.get(connectorId) ?? null;
}

function isOnline(connectorId) {

    return get(connectorId) !== null;

}

/**
 * List Connected Companies
 */
function list() {

    return Array.from(connectors.keys());

}

module.exports = {
    initializeRegistrationState,
    registrationHandler,
    applyCompanyAvailability,
    assertCompanyRegistration,
    prepareCompanyRegistrations,
    commitCompanyRegistrations,
    emitCompanyRegistration,
    resolveDirectRegistration,
    register,
    get,
    waitForConnector,
    getAny,
    registerPending,
    getPending,
    getPendingById,
    getCurrentPendingConnector,
    getPendingConnectors,
    getPendingByGuid,
    getPendingSocketsForRecovery,
      remove,
    getRegistrationGeneration,
    isOnline,
    list

};