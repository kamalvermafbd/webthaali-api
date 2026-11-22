const registry = require("./connectorRegistry");


const {
    publishConnectorHealth,
    updateConnectorHealth
} = require("./connectorHealth");

const {
    claimConnectorSocketOwnership,
    inspectConnectorRecoveryState,
    updateConnectorHealthIfSocketOwner,
    getConnectorWatchdogStatus,
    updateConnectorTallyQueueStatus,
    renewConnectorSocketLease
} = require("./connectorHealthStore");
// 060926 start
const crypto = require("crypto");
// 060926 end

const {
    createClient
} = require("@supabase/supabase-js");

const supabase =
    createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_SERVICE_KEY
    );

const ServerProtocolReceiver =
    require("../utils/protocol/ServerProtocolReceiver");

const masterFinalAckReceipts =
    require("../utils/masterFinalAckReceipts");

const {
    updateSyncProgress
} = require("../utils/syncProgress");

const {
    verifyGetMastersProgress,
    revokeGetMastersProgressForSocket
} = require("../utils/getMastersProgressAuthorization");

const {
    dispatchRecoveredWaitingBatch
} = require("../workers/secondQueueExistingBatch");

const ENABLE_WAITING_CONNECTOR_RECOVERY = false;
// ============================================================
// RECOVER WAITING BATCHES WHEN TALLY COMPANY COMES BACK
// ============================================================

async function recoverWaitingBatchesForCompanies({
    companyGuids,
    socket
}) {

    if (socket.waitingBatchRecoveryRunning === true) {
        return;
    }

    socket.waitingBatchRecoveryRunning = true;

    try {

        if (
            !Array.isArray(companyGuids) ||
            companyGuids.length === 0
        ) {
            return;
        }

        const cleanGuids = [
            ...new Set(
                companyGuids
                    .map(guid => String(guid).trim())
                    .filter(Boolean)
            )
        ];

        // ----------------------------------------------------
        // Find companies linked to these Tally GUIDs
        // ----------------------------------------------------

        const [
            clientResult,
            caResult
        ] = await Promise.all([

            supabase
                .from("company")
                .select(`
                    company_code,
                    client_tally_company_guid,
                    client_connector_id
                `)
                .in(
                    "client_tally_company_guid",
                    cleanGuids
                ),

            supabase
                .from("company")
                .select(`
                    company_code,
                    ca_tally_company_guid,
                    ca_connector_id
                `)
                .in(
                    "ca_tally_company_guid",
                    cleanGuids
                )

        ]);

        if (
            clientResult.error ||
            caResult.error
        ) {

            throw new Error(
                clientResult.error?.message ||
                caResult.error?.message
            );

        }

        const companies = [

            ...(clientResult.data || []).map(row => ({
                company_code:
                    row.company_code,

                tally_owner:
                    "USER",

                tally_guid:
                    row.client_tally_company_guid,

                connector_id:
                    row.client_connector_id
            })),

            ...(caResult.data || []).map(row => ({
                company_code:
                    row.company_code,

                tally_owner:
                    "CA",

                tally_guid:
                    row.ca_tally_company_guid,

                connector_id:
                    row.ca_connector_id
            }))

       ].filter(row =>
    row.connector_id &&
    socket.connectorIds?.has(String(row.connector_id)) &&
    registry.get(String(row.connector_id)) === socket
);

        if (companies.length === 0) {
            return;
        }

        // ----------------------------------------------------
        // Check WAITING_CONNECTOR batches
        // belonging to these companies
        // ----------------------------------------------------

        for (const company of companies) {

            const {
                data: batches,
                error: batchError
            } = await supabase
                .from("sync_batches")
                .select("*")
                .eq(
                    "company_code",
                    company.company_code
                )
                .eq(
                    "tally_owner",
                    company.tally_owner
                )
                
                .eq(
                    "batch_status",
                    "PENDING"
                )
                .eq(
                    "worker_status",
                    "PENDING"
                )
              .eq(
    "current_stage",
    "WAITING_CONNECTOR"
)
.is("worker_id", null)
.order(
                    "created_at",
                    {
                        ascending: true
                    }
                );

            if (batchError) {

                console.error(
                    "âŒ WAITING BATCH LOOKUP FAILED:",
                    {
                        company_code:
                            company.company_code,

                        tally_owner:
                            company.tally_owner,

                        error:
                            batchError.message
                    }
                );

                continue;
            }

            if (
                !batches ||
                batches.length === 0
            ) {
                continue;
            }

            // ------------------------------------------------
            // Restart every waiting batch for this company
            // ------------------------------------------------

          for (const batch of batches) {

    try {

        // Never auto-recover a batch with uncertain Tally outcome.
        if (
            String(batch.error_message || "")
                .startsWith("CONNECTOR_OUTCOME_UNKNOWN:")
        ) {
            console.warn(
                "[WAITING BATCH RECOVERY] UNCERTAIN OUTCOME â€” MANUAL REVIEW:",
                batch.batch_id
            );
            continue;
        }

        const watchdog = await getConnectorWatchdogStatus(
                String(company.connector_id)
            );

            if (
                !watchdog ||
                watchdog.available !== true ||
                watchdog.connection_status !== "CONNECTED" ||
                watchdog.operation_status !== "FREE"
            ) {
                console.log(
                    "â³ WAITING BATCH RECOVERY DEFERRED: WATCHDOG NOT AVAILABLE",
        {
            batch_id: batch.batch_id,
            connector_id: company.connector_id,
            connection_status: watchdog?.connection_status,
            operation_status: watchdog?.operation_status,
            busy: watchdog?.busy,
            quarantined: watchdog?.quarantined,
            active_operation: watchdog?.active_operation,
            lock_ttl_ms: watchdog?.lock_ttl_ms
        }
    );

    break;
}

        if (
            !socket.connected ||
            socket.tallyStatus !== "ONLINE" ||
            !socket.connectorIds?.has(String(company.connector_id)) ||
            registry.get(String(company.connector_id)) !== socket
        ) {
            console.warn(
                "âš ï¸ WAITING BATCH RECOVERY STOPPED: CONNECTOR NOT READY",
                {
                    batch_id: batch.batch_id,
                    connector_id: company.connector_id
                }
            );
            break;
        }
                    console.log(
                        "ðŸ”„ TALLY COMPANY BACK: RECOVERING WAITING BATCH",
                        {
                            batch_id:
                                batch.batch_id,

                            company_code:
                                company.company_code,

                            tally_owner:
                                company.tally_owner,

                            tally_guid:
                                company.tally_guid,

                            connector_id:
                                company.connector_id
                        }
                    );

                if (!ENABLE_WAITING_CONNECTOR_RECOVERY) {
    console.warn(
        "[WAITING BATCH RECOVERY] REDISPATCH DEFERRED",
        {
            batch_id: batch.batch_id,
            reason: "Recovery feature not activated"
        }
    );
    continue;
}

const { data: recovered, error: recoveryError } =
    await supabase.rpc("recover_waiting_normal_sync_batch", {
        p_batch_id: batch.batch_id,
        p_expected_execution_claim_token:
            batch.execution_claim_token
    });

if (recoveryError) {
    console.error(
        "[WAITING BATCH RECOVERY] RPC FAILED",
        {
            batch_id: batch.batch_id,
            error: recoveryError.message
        }
    );
    continue;
}

if (recovered !== true) {
    console.warn(
        "[WAITING BATCH RECOVERY] RPC REJECTED",
        {
            batch_id: batch.batch_id
        }
    );
    continue;
}

const dispatchResult =
    await dispatchRecoveredWaitingBatch(batch);

console.log(
    "[WAITING BATCH RECOVERY] DISPATCH RESULT",
    {
        batch_id: batch.batch_id,
        result: dispatchResult
    }
);

continue;
           

                } catch (batchRecoveryError) {

                    console.error(
                        "âŒ WAITING BATCH RECOVERY ERROR:",
                        {
                            batch_id:
                                batch.batch_id,

                            company_code:
                                company.company_code,

                            error:
                                batchRecoveryError.message
                        }
                    );

                }

            }

        }

       } catch (error) {

        console.error(
            "âŒ COMPANY BATCH RECOVERY ERROR:",
            error
        );

    } finally {
        socket.waitingBatchRecoveryRunning = false;
    }
}


function registerEvents(io) {

    io.on("connection", (socket) => {
registry.initializeRegistrationState(socket);
        console.log("================================");
        console.log("âœ… Connector Connected");
        console.log("Socket ID :", socket.id);
        console.log("================================");

       // registry.registerPending(socket);

       // 060926 start
const pendingConnectorId =
    `TMP-${crypto.randomUUID()}`;

socket.pendingConnectorId =
    pendingConnectorId;

registry.registerPending(
    socket,
    pendingConnectorId
);

console.log(
    "ðŸ†• TEMP CONNECTOR CREATED:",
    {
        socket_id: socket.id,
        pending_connector_id:
            pendingConnectorId
    }
);
// 060926 end

   socket.protocolReceiver = null;

function activateProtocolReceiver() {
    if (!socket.connected) {
        return false;
    }

    const hasRegisteredOwner = [
        ...(socket.connectorIds || []),
        socket.connectorId
    ]
        .filter(Boolean)
        .some(connectorId =>
            registry.get(String(connectorId)) === socket
        );

    if (!hasRegisteredOwner) {
        return false;
    }

    if (!socket.protocolReceiver) {
    const receiver = new ServerProtocolReceiver(socket);

    try {
        receiver.start();
    } catch (error) {
        // Never retain a receiver whose start() failed.
        // A later registration must retry activation.
        socket.protocolReceiver = null;
        throw error;
    }

    // Publish only after successful activation.
    socket.protocolReceiver = receiver;
}

    return true;
}
        masterFinalAckReceipts.install(socket);

socket.on(
    "identifyConnector",
    registry.registrationHandler(socket, async (data) => {

  if (
    socket.identificationInProgress === true ||
    socket.directRegistrationInProgress === true
) {
    console.warn(
        "[CONNECTOR IDENTIFY SKIPPED] Registration already in progress",
        { socket_id: socket.id }
    );

    socket.emit("connector:registration:status", {
        connector_id: null,
        registered: false,
        reason: "REGISTRATION_IN_PROGRESS"
    });

    return;
}

socket.identificationInProgress = true;

    console.log("[CONNECTOR IDENTIFY RECEIVED]", {
        socket_id: socket.id,
        connected: socket.connected,
        guid_count: Array.isArray(data?.company_guids)
            ? data.company_guids.length
            : 0,
        received_at: new Date().toISOString()
    });

    try {

        const rawQueueStatus = data?.tally_queue_status;

        const isValidQueueStatus =
    rawQueueStatus !== null &&
    typeof rawQueueStatus === "object" &&
    !Array.isArray(rawQueueStatus) &&
    typeof rawQueueStatus.idle === "boolean" &&
    Number.isSafeInteger(rawQueueStatus.pendingOperations) &&
    rawQueueStatus.pendingOperations >= 0 &&
    Number.isSafeInteger(rawQueueStatus.runningOperations) &&
       rawQueueStatus.runningOperations >= 0 &&
    Number.isSafeInteger(rawQueueStatus.operationGeneration) &&
    rawQueueStatus.operationGeneration >= 0 &&
  (
    rawQueueStatus.idle === false ||
    (
        rawQueueStatus.pendingOperations === 0 &&
        rawQueueStatus.runningOperations === 0
    )
)

const tallyQueueStatus = isValidQueueStatus
    ? {
        idle: rawQueueStatus.idle,
        pendingOperations: rawQueueStatus.pendingOperations,
        runningOperations: rawQueueStatus.runningOperations,
        operationGeneration: rawQueueStatus.operationGeneration
    }
    : null;

const previousQueueStatus = socket.reportedTallyQueueStatus;

const isNewerQueueStatus =
    tallyQueueStatus !== null &&
    (
        !previousQueueStatus ||
        tallyQueueStatus.operationGeneration >
            previousQueueStatus.operationGeneration ||
        (
            tallyQueueStatus.operationGeneration ===
                previousQueueStatus.operationGeneration &&
            tallyQueueStatus.idle === previousQueueStatus.idle &&
            tallyQueueStatus.pendingOperations ===
                previousQueueStatus.pendingOperations &&
            tallyQueueStatus.runningOperations ===
                previousQueueStatus.runningOperations
        )
    );

if (isNewerQueueStatus) {
    socket.reportedTallyQueueStatus = tallyQueueStatus;
} else {
    console.warn(
        "[CONNECTOR IDENTIFY] Invalid, stale or conflicting queue status ignored",
        {
            socket_id: socket.id,
            received_status: tallyQueueStatus,
            previous_status: previousQueueStatus || null
        }
    );
}

console.log("ðŸ“‹ CONNECTOR TALLY QUEUE STATUS:", {
    socket_id: socket.id,
    queue_status: socket.reportedTallyQueueStatus || null
});
       
const company_guids =
    registry.applyCompanyAvailability(socket, {
        company_guids: Array.isArray(data?.company_guids)
            ? data.company_guids
            : [],
        company_guids_mode:
            data?.company_guids_mode || "add"
    });


        console.log(
            "IDENTIFY CONNECTOR GUIDS:",
            company_guids
        );

       if (!company_guids.length) {

    console.error(
        "âŒ No Tally company GUIDs received"
    );

    socket.emit("connector:registration:status", {
        connector_id: null,
        registered: false,
        reason: "NO_COMPANY_GUIDS"
    });

    return;
}

        const [
            clientResult,
            caResult
        ] = await Promise.all([

            supabase
                .from("company")
                .select(
                    "company_code, client_tally_company_guid, client_connector_id"
                )
                .in(
                    "client_tally_company_guid",
                    company_guids
                ),

            supabase
                .from("company")
                .select(
                    "company_code, ca_tally_company_guid, ca_connector_id"
                )
                .in(
                    "ca_tally_company_guid",
                    company_guids
                )

        ]);

        if (
            clientResult.error ||
            caResult.error
        ) {

            throw new Error(
                clientResult.error?.message ||
                caResult.error?.message
            );

        }

        const matches = [

            ...(clientResult.data || []).map(row => ({
                company_code:
                    row.company_code,
                tally_owner: "USER",
                company_guid:
                    row.client_tally_company_guid,

                connector_id:
                    row.client_connector_id
            })),

            ...(caResult.data || []).map(row => ({
                company_code:
                    row.company_code,
                 tally_owner: "CA",
                company_guid:
                    row.ca_tally_company_guid,

                connector_id:
                    row.ca_connector_id
            }))

    ].filter(
    row => row.connector_id
);

    
   const preparedRegistrations =
    registry.prepareCompanyRegistrations(socket, matches);

console.log(
    "ðŸ“‹ TALLY GUIDS AVAILABLE ON SOCKET:",
    {
        socket_id: socket.id,
        company_guids: socket.companyGuids
    }
);

// ==========================================
// REGISTER EXISTING CONNECTORS FROM DB
// ==========================================

const connectorIds = [
    ...new Set(
        matches
            .map(row => row.connector_id)
            .filter(Boolean)
    )
];

if (connectorIds.length === 0) {
    console.warn(
        "âš ï¸ CONNECTOR IDENTIFICATION: NO DATABASE MAPPING",
        {
            socket_id: socket.id,
            company_guid_count: company_guids.length
        }
    );

    socket.emit("connector:registration:status", {
        connector_id: null,
        registered: false,
        reason: "NO_CONNECTOR_MAPPING"
    });

    return;
}

for (const connectorId of connectorIds) {

        if (!socket.connected) {
        console.warn(
            "[CONNECTOR IDENTIFY STOPPED] Socket disconnected before ownership claim",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );
        break;
    }
const recoveryState = await inspectConnectorRecoveryState(
    connectorId
).catch(error => {
    console.error(
        "[CONNECTOR IDENTIFY] Recovery inspection failed",
        {
            connector_id: connectorId,
            error: error.message
        }
    );
    return null;
});
if (recoveryState?.quarantined === true && socket.connected) {
    // Candidate only: DB-mapped identity, NOT registered ownership.
    // Never use this mapping to authorize quarantine release.
    if (!socket.reconcileRecoveryCandidateIds) {
        socket.reconcileRecoveryCandidateIds = new Set();
    }

    socket.reconcileRecoveryCandidateIds.add(
        String(connectorId)
    );

    // Read-only probe. A new process cannot attest
    // to an old execution.
    const {
        sendManualReconcileRecoveryIdentityRequest
    } = require(
        "./sendManualReconcileRecoveryIdentityRequest"
    );

    sendManualReconcileRecoveryIdentityRequest({
        connectorId: String(connectorId),
        expectedSocketId: socket.id,
        timeoutMs: 10000
    }).then(result => {
        console.log(
            "[RECONCILE RECOVERY] PROCESS IDENTITY OBSERVED",
            {
                connector_id: connectorId,
                socket_id: socket.id,
                identity: result.identity,
                settlementEligible: false
            }
        );
    }).catch(error => {
        console.warn(
            "[RECONCILE RECOVERY] IDENTITY PROBE FAILED",
            {
                connector_id: connectorId,
                socket_id: socket.id,
                error: error.message
            }
        );
    });
}
if (!recoveryState || recoveryState.quarantined) {
    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: recoveryState
            ? "RECOVERY_VERIFICATION_REQUIRED"
            : "RECOVERY_INSPECTION_FAILED"
    });

    continue;
}
    const claimed = await claimConnectorSocketOwnership(
        connectorId,
        socket.id
    ).catch((error) => {
        console.error(
            "âŒ AUTO-REGISTER OWNERSHIP CLAIM FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                error: error.message
            }
        );

             return { claimed: false, health: null };
    });

    if (!socket.connected && claimed.claimed) {
        console.warn(
            "[CONNECTOR IDENTIFY] Socket disconnected after ownership claim",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

        await updateConnectorHealthIfSocketOwner(
            connectorId,
            socket.id,
            {
                connection_status: "DISCONNECTED",
                operation_status: "UNKNOWN"
            }
        ).catch(error => {
            console.error(
                "[CONNECTOR IDENTIFY] Disconnect rollback failed",
                {
                    connector_id: connectorId,
                    error: error.message
                }
            );
        });

                break;
    }

    if (!socket.connected) {
        console.warn(
            "[CONNECTOR IDENTIFY STOPPED] Socket disconnected after failed ownership claim",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );
        break;
    }

    if (!claimed.claimed) {
        console.warn(
            "âš ï¸ EXISTING CONNECTOR AUTO-REGISTER SKIPPED: ownership unavailable",
            {
               socket_id: socket.id,
connector_id: connectorId,
current_owner: claimed.health?.socket_id || null,
reason: claimed.reason || "UNSPECIFIED",
health_status: claimed.health?.connection_status || null,
health_operation_status: claimed.health?.operation_status || null
            }
        );
socket.emit("connector:registration:status", {
    connector_id: connectorId,
    registered: false,
    reason: claimed.reason || "OWNERSHIP_UNAVAILABLE"
});
        continue;
    }

    const registered = registry.register(
        connectorId,
        socket
    );

    if (!registered) {

   // Registry rejected local ownership.
// Do not change Redis connection status here.
// An existing same-socket registration may still be active.
console.warn(
    "[CONNECTOR IDENTIFY] Registry rejected; Redis ownership retained",
    {
        connector_id: connectorId,
        socket_id: socket.id
    }
);

        console.error(
            "âŒ EXISTING CONNECTOR AUTO REGISTER FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );
        socket.emit("connector:registration:status", {
    connector_id: connectorId,
    registered: false,
    reason: "REGISTRY_REGISTRATION_FAILED"
});
        continue;
    }

   
    let healthUpdate;

    try {
        healthUpdate =
            await updateConnectorHealthIfSocketOwner(
                connectorId,
                socket.id,
                {
                    connection_status: "CONNECTED",
            operation_status:
    socket.tallyRequestActive === true
        ? "BUSY"
        : "UNKNOWN",
                    last_heartbeat_at:
                        socket.lastHeartbeat
                            ? new Date(socket.lastHeartbeat).toISOString()
                            : null
                }
            );
    } catch (error) {
        console.error(
            "âŒ AUTO-REGISTER HEALTH UPDATE FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                error: error.message
            }
        );

        healthUpdate = { updated: false };
    }

        if (healthUpdate.updated && !socket.connected) {
        console.warn(
            "[CONNECTOR IDENTIFY] Socket disconnected during health update",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

        await updateConnectorHealthIfSocketOwner(
            connectorId,
            socket.id,
            {
                connection_status: "DISCONNECTED",
                operation_status: "UNKNOWN"
            }
        ).catch(error => {
            console.error(
                "[CONNECTOR IDENTIFY] Post-update disconnect rollback failed",
                {
                    connector_id: connectorId,
                    error: error.message
                }
            );
        });

        registry.remove(connectorId, socket);
        break;
    }

   if (!healthUpdate?.updated) {
    console.error(
        "[CONNECTOR IDENTIFY] Health ownership not verified",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            reason: "HEALTH_UPDATE_UNVERIFIED"
        }
    );

    // Revoke local authorization without changing
    // uncertain Redis ownership or operation state.
    registry.remove(connectorId, socket);

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "HEALTH_UPDATE_UNVERIFIED"
    });

    continue;
}

        if (!socket.connected) {
        console.warn(
            "[CONNECTOR IDENTIFY] Socket disconnected before registration success",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

        await updateConnectorHealthIfSocketOwner(
            connectorId,
            socket.id,
            {
                connection_status: "DISCONNECTED",
                operation_status: "UNKNOWN"
            }
        ).catch(error => {
            console.error(
                "[CONNECTOR IDENTIFY] Final disconnect rollback failed",
                {
                    connector_id: connectorId,
                    error: error.message
                }
            );
        });

        registry.remove(connectorId, socket);
        break;
    }
let protocolActivated = false;

try {
    protocolActivated = activateProtocolReceiver();
} catch (error) {
    console.error(
        "[CONNECTOR IDENTIFY] Protocol activation threw",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            error: error.message
        }
    );
}

if (!protocolActivated) {
    console.error(
        "[CONNECTOR IDENTIFY] Protocol activation failed",
        {
            connector_id: connectorId,
            socket_id: socket.id
        }
    );

    registry.remove(connectorId, socket);

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "PROTOCOL_ACTIVATION_FAILED"
    });

    continue;
}
   console.log(
    "âœ… EXISTING CONNECTOR AUTO REGISTERED WITH OWNERSHIP",
    {
        socket_id: socket.id,
        connector_id: connectorId
    }
);

if (isNewerQueueStatus && socket.connected) {
    try {
        const result = await updateConnectorTallyQueueStatus(
            connectorId,
            socket.id,
            tallyQueueStatus
        );

        if (!result.updated && !result.unchanged) {
            console.warn(
                "[CONNECTOR IDENTIFY] Initial queue status rejected",
                {
                    socket_id: socket.id,
                    connector_id: connectorId,
                    reason: result.reason
                }
            );
        }
    } catch (error) {
        console.error(
            "[CONNECTOR IDENTIFY] Initial queue status persistence failed",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                error: error.message
            }
        );
    }
}

if (!socket.connected) {
    await updateConnectorHealthIfSocketOwner(
        connectorId,
        socket.id,
        {
            connection_status: "DISCONNECTED",
            operation_status: "UNKNOWN"
        }
    ).catch(error => {
        console.error(
            "[CONNECTOR IDENTIFY] Disconnect rollback failed",
            {
                connector_id: connectorId,
                error: error.message
            }
        );
    });

    registry.remove(connectorId, socket);
    break;
}


let committedRegistrations = [];

try {
    committedRegistrations =
        registry.commitCompanyRegistrations(
            socket,
            preparedRegistrations.filter(
                registration =>
                    registration.connector_id === connectorId
            )
        );
} catch (error) {
    console.error(
        "[CONNECTOR IDENTIFY] Company context commit failed",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            error: error.message
        }
    );

    socket.emit("connector:registration:context", {
        connector_id: connectorId,
        socket_id: socket.id,
        context_resolved: false,
        reason: "COMPANY_CONTEXT_COMMIT_FAILED"
    });
}

for (const registration of committedRegistrations) {
    registry.emitCompanyRegistration(socket, registration);
}


socket.emit("connector:registration:status", {
    connector_id: connectorId,
    registered: true,
    socket_id: socket.id
});
}

// ==========================================
// RECOVER WAITING BATCHES
// ==========================================


        const hasOwnedConnector =
            connectorIds.some(connectorId =>
                socket.connectorIds?.has(connectorId) &&
                registry.get(connectorId) === socket
            );

        if (hasOwnedConnector) {
    if (socket.tallyStatus === "ONLINE") {
        await recoverWaitingBatchesForCompanies({
            companyGuids: company_guids,
            socket
        });
    } else {
        console.log(
            "â³ WAITING BATCH RECOVERY DEFERRED: TALLY NOT CONFIRMED ONLINE",
            { socket_id: socket.id, tally_status: socket.tallyStatus || "UNKNOWN" }
        );
    }
        } else {
            console.warn(
                "âš ï¸ BATCH RECOVERY SKIPPED: SOCKET OWNS NO REGISTERED CONNECTOR",
                {
                    socket_id: socket.id,
                    connector_ids: connectorIds
                }
            );
        }

   } catch (err) {

    console.error(
        "âŒ CONNECTOR IDENTITY ERROR:",
        err
    );

       socket.emit("connector:registration:status", {
        connector_id: null,
        registered: false,
        reason: "IDENTIFICATION_ERROR"
    });

} finally {
    socket.identificationInProgress = false;
}

    })
);

// Connector Register


        // Connector Register
        socket.on(
    "register",
    registry.registrationHandler(socket, async (data) => {
            console.log("Register Request :", data);

            const connectorId =
                String(data?.connector_id || "").trim();

            if (!connectorId) {
                console.error(
                    "âŒ CONNECTOR REGISTRATION REJECTED: connector_id missing",
                    { socket_id: socket.id }
                );
                return;
            }
if (
    !socket.connected ||
    socket.identificationInProgress === true ||
    socket.directRegistrationInProgress === true
) {
    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: !socket.connected
            ? "SOCKET_DISCONNECTED"
            : "REGISTRATION_IN_PROGRESS"
    });

    return;
}

socket.directRegistrationInProgress = true;

// Capture identity before this registration attempt.
const previousConnectorId = socket.connectorId;
const previousCompanyCode = socket.companyCode;
const previousCompanyGuid = socket.companyGuid;

// Track whether this connector alias existed before
// the current registration attempt.
const registrationExistedBeforeAttempt =
    registry.get(connectorId) === socket &&
    socket.connectorIds?.has(connectorId) === true;

// Only a newly acquired local alias may be removed
// when this registration attempt fails.
let registrationAcquiredThisAttempt = false;

try {


const resolvedDirect =
    await registry.resolveDirectRegistration(supabase, data);

const preparedDirectRegistrations =
    registry.prepareCompanyRegistrations(
        socket,
        resolvedDirect.contexts
    );

if (!socket.connected) {
    return;
}

function restoreRegistrationIdentity() {
    // Do not overwrite identity changed by another handler.
    if (socket.connectorId !== connectorId) {
        return;
    }

    socket.connectorId = previousConnectorId;
    socket.companyCode = previousCompanyCode;
    socket.companyGuid = previousCompanyGuid;
}

const recoveryState = await inspectConnectorRecoveryState(
    connectorId
).catch(error => {
    console.error(
        "[CONNECTOR REGISTER] Recovery inspection failed",
        {
            connector_id: connectorId,
            error: error.message
        }
    );
    return null;
});

if (!recoveryState || recoveryState.quarantined) {
    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: recoveryState
            ? "RECOVERY_VERIFICATION_REQUIRED"
            : "RECOVERY_INSPECTION_FAILED"
    });

    return;
}

            const claimed = await claimConnectorSocketOwnership(
                connectorId,
                socket.id
            ).catch((error) => {
                console.error(
                    "âŒ CONNECTOR OWNERSHIP CLAIM FAILED",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        error: error.message
                    }
                );
                return { claimed: false, health: null };
            });

                        if (!socket.connected) {
                console.warn(
                    "[CONNECTOR REGISTER STOPPED] Socket disconnected after ownership claim",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId
                    }
                );

                if (claimed.claimed) {
                    await updateConnectorHealthIfSocketOwner(
                        connectorId,
                        socket.id,
                        {
                            connection_status: "DISCONNECTED",
                            operation_status: "UNKNOWN"
                        }
                    ).catch(error => {
                        console.error(
                            "[CONNECTOR REGISTER] Disconnect rollback failed",
                            {
                                connector_id: connectorId,
                                error: error.message
                            }
                        );
                    });
                }

                return;
            }

           if (!claimed.claimed) {
    console.error(
        "CONNECTOR REGISTRATION REJECTED: ownership unavailable",
        {
            socket_id: socket.id,
            connector_id: connectorId,
            current_owner: claimed.health?.socket_id || null,
            reason: claimed.reason || "UNSPECIFIED"
        }
    );

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: claimed.reason || "OWNERSHIP_UNAVAILABLE"
    });

    return;
}

            
const registered = registry.register(
    connectorId,
    socket
);

if (
    registered === true &&
    !registrationExistedBeforeAttempt
) {
    registrationAcquiredThisAttempt = true;
}
if (!registered) {

    // Local registry rejected registration.
// Do not mark Redis DISCONNECTED: the same socket may
// already own this connector and be serving operations.
// Leave Redis ownership to its existing lease and guards.
console.warn(
    "[CONNECTOR REGISTER] Registry rejected; Redis ownership retained",
    {
        connector_id: connectorId,
        socket_id: socket.id
    }
);

                if (socket.connectorId === connectorId) {
                    socket.connectorId = previousConnectorId || null;
                }

                

                console.error(
                    "âŒ CONNECTOR REGISTRATION REJECTED",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        company_code: data.company_code
                    }
                );

                return;
            }

            
socket.companyCode = data.company_code;
socket.companyGuid = data.company_guid;
socket.connectorId = connectorId;


           
            
            let healthUpdate;

            try {
                healthUpdate =
                    await updateConnectorHealthIfSocketOwner(
                        connectorId,
                        socket.id,
                        {
                            connection_status: "CONNECTED",
                                                       operation_status:
                                socket.tallyRequestActive === true
                                    ? "BUSY"
                                    : "UNKNOWN",
                            last_heartbeat_at:
                                socket.lastHeartbeat
                                    ? new Date(socket.lastHeartbeat).toISOString()
                                    : null
                        }
                    );
            } catch (error) {
                console.error(
                    "âŒ CONNECTOR HEALTH UPDATE FAILED",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        error: error.message
                    }
                );

                healthUpdate = { updated: false };
            }

                        if (healthUpdate.updated && !socket.connected) {
                console.warn(
                    "[CONNECTOR REGISTER] Socket disconnected during health update",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId
                    }
                );

                await updateConnectorHealthIfSocketOwner(
                    connectorId,
                    socket.id,
                    {
                        connection_status: "DISCONNECTED",
                        operation_status: "UNKNOWN"
                    }
                ).catch(error => {
                    console.error(
                        "[CONNECTOR REGISTER] Post-update disconnect rollback failed",
                        {
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                });

                if (
    registrationAcquiredThisAttempt &&
    !registrationExistedBeforeAttempt
) {
    registry.remove(connectorId, socket);
}

restoreRegistrationIdentity();
                return;
            }

          if (!healthUpdate?.updated) {
    console.error(
        "[CONNECTOR REGISTER] Health ownership not verified",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            reason: "HEALTH_UPDATE_UNVERIFIED"
        }
    );

    // Fail closed locally. Do not mark a connected
    // socket DISCONNECTED in Redis on an uncertain result.
    if (
    registrationAcquiredThisAttempt &&
    !registrationExistedBeforeAttempt
) {
    registry.remove(connectorId, socket);
}

restoreRegistrationIdentity();

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "HEALTH_UPDATE_UNVERIFIED"
    });

    return;
}

                        if (!socket.connected) {
                console.warn(
                    "[CONNECTOR REGISTER] Socket disconnected before registration success",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId
                    }
                );

                await updateConnectorHealthIfSocketOwner(
                    connectorId,
                    socket.id,
                    {
                        connection_status: "DISCONNECTED",
                        operation_status: "UNKNOWN"
                    }
                ).catch(error => {
                    console.error(
                        "[CONNECTOR REGISTER] Final disconnect rollback failed",
                        {
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                });

             if (
    registrationAcquiredThisAttempt &&
    !registrationExistedBeforeAttempt
) {
    registry.remove(connectorId, socket);
}

restoreRegistrationIdentity();
            return;
            }
let protocolActivated = false;

try {
    protocolActivated = activateProtocolReceiver();
} catch (error) {
    console.error(
        "[CONNECTOR REGISTER] Protocol activation threw",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            error: error.message
        }
    );
}

if (!protocolActivated) {
    console.error(
        "[CONNECTOR REGISTER] Protocol activation failed",
        {
            connector_id: connectorId,
            socket_id: socket.id
        }
    );

    if (
        registrationAcquiredThisAttempt &&
        !registrationExistedBeforeAttempt
    ) {
        registry.remove(connectorId, socket);
    }

    restoreRegistrationIdentity();

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "PROTOCOL_ACTIVATION_FAILED"
    });

    return;
}
            console.log("âœ… CONNECTOR REGISTERED WITH OWNERSHIP", {
                socket_id: socket.id,
                connector_id: connectorId,
                company_code: data.company_code
            });



let committedDirectRegistrations = [];

try {
    committedDirectRegistrations =
        registry.commitCompanyRegistrations(
            socket,
            preparedDirectRegistrations.filter(
                registration =>
                    registration.connector_id === connectorId
            )
        );
} catch (error) {
    console.error(
        "[CONNECTOR REGISTER] Company context commit failed",
        {
            connector_id: connectorId,
            socket_id: socket.id,
            error: error.message
        }
    );
}

for (const registration of committedDirectRegistrations) {
    registry.emitCompanyRegistration(socket, registration);
}

if (committedDirectRegistrations.length === 0) {
    socket.emit("connector:registration:context", {
        connector_id: connectorId,
        socket_id: socket.id,
        context_resolved: false,
        reason: "LEGACY_CONTEXT_UNRESOLVED"
    });
}

    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: true,
        socket_id: socket.id
    });

} catch (error) {
    console.error(
        "[CONNECTOR REGISTER] Unexpected registration error",
        {
            socket_id: socket.id,
            connector_id: connectorId,
            error: error?.message || String(error)
        }
    );

       // Preserve an existing same-socket registration if
// this repeat attempt fails unexpectedly.
// Revoke only an alias newly acquired by this attempt.
if (
    registrationAcquiredThisAttempt === true &&
    !registrationExistedBeforeAttempt
) {
    registry.remove(connectorId, socket);
}
    // Restore previous identity only if its local registration
    // is still valid. Never restore a stale connector alias.
    if (socket.connectorId === connectorId) {
        const previousIdentityStillValid =
            previousConnectorId &&
            registry.get(previousConnectorId) === socket;

        socket.connectorId = previousIdentityStillValid
            ? previousConnectorId
            : null;

        socket.companyCode = previousIdentityStillValid
            ? previousCompanyCode
            : null;

        socket.companyGuid = previousIdentityStillValid
            ? previousCompanyGuid
            : null;
    }

    // Redis ownership and operation locks are not modified.
    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "REGISTRATION_ERROR"
    });
} finally {
    socket.directRegistrationInProgress = false;
}
    })
);


socket.on("getMastersProgress", async (data) => {

       if (!socket.connected || !data || typeof data !== "object") {
        return;
    }

    const authorization = verifyGetMastersProgress({
        socket,
        requestId: data.requestId,
        batchId: data.batchId
    });

    if (!authorization) {
        console.warn(
            "[SYNC PROGRESS] Rejected unauthorized getMasters progress",
            {
                socket_id: socket.id,
                request_id: data.requestId || null,
                batch_id: data.batchId || null
            }
        );
        return;
    }

    // Verify ownership of the exact connector used by
    // this durable getMasters execution.
    if (
        registry.get(authorization.connectorId) !== socket ||
        !(
            socket.connectorIds instanceof Set &&
            socket.connectorIds.has(authorization.connectorId)
        )
    ) {
        console.warn(
            "[SYNC PROGRESS] Rejected: connector ownership mismatch",
            {
                socket_id: socket.id,
                connector_id: authorization.connectorId
            }
        );
        return;
    }

    socket.lastHeartbeat =
        Date.now();

    socket.lastTallyActivity =
        Date.now();

             try {
        await updateConnectorHealthIfSocketOwner(
            authorization.connectorId,
            socket.id,
            {
                connection_status: "CONNECTED",
                operation_status:
                    socket.tallyRequestActive === true
                        ? "BUSY"
                        : "UNKNOWN",
                last_heartbeat_at:
                    new Date(socket.lastHeartbeat).toISOString()
            }
        );
    } catch (error) {
        console.error(
            "[SYNC PROGRESS] Connector health update failed",
            {
                connector_id: authorization.connectorId,
                socket_id: socket.id,
                error: error.message
            }
        );
    }

    console.log(
        "ðŸ“Š TALLY PROGRESS :",
        {
            batchId: data.batchId,
            stage: data.stage,
            progress: data.progress
        }
    );

    if (!data?.batchId) {
        return;
    }

    try {

       await updateSyncProgress({
    supabase,
    batchId: authorization.batchId,
    execution_claim_token:
        authorization.executionClaimToken,
    stage: data.stage,
    progress: data.progress,
    action: `TALLY_${data.stage}`
});

    } catch (err) {

        console.error(
            "âŒ SYNC PROGRESS UPDATE FAILED:",
            err.message
        );

    }

});



        // ============================================================
        // SHARED HEALTH UPDATES â€” CURRENT SOCKET OWNER ONLY
        // ============================================================

        async function updateOwnedSocketHealth(patch) {
    const connectorIds = new Set([
        ...(socket.connectorIds || []),
        socket.connectorId
    ].filter(Boolean));

    for (const connectorId of connectorIds) {
        if (
            !socket.connected ||
            registry.get(connectorId) !== socket ||
            !socket.connectorIds?.has(connectorId)
        ) {
            continue;
        }

        try {
            const result =
                await updateConnectorHealthIfSocketOwner(
                    connectorId,
                    socket.id,
                    patch
                );

            if (!result.updated) {
                console.warn(
                    "[SHARED HEALTH UPDATE] Ownership not verified",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId
                    }
                );
            }
        } catch (error) {
            console.error(
                "[SHARED HEALTH UPDATE] Failed",
                {
                    socket_id: socket.id,
                    connector_id: connectorId,
                    error: error.message
                }
            );
        }
    }
}

        // ============================================================
        // PROTOCOL HEARTBEAT
        // ============================================================

    socket.on("tally:status", async (data = {}) => {

    if (!socket.connected) {
        return;
    }

    const status = data.status;

    if (status !== "ONLINE" && status !== "OFFLINE") {
        return;
    }

    // Verified Tally status received from connector monitor.
    socket.tallyStatus = status;
    socket.lastTallyStatusAt = Date.now();

    await updateOwnedSocketHealth({
            tally_status: status
        });

    socket.tallyCompanies =
        status === "ONLINE" && Array.isArray(data.companies)
            ? data.companies
            : [];

      console.log(
        "[TALLY STATUS]",
        "socket:", socket.id,
        "status:", status
    );

    if (
        status === "ONLINE" &&
        Array.isArray(socket.companyGuids) &&
        socket.companyGuids.length > 0
    ) {
        recoverWaitingBatchesForCompanies({
            companyGuids: socket.companyGuids,
            socket
        }).catch(error => {
            console.error(
                "âŒ TALLY ONLINE WAITING BATCH RECOVERY FAILED:",
                error
            );
        });
    }
});

        socket.on("protocol:heartbeat", async () => {
    if (!socket.connected) {
        return;
    }
socket.lastHeartbeat = Date.now();

await updateOwnedSocketHealth({
    connection_status: "CONNECTED",
    operation_status:
        socket.tallyRequestActive === true
            ? "BUSY"
            : "UNKNOWN",
    last_heartbeat_at:
        new Date(socket.lastHeartbeat).toISOString()
});
   // Renew Redis lease only for connectors registered to this socket.
// A rejected renewal must never trigger an ownership takeover.
const leaseConnectorIds = new Set([
    ...(socket.connectorIds || []),
    socket.connectorId
].filter(Boolean));

for (const connectorId of leaseConnectorIds) {
    if (
        !socket.connected ||
        registry.get(connectorId) !== socket
    ) {
        continue;
    }

    try {
        const lease = await renewConnectorSocketLease(
            connectorId,
            socket.id
        );

      if (!lease.renewed) {
    console.warn(
        "[CONNECTOR LEASE] Renewal rejected; registration requires verification",
        {
            connector_id: connectorId,
            socket_id: socket.id
        }
    );

   // Remove only this socket's local mapping.
// Never change Redis ownership or operation locks here.
registry.remove(connectorId, socket);

// Notify this exact socket that its registration is no longer valid.
// Re-registration must pass the existing ownership and quarantine guards.
if (socket.connected) {
    socket.emit("connector:registration:status", {
        connector_id: connectorId,
        registered: false,
        reason: "REDIS_LEASE_LOST",
        socket_id: socket.id
    });
}
}
    } catch (error) {
        console.error(
            "[CONNECTOR LEASE] Renewal failed",
            {
                connector_id: connectorId,
                socket_id: socket.id,
                error: error.message
            }
        );
    }
}

            if (
    socket.tallyRequestActive !== true &&
    socket.tallyStatus === "ONLINE" &&
    Array.isArray(socket.companyGuids) &&
    socket.companyGuids.length > 0
) {
    await recoverWaitingBatchesForCompanies({
        companyGuids: socket.companyGuids,
        socket
    });
}
        });

                // ============================================================
        // CONNECTOR TALLY QUEUE STATUS REPORT
        // ============================================================

       socket.on("tally:queue:status", async (data = {}) => {
    const valid =
        data !== null &&
        typeof data === "object" &&
        !Array.isArray(data) &&
        typeof data.idle === "boolean" &&
        Number.isSafeInteger(data.pendingOperations) &&
        data.pendingOperations >= 0 &&
        Number.isSafeInteger(data.runningOperations) &&
        data.runningOperations >= 0 &&
        Number.isSafeInteger(data.operationGeneration) &&
        data.operationGeneration >= 0 &&
       (
    data.idle === false ||
    (
        data.pendingOperations === 0 &&
        data.runningOperations === 0
    )
);

    if (!valid || !socket.connected) {
        console.warn(
            "[CONNECTOR TALLY QUEUE STATUS] Invalid status or disconnected socket",
            { socket_id: socket.id }
        );
        return;
    }

    const status = {
        idle: data.idle,
        pendingOperations: data.pendingOperations,
        runningOperations: data.runningOperations,
        operationGeneration: data.operationGeneration
    };

    const connectorIds = [
        ...new Set([
            ...(socket.connectorIds || []),
            socket.connectorId
        ].filter(Boolean))
    ].filter(connectorId =>
        registry.get(connectorId) === socket
    );

    if (connectorIds.length === 0) {
        console.warn(
            "[CONNECTOR TALLY QUEUE STATUS] No registered connector",
            { socket_id: socket.id }
        );
        return;
    }

    for (const connectorId of connectorIds) {
        if (
            !socket.connected ||
            registry.get(connectorId) !== socket
        ) {
            continue;
        }

        try {
            const result = await updateConnectorTallyQueueStatus(
                connectorId,
                socket.id,
                status
            );

            if (!result.updated && !result.unchanged) {
                console.warn(
                    "[CONNECTOR TALLY QUEUE STATUS] Redis update rejected",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        reason: result.reason
                    }
                );
                continue;
            }

            console.log(
                "[CONNECTOR TALLY QUEUE STATUS] Redis accepted",
                {
                    socket_id: socket.id,
                    connector_id: connectorId,
                    updated: result.updated,
                    unchanged: result.unchanged,
                    queue_status: status
                }
            );
        } catch (error) {
            console.error(
                "[CONNECTOR TALLY QUEUE STATUS] Redis update failed",
                {
                    socket_id: socket.id,
                    connector_id: connectorId,
                    error: error.message
                }
            );
        }
    }
});

        // ============================================================
        // TALLY REQUEST START
        // ============================================================

        socket.on("tally:request:start", async (data) => {

    if (!socket.connected) {
        return;
    }

    socket.tallyRequestActive = true;
            socket.tallyRequestStartedAt =
                data?.timestamp || Date.now();
            socket.lastTallyActivity = Date.now();

            await updateOwnedSocketHealth({
                connection_status: "CONNECTED",
                operation_status: "BUSY",
                last_heartbeat_at:
                    socket.lastHeartbeat
                        ? new Date(socket.lastHeartbeat).toISOString()
                        : null
            });

            console.log("ðŸŸ¢ TALLY REQUEST START:", data?.batchId);
        });

        // ============================================================
        // TALLY REQUEST END
        // ============================================================

        socket.on("tally:request:end", async (data) => {

    if (!socket.connected) {
        return;
    }

    socket.tallyRequestActive = false;
            socket.tallyRequestStartedAt = 0;
            socket.lastTallyActivity = Date.now();

            await updateOwnedSocketHealth({
                connection_status: "CONNECTED",
                operation_status: "UNKNOWN",
                last_heartbeat_at:
                    socket.lastHeartbeat
                        ? new Date(socket.lastHeartbeat).toISOString()
                        : null
            });

            console.log("âœ… TALLY REQUEST END:", data?.batchId);
            if (
    socket.tallyStatus === "ONLINE" &&
    Array.isArray(socket.companyGuids) &&
    socket.companyGuids.length > 0
) {
    recoverWaitingBatchesForCompanies({
        companyGuids: socket.companyGuids,
        socket
    }).catch(error => {
        console.error(
            "âŒ CONNECTOR FREE WAITING BATCH RECOVERY FAILED:",
            error
        );
    });
}
        });

        // ============================================================
        // TALLY REQUEST ERROR
        // ============================================================

        socket.on("tally:request:error", async (data) => {

    if (!socket.connected) {
        return;
    }

    socket.tallyRequestActive = false;
            socket.tallyRequestStartedAt = 0;
            socket.lastTallyActivity = Date.now();

            await updateOwnedSocketHealth({
                connection_status: "CONNECTED",
                operation_status: "RECOVERING",
                last_heartbeat_at:
                    socket.lastHeartbeat
                        ? new Date(socket.lastHeartbeat).toISOString()
                        : null
            });



            console.log("âŒ TALLY REQUEST ERROR:", data?.batchId);
        });

        // ============================================================
        // DISCONNECT â€” ONLY THE CURRENT OWNER CAN MARK DISCONNECTED
        // ============================================================

        socket.on("disconnect", async (reason) => {

    // Immediately revoke active getMasters progress authorization.
    revokeGetMastersProgressForSocket(socket);
    console.log("================================");
    console.log("âŒ Connector Disconnected");
    console.log("Socket ID :", socket.id);
    console.log("Reason :", reason);

    if (socket.protocolReceiver) {
        socket.protocolReceiver.cancelCollection(
            `Connector disconnected: ${reason}`
        );
    }

            const connectorIds = new Set([
                ...(socket.connectorIds || []),
                socket.connectorId
            ].filter(Boolean));
// Remove pending selection state immediately.
registry.remove(null, socket);

// Remove disconnected socket mappings before awaiting Redis.
// Registry.remove() only deletes a mapping owned by this socket.
for (const connectorId of connectorIds) {
    registry.remove(connectorId, socket);
}

// Persist disconnected health for the same socket owner.
for (const connectorId of connectorIds) {
                try {
                    const result =
                        await updateConnectorHealthIfSocketOwner(
                            connectorId,
                            socket.id,
                            {
                                connection_status: "DISCONNECTED",
                                operation_status: "UNKNOWN"
                            }
                        );

                    if (!result.updated) {
                        console.log(
                            "â„¹ï¸ DISCONNECT HEALTH UPDATE SKIPPED: SOCKET NO LONGER OWNS CONNECTOR",
                            {
                                socket_id: socket.id,
                                connector_id: connectorId
                            }
                        );
                    }
                } catch (error) {
                    console.error(
                        "âŒ DISCONNECT HEALTH UPDATE FAILED",
                        {
                            socket_id: socket.id,
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                }

                
            }

            console.log("================================");
        });



    });

}

module.exports = {
    registerEvents
};
