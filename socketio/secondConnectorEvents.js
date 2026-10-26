const registry = require("./connectorRegistry");

const {
    dispatchBatch,
} = require("../workers/secondQueueExistingBatch");

const {
    publishConnectorHealth,
    updateConnectorHealth
} = require("./connectorHealth");

const {
    claimConnectorSocketOwnership,
    updateConnectorHealthIfSocketOwner,
    getConnectorWatchdogStatus
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


const {
    updateSyncProgress
} = require("../utils/syncProgress");

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
                    "❌ WAITING BATCH LOOKUP FAILED:",
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
                "[WAITING BATCH RECOVERY] UNCERTAIN OUTCOME — MANUAL REVIEW:",
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
                    "⏳ WAITING BATCH RECOVERY DEFERRED: WATCHDOG NOT AVAILABLE",
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
                "⚠️ WAITING BATCH RECOVERY STOPPED: CONNECTOR NOT READY",
                {
                    batch_id: batch.batch_id,
                    connector_id: company.connector_id
                }
            );
            break;
        }
                    console.log(
                        "🔄 TALLY COMPANY BACK: RECOVERING WAITING BATCH",
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

                    // ----------------------------------------
                    // Atomic reset
                    // ----------------------------------------

                    const {
                        data: resetBatch,
                        error: resetError
                    } = await supabase
                        .from("sync_batches")
                        .update({

                            batch_status:
                                "PENDING",

                            worker_status:
                                "PENDING",

                            worker_id:
                                null,

                            locked_at:
                                null,

                            heartbeat_at:
                                null,

                            completed_at:
                                null,

                            error_message:
                                null,

                            current_stage:
                                "PENDING",

                            current_module:
                                "MASTERS",

                            current_action:
                                "PENDING"

                        })
                        .eq(
                            "id",
                            batch.id
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
                        .select()
                        .maybeSingle();

                    if (resetError) {

                        console.error(
                            "❌ WAITING BATCH RESET FAILED:",
                            {
                                batch_id:
                                    batch.batch_id,

                                error:
                                    resetError.message
                            }
                        );

                        continue;
                    }

                    // ----------------------------------------
                    // Another process already handled it
                    // ----------------------------------------

                    if (!resetBatch) {

                        console.log(
                            "⚠️ WAITING BATCH ALREADY HANDLED:",
                            batch.batch_id
                        );

                        continue;
                    }

                   // ----------------------------------------
// Dispatch SAME batch
// ----------------------------------------

if (
    !socket.connected ||
    socket.tallyStatus !== "ONLINE" ||
    !socket.connectorIds?.has(String(company.connector_id)) ||
    registry.get(String(company.connector_id)) !== socket
) {
    const { data: restoredBatch, error: restoreError } = await supabase
        .from("sync_batches")
        .update({
            current_stage: "WAITING_CONNECTOR"
        })
        .eq("id", resetBatch.id)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "PENDING")
        .is("worker_id", null)
        .select("id")
        .maybeSingle();

    if (restoreError || !restoredBatch) {
        console.error(
            "❌ WAITING BATCH RESTORE FAILED:",
            {
                batch_id: batch.batch_id,
                error: restoreError?.message || "Batch state changed"
            }
        );
    } else {
        console.warn(
            "⏳ WAITING BATCH RESTORED: CONNECTOR NOT READY",
            {
                batch_id: batch.batch_id,
                connector_id: company.connector_id
            }
        );
    }

    break;
}

try {
    const dispatchResult = await dispatchBatch(resetBatch);

if (!dispatchResult?.success || dispatchResult.queued !== true) {
    const dispatchError = new Error(
        `WAITING_BATCH_DISPATCH_NOT_QUEUED: ${batch.batch_id}`
    );

    dispatchError.code = "WAITING_BATCH_DISPATCH_NOT_QUEUED";

    throw dispatchError;
}
} catch (dispatchError) {

    if (dispatchError.code !== "BATCH_QUEUE_CONFIRMED_NOT_FOUND") {
        console.error(
            "❌ WAITING BATCH DISPATCH STATE UNCERTAIN — RESTORE SKIPPED:",
            {
                batch_id: batch.batch_id,
                error: dispatchError.message
            }
        );

        // BullMQ job may exist. Preserve current DB reservation.
        break;
    }

    const { data: restoredBatch, error: restoreError } = await supabase
        .from("sync_batches")
        .update({
            current_stage: "WAITING_CONNECTOR"
        })
        .eq("id", resetBatch.id)
        .eq("batch_status", "PENDING")
        .eq("worker_status", "PENDING")
        .eq("current_stage", "PENDING")
        .is("worker_id", null)
        .select("id")
        .maybeSingle();

    if (restoreError || !restoredBatch) {
        console.error(
            "❌ DISPATCH FAILURE BATCH RESTORE FAILED:",
            {
                batch_id: batch.batch_id,
                dispatch_error: dispatchError.message,
                restore_error: restoreError?.message || "Batch state changed"
            }
        );
    } else {
        console.warn(
            "⏳ DISPATCH FAILED: BATCH RESTORED TO WAITING_CONNECTOR",
            {
                batch_id: batch.batch_id,
                error: dispatchError.message
            }
        );
    }

    break;
}

                    console.log(
                        "✅ WAITING BATCH RESTARTED:",
                        {
                            batch_id:
                                batch.batch_id,

                            company_code:
                                company.company_code,

                            connector_id:
                                company.connector_id
                        }
                    );

                } catch (batchRecoveryError) {

                    console.error(
                        "❌ WAITING BATCH RECOVERY ERROR:",
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
            "❌ COMPANY BATCH RECOVERY ERROR:",
            error
        );

    } finally {
        socket.waitingBatchRecoveryRunning = false;
    }
}


function registerEvents(io) {

    io.on("connection", (socket) => {

        console.log("================================");
        console.log("✅ Connector Connected");
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
    "🆕 TEMP CONNECTOR CREATED:",
    {
        socket_id: socket.id,
        pending_connector_id:
            pendingConnectorId
    }
);
// 060926 end

        socket.protocolReceiver =
            new ServerProtocolReceiver(
                socket
            );

        socket.protocolReceiver.start();

socket.on("identifyConnector", async (data) => {

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
    rawQueueStatus.operationGeneration >= 0;

const tallyQueueStatus = isValidQueueStatus
    ? {
        idle: rawQueueStatus.idle,
        pendingOperations: rawQueueStatus.pendingOperations,
        runningOperations: rawQueueStatus.runningOperations,
        operationGeneration: rawQueueStatus.operationGeneration
    }
    : null;

socket.reportedTallyQueueStatus = tallyQueueStatus;

        console.log("📋 CONNECTOR TALLY QUEUE STATUS:", {
            socket_id: socket.id,
            queue_status: tallyQueueStatus
        });

        const company_guids =
            Array.isArray(data?.company_guids)
                ? [...new Set(
                    data.company_guids
                        .map(guid => String(guid).trim())
                        .filter(Boolean)
                )]
                : [];

        console.log(
            "IDENTIFY CONNECTOR GUIDS:",
            company_guids
        );

        if (!company_guids.length) {

            console.error(
                "❌ No Tally company GUIDs received"
            );

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

                company_guid:
                    row.client_tally_company_guid,

                connector_id:
                    row.client_connector_id
            })),

            ...(caResult.data || []).map(row => ({
                company_code:
                    row.company_code,

                company_guid:
                    row.ca_tally_company_guid,

                connector_id:
                    row.ca_connector_id
            }))

    ].filter(
    row => row.connector_id
);

    
   socket.companyGuids =
    company_guids;

console.log(
    "📋 TALLY GUIDS AVAILABLE ON SOCKET:",
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


for (const connectorId of connectorIds) {
    const claimed = await claimConnectorSocketOwnership(
        connectorId,
        socket.id
    ).catch((error) => {
        console.error(
            "❌ AUTO-REGISTER OWNERSHIP CLAIM FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                error: error.message
            }
        );

        return { claimed: false, health: null };
    });

    if (!claimed.claimed) {
        console.warn(
            "⚠️ EXISTING CONNECTOR AUTO-REGISTER SKIPPED: ownership unavailable",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                current_owner: claimed.health?.socket_id || null
            }
        );

        continue;
    }

    const registered = registry.register(
        connectorId,
        socket
    );

    if (!registered) {
        await updateConnectorHealthIfSocketOwner(
            connectorId,
            socket.id,
            {
                connection_status: "DISCONNECTED",
                operation_status: "UNKNOWN"
            }
        ).catch((error) => {
            console.error(
                "❌ AUTO-REGISTER OWNERSHIP ROLLBACK FAILED",
                {
                    connector_id: connectorId,
                    error: error.message
                }
            );
        });

        console.error(
            "❌ EXISTING CONNECTOR AUTO REGISTER FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

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
            "❌ AUTO-REGISTER HEALTH UPDATE FAILED",
            {
                socket_id: socket.id,
                connector_id: connectorId,
                error: error.message
            }
        );

        healthUpdate = { updated: false };
    }


    if (!healthUpdate.updated) {
        console.error(
            "❌ AUTO-REGISTER HEALTH UPDATE REJECTED",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

                     let rollbackUpdated = false;

        try {
            const rollback =
                await updateConnectorHealthIfSocketOwner(
                    connectorId,
                    socket.id,
                    {
                        connection_status: "DISCONNECTED",
                        operation_status: "UNKNOWN"
                    }
                );

            rollbackUpdated = rollback.updated;
        } catch (error) {
            console.error(
                "❌ AUTO-REGISTER OWNERSHIP ROLLBACK FAILED",
                {
                    connector_id: connectorId,
                    socket_id: socket.id,
                    error: error.message
                }
            );
        }

        registry.remove(connectorId, socket);

        console.warn(
            "AUTO-REGISTER OWNERSHIP ROLLBACK:",
            {
                connector_id: connectorId,
                socket_id: socket.id,
                rollback_updated: rollbackUpdated
            }
        );

        continue;
    }

    console.log(
        "✅ EXISTING CONNECTOR AUTO REGISTERED WITH OWNERSHIP",
        {
            socket_id: socket.id,
            connector_id: connectorId
        }
    );
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
            "⏳ WAITING BATCH RECOVERY DEFERRED: TALLY NOT CONFIRMED ONLINE",
            { socket_id: socket.id, tally_status: socket.tallyStatus || "UNKNOWN" }
        );
    }
        } else {
            console.warn(
                "⚠️ BATCH RECOVERY SKIPPED: SOCKET OWNS NO REGISTERED CONNECTOR",
                {
                    socket_id: socket.id,
                    connector_ids: connectorIds
                }
            );
        }

    } catch (err) {

        console.error(
            "❌ CONNECTOR IDENTITY ERROR:",
            err
        );

    }

});


// Connector Register


        // Connector Register
        socket.on("register", async (data) => {
            console.log("Register Request :", data);

            const connectorId =
                String(data?.connector_id || "").trim();

            if (!connectorId) {
                console.error(
                    "❌ CONNECTOR REGISTRATION REJECTED: connector_id missing",
                    { socket_id: socket.id }
                );
                return;
            }

            const previousConnectorId = socket.connectorId;

            const claimed = await claimConnectorSocketOwnership(
                connectorId,
                socket.id
            ).catch((error) => {
                console.error(
                    "❌ CONNECTOR OWNERSHIP CLAIM FAILED",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        error: error.message
                    }
                );
                return { claimed: false, health: null };
            });

            if (!claimed.claimed) {
                console.error(
                    "❌ CONNECTOR REGISTRATION REJECTED: ownership unavailable",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        current_owner: claimed.health?.socket_id || null
                    }
                );
                return;
            }

            
const registered = registry.register(
    connectorId,
    socket
);

if (!registered) {

                try {
                    await updateConnectorHealthIfSocketOwner(
                        connectorId,
                        socket.id,
                        {
                            connection_status: "DISCONNECTED",
                            operation_status: "UNKNOWN"
                        }
                    );
                } catch (error) {
                    console.error(
                        "❌ OWNERSHIP ROLLBACK FAILED",
                        {
                            socket_id: socket.id,
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                }

                if (socket.connectorId === connectorId) {
                    socket.connectorId = previousConnectorId || null;
                }

                registry.remove(connectorId, socket);

                console.error(
                    "❌ CONNECTOR REGISTRATION REJECTED",
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


            if (
                previousConnectorId &&
                previousConnectorId !== connectorId
            ) {
                registry.remove(previousConnectorId, socket);
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
                                    : "FREE",
                            last_heartbeat_at:
                                socket.lastHeartbeat
                                    ? new Date(socket.lastHeartbeat).toISOString()
                                    : null
                        }
                    );
            } catch (error) {
                console.error(
                    "❌ CONNECTOR HEALTH UPDATE FAILED",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId,
                        error: error.message
                    }
                );

                healthUpdate = { updated: false };
            }

            if (!healthUpdate.updated) {
                try {
                    await updateConnectorHealthIfSocketOwner(
                        connectorId,
                        socket.id,
                        {
                            connection_status: "DISCONNECTED",
                            operation_status: "UNKNOWN"
                        }
                    );
                } catch (error) {
                    console.error(
                        "❌ CONNECTOR OWNERSHIP ROLLBACK FAILED",
                        {
                            socket_id: socket.id,
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                }

                registry.remove(connectorId, socket);

                console.error(
                    "❌ CONNECTOR REGISTRATION ROLLED BACK",
                    {
                        socket_id: socket.id,
                        connector_id: connectorId
                    }
                );

                return;
            }


            console.log("✅ CONNECTOR REGISTERED WITH OWNERSHIP", {
                socket_id: socket.id,
                connector_id: connectorId,
                company_code: data.company_code
            });
        });

        socket.on("testExport", () => {

            console.log("================================");
            console.log("📦 Test Export Request");
            console.log("Sending XML to Connector...");
            console.log("================================");

            socket.emit("export", {
                xml: "<TEST>HELLO TALLY</TEST>"
            });

        });

socket.on("getMastersProgress", async (data) => {

    socket.lastHeartbeat =
        Date.now();

    socket.lastTallyActivity =
        Date.now();

            await updateOwnedSocketHealth({
        connection_status: "CONNECTED",
        operation_status:
            socket.tallyRequestActive === true
                ? "BUSY"
                : "FREE",
        last_heartbeat_at:
            new Date(socket.lastHeartbeat).toISOString()
    });

    console.log(
        "📊 TALLY PROGRESS :",
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
            batchId: data.batchId,
            stage: data.stage,
            progress: data.progress,
            action: `TALLY_${data.stage}`
        });

    } catch (err) {

        console.error(
            "❌ SYNC PROGRESS UPDATE FAILED:",
            err.message
        );

    }

});



        // ============================================================
        // SHARED HEALTH UPDATES — CURRENT SOCKET OWNER ONLY
        // ============================================================

        async function updateOwnedSocketHealth(patch) {
            const connectorIds = new Set([
                ...(socket.connectorIds || []),
                socket.connectorId
            ].filter(Boolean));

            for (const connectorId of connectorIds) {
                try {
                    const result =
                        await updateConnectorHealthIfSocketOwner(
                            connectorId,
                            socket.id,
                            patch
                        );

                    if (!result.updated) {
                        console.warn(
                            "⚠️ SHARED HEALTH UPDATE SKIPPED: SOCKET IS NOT OWNER",
                            {
                                socket_id: socket.id,
                                connector_id: connectorId
                            }
                        );
                    }
                } catch (error) {
                    console.error(
                        "❌ SHARED HEALTH UPDATE FAILED",
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

        socket.on("tally:status", (data = {}) => {
    const status = data.status;

    if (status !== "ONLINE" && status !== "OFFLINE") {
        return;
    }

    // Verified Tally status received from connector monitor.
    socket.tallyStatus = status;
    socket.lastTallyStatusAt = Date.now();

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
                "❌ TALLY ONLINE WAITING BATCH RECOVERY FAILED:",
                error
            );
        });
    }
});

        socket.on("protocol:heartbeat", async () => {
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
        // TALLY REQUEST START
        // ============================================================

        socket.on("tally:request:start", async (data) => {
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

            console.log("🟢 TALLY REQUEST START:", data?.batchId);
        });

        // ============================================================
        // TALLY REQUEST END
        // ============================================================

        socket.on("tally:request:end", async (data) => {
            socket.tallyRequestActive = false;
            socket.tallyRequestStartedAt = 0;
            socket.lastTallyActivity = Date.now();

            await updateOwnedSocketHealth({
                connection_status: "CONNECTED",
                operation_status: "FREE",
                last_heartbeat_at:
                    socket.lastHeartbeat
                        ? new Date(socket.lastHeartbeat).toISOString()
                        : null
            });

            console.log("✅ TALLY REQUEST END:", data?.batchId);
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
            "❌ CONNECTOR FREE WAITING BATCH RECOVERY FAILED:",
            error
        );
    });
}
        });

        // ============================================================
        // TALLY REQUEST ERROR
        // ============================================================

        socket.on("tally:request:error", async (data) => {
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



            console.log("❌ TALLY REQUEST ERROR:", data?.batchId);
        });

        // ============================================================
        // DISCONNECT — ONLY THE CURRENT OWNER CAN MARK DISCONNECTED
        // ============================================================

        socket.on("disconnect", async (reason) => {
    console.log("================================");
    console.log("❌ Connector Disconnected");
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
                            "ℹ️ DISCONNECT HEALTH UPDATE SKIPPED: SOCKET NO LONGER OWNS CONNECTOR",
                            {
                                socket_id: socket.id,
                                connector_id: connectorId
                            }
                        );
                    }
                } catch (error) {
                    console.error(
                        "❌ DISCONNECT HEALTH UPDATE FAILED",
                        {
                            socket_id: socket.id,
                            connector_id: connectorId,
                            error: error.message
                        }
                    );
                }

                registry.remove(connectorId, socket);
            }

            console.log("================================");
        });



    });

}

module.exports = {
    registerEvents
};