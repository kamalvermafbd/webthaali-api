const registry = require("./connectorRegistry");

const {
    dispatchBatch,
    dispatchConnectorLossRetry
} = require("../workers/secondQueueExistingBatch");

const {
publishConnectorHealth,
updateConnectorHealth,
getSharedConnectorHealth
} = require("./connectorHealth");
const {
    claimConnectorSocketOwnership,
    updateConnectorHealthIfSocketOwner
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

        ].filter(
            row => row.connector_id
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

                    await dispatchBatch(
                        resetBatch
                    );

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

    }

}


async function retryLatestConnectorLossBatchForCompanies(companyGuids) {
    if (!Array.isArray(companyGuids) || !companyGuids.length) {
        return;
    }

    const cleanGuids = [
        ...new Set(
            companyGuids
                .map(guid => String(guid).trim())
                .filter(Boolean)
        )
    ];

    const [clientResult, caResult] = await Promise.all([
        supabase
            .from("company")
            .select("company_code, client_tally_company_guid")
            .in("client_tally_company_guid", cleanGuids),

        supabase
            .from("company")
            .select("company_code, ca_tally_company_guid")
            .in("ca_tally_company_guid", cleanGuids)
    ]);

    if (clientResult.error || caResult.error) {
        throw new Error(
            clientResult.error?.message ||
            caResult.error?.message
        );
    }

    const companyScopes = new Map();

    for (const row of clientResult.data || []) {
        companyScopes.set(
            `${row.company_code}:USER`,
            {
                company_code: row.company_code,
                tally_owner: "USER"
            }
        );
    }

    for (const row of caResult.data || []) {
        companyScopes.set(
            `${row.company_code}:CA`,
            {
                company_code: row.company_code,
                tally_owner: "CA"
            }
        );
    }

    for (const scope of companyScopes.values()) {
        try {
            // Select the latest batch FIRST, regardless of status.
            /* 211026
            const { data: latestBatch, error } = await supabase
                .from("sync_batches")
                .select("*")
                .eq("company_code", scope.company_code)
                .eq("tally_owner", scope.tally_owner)
                .order("created_at", { ascending: false })
                .limit(1)
                .maybeSingle();
                */

                
            // Select the latest batch FIRST, regardless of status.
            const { data: latestBatch, error } = await supabase
                .from("sync_batches")
                .select("*")
                .eq("company_code", scope.company_code)
                .eq("tally_owner", scope.tally_owner)
                .order("created_at", { ascending: false })
                .order("id", { ascending: false })
                .limit(1)
                .maybeSingle();


            if (error) {
                throw error;
            }

            if (!latestBatch) {
                continue;
            }

                                   // Only retry the latest FAILED batch.
            if (
                latestBatch.batch_status !== "FAILED" ||
                latestBatch.worker_status !== "FAILED"
            ) {
                console.log(
                    "FAILED RETRY SKIPPED: LATEST BATCH NOT ELIGIBLE",
                    {
                        company_code: scope.company_code,
                        tally_owner: scope.tally_owner,
                        batch_id: latestBatch.batch_id,
                        batch_status: latestBatch.batch_status,
                        worker_status: latestBatch.worker_status
                    }
                );

                continue;
            }

            await dispatchConnectorLossRetry(latestBatch);
        } catch (error) {
            console.error(
                "LATEST CONNECTOR RETRY FAILED:",
                {
                    company_code: scope.company_code,
                    tally_owner: scope.tally_owner,
                    error: error.message
                }
            );
        }
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
/* 030926 comented
        // Connector Identity Resolve
socket.on("identifyConnector", async (data) => {

    try {

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

        const uniqueMatches =
            new Map();

        for (const match of matches) {

            uniqueMatches.set(
                `${match.company_code}:${match.connector_id}`,
                match
            );

        }

        if (uniqueMatches.size !== 1) {

            console.error(
                "❌ CONNECTOR IDENTITY NOT UNIQUE",
                {
                    socket_id: socket.id,
                    matches: [...uniqueMatches.values()]
                }
            );

            return;
        }

        const identity =
            [...uniqueMatches.values()][0];

        socket.companyCode =
            identity.company_code;

        socket.companyGuid =
            identity.company_guid;

        socket.connectorId =
            identity.connector_id;

        const registered =
            registry.register(
                identity.connector_id,
                socket
            );

        if (!registered) {

            console.error(
                "❌ CONNECTOR AUTO REGISTRATION REJECTED",
                identity
            );

            return;
        }

        console.log(
            "✅ CONNECTOR AUTO REGISTERED",
            {
                socket_id: socket.id,
                company_code:
                    identity.company_code,
                connector_id:
                    identity.connector_id,
                company_guid:
                    identity.company_guid
            }
        );

    } catch (err) {

        console.error(
            "❌ CONNECTOR IDENTITY ERROR:",
            err
        );

    }

});
*/
//030926 added

// Connector Identity Resolve
socket.on("identifyConnector", async (data) => {

    try {

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


        // ==========================================
        // ONE CONNECTOR CAN SERVE MULTIPLE COMPANIES
        // ==========================================
/*
        const connectorIds =
            [
                ...new Set(
                    matches.map(
                        row => row.connector_id
                    )
                )
            ];


        if (connectorIds.length !== 1) {

            console.error(
                "❌ CONNECTOR IDENTITY NOT RESOLVED",
                {
                    socket_id: socket.id,
                    connector_ids: connectorIds,
                    matches
                }
            );

            return;
        }

*/
      // Connector is not assigned during startup.
// Startup only records which Tally companies are available.
    
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
/* 221026
for (const connectorId of connectorIds) {

    const registered =
        registry.register(
            connectorId,
            socket
        );

    if (registered) {

        console.log(
            "✅ EXISTING CONNECTOR AUTO REGISTERED",
            {
                socket_id: socket.id,
                connector_id: connectorId
            }
        );

    }
}
    */



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
                            : "FREE",
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


        // ==========================================
        // RECOVER WAITING BATCHES — VALID OWNER ONLY
        // ==========================================

        const hasOwnedConnector =
            connectorIds.some(connectorId =>
                socket.connectorIds?.has(connectorId) &&
                registry.get(connectorId) === socket
            );

        if (hasOwnedConnector) {
            await recoverWaitingBatchesForCompanies({
                companyGuids: company_guids,
                socket
            });

            await retryLatestConnectorLossBatchForCompanies(
                company_guids
            );
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
/* 221026
socket.on("register", (data) => {

            console.log(
                "Register Request :",
                data
            );

                socket.companyCode =
                data.company_code;

            socket.companyGuid =
                data.company_guid;

            socket.connectorId =
                data.connector_id;

            console.log("REGISTER TRACE:", {
                socket_id: socket.id,
                connector_id: data.connector_id,
                company_code: data.company_code,
                computer_name: data.computer_name
            });


const registered =
    registry.register(
        data.connector_id,
        socket
    );

if (!registered) {

    console.error(
        "❌ CONNECTOR REGISTRATION REJECTED",
        {
            socket_id: socket.id,
            connector_id: data.connector_id,
            company_code: data.company_code
        }
    );

    return;
}

publishConnectorHealth(data.connector_id, {
    connection_status: "CONNECTED",
    socket_id: socket.id,
    operation_status:
        socket.tallyRequestActive === true
            ? "BUSY"
            : "FREE",
    last_heartbeat_at:
        socket.lastHeartbeat
            ? new Date(socket.lastHeartbeat).toISOString()
            : null
});


});
*/

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



/* 221026
        socket.on("protocol:heartbeat", () => {
            socket.lastHeartbeat = Date.now();
        });

        socket.on("tally:request:start", (data) => {
    socket.tallyRequestActive = true;
    socket.tallyRequestStartedAt = data.timestamp || Date.now();
    socket.lastTallyActivity = Date.now();

    console.log("🟢 TALLY REQUEST START:", data.batchId);
});

socket.on("tally:request:end", (data) => {
    socket.tallyRequestActive = false;
    socket.tallyRequestStartedAt = 0;
    socket.lastTallyActivity = Date.now();

    console.log("✅ TALLY REQUEST END:", data.batchId);
});

socket.on("tally:request:error", (data) => {
    socket.tallyRequestActive = false;
    socket.tallyRequestStartedAt = 0;
    socket.lastTallyActivity = Date.now();

    console.log("❌ TALLY REQUEST ERROR:", data.batchId);
});

        socket.on("disconnect", (reason) => {

            console.log("================================");
            console.log("❌ Connector Disconnected");
            console.log("Socket ID :", socket.id);
            console.log("Reason :", reason);


            if (socket.connectorIds) {

                for (const connectorId of socket.connectorIds) {

                    registry.remove(
                        connectorId,
                        socket
                    );

                }

            }
            console.log("================================");

        });

*/

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

        socket.on("protocol:heartbeat", async () => {
            socket.lastHeartbeat = Date.now();

            await updateOwnedSocketHealth({
                connection_status: "CONNECTED",
                operation_status:
                    socket.tallyRequestActive === true
                        ? "BUSY"
                        : "FREE",
                last_heartbeat_at:
                    new Date(socket.lastHeartbeat).toISOString()
            });
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
                operation_status: "FREE",
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