const registry = require("./connectorRegistry");

const {
    dispatchBatch
} = require("../workers/queueExistingBatch");

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

// ==========================================
// RECOVER WAITING BATCHES
// ==========================================

await recoverWaitingBatchesForCompanies({
    companyGuids: company_guids,
    socket
});
    } catch (err) {

        console.error(
            "❌ CONNECTOR IDENTITY ERROR:",
            err
        );

    }

});

// Connector Register
socket.on("register", (data) => {

            console.log(
                "Register Request :",
                data
            );
/*
020926
            socket.companyCode =
                data.company_code;

            socket.companyGuid =
                data.company_guid;

            socket.connectorId =
                data.connector_id;
                
                console.log("REGISTER TRACE:", {
                
                */

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
});

/*020926
            registry.register(
                data.connector_id,
                socket
            );

        });

socket.on("register", (data) => {

    console.log(
        "Register Request :",
        data
    );

    socket.connectorId =
        data.connector_id;

    registry.register(
        data.connector_id,
        socket
    );

});
*/
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
/*040926
            if (socket.connectorId) {

                registry.remove(
                    socket.connectorId,
                    socket
                );

            }
*/

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

    });

}

module.exports = {
    registerEvents
};