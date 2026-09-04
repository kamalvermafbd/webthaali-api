const registry = require("./connectorRegistry");

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

function registerEvents(io) {

    io.on("connection", (socket) => {

        console.log("================================");
        console.log("✅ Connector Connected");
        console.log("Socket ID :", socket.id);
        console.log("================================");

        registry.registerPending(socket);

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

/* 040926
        const registered =
            registry.register(
                connector_id,
                socket
            );


        if (!registered) {

            console.error(
                "❌ CONNECTOR AUTO REGISTRATION REJECTED",
                {
                    socket_id: socket.id,
                    connector_id
                }
            );

            return;
        }


        console.log(
            "✅ CONNECTOR AUTO REGISTERED",
            {
                socket_id: socket.id,
                connector_id,
                company_codes:
                    socket.companyCodes,
                matched_companies:
                    matches
            }
        );
*/
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

        socket.on("getMastersProgress", (data) => {

            socket.lastHeartbeat =
                Date.now();

            console.log(
                "💓 Heartbeat :",
                data.batchId
            );

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