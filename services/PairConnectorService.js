const crypto = require("crypto");

const {
    createClient
} = require("@supabase/supabase-js");

const {
    sendToConnector
} = require("../socketio/sendToConnector");

const registry =
    require("../socketio/connectorRegistry");



const supabase =
    createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_SERVICE_KEY
    );


async function pairConnector({
    company_code,
    tally_company,
    tally_company_guid,
    is_ca
}) {

    const connectorField =
        is_ca
            ? "ca_connector_id"
            : "client_connector_id";


    if (!company_code || !tally_company_guid) {

        return {
            success: false,
            error:
                "company_code or tally_company_guid missing"
        };

    }


    const {
        data: companyData,
        error: companyError
    } = await supabase

        .from("company")

        .select(
            `company_code, ${connectorField}`
        )

        .eq(
            "company_code",
            company_code
        )

        .single();


    if (companyError) {

        return {
            success: false,
            error: companyError.message
        };

    }


    let socket = null;

    let connector_id =
        companyData?.[connectorField];


    // ==========================================
    // EXISTING CONNECTOR
    // ==========================================
/*
    if (connector_id) {

        socket =
            registry.get(
                connector_id,
                company_code
            );


        // Existing connector not registered
        // but a pending connector is available

        if (!socket) {

            return {
                success: false,
                error: "Linked connector offline"
            };

        }

    }
*/

    // ==========================================
// EXISTING CONNECTOR
// ==========================================

if (connector_id) {

    socket =
        registry.get(
            connector_id,
            company_code
        );


    // Existing connector is not registered yet.
    // Try the pending connector for pairing.

    if (!socket) {

        socket =
            registry.getPending();

        if (!socket) {

            return {
                success: false,
                error: "Linked connector offline"
            };

        }

    }

    if (
    !socket.companyGuids ||
    !socket.companyGuids.includes(
        tally_company_guid
    )
) {

    return {
        success: false,
        error:
            "Selected Tally company is not available on this connector"
    };

}

}
    // ==========================================
    // FIRST TIME PAIRING
    // ==========================================

    else {

        socket =
            registry.getPending();


        if (!socket) {

            return {
                success: false,
                error:
                    "No pending connector available"
            };

        }


        if (
                !socket.companyGuids ||
                !socket.companyGuids.includes(
                    tally_company_guid
                )
            ) {

                return {
                    success: false,
                    error:
                        "Selected Tally company is not available on this connector"
                };

            }

        connector_id =
            `CON-${crypto.randomUUID()}`;


        const {
            error: connectorSaveError
        } = await supabase

            .from("company")

            .update({
                [connectorField]:
                    connector_id
            })

            .eq(
                "company_code",
                company_code
            );


        if (connectorSaveError) {

            return {
                success: false,
                error:
                    connectorSaveError.message
            };

        }

    }


    console.log(
        "PAIR TRACE:",
        {
            company_code,
            connector_id,
            socket_id: socket?.id,
            socket_connector_id: socket?.connectorId,
            socket_company_code: socket?.companyCode,
            socket_computer: socket?.computerName
        }
    );


    const result =
        await sendToConnector(
            socket,
            "pair",
            {
                company_code,
                company_name: tally_company,
                company_guid: tally_company_guid,
                connector_id
            }
        );


    console.log(
        "PAIR RESULT :",
        result
    );


    return result;
}

async function pairConnectorForBatch({
    company_code,
    tally_owner
}) {

    const owner = String(tally_owner || "")
        .trim()
        .toUpperCase();

    if (!company_code || !owner) {
        return {
            success: false,
            error: "company_code or tally_owner missing"
        };
    }

    const is_ca = owner === "CA";

    const tallyCompanyField =
        is_ca
            ? "ca_tally_company"
            : "client_tally_company";

    const tallyGuidField =
        is_ca
            ? "ca_tally_company_guid"
            : "client_tally_company_guid";

    const {
        data: companyData,
        error: companyError
    } = await supabase
        .from("company")
        .select(
            `company_code, ${tallyCompanyField}, ${tallyGuidField}`
        )
        .eq("company_code", company_code)
        .single();

    if (companyError) {
        return {
            success: false,
            error: companyError.message
        };
    }

    return pairConnector({
        company_code,
        tally_company:
            companyData?.[tallyCompanyField],
        tally_company_guid:
            companyData?.[tallyGuidField],
        is_ca
    });
}

module.exports = {
    pairConnector,
    pairConnectorForBatch
};