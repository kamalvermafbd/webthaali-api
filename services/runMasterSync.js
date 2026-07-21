const { supabase } = require("../config/supabase");

const registry =
    require("../socket/registry");

const sendToConnector =
    require("../socket/sendToConnector");

const saveMasters =
    require("./saveMasters");

async function runMasterSync({

    company_code,

    tally_owner

}) {

    // =========================
    // VALIDATION
    // =========================

    if (!company_code) {

        throw new Error(
            "company_code missing"
        );

    }

    if (

        tally_owner !== "CA" &&

        tally_owner !== "USER"

    ) {

        throw new Error(
            "Invalid tally_owner"
        );

    }

    // =========================
    // GET COMPANY
    // =========================

    const {

        data: company,

        error

    } = await supabase

        .from("company")

        .select("*")

        .eq(
            "company_code",
            company_code
        )

        .single();

    if (error || !company) {

        throw new Error(
            "Company not found"
        );

    }

    // =========================
    // DECIDE TALLY COMPANY
    // =========================

    const tallyCompany =

        tally_owner === "CA"

            ? company.ca_tally_company

            : company.client_tally_company;

    if (!tallyCompany) {

        throw new Error(
            "Tally company not mapped"
        );

    }

    // =========================
    // CONNECTOR
    // =========================

    const socket =
        registry.get(company_code);

    if (!socket) {

        throw new Error(
            "Connector offline"
        );

    }

    // =========================
    // IMPORT FROM CONNECTOR
    // =========================

    const result =
        await sendToConnector(

            socket,

            "getMasters",

            {

                company:
                    tallyCompany

            }

        );

    if (!result.success) {

        throw new Error(

            result.error ||

            "Unable to import masters"

        );

    }

    // =========================
    // SAVE DATABASE
    // =========================

    const saveResult =
        await saveMasters({

            company_code,

            tally_owner,

            ...result

        });

    // =========================
    // RETURN
    // =========================

    return {

        success: true,

        summary:
            result.summary,

        saveSummary:
            saveResult

    };

}

module.exports =

    runMasterSync;