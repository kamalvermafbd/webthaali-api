const registry = require("../socketio/connectorRegistry");
const { sendToConnector } = require("../socketio/sendToConnector");
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function getSyncMasterData({
    company_code,
    tally_owner,
  //  master_type
}) {

    const socket = registry.get(company_code);

    if (!socket) {
        return {
            success: false,
            error: "Connector offline"
        };
    }



    const { data: company } = await supabase
    .from("company")
    .select("ca_tally_company, client_tally_company")
    .eq("company_code", company_code)
    .single();

const tallyCompany =
    tally_owner === "CA"
        ? company.ca_tally_company
        : company.client_tally_company;

        console.log("company_code =", company_code);
console.log("tally_owner =", tally_owner);
console.log("tallyCompany =", tallyCompany);
const result = await sendToConnector(
    socket,
    "getTallyMappingData",
    {
        company: tallyCompany
    }
);

const salesResult = await sendToConnector(
    socket,
    "getSalesVouchers",
    {
        company: tallyCompany
    }
);

console.dir(salesResult, { depth: null });

    console.log("======================================");
console.log("GET TALLY MAPPING DATA RESPONSE");
console.dir(result, { depth: null });
console.log("======================================");

/*let tallyData = [];

if (master_type === "STOCK") {
    tallyData = result.data.stock || [];
}
else if (master_type === "UNIT") {
    tallyData = result.data.units || [];
}

else if (master_type === "SALES_LEDGER") {
    tallyData = result.data.salesGL || [];
}

else if (master_type === "TAX_LEDGER") {
    tallyData = result.data.taxGL || [];
}

else if (master_type === "DEBTOR") {
    tallyData = result.data.debtors || [];
}
else {
    return {
        success: false,
        error: "Invalid master_type"
    };
}
*/
const tallyData = {
    STOCK: result.data.stock || [],
    UNIT: result.data.units || [],
    SALE_GL: result.data.salesGL || [],
    TAX_GL: result.data.taxGL || [],
    DEBTOR: result.data.debtors || []
};
//console.log("MASTER TYPE :", master_type);
//console.log("TALLY DATA COUNT :", tallyData.length);
console.dir(tallyData, { depth: null });


/*
let mappingType = master_type;

if (master_type === "SALES_LEDGER") {
    mappingType = "SALE_GL";
}

if (master_type === "TAX_LEDGER") {
    mappingType = "TAX_GL";
}
    */
   


    
   return {
    success: true,
    data: tallyData
};

}


/*
async function getTrialBalance({
    company_code,
    tally_owner,
    asOnDate
}) {

    const socket = registry.get(company_code);

    if (!socket) {
        return {
            success: false,
            error: "Connector offline"
        };
    }

    const { data: company } = await supabase
        .from("company")
        .select("ca_tally_company, client_tally_company")
        .eq("company_code", company_code)
        .single();

    const tallyCompany =
        tally_owner === "CA"
            ? company.ca_tally_company
            : company.client_tally_company;

    // Get Books Beginning from existing connector/master flow
    const masterResult = await sendToConnector(
        socket,
        "getMasters",
        {
            company: tallyCompany
        }
    );

    const booksBeginningFrom =
        masterResult?.summary?.booksBeginningFrom;

    if (!booksBeginningFrom) {
        return {
            success: false,
            error: "Books Beginning date not available"
        };
    }

    return await sendToConnector(
        socket,
        "getTrialBalance",
        {
            company: tallyCompany,
            booksBeginningFrom,
            asOnDate
        }
    );
}

*/
async function getTrialBalance({
    company_code,
    tally_owner
}) {

    const socket = registry.get(company_code);

    if (!socket) {
        return {
            success: false,
            error: "Connector offline"
        };
    }

    // =========================
    // GET TALLY COMPANY
    // =========================

    const { data: company, error: companyError } =
        await supabase
            .from("company")
            .select(
                "ca_tally_company, client_tally_company"
            )
            .eq("company_code", company_code)
            .single();

    if (companyError || !company) {
        return {
            success: false,
            error: "Company not found"
        };
    }

    const tallyCompany =
        tally_owner === "CA"
            ? company.ca_tally_company
            : company.client_tally_company;

    // =========================
    // GET TRIAL BALANCE
    // DATE IS CONTROLLED
    // INSIDE CONNECTOR
    // =========================

    const tbResult =
        await sendToConnector(
            socket,
            "getTrialBalance",
            {
                company: tallyCompany
            }
        );

    if (!tbResult?.success) {
        return tbResult;
    }

    const tallyLedgers =
        Array.isArray(tbResult.data)
            ? tbResult.data
            : [];

    console.log("======================================");
    console.log("TRIAL BALANCE RECEIVED");
    console.log("COMPANY CODE :", company_code);
    console.log("TALLY OWNER  :", tally_owner);
    console.log("TALLY COMPANY:", tallyCompany);
    console.log("COUNT        :", tallyLedgers.length);
    console.log("======================================");

    // =========================
    // GET ONLY ASSETS + LIABILITIES
    // FOR SAME COMPANY + OWNER
    // =========================

    const { data: dbLedgers, error: dbError } =
        await supabase
            .from("tally_sync_ledgers")
            .select(`
                id,
                guid,
                nature,
                tally_balance,
                db_balance
            `)
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .in("nature", [
                "Assets",
                "Liabilities"
            ]);

    if (dbError) {
        throw dbError;
    }

    // =========================
    // GUID MAP
    // =========================

    const dbLedgerMap = new Map();

    for (const ledger of dbLedgers || []) {

        if (!ledger.guid) {
            continue;
        }

        dbLedgerMap.set(
            String(ledger.guid).trim(),
            ledger
        );
    }

        // =========================
    // PREPARE DB UPDATES
    // =========================

    const updates = [];
    let skipped = 0;
    let updated = 0;


    for (const tallyLedger of tallyLedgers) {

        const guid =
            String(
                tallyLedger.guid || ""
            ).trim();

        if (!guid) {
            skipped++;
            continue;
        }

        const dbLedger =
            dbLedgerMap.get(guid);

        if (!dbLedger) {
            skipped++;
            continue;
        }

        if (
            dbLedger.nature !== "Assets" &&
            dbLedger.nature !== "Liabilities"
        ) {
            skipped++;
            continue;
        }

        const tallyBalance =
            Number(
                tallyLedger.closingBalance || 0
            );

        const dbBalance =
            Number(
                dbLedger.db_balance || 0
            );

        const balanceDifference =
            tallyBalance - dbBalance;

        updates.push({
            id: dbLedger.id,
            tally_balance: tallyBalance,
            balance_difference: balanceDifference,
            updated_at: new Date().toISOString()
        });
    }

    console.log("======================================");
    console.log("TB DB UPDATES PREPARED");
    console.log("UPDATES :", updates.length);
    console.log("SKIPPED :", skipped);
    console.log("======================================");


    // =========================
// BULK UPDATE
// =========================

// =========================
// BULK DB UPDATE
// =========================

if (updates.length > 0) {

    const { data: updatedCount, error: updateError } =
        await supabase.rpc(
            "update_trial_balance",
            {
                p_updates: updates
            }
        );

    if (updateError) {

        console.error(
            "TB BULK UPDATE FAILED:",
            updateError
        );

        throw updateError;
    }

    updated = Number(updatedCount || 0);
}
    
    console.log("======================================");
    console.log("TRIAL BALANCE DB UPDATE COMPLETE");
    console.log("TALLY LEDGERS :", tallyLedgers.length);
    console.log("DB ELIGIBLE   :", dbLedgers?.length || 0);
    console.log("UPDATED       :", updated);
    console.log("SKIPPED       :", skipped);
    console.log("======================================");

    return {
        success: true,
        data: tallyLedgers,
        summary: {
            tallyLedgers: tallyLedgers.length,
            dbEligible: dbLedgers?.length || 0,
            updated,
            skipped
        }
    };
}

module.exports = {
    getSyncMasterData,
    getTrialBalance
};