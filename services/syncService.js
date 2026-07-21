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

    return await sendToConnector(
        socket,
        "getTrialBalance",
        {
            company: tallyCompany,
asOnDate
        }
    );

}

module.exports = {
    getSyncMasterData,
    getTrialBalance
};