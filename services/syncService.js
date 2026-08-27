const registry = require("../socketio/connectorRegistry");
const { sendToConnector } = require("../socketio/sendToConnector");
const { createClient } = require("@supabase/supabase-js");
const {
    saveStockGodownBalances
} = require("./saveStockGodownBalances");

const fs = require("fs");

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
    sync_batch_id
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
// WAIT FOR TRIAL BALANCE PROTOCOL DATA
// =========================

const tbRowsPromise =
    socket.protocolReceiver.waitForCollection();


// =========================
// REQUEST TRIAL BALANCE
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


// =========================
// ALLOW CONNECTOR TO SEND CHUNKS
// =========================

socket.protocolReceiver.sendReady(
    "getTrialBalance"
);


// =========================
// WAIT FOR ALL TRIAL BALANCE CHUNKS
// =========================

const tallyLedgers =
    await tbRowsPromise;
    
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
                 opening_balance,
                opening_balance_type,
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
// CALCULATE DB BALANCE
// FROM VOUCHER LEDGERS
// =========================

// =========================
// LOOKUP DB BALANCE FROM VIEW
// =========================

const { data: dbBalances, error: dbBalanceError } =
    await supabase
        .from("tally_ledger_db_balances")
        .select(`
            company_code,
            tally_owner,
            ledger_guid,
            ledger_name,
            db_balance
        `)
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner);

if (dbBalanceError) {
    throw dbBalanceError;
}

const dbBalanceMap = new Map();

for (const row of dbBalances || []) {

    const guid =
        String(row.ledger_guid || "").trim();

    if (!guid) {
        continue;
    }

    dbBalanceMap.set(
        guid,
        Number(row.db_balance || 0)
    );
}

console.log("======================================");
console.log("DB BALANCE VIEW LOOKUP");
console.log("ROWS :", dbBalances?.length || 0);
console.log("MAP  :", dbBalanceMap.size);
console.log("======================================");

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
                dbBalanceMap.get(guid) || 0
            );

        const openingBalance =
    Number(
        dbLedger.opening_balance || 0
    );

const openingType =
    String(
        dbLedger.opening_balance_type || ""
    ).trim().toUpperCase();

const openingImpact =
    openingType === "CR"
        ? openingBalance
        : openingType === "DR"
            ? -openingBalance
            : 0;

const balanceDifference =
    tallyBalance -
    dbBalance -
    openingImpact;

       updates.push({
    id: dbLedger.id,
    tally_balance: tallyBalance,
    db_balance: dbBalance,
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

    const { data: differences, error: recoError } =
    await supabase
        .from("balance_difference_view")
        .select("id")
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner);

if (recoError) {
    throw recoError;
}

const ledgerReconciliationPassed =
    !differences || differences.length === 0;

if (sync_batch_id) {
    const { error } = await supabase
        .from("sync_batches")
        .update({
            ledger_reconciliation_completed:
                ledgerReconciliationPassed
        })
        .eq("batch_id", sync_batch_id);

    if (error) throw error;
}

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

// ==========================================
// GET STOCK GODOWN BALANCE
// ==========================================

async function getStockGodownBalance({
    company_code,
    tally_owner,
    sync_batch_id
}) {
    const socket =
        registry.get(company_code);

    if (!socket) {

        return {
            success: false,
            error: "Connector offline"
        };

    }


    // =========================
    // GET TALLY COMPANY
    // =========================

    const {
        data: company,
        error: companyError
    } =
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
    // GET BOOKS BEGINNING
    // =========================

    const masterResult =
        await sendToConnector(
            socket,
            "getMasters",
            {
                company: tallyCompany
            }
        );


    const booksBeginningFrom =
        masterResult
            ?.summary
            ?.booksBeginningFrom;


    if (!booksBeginningFrom) {

        return {
            success: false,
            error:
                "Books Beginning date not available"
        };

    }


    // =========================
    // GET GODOWN-WISE STOCK
    // =========================

    // =========================
// WAIT FOR GODOWN PROTOCOL DATA
// =========================

const stockRowsPromise =
    socket.protocolReceiver.waitForCollection();

const stockResult =
    await sendToConnector(
        socket,
        "getStockGodownSummary",
        {
            company: tallyCompany,
            booksBeginningFrom
        }
    );

if (!stockResult?.success) {

    return stockResult;

}

socket.protocolReceiver.sendReady(
    "getStockGodownSummary"
);


// =========================
// WAIT FOR ALL CHUNKS
// =========================

const stock =
    await stockRowsPromise;

// =========================
// SAVE GODOWN STOCK TO DB
// =========================

const saveResult =
    await saveStockGodownBalances({

        company_code,

        tally_owner,

        sync_batch_id,

        stockGodownBalances:
            stock

    });

console.log(
    "STOCK GODOWN DB SAVE:",
    saveResult
);

const { data: differences, error: recoError } =
    await supabase
        .from("stock_godown_reconciliation_view")
        .select("id")
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner);

if (recoError) {
    throw recoError;
}

const stockReconciliationPassed =
    !differences || differences.length === 0;

if (sync_batch_id) {
    const { error } = await supabase
        .from("sync_batches")
        .update({
            stock_reconciliation_completed:
                stockReconciliationPassed
        })
        .eq("batch_id", sync_batch_id);

    if (error) {
        throw error;
    }
}

fs.writeFileSync(
    "./stock-godown-server-result.json",
    JSON.stringify(
        stock,
        null,
        2
    ),
    "utf8"
);

console.log(
    "======================================"
);

console.log(
    "STOCK GODOWN DATA FROM CONNECTOR"
);

console.dir(
    stock,
    {
        depth: null
    }
);

console.log(
    "TOTAL STOCK GODOWN ROWS:",
    stock.length
);

console.log(
    "======================================"
);
    console.log(
        "======================================"
    );

    console.log(
        "STOCK GODOWN BALANCE RECEIVED"
    );

    console.log(
        "COMPANY CODE :",
        company_code
    );

    console.log(
        "TALLY OWNER  :",
        tally_owner
    );

    console.log(
        "TALLY COMPANY:",
        tallyCompany
    );

    console.log(
        "ROWS         :",
        stock.length
    );

    console.log(
        "======================================"
    );


    return {

        success: true,

        data: stock,

        summary: {

            stockGodownRows:
                stock.length,

            dbSaved:
                saveResult?.total || 0

        }

    };

}


module.exports = {
    getSyncMasterData,
    getTrialBalance,
    getStockGodownBalance
};