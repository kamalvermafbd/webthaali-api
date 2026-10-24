const registry = require("../socketio/connectorRegistry");
const { sendToConnector } = require("../socketio/sendToConnector");
const { withGlobalConnectorLock } = require("../socketio/connectorOperationCoordinator");
/* 2410126
const crypto = require("crypto");
const {
    acquireConnectorOperationLock,
    renewConnectorOperationLock,
    releaseConnectorOperationLock
} = require("../socketio/connectorHealthStore");
*/

const { createClient } = require("@supabase/supabase-js");
const {
    saveStockGodownBalances
} = require("./saveStockGodownBalances");

const fs = require("fs");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

/* 241026
async function withConnectorLock(connectorId, operation) {

    connectorId = String(connectorId || "").trim();

    if (!connectorId) {
        throw new Error(
            "Connector operation lock failed: connectorId missing"
        );
    }

    if (typeof operation !== "function") {
        throw new Error(
            `Invalid connector operation for ${connectorId}`
        );
    }

    const ownerToken = crypto.randomUUID();

    const LOCK_TTL_MS = 120000;
    const LOCK_RENEW_MS = 30000;
    const MAX_LOCK_WAIT_MS = 5 * 60 * 1000;

    const lockWaitStartedAt = Date.now();

    let lockAcquired = false;
let acquiredSocket = null;

    // Wait until connector becomes free
    while (!lockAcquired) {

        if (
            Date.now() - lockWaitStartedAt >= MAX_LOCK_WAIT_MS
        ) {
            throw new Error(
                `Connector lock wait timeout after 5 minutes: ${connectorId}`
            );
        }

        const socket = registry.get(connectorId);

if (!socket?.connected) {
    throw new Error(
        `Connector disconnected while waiting for lock: ${connectorId}`
    );
}

if (!socket.protocolReceiver) {
    throw new Error(
        `Connector protocol receiver unavailable: ${connectorId}`
    );
}

lockAcquired = await acquireConnectorOperationLock(
    connectorId,
    ownerToken,
    { ttlMs: LOCK_TTL_MS }
);
if (lockAcquired) {

    if (
    !socket.connected ||
    !socket.protocolReceiver ||
    registry.get(connectorId) !== socket
) {
        await releaseConnectorOperationLock(
            connectorId,
            ownerToken
        );

        throw new Error(
            `Connector changed during lock acquisition: ${connectorId}`
        );
    }

    acquiredSocket = socket;
}
        if (!lockAcquired) {
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

   let renewalTimer = null;
let renewing = false;
let lockLost = false;
let operationSucceeded = false;



try {

    if (
        !acquiredSocket?.connected ||
        !acquiredSocket.protocolReceiver ||
        registry.get(connectorId) !== acquiredSocket
    ) {
        throw new Error(
            `Connector unavailable or changed after lock acquisition: ${connectorId}`
        );
    }

        renewalTimer = setInterval(async () => {

            if (renewing || lockLost) return;

            renewing = true;

            try {

                const renewed =
                    await renewConnectorOperationLock(
                        connectorId,
                        ownerToken,
                        LOCK_TTL_MS
                    );

                if (!renewed) {

                    lockLost = true;

                    console.error(
                        "CONNECTOR LOCK LOST:",
                        connectorId
                    );

                    if (acquiredSocket?.connected) {
                            acquiredSocket.disconnect(true);
                        }
                }

            } catch (error) {

                lockLost = true;

                console.error(
                    "CONNECTOR LOCK RENEWAL FAILED:",
                    connectorId,
                    error.message
                );

                if (acquiredSocket?.connected) {
                    acquiredSocket.disconnect(true);
                }

            } finally {
                renewing = false;
            }

        }, LOCK_RENEW_MS);

        const result = await operation();

// Stop new renewals before waiting for an active renewal.
if (renewalTimer) {
    clearInterval(renewalTimer);
    renewalTimer = null;
}

// Wait for any in-progress lock renewal.
while (renewing) {
    await new Promise(resolve => setTimeout(resolve, 50));
}
if (
    !acquiredSocket.connected ||
    registry.get(connectorId) !== acquiredSocket
) {
    throw new Error(
        `Connector disconnected or changed during operation: ${connectorId}`
    );
}
if (lockLost) {
    throw new Error(
        `Connector lock lost during operation: ${connectorId}`
    );
}

const stillOwned = await renewConnectorOperationLock(
    connectorId,
    ownerToken,
    LOCK_TTL_MS
);

if (!stillOwned) {
    lockLost = true;

    if (acquiredSocket.connected) {
        acquiredSocket.disconnect(true);
    }

    throw new Error(
        `Connector lock ownership lost before completion: ${connectorId}`
    );
}

operationSucceeded = true;

return result;

    } finally {

        if (renewalTimer) {
    clearInterval(renewalTimer);
    renewalTimer = null;
}

        while (renewing) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        try {
    const released = await releaseConnectorOperationLock(
    connectorId,
    ownerToken
);
if (!released) {
    if (operationSucceeded) {
        throw new Error(
            `Connector lock release ownership mismatch: ${connectorId}`
        );
    }

    console.error(
        "CONNECTOR LOCK RELEASE NOT OWNED:",
        connectorId
    );

    if (acquiredSocket?.connected) {
        acquiredSocket.disconnect(true);
    }
}
} catch (error) {
    console.error(
        "CONNECTOR LOCK RELEASE FAILED:",
        connectorId,
        error
    );

    if (acquiredSocket?.connected) {
    acquiredSocket.disconnect(true);
}

    if (operationSucceeded) {
        throw error;
    }

    // Preserve the original operation error.
}
    }
}
*/

async function getSyncMasterData({
    company_code,
    tally_owner,
  //  master_type
}) {

   const is_ca =
    String(tally_owner).toUpperCase() === "CA";

const connectorField =
    is_ca
        ? "ca_connector_id"
        : "client_connector_id";

const {
    data: connectorCompany,
    error: connectorError
} = await supabase
    .from("company")
    .select(`company_code, ${connectorField}`)
    .eq("company_code", company_code)
    .single();

if (connectorError) {
    return {
        success: false,
        error: connectorError.message
    };
}

const connector_id =
    connectorCompany?.[connectorField];

if (!connector_id) {
    return {
        success: false,
        error: "Connector not paired"
    };
}

const socket =
    registry.get(connector_id);

if (!socket) {
    return {
        success: false,
        error: "Connector offline"
    };
}



    const { data: company, error: companyError } = await supabase
    .from("company")
    .select("ca_tally_company, client_tally_company")
    .eq("company_code", company_code)
    .single();

if (companyError || !company) {
    return {
        success: false,
        error: "Company not found"
    };
}

const tallyCompany =
    is_ca
        ? company.ca_tally_company
        : company.client_tally_company;

        console.log("company_code =", company_code);
console.log("tally_owner =", tally_owner);
console.log("tallyCompany =", tallyCompany);
const { result, salesResult } = await withGlobalConnectorLock(
    connector_id,
    async (lockedSocket, signal) => {
      if (
    !lockedSocket.connected ||
    registry.get(connector_id) !== lockedSocket ||
    signal.aborted
) {
    throw new Error(
        "Connector disconnected, changed or operation aborted while holding global lock"
    );
}

      const result = await sendToConnector(
    lockedSocket,
    "getTallyMappingData",
    {
        company: tallyCompany
    },
    { signal }
);
        if (!result?.success) {
    return {
        result,
        salesResult: null
    };
}

        const salesResult = await sendToConnector(
    lockedSocket,
    "getSalesVouchers",
    {
        company: tallyCompany
    },
    { signal }
);

               return { result, salesResult };
    }
);

if (!result?.success) {
    return {
        success: false,
        error: result?.error || "Tally Mapping Data request failed"
    };
}

if (!salesResult?.success) {
    return {
        success: false,
        error: salesResult?.error || "Sales Vouchers request failed"
    };
}

if (!result.data || typeof result.data !== "object") {
    return {
        success: false,
        error: "Invalid Tally Mapping Data response"
    };
}

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
   const is_ca =
    String(tally_owner).toUpperCase() === "CA";

const connectorField =
    is_ca
        ? "ca_connector_id"
        : "client_connector_id";

const {
    data: connectorCompany,
    error: connectorError
} = await supabase
    .from("company")
    .select(`company_code, ${connectorField}`)
    .eq("company_code", company_code)
    .single();

if (connectorError) {
    return {
        success: false,
        error: connectorError.message
    };
}

const connector_id =
    connectorCompany?.[connectorField];

if (!connector_id) {
    return {
        success: false,
        error: "Connector not paired"
    };
}

const socket =
    registry.get(connector_id);

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

const lockedResult = await withGlobalConnectorLock(
    connector_id,
    async (lockedSocket, signal) => {

     if (
    !lockedSocket.connected ||
    registry.get(connector_id) !== lockedSocket ||
    signal.aborted
) {
    throw new Error(
        "Connector disconnected, changed or operation aborted while holding Trial Balance global lock"
    );
}
        const tbRowsPromise =
    lockedSocket.protocolReceiver.waitForCollection();

tbRowsPromise.catch(() => {});

        let tbResult;

try {
    tbResult = await sendToConnector(
    lockedSocket,
    "getTrialBalance",
    {
        company: tallyCompany
    },
    { signal }
);
} catch (error) {
    lockedSocket.protocolReceiver.cancelCollection(
        "Trial Balance request error"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    throw error;
}

        if (!tbResult?.success) {

    lockedSocket.protocolReceiver.cancelCollection(
        "Trial Balance request failed"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    return { tbResult };
}
        let tallyLedgers;

try {
    lockedSocket.protocolReceiver.sendReady(
        "getTrialBalance"
    );
    tallyLedgers = await tbRowsPromise;
} catch (error) {
    lockedSocket.protocolReceiver.cancelCollection(
        "Trial Balance streaming error"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    throw error;
}

return { tbResult, tallyLedgers };
    }
);

if (!lockedResult.tbResult?.success) {
    return lockedResult.tbResult;
}

const tallyLedgers = lockedResult.tallyLedgers;
    
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

    console.log("===== LEDGER RECON DEBUG =====");
console.log("SYNC BATCH ID :", sync_batch_id);
console.log("DIFFERENCES COUNT :", differences?.length || 0);
console.log("LEDGER PASSED :", ledgerReconciliationPassed);
console.log("DIFFERENCES :", differences);

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
console.log(
    "LEDGER FLAG UPDATE COMPLETE:",
    sync_batch_id,
    ledgerReconciliationPassed
);
    return {
        success: true,
        data: tallyLedgers,
        summary: {
            tallyLedgers: tallyLedgers.length,
            dbEligible: dbLedgers?.length || 0,
            updated,
            skipped,
            ledgerReconciliationPassed
        }
    };
}

// ==========================================
// GET STOCK GODOWN BALANCE
// ==========================================

async function getStockGodownBalance({
    company_code,
    tally_owner,
    sync_batch_id,
     booksBeginningFrom
}) {
   const is_ca =
    String(tally_owner).toUpperCase() === "CA";

const connectorField =
    is_ca
        ? "ca_connector_id"
        : "client_connector_id";

const {
    data: connectorCompany,
    error: connectorError
} = await supabase
    .from("company")
    .select(`company_code, ${connectorField}`)
    .eq("company_code", company_code)
    .single();

if (connectorError) {
    return {
        success: false,
        error: connectorError.message
    };
}

const connector_id =
    connectorCompany?.[connectorField];

if (!connector_id) {
    return {
        success: false,
        error: "Connector not paired"
    };
}

const socket =
    registry.get(connector_id);

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
/*
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

*/

if (!booksBeginningFrom) {
    return {
        success: false,
        error: "Books Beginning date not available"
    };
}
    // =========================
    // GET GODOWN-WISE STOCK
    // =========================

    // =========================
// WAIT FOR GODOWN PROTOCOL DATA
// =========================

const lockedResult = await withGlobalConnectorLock(
    connector_id,
    async (lockedSocket, signal) => {
       if (
    !lockedSocket.connected ||
    registry.get(connector_id) !== lockedSocket ||
    signal.aborted
) {
    throw new Error(
        "Connector disconnected, changed or operation aborted while holding Stock Godown global lock"
    );
}

const stockRowsPromise =
    lockedSocket.protocolReceiver.waitForCollection();

stockRowsPromise.catch(() => {});

    let stockResult;

try {
    stockResult = await sendToConnector(
        lockedSocket,
        "getStockGodownSummary",
        {
            company: tallyCompany,
            booksBeginningFrom
        },
        { signal }
    );
} catch (error) {
    lockedSocket.protocolReceiver.cancelCollection(
        "Stock Godown request error"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    throw error;
}

if (!stockResult?.success) {
    lockedSocket.protocolReceiver.cancelCollection(
        "Stock Godown request failed"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    return { stockResult };
}

let stock;

try {
    lockedSocket.protocolReceiver.sendReady(
        "getStockGodownSummary"
    );

    stock = await stockRowsPromise;
} catch (error) {
    lockedSocket.protocolReceiver.cancelCollection(
        "Stock Godown streaming error"
    );

    if (lockedSocket.connected) {
        lockedSocket.disconnect(true);
    }

    throw error;
}

return { stockResult, stock };
    }
);

if (!lockedResult.stockResult?.success) {
    return lockedResult.stockResult;
}

const stock = lockedResult.stock;

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
                saveResult?.total || 0,

             stockReconciliationPassed

        }

    };

}


module.exports = {
    getSyncMasterData,
    getTrialBalance,
    getStockGodownBalance
};