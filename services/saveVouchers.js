const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const VoucherIntegrityService = require("./VoucherIntegrityService");

const fs = require("fs");

const ReconciliationEngine =
    require("../reconciliations/ReconciliationEngine");



const {
    buildVoucherRow,
    buildLedgerRows,
    buildInventoryRows,
    buildStockVoucherRows,
    buildBillAllocationRows,
    buildCostCentreRows,
} = require("./voucherRowBuilder");

async function loadLedgerMap({
    company_code,
    tally_owner
}) {

    const { data, error } =
        await supabase
            .from("tally_sync_ledgers")
            .select("guid,parent,name")
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner);


    if(error){
        throw new Error(
            "Failed to load ledger master : " + error.message
        );
    }


    return new Map(
        (data || []).map(row => [
            row.guid,
            row
        ])
    );

}

async function validateNewVoucher(args) {

    const {

    parsedVoucher,

    existingVoucherMap

} = args;

const guid =
    parsedVoucher.header?.guid?.trim();

   const existingAlterId =
    existingVoucherMap.get(guid);

    if (existingAlterId === undefined) {

  return buildValidationResult({

    action: VALIDATION_ACTION.INSERT

});

    }

    return validateAlterId({

    ...args,

    existingAlterId

});

}

async function validateAlterId({

    company_code,

    tally_owner,

    parsedVoucher,

    runId,

    existingAlterId

}) {

    const guid =
        parsedVoucher.header?.guid?.trim();

    const incomingAlterId =
        Number(parsedVoucher.header?.alterid);

  if (existingAlterId !== incomingAlterId) {

   return buildValidationResult({

    action: VALIDATION_ACTION.UPDATE

});

}

return runIntegrityValidation({

    company_code,

    tally_owner,

    parsedVoucher,

    runId

});

}

async function runIntegrityValidation(args) {

    return VoucherIntegrityService.validateVoucher(args);

}

const VALIDATION_ACTION = {

    INSERT: "INSERT",

    UPDATE: "UPDATE",

    FORCE_UPDATE: "FORCE_UPDATE",

    SKIP: "SKIP",

};

function getVoucherGuids(rows) {

    return rows.map(

        row => row.guid

    );

}

function getRowsToSave({

    voucherRows,

    newVoucherRows,

    changedVoucherRows

}) {

    return voucherRows.filter(

        row =>

            newVoucherRows.includes(row.guid) ||

            changedVoucherRows.includes(row.guid)

    );

}

function buildValidationResult({

    action,

    valid = true,

    requiresRepair = false,

    reasons = []

}) {

    return {

        action,

        valid,

        requiresRepair,

        reasons

    };

}

/* 28.07.26 removed

async function processValidationResult({

    integrityResult,

    header,

    sync_batch_id,

    company_code,

    tally_owner,

    newVoucherRows,

    changedVoucherRows,

    unchangedVoucherGuids

}) {

    switch (integrityResult.action) {

        case VALIDATION_ACTION.INSERT:

            await addSyncValidationLog({

                sync_batch_id,

                company_code,

                tally_owner,

                voucher_guid: header.guid.trim(),

                voucher_number: header.voucherNumber,

                voucher_type: header.voucherType,

                action: VALIDATION_ACTION.INSERT,

                validator_name: "VoucherIntegrityService",

                reason: "Voucher Not Found"

            });

            newVoucherRows.push(header.guid.trim());

            return false;

        case VALIDATION_ACTION.UPDATE:

            await addSyncValidationLog({

                sync_batch_id,

                company_code,

                tally_owner,

                voucher_guid: header.guid.trim(),

                voucher_number: header.voucherNumber,

                voucher_type: header.voucherType,

                action: VALIDATION_ACTION.UPDATE,

                validator_name: "VoucherIntegrityService",

                reason: "AlterId Changed"

            });

            changedVoucherRows.push(header.guid.trim());

            return false;

        case VALIDATION_ACTION.SKIP:

        fs.appendFileSync(
    "./logs/queue-debug.jsonl",
    JSON.stringify({
        stage: "SKIP_DELETE",
        sync_batch_id,
        voucher_guid: header.guid.trim()
    }) + "\n"
);

            await supabase
                .from("sync_exe_queue")
                .delete()
                .eq("sync_id", sync_batch_id)
                .eq("voucher_guid", header.guid.trim());

            unchangedVoucherGuids.push(header.guid.trim());

            return true;

        case VALIDATION_ACTION.FORCE_UPDATE:

            await addSyncValidationLog({

                sync_batch_id,

                company_code,

                tally_owner,

                voucher_guid: header.guid.trim(),

                voucher_number: header.voucherNumber,

                voucher_type: header.voucherType,

                action: VALIDATION_ACTION.FORCE_UPDATE,

                validator_name: "VoucherIntegrityService",

                reason: integrityResult.reasons.join(" | ")

            });

            changedVoucherRows.push(header.guid.trim());

            return false;

    }

    return false;

}
*/


async function deleteVoucherLedgers({

    company_code,

    tally_owner,

    voucherGuids

}) {

    const { error } = await supabase

        .from("tally_voucher_ledgers")

        .delete()

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", voucherGuids);

    if (error) {

        throw new Error(

            "Failed to delete Voucher Ledgers: " +

            error.message

        );

    }

}

async function deleteVoucherInventory({

    company_code,

    tally_owner,

    voucherGuids

}) {

    const { error } = await supabase

        .from("tally_voucher_inventory")

        .delete()

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", voucherGuids);

    if (error) {

        throw new Error(

            "Failed to delete Voucher Inventory: " +

            error.message

        );

    }

}

async function deleteStockVouchers({

    company_code,

    tally_owner,

    voucherGuids

}) {

    const { error } = await supabase

        .from("tally_stock_vouchers")

        .delete()

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", voucherGuids);

    if (error) {

        throw new Error(

            "Failed to delete Stock Vouchers: " +

            error.message

        );

    }

}

async function deleteBillAllocations({

    company_code,

    tally_owner,

    voucherGuids

}) {

    const { error } = await supabase

        .from("tally_bill_allocations")

        .delete()

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", voucherGuids);


    if (error) {

        throw new Error(

            "Failed to delete Bill Allocations: " +

            error.message

        );

    }

}



async function deleteCostCentres({

    company_code,

    tally_owner,

    voucherGuids

}) {

    const { error } = await supabase

        .from("tally_costcentre_allocations")

        .delete()

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", voucherGuids);


    if (error) {

        throw new Error(
            "Failed to delete Cost Centres: " +
            error.message
        );

    }

}


async function saveVoucher({

    voucher,

    sync_batch_id,

    company_code,

    tally_owner

}) {

    
    return saveVoucherHeaders({

        rowsToSave: [

            voucher

        ],

        sync_batch_id,

        company_code,

        tally_owner

    });

}



async function saveVoucherHeaders({

   
    rowsToSave,

    sync_batch_id,

    company_code,

    tally_owner

}) {

   



    if (rowsToSave.length === 0) {

        return 0;

    }

    const { error } = await supabase

        .from("tally_vouchers")

        .upsert(rowsToSave, {

            onConflict:
                "company_code,tally_owner,guid"

        });

    if (error) {

        throw new Error(

            "Failed to save Vouchers: " +

            error.message

        );

    }

    return rowsToSave.length;

}

async function saveVoucherLedgers({

    ledgerRows,
    sync_batch_id = null

}) {

    if (ledgerRows.length === 0) {

        return;

    }

    if (sync_batch_id) {

    ledgerRows = ledgerRows.map(row => ({

        ...row,

        sync_batch_id

    }));

}

    const { error } = await supabase

        .from("tally_voucher_ledgers")

        .insert(ledgerRows);

    if (error) {

        throw new Error(

            "Failed to save Voucher Ledgers: " +

            error.message

        );

    }

}


async function saveBillAllocations({

    billAllocationRows,
    sync_batch_id = null

}) {

    if (billAllocationRows.length === 0) {

        return;

    }


    if (sync_batch_id) {

        billAllocationRows = billAllocationRows.map(row => ({

            ...row,

            sync_batch_id

        }));

    }


    const { error } = await supabase

        .from("tally_bill_allocations")

        .insert(billAllocationRows);


    if (error) {

        throw new Error(

            "Failed to save Bill Allocations: " +

            error.message

        );

    }

}


async function saveCostCentres({

    costCentreRows,

    sync_batch_id = null

}) {

    if (costCentreRows.length === 0) {
        return;
    }


    if (sync_batch_id) {

        costCentreRows =
            costCentreRows.map(row => ({

                ...row,

                sync_batch_id

            }));

    }


    const { error } = await supabase

        .from("tally_costcentre_allocations")

        .insert(costCentreRows);


    if (error) {

        throw new Error(
            "Failed to save Cost Centres: " +
            error.message
        );

    }

}

async function saveVoucherInventory({

    inventoryRows,
    sync_batch_id = null

}) {

    if (inventoryRows.length === 0) {

        return;

    }

    if (sync_batch_id) {

    inventoryRows = inventoryRows.map(row => ({

        ...row,

        sync_batch_id

    }));

}

    const { error } = await supabase

        .from("tally_voucher_inventory")

        .insert(inventoryRows);

    if (error) {

        throw new Error(

            "Failed to save Voucher Inventory: " +

            error.message

        );

    }

}

async function saveStockVouchers({

    stockVoucherRows,
    sync_batch_id = null,

    STOCK_DEBUG_FILE

}) {

    if (stockVoucherRows.length === 0) {

        return;

    }

    if (sync_batch_id) {

    stockVoucherRows = stockVoucherRows.map(row => ({

        ...row,

        sync_batch_id

    }));

}

    const { error } = await supabase

        .from("tally_stock_vouchers")

        .insert(stockVoucherRows);

    if (error) {

        fs.appendFileSync(

            STOCK_DEBUG_FILE,

            JSON.stringify({

                stage: "insert_error",

                error

            }) + "\n"

        );

        throw new Error(

            "Failed to save Stock Vouchers: " +

            error.message

        );

    }

    fs.appendFileSync(

        STOCK_DEBUG_FILE,

        JSON.stringify({

            stage: "insert_success",

            inserted: stockVoucherRows.length

        }) + "\n"

    );

}


async function saveVoucherExecutionData({

    company_code,

    tally_owner,

    sync_batch_id,

    rowsToSave,

    voucherGuids,

    ledgerRows,

    inventoryRows,

    stockVoucherRows,

     billAllocationRows,

     costCentreRows,

    STOCK_DEBUG_FILE

}) {

    const success =
        await saveVoucherHeaders({

            rowsToSave,

            sync_batch_id,

            company_code,

            tally_owner

        });

    await deleteVoucherLedgers({

        company_code,

        tally_owner,

        voucherGuids

    });

    await deleteStockVouchers({

        company_code,

        tally_owner,

        voucherGuids

    });

    await deleteBillAllocations({

    company_code,

    tally_owner,

    voucherGuids

});

await deleteCostCentres({

    company_code,

    tally_owner,

    voucherGuids

});

    await saveVoucherLedgers({

    ledgerRows,
    sync_batch_id

});

await saveBillAllocations({

    billAllocationRows,
    sync_batch_id

});

await saveCostCentres({

    costCentreRows,

    sync_batch_id

});

    await deleteVoucherInventory({

        company_code,

        tally_owner,

        voucherGuids

    });

    await saveVoucherInventory({

    inventoryRows,
    sync_batch_id

});

    await saveStockVouchers({

    stockVoucherRows,
    sync_batch_id,
    STOCK_DEBUG_FILE

});

    return success;

}

async function deleteMissingVouchers({

    company_code,

    tally_owner,

    deletedVoucherGuids

}) {

    if (deletedVoucherGuids.length === 0) {

        return;

    }

    await supabase
        .from("tally_voucher_inventory")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("voucher_guid", deletedVoucherGuids);

    await supabase
        .from("tally_voucher_ledgers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("voucher_guid", deletedVoucherGuids);

    await supabase
        .from("tally_stock_vouchers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("voucher_guid", deletedVoucherGuids);

    await supabase
        .from("tally_vouchers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("guid", deletedVoucherGuids);

    await supabase
    .from("tally_bill_allocations")
    .delete()
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner)
    .in("voucher_guid", deletedVoucherGuids);

    await supabase
    .from("tally_costcentre_allocations")
    .delete()
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner)
    .in("voucher_guid", deletedVoucherGuids);

}

async function loadExistingVoucherMap({

    company_code,

    tally_owner

}) {

    const {

        data: existingVouchers,

        error

    } = await supabase

        .from("tally_vouchers")

        .select("guid, alterid")

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner);

    if (error) {

        throw new Error(

            "Failed to load existing vouchers: " +

            error.message

        );

    }

    return new Map(

        (existingVouchers || []).map(v => [

            v.guid,

            Number(v.alterid)

        ])

    );

}

/*29.07.26
async function loadExistingVoucherGuidMap({

    company_code,

    tally_owner

}) {

    const { data, error } = await supabase

        .from("tally_vouchers")

        .select("guid")

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner);

    if (error) {

        throw new Error(
            "Failed to load existing voucher GUIDs : " +
            error.message
        );

    }

   return new Map(
    (data || []).map(row => [
        row.guid,
        true
    ])
);

}
*/

async function saveVoucherGuids({
    company_code,
    tally_owner,
    sync_batch_id,
    voucherGuids = []
}) {
const { data: batch, error } = await supabase
    .from("sync_batches")
    .select("*")
    .eq("batch_id", sync_batch_id)
    .single();

console.log("================================");
console.log("SAVE VOUCHER GUIDS");
console.log("sync_batch_id :", sync_batch_id);
console.log("batch :", batch);
console.log("error :", error);
console.log("================================");

if (
    error ||
    !batch?.incremental_post_completed ||
    !batch?.guid_scan_completed
) {

    console.log(
        "Incremental posting or GUID scan not completed."
    );

    return;

}

/*
     const existingVoucherMap =
        await loadExistingVoucherGuidMap({

            company_code,

            tally_owner

        });
*/
    const incomingVoucherGuids =
    new Set(
        voucherGuids
            .map(v =>
                typeof v === "string"
                    ? v.trim()
                    : v.guid?.trim()
            )
            .filter(Boolean)
    );

        //--------------------------------------------------
    // Reconcile Voucher GUIDs
    //--------------------------------------------------

    const reconciliationResult =
        await ReconciliationEngine.reconcile({

            company_code,

            tally_owner,

            table: "tally_vouchers",

            guidField: "guid",

            incomingGuids: incomingVoucherGuids

        });


        fs.writeFileSync(
    "./logs/incoming-voucher-guids.json",
    JSON.stringify(
        {
            totalIncoming: incomingVoucherGuids.size,
            guids: [...incomingVoucherGuids].sort()
        },
        null,
        2
    )
);

/*

fs.appendFileSync(
    "./logs/voucher-guid-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_COMPARE",
        company_code,
        tally_owner,
     dbCount: reconciliationResult.summary.totalDb,
    incomingCount: reconciliationResult.summary.totalIncoming,
    matched: reconciliationResult.summary.matched,
    status: reconciliationResult.summary.status
    }) + "\n"
);
        */
/*
const guidsToDelete = [];
    

 for (const [guid] of existingVoucherMap) {

    if (!incomingVoucherGuids.has(guid)) {

        guidsToDelete.push(guid);

    }

}
*/

const guidsToDelete =
    reconciliationResult.missingInTally;


    fs.writeFileSync(
    "./logs/missing-vouchers.json",
    JSON.stringify(
        {
            totalMissing:
                reconciliationResult.missingInDB.length,

            guids:
                [...reconciliationResult.missingInDB].sort()
        },
        null,
        2
    )
);

//--------------------------------------------------
// Missing vouchers pipeline
//--------------------------------------------------
if (reconciliationResult.missingInDB.length > 0) {

    console.log(
        "Missing vouchers found :",
        reconciliationResult.missingInDB.length
    );

    fs.appendFileSync(
        "./logs/api-flow.jsonl",
        JSON.stringify({
            stage: "REQUEST_MISSING_VOUCHERS",
            count: reconciliationResult.missingInDB.length
        }) + "\n"
    );

    return {
    status: "WAITING_FOR_MISSING_VOUCHERS",
    missingVoucherGuids: reconciliationResult.missingInDB
};

}

fs.writeFileSync(
    "./logs/db-voucher-guids.json",
    JSON.stringify(
      {
    totalDb: reconciliationResult.summary.totalDb,
    guids: [...reconciliationResult.matched, ...reconciliationResult.missingInTally].sort()
},
        null,
        2
    )
);



fs.writeFileSync(
    "./logs/voucher-guid-compare.json",
    JSON.stringify(
        {
    ...reconciliationResult.summary,
    guidsToDelete: [...guidsToDelete].sort()
}, null, 2)
);

if (guidsToDelete.length > 0) {

    fs.appendFileSync(
        "./logs/voucher-guid-debug.jsonl",
        JSON.stringify({
            stage: "DELETE_EXECUTION",
            company_code,
            tally_owner,
            deleteCount: guidsToDelete.length,
            deletedVoucherGuids: guidsToDelete
        }) + "\n"
    );

    await deleteMissingVouchers({
        company_code,
        tally_owner,
        deletedVoucherGuids: guidsToDelete
    });

}

if (reconciliationResult.missingInDB.length === 0) {

    await supabase
        .from("sync_batches")
        .update({
            reconciliation_completed: true
        })
        .eq("batch_id", sync_batch_id);

}

}

const STOCK_DEBUG_FILE =
    "./logs/stock-movement-debug.jsonl";



console.log("===== SAVEVOUCHERS.JS LOADED =====");

fs.appendFileSync(
    "./logs/test.log",
    "SAVEVOUCHERS.JS LOADED\n"
);

async function saveVouchers({
    company_code,
    tally_owner,
    sync_batch_id,
    country,
    vouchers = [],
     allVoucherGuids = []
}) {

      fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "SAVE_VOUCHERS_START",
        company_code,
        tally_owner,
        sync_batch_id,
        vouchers: vouchers.length,
        allVoucherGuids: allVoucherGuids.length
    }) + "\n"
);
/* 29.07.26
    if (!Array.isArray(vouchers) || vouchers.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }
*/


if (!Array.isArray(vouchers)) {

    vouchers = [];

}

const now = new Date().toISOString();

const runId =
    `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const voucherRows = [];

const ledgerRows = [];

const inventoryRows = [];

const stockVoucherRows = [];

const billAllocationRows = [];

const costCentreRows = [];

let existingVoucherMap = new Map();

let ledgerMap = new Map();

ledgerMap =
    await loadLedgerMap({
        company_code,
        tally_owner
    });

if (vouchers.length > 0) {

    existingVoucherMap =
        await loadExistingVoucherMap({
            company_code,
            tally_owner
        });

}




/*

function getExistingAlterId(guid) {

    return existingVoucherMap.get(guid);

}
    */
// Temporary collections.
// These will be removed after Queue Executor is implemented.

const newVoucherRows = [];

const changedVoucherRows = [];

const unchangedVoucherGuids = [];

const incomingVoucherGuids = [];

/* (28.07.26)
const deletedVoucherGuids = [];


    const incomingVoucherGuids = new Set(
    vouchers
        .map(v => v.header?.guid?.trim())
        .filter(Boolean)
);

for (const guid of existingVoucherMap.keys()) {

    if (!incomingVoucherGuids.has(guid)) {

        deletedVoucherGuids.push(guid);

    }

}

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "DELETED_VOUCHERS",
        company_code,
        tally_owner,
        deletedVoucherGuids: deletedVoucherGuids.length,
        first10Deleted: deletedVoucherGuids.slice(0, 10)
    }) + "\n"
);

if (ENABLE_QUEUE_EXECUTION) {

    await deleteMissingVouchers({

        company_code,

        tally_owner,

        deletedVoucherGuids

    });

}


if (ENABLE_QUEUE_EXECUTION) {

    await cleanupOrphanVoucherData({
        company_code,
        tally_owner
    });

}
*/

for (const voucher of vouchers) {

        const header = voucher.header || {};

       if (!header.guid?.trim()) {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Voucher "${header.voucherNumber}" because GUID is missing.`
            );

            continue;

        }

        incomingVoucherGuids.push(
                header.guid.trim()
        );

// Validation pipeline:
//
// NEW_VOUCHER
//      ↓
// ALTER_ID
//      ↓
// INTEGRITY
//
// Every stage can nominate a GUID into sync_exe_queue.
// Execution is handled later by Queue Executor.

let integrityResult;

try {

    console.log("BEFORE VALIDATE", header.guid);

integrityResult =
    await validateNewVoucher({
        company_code,
        tally_owner,
        parsedVoucher: voucher,
        runId,
        existingVoucherMap,
    });

    fs.appendFileSync(
    "./logs/integrity-result.jsonl",
    JSON.stringify({
        guid: header.guid,
        action: integrityResult.action,
        reasons: integrityResult.reasons
    }) + "\n"
);

console.log("AFTER VALIDATE", header.guid);

} catch (err) {

   

    throw err;
}

    
console.log(
    "Integrity Result :",
    header.voucherNumber,
    integrityResult.action
);

// Queue nomination happens here.
// These arrays are temporary and will be replaced
// by Queue Executor in the final architecture.
switch (integrityResult.action) {

    case VALIDATION_ACTION.INSERT:

        newVoucherRows.push(
            header.guid.trim()
        );

        break;

    case VALIDATION_ACTION.UPDATE:

        changedVoucherRows.push(
            header.guid.trim()
        );

        break;

    case VALIDATION_ACTION.FORCE_UPDATE:

        changedVoucherRows.push(
            header.guid.trim()
        );

        break;

    case VALIDATION_ACTION.SKIP:

        unchangedVoucherGuids.push(
            header.guid.trim()
        );

        continue;

    default:

        continue;

}


voucherRows.push(

    buildVoucherRow({

        header,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    })

);

ledgerRows.push(
    ...buildLedgerRows({
        voucher,
        company_code,
        tally_owner,
        ledgerMap
    })
);

    inventoryRows.push(
    ...buildInventoryRows({
        voucher,
        company_code,
        tally_owner
    })
);
    
stockVoucherRows.push(
    ...buildStockVoucherRows({
        voucher,
        company_code,
        tally_owner
    })
);

billAllocationRows.push(
    ...buildBillAllocationRows({
        voucher,
        company_code,
        tally_owner,
        country,
        ledgerMap
    })
);

costCentreRows.push(
    ...buildCostCentreRows({
        voucher,
        company_code,
        tally_owner
    })
);

console.log(
    "BILL ALLOCATION ROWS:",
    billAllocationRows
);

        }


        

        let success = 0;

        let rowsToSave = [];

let voucherGuids = [];

if (voucherRows.length > 0){

      rowsToSave = getRowsToSave({

    voucherRows,

    newVoucherRows,

    changedVoucherRows

});

voucherGuids =
    getVoucherGuids(rowsToSave);
        
fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_DELETE",
        company_code,
        tally_owner,
        totalVoucherRows: voucherRows.length,
        rowsToSave: rowsToSave.length,
        newVoucherRows: newVoucherRows.length,
        changedVoucherRows: changedVoucherRows.length,
        unchangedVoucherGuids: unchangedVoucherGuids.length,
        voucherGuids: voucherGuids.length,
        first10VoucherGuids: voucherGuids.slice(0, 10)
    }) + "\n"
);


success =
    await saveVoucherExecutionData({

        company_code,

        tally_owner,

        sync_batch_id,

        rowsToSave,

        voucherGuids,

        ledgerRows,

        inventoryRows,

        stockVoucherRows,

        billAllocationRows,

         costCentreRows,

        STOCK_DEBUG_FILE

    });

}

 const { error: postUpdateError } = await supabase
    .from("sync_batches")
    .update({
        incremental_post_completed: true
    })
    .eq("batch_id", sync_batch_id)
    .select();

if (postUpdateError) {
    throw postUpdateError;
}

console.log(
    "INCREMENTAL POST COMPLETED UPDATED"
);


    fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "SAVE_EXECUTION_COMPLETED",
        success,
        rowsToSave: rowsToSave.length
    }) + "\n"
);


 /*   
allVoucherGuids = [];
*/
//temp code

  const validGuids = allVoucherGuids
    .map(v =>
        typeof v === "string"
            ? v.trim()
            : v.guid?.trim()
    )
    .filter(Boolean);

if (validGuids.length === 0) {

    console.log("GUID scan failed. Delete skipped.");

    return {
        total: vouchers.length,
        success,
        failed: vouchers.length - success
    };

}
/* 29.07.26
await supabase
    .from("sync_batches")
    .update({

        guid_scan_completed: true,

        total_guids: validGuids.length,

        total_vouchers: vouchers.length,

        current_module: "GUID_SCAN"

    })

   .eq("batch_id", sync_batch_id);
*/

const { data, error } = await supabase
    .from("sync_batches")
    .update({
        guid_scan_completed: true,
        //total_guids: validGuids.length,
        total_vouchers: vouchers.length,
        current_module: "GUID_SCAN"
    })
    .eq("batch_id", sync_batch_id)
    .select();

    const { data: verifyBatch, error: verifyError } = await supabase
    .from("sync_batches")
    .select("*")
    .eq("batch_id", sync_batch_id)
    .single();

console.log("================================");
console.log("AFTER GUID UPDATE");
console.log("sync_batch_id :", sync_batch_id);
console.log("batch :", verifyBatch);
console.log("error :", verifyError);
console.log("================================");

console.log("================================");
console.log("GUID UPDATE");
console.log("sync_batch_id :", sync_batch_id);
console.log("data :", data);
console.log("error :", error);
console.log("================================");

const { data: verify } = await supabase
    .from("sync_batches")
    .select("batch_id,reconciliation_completed")
    .eq("batch_id", sync_batch_id)
    .single();

console.log("VERIFY AFTER UPDATE :", verify);
   
    fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "GUID_SCAN_UPDATED",
        validGuids: validGuids.length
    }) + "\n"
);


fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "CALLING_SAVE_VOUCHER_GUIDS"
    }) + "\n"
);




const reconciliationStatus =
    await saveVoucherGuids({
        company_code,
        tally_owner,
        sync_batch_id,
        voucherGuids: allVoucherGuids
    });

fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "SAVE_VOUCHER_GUIDS_COMPLETED",
        reconciliationStatus
    }) + "\n"
);

if (
    reconciliationStatus?.status ===
    "WAITING_FOR_MISSING_VOUCHERS"
) {

    console.log(
        "Waiting for missing vouchers from connector..."
    );

    return reconciliationStatus;

}

await supabase
    .from("sync_batches")
    .update({

        batch_status: "COMPLETED",

        current_module: "COMPLETED",

        processed_vouchers: success,

        failed_vouchers: vouchers.length - success,

        completed_at: new Date().toISOString()

    })

    .eq("batch_id", sync_batch_id);

fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "BATCH_COMPLETED"
    }) + "\n"
);


    return {

    total: vouchers.length,

    success,

    failed: vouchers.length - success

};

}

async function cleanupOrphanVoucherData({
    company_code,
    tally_owner
}) {

    //------------------------------------------
    // Load valid voucher GUIDs
    //------------------------------------------

    const { data: vouchers, error } =
        await supabase
            .from("tally_vouchers")
            .select("guid")
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner);

    if (error) {

        throw new Error(
            "Failed to load voucher GUIDs : " +
            error.message
        );

    }

   const validVoucherGuids = new Set(
    (vouchers || []).map(v => v.guid)
);

    //------------------------------------------
    // No vouchers left
    //------------------------------------------

    fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "ORPHAN_CHECK",
        company_code,
        tally_owner,
        validVoucherGuids: validVoucherGuids.size
    }) + "\n"
);

    if (validVoucherGuids.size === 0) {

        await supabase
            .from("tally_voucher_ledgers")
            .delete()
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner);

        await supabase
            .from("tally_voucher_inventory")
            .delete()
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner);

        await supabase
        .from("tally_stock_vouchers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner);

        return;

    }

//------------------------------------------
// Load Ledger GUIDs
//------------------------------------------

const {
    data: ledgerRows,
    error: ledgerError
} = await supabase
    .from("tally_voucher_ledgers")
    .select("voucher_guid")
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

if (ledgerError) {

    throw new Error(
        "Failed to load voucher ledgers : " +
        ledgerError.message
    );

}

const orphanLedgerGuids =
    [...new Set(
        (ledgerRows || [])
            .map(r => r.voucher_guid)
            .filter(
                guid =>
                    !validVoucherGuids.has(guid)
            )
    )];

if (orphanLedgerGuids.length > 0) {

    const { error: deleteLedgerError } =
        await supabase
            .from("tally_voucher_ledgers")
            .delete()
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .in("voucher_guid", orphanLedgerGuids);


            const { count: ledgerCount } = await supabase
    .from("tally_voucher_ledgers")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "AFTER_LEDGER_DELETE",
        remainingRows: ledgerCount
    }) + "\n"
);

    if (deleteLedgerError) {

        throw new Error(
            "Failed to cleanup orphan ledgers : " +
            deleteLedgerError.message
        );

    }

}

//------------------------------------------
// Load Inventory GUIDs
//------------------------------------------

const {
    data: inventoryRows,
    error: inventoryError
} = await supabase
    .from("tally_voucher_inventory")
    .select("voucher_guid")
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

if (inventoryError) {

    throw new Error(
        "Failed to load voucher inventory : " +
        inventoryError.message
    );

}

const orphanInventoryGuids =
    [...new Set(
        (inventoryRows || [])
            .map(r => r.voucher_guid)
            .filter(
                guid =>
                   !validVoucherGuids.has(guid)
            )
    )];

if (orphanInventoryGuids.length > 0) {

    const { error: deleteInventoryError } =
        await supabase
            .from("tally_voucher_inventory")
            .delete()
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .in("voucher_guid", orphanInventoryGuids);

            const { count: inventoryCount } = await supabase
    .from("tally_voucher_inventory")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "AFTER_INVENTORY_DELETE",
        remainingRows: inventoryCount
    }) + "\n"
);

    if (deleteInventoryError) {

        throw new Error(
            "Failed to cleanup orphan inventory : " +
            deleteInventoryError.message
        );

    }

}

//------------------------------------------
// Load Stock Voucher GUIDs
//------------------------------------------

const {
    data: stockVoucherRows,
    error: stockVoucherError
} = await supabase
    .from("tally_stock_vouchers")
    .select("voucher_guid")
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

if (stockVoucherError) {

    throw new Error(
        "Failed to load stock vouchers : " +
        stockVoucherError.message
    );

}

const orphanStockVoucherGuids =
    [...new Set(
        (stockVoucherRows || [])
            .map(r => r.voucher_guid)
            .filter(
                guid =>
                    !validVoucherGuids.has(guid)
            )
    )];

if (orphanStockVoucherGuids.length > 0) {

    const { count: stockBefore } = await supabase
    .from("tally_stock_vouchers")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);


    const { error: deleteStockVoucherError } =
        await supabase
            .from("tally_stock_vouchers")
            .delete()
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .in("voucher_guid", orphanStockVoucherGuids);

    if (deleteStockVoucherError) {

        throw new Error(
            "Failed to cleanup orphan stock vouchers : " +
            deleteStockVoucherError.message
        );

    }

}

}

module.exports = {

    saveVouchers,
     saveVoucherGuids,

    saveVoucherExecutionData,

    deleteVoucherLedgers,

    deleteVoucherInventory,

    deleteStockVouchers,

    deleteBillAllocations,

    saveVoucher,

    deleteCostCentres,

    saveCostCentres,

    saveVoucherHeaders,

    saveVoucherLedgers,

    saveVoucherInventory,

    saveStockVouchers

};
