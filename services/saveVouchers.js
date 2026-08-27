const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

//const VoucherIntegrityService = require("./VoucherIntegrityService");

const fs = require("fs");
/*
const ReconciliationEngine =
    require("../reconciliations/ReconciliationEngine");
*/

const ReconciliationManager =
    require("../sync-engine/ReconciliationManager");
    
const VoucherExecutionService =
    require("../sync-engine/VoucherExecutionService");

const VoucherRowBuilder =
    require("./VoucherRowBuilder");


const VoucherValidationService =
    require("../sync-engine/VoucherValidationService");

const {
    filterRowsByVoucherGuids
} = require("./VoucherChildRowFilter");

const {
    buildLedgerRows,
    buildInventoryRows,
    buildStockVoucherRows,
    buildBillAllocationRows,
    buildCostCentreRows
} = require("./VoucherChildRowBuilder");

const BatchStatusManager =
require("../sync-engine/BatchStatusManager");

/*const BatchManager =
    require("../sync-engine/BatchManager");

const {
    buildVoucherOperations
} = require("../sync-engine/VoucherOperationBuilder");
*/
const {
    VALIDATION_ACTION,
    TABLES,
    VOUCHER_COLUMNS
} = require("../sync-engine/constants");

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

/* 070826
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

*/

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

    const newVoucherGuidSet =
        new Set(newVoucherRows);

    const changedVoucherGuidSet =
        new Set(changedVoucherRows);

    return voucherRows.filter(
        row =>
            newVoucherGuidSet.has(row.guid) ||
            changedVoucherGuidSet.has(row.guid)
    );
}


/* 070826
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
*/

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

*/

/* 080826
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

            "Failed to soft delete Voucher Ledgers: " +

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

            "Failed to soft delete Voucher Inventory: " +

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

            "Failed to soft delete Stock Vouchers: " +

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

            "Failed to soft delete Bill Allocations: " +

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

            "Failed to soft delete Cost Centres: " +

            error.message

        );

    }

}

*/

/*
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
*/

async function saveVoucher({
    voucher,
    sync_batch_id,
    company_code,
    tally_owner
}) {

    const guid =
        voucher?.guid ||
        voucher?.header?.guid;

    const result =
        await VoucherExecutionService.execute({

            company_code,

            tally_owner,

            sync_batch_id,

            rowsToSave: [voucher],

            allVoucherGuids:
                guid ? [guid] : [],

            voucherGuids:
                guid ? [guid] : [],

            ledgerRows: [],

            inventoryRows: [],

            stockVoucherRows: [],

            billAllocationRows: [],

            costCentreRows: []

        });

    return result;
}

/* 088026
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
*/

async function saveVoucherExecutionData({

    company_code,

    tally_owner,

    sync_batch_id,

    syncMode,

    executionMode = "NORMAL",

    orphanGuids = {},

    repairVoucherGuids = [],

    childRepairTables,

    rowsToSave,

    allVoucherRows,

    allVoucherGuids,

    voucherGuids,

    changedVoucherGuids,

    ledgerRows,

    inventoryRows,

    stockVoucherRows,

    billAllocationRows,

    costCentreRows,

    STOCK_DEBUG_FILE

}) {

    console.log(">>> saveVoucherExecutionData START");

    const result =
    await VoucherExecutionService.execute({

        company_code,

        tally_owner,

        sync_batch_id,

        syncMode,

        executionMode,

        orphanGuids,

        repairVoucherGuids,

        childRepairTables,

        rowsToSave,

        allVoucherRows,

        allVoucherGuids,

        voucherGuids,

        changedVoucherGuids,
        
        ledgerRows,

        inventoryRows,

        stockVoucherRows,

        billAllocationRows,

        costCentreRows

    });

return result;


}


/* 080826
async function deleteMissingVouchers({

    company_code,

    tally_owner,

    deletedVoucherGuids

}) {

    if (deletedVoucherGuids.length === 0) {

        return;

    }


    const now = new Date().toISOString();



    await supabase

        .from("tally_voucher_inventory")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", deletedVoucherGuids);




    await supabase

        .from("tally_voucher_ledgers")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", deletedVoucherGuids);




    await supabase

        .from("tally_stock_vouchers")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", deletedVoucherGuids);




    await supabase

        .from("tally_bill_allocations")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", deletedVoucherGuids);




    await supabase

        .from("tally_costcentre_allocations")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("voucher_guid", deletedVoucherGuids);




    await supabase

        .from("tally_vouchers")

        .update({

            is_deleted:true,

            updated_at:now

        })

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("guid", deletedVoucherGuids);


}
*/

async function loadExistingVoucherMap({
    company_code,
    tally_owner
}) {

    const voucherColumns =
        VOUCHER_COLUMNS[TABLES.VOUCHERS];

    const pageSize = 1000;
    let from = 0;
    const existingVouchers = [];

    while (true) {

        const {
            data,
            error
        } = await supabase
            .from("tally_vouchers")
            .select([
                voucherColumns.GUID,
                voucherColumns.ALTER_ID,
                voucherColumns.IS_DELETED
            ].join(", "))
            .eq("company_code", company_code)
            .eq("tally_owner", tally_owner)
            .range(from, from + pageSize - 1);

        if (error) {
            throw new Error(
                "Failed to load existing vouchers: " +
                error.message
            );
        }

        existingVouchers.push(...(data || []));

        if (!data || data.length < pageSize) {
            break;
        }

        from += pageSize;
    }

    return new Map(
        existingVouchers.map(v => [
            String(v[voucherColumns.GUID]).trim(),
            {
                alterid:
                    Number(v[voucherColumns.ALTER_ID]),

                is_deleted:
                    v[voucherColumns.IS_DELETED] === true
            }
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


/* 080826 
async function saveVoucherGuids({
    company_code,
    tally_owner,
    sync_batch_id
}) {

const batch =
    await BatchStatusManager.loadBatch({

        batch_id: sync_batch_id

    });

const error =

    batch ? null :

    new Error("Batch not found");

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

        //--------------------------------------------------
    // Reconcile Voucher GUIDs
    //--------------------------------------------------

    const reconciliationResult =
    await ReconciliationManager.reconcile({

        company_code,

        tally_owner,

        table: "tally_vouchers",

        guidField: "guid",

        sync_batch_id,

        module: "VOUCHER",

        entity_type: "VOUCHER"

    });


        fs.writeFileSync(
    "./logs/incoming-voucher-guids.json",
    JSON.stringify(
        {
            totalIncoming: reconciliationResult.summary.totalIncoming,
            missingInDB: reconciliationResult.summary.missingInDB,
            matched: reconciliationResult.summary.matched
        },
        null,
        2
    )
);



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

await BatchStatusManager.updateFields({

    batch_id: sync_batch_id,

    fields: {

        reconciliation_completed: true

    }

});

}
*/




const STOCK_DEBUG_FILE =
    "./logs/stock-movement-debug.jsonl";



console.log("===== SAVEVOUCHERS.JS LOADED =====");

/*
fs.appendFileSync(
    "./logs/test.log",
    "SAVEVOUCHERS.JS LOADED\n"
);
*/

async function saveVouchers({
    company_code,
    tally_owner,
    sync_batch_id,
    syncMode,
    country,
    vouchers = [],
    allVoucherGuids = [],

    // PHASE 2 CHILD RECONCILIATION
    executionMode = "NORMAL",

    // Child tables that actually need repair
    childRepairTables = [],
    orphanGuids = {},
    repairVoucherGuids = []
}) {

    console.log("=== SAVE VOUCHERS RECEIVED ===", {
    vouchersCount: Array.isArray(vouchers) ? vouchers.length : 0,
    allVoucherGuidsCount:
        Array.isArray(allVoucherGuids)
            ? allVoucherGuids.length
            : 0,

    targetGuidPresent:
        Array.isArray(allVoucherGuids) &&
        allVoucherGuids.some(
            x =>
                (typeof x === "string"
                    ? x
                    : x?.guid) ===
                "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b6a"
        )
});

/*
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


 29.07.26
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

const isFullSync = syncMode === "FULL";

const isChildReconciliation =
    executionMode === "CHILD_RECONCILIATION";

const isAlterRecovery =
    executionMode === "ALTER_RECOVERY";

const repairChildTables =
    new Set(childRepairTables || []);

const voucherRows = [];

const allVoucherRows = [];

const mismatchGuidSet =
    new Set(
        (repairVoucherGuids || [])
            .map(x =>
                typeof x === "string"
                    ? x.trim()
                    : x?.guid?.trim()
            )
            .filter(Boolean)
    );

let ledgerRows = [];

let inventoryRows = [];

let stockVoucherRows = [];

let billAllocationRows = [];

let costCentreRows = [];



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
const billAllocationVoucherGuids = new Set();

const costCentreAllocationVoucherGuids = new Set();

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

        allVoucherRows.push(
            VoucherRowBuilder.build({
                header,
                stockInCount: voucher.stockInCount,
                stockOutCount: voucher.stockOutCount,
                company_code,
                tally_owner,
                sync_batch_id,
                now
            })
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

    const existingVoucher =
    existingVoucherMap.get(header.guid.trim());

integrityResult = {
    action:
        existingVoucher === undefined
            ? VALIDATION_ACTION.INSERT
            : existingVoucher.is_deleted
            ? VALIDATION_ACTION.UPDATE
                : Number(header.alterid) >
                Number(existingVoucher.alterid)
                    ? VALIDATION_ACTION.UPDATE
                    : VALIDATION_ACTION.SKIP,

    reasons: []
};


/*
integrityResult =
    await VoucherValidationService
        .validateNewVoucher({

            company_code,

            tally_owner,

            parsedVoucher: voucher,

            runId,

            existingVoucherMap

        });

    fs.appendFileSync(
    "./logs/integrity-result.jsonl",
    JSON.stringify({
        guid: header.guid,
        action: integrityResult.action,
        reasons: integrityResult.reasons
    }) + "\n"
);
*/
console.log("AFTER VALIDATE", header.guid);

} catch (err) {

   

    throw err;
}

    
console.log(
    "Integrity Result :",
    header.voucherNumber,
    integrityResult.action
);

if (isFullSync) {

    fs.appendFileSync(
        `./logs/FULL-VOUCHER-VALIDATION-${sync_batch_id}.jsonl`,
        JSON.stringify({
            guid: header.guid,
            voucherNumber: header.voucherNumber,
            alterid: header.alterid,
            action: integrityResult.action
        }) + "\n"
    );

}
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

    if (isAlterRecovery) {

        changedVoucherRows.push(
            header.guid.trim()
        );

        break;
    }

    if (!isChildReconciliation) {

        unchangedVoucherGuids.push(
            header.guid.trim()
        );

        continue;
    }

    break;

    default:

        continue;

}


voucherRows.push(

    VoucherRowBuilder.build({

        header,

        stockInCount: voucher.stockInCount,
        stockOutCount: voucher.stockOutCount,

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

for (
    const row of billAllocationRows
) {
    if (
        row.voucher_guid ===
        header.guid.trim()
    ) {
        billAllocationVoucherGuids.add(
            row.voucher_guid
        );
    }
}

costCentreRows.push(
    ...buildCostCentreRows({
        voucher,
        company_code,
        tally_owner
    })
);

for (
    const row of costCentreRows
) {
    if (
        row.voucher_guid ===
        header.guid.trim()
    ) {
        costCentreAllocationVoucherGuids.add(
            row.voucher_guid
        );
    }
}

const currentVoucherRow =
    voucherRows[voucherRows.length - 1];

currentVoucherRow.has_bill_allocation =
    billAllocationVoucherGuids.has(
        header.guid.trim()
    );

currentVoucherRow.has_costcentre_allocation =
    costCentreAllocationVoucherGuids.has(
        header.guid.trim()
    );

console.log(
    "BILL ALLOCATION ROWS:",
    billAllocationRows.length
);

        }


        

        let success = 0;

        let rowsToSave = [];

let voucherGuids = [];

console.log("voucherRows:", voucherRows.length);
console.log("voucherGuids:", voucherGuids.length);
console.log("vouchers:", vouchers.length);


const incomingGuidSet =
    new Set(
        vouchers
            .map(v => v?.header?.guid?.trim())
            .filter(Boolean)
    );

// =====================================================
// FULL GUID DISCOVERY MUST NOT USE INCOMING VOUCHERS
// AS THE "MISSING" BASE.
//
// Tally full GUID scan = snapshot/discovery list.
// Genuine missing vouchers are determined by the
// ReconciliationManager against DB active GUIDs.
// =====================================================

const missingVoucherGuids = [];

        fs.writeFileSync(
    `./logs/FULL-VOUCHER-GUID-DEBUG-${sync_batch_id}.json`,
    JSON.stringify(
        {
            stage: "FULL_VOUCHER_GUID_VALIDATION",

            sync_batch_id,

            allVoucherGuids:
                allVoucherGuids.length,

            incomingVouchers:
                vouchers.length,

            incomingGuidSet:
                incomingGuidSet.size,

            missingVoucherGuids:
                missingVoucherGuids.length,

            firstMissing:
                missingVoucherGuids.slice(0, 20),

            lastMissing:
                missingVoucherGuids.slice(-20)

        },
        null,
        2
    )
);

console.log(
    "FULL GUID CHECK:",
    {
        allVoucherGuids: allVoucherGuids.length,
        incomingVouchers: vouchers.length,
        missingVoucherGuids: missingVoucherGuids.length
    }
);
// =====================================================
// DO NOT TRIGGER voucherByGuid FROM FULL GUID DISCOVERY.
// Genuine missing vouchers are returned later by the
// Sync Engine reconciliation result.
// =====================================================

if (missingVoucherGuids.length > 0) {

    return {
        status: "WAITING_FOR_MISSING_VOUCHERS",

        missingVoucherGuids

    };

}

if (voucherRows.length > 0 || allVoucherGuids.length > 0) {

      if (isChildReconciliation) {

    rowsToSave = voucherRows;

} else {

    rowsToSave = getRowsToSave({

        voucherRows,

        newVoucherRows,

        changedVoucherRows

    });


    fs.writeFileSync(
    `./logs/VOUCHER-SAVE-DEBUG-${sync_batch_id}.json`,
    JSON.stringify({
        stage: "ROWS_TO_SAVE",
        sync_batch_id,

        targetGuid:
            "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b73",

        targetVoucher:
            vouchers.find(v =>
                v?.header?.guid?.trim() ===
                "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b73"
            ) || null,

        targetVoucherRow:
            rowsToSave.find(row =>
                row?.guid?.trim() ===
                "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b73"
            ) || null,

        rowsToSaveCount:
            rowsToSave.length,

        changedVoucherRows:
            changedVoucherRows.length,

        newVoucherRows:
            newVoucherRows.length
    }, null, 2),
    "utf8"
);

}

voucherGuids =
    getVoucherGuids(rowsToSave);
        


ledgerRows =
    filterRowsByVoucherGuids({

        voucherGuids,

        rows: ledgerRows

    });


inventoryRows =
    filterRowsByVoucherGuids({

        voucherGuids,

        rows: inventoryRows

    });

stockVoucherRows =
    filterRowsByVoucherGuids({

        voucherGuids,

        rows: stockVoucherRows

    });

billAllocationRows =
    filterRowsByVoucherGuids({

        voucherGuids,

        rows: billAllocationRows

    });

costCentreRows =
    filterRowsByVoucherGuids({

        voucherGuids,

        rows: costCentreRows

    });

if (isChildReconciliation) {

    if (
        !repairChildTables.has(
            TABLES.VOUCHER_LEDGERS
        )
    ) {
        ledgerRows = [];
    }

    if (
        !repairChildTables.has(
            TABLES.VOUCHER_INVENTORY
        )
    ) {
        inventoryRows = [];
    }

    if (
        !repairChildTables.has(
            TABLES.STOCK_VOUCHERS
        )
    ) {
        stockVoucherRows = [];
    }

    if (
        !repairChildTables.has(
            TABLES.BILL_ALLOCATIONS
        )
    ) {
        billAllocationRows = [];
    }

    if (
        !repairChildTables.has(
            TABLES.COST_CENTRE_ALLOCATIONS
        )
    ) {
        costCentreRows = [];
    }

    console.log(
    "CHILD REPAIR FILTERED ROWS:",
    {
        ledger: ledgerRows?.length || 0,
        inventory: inventoryRows?.length || 0,
        stock: stockVoucherRows?.length || 0,
        bill: billAllocationRows?.length || 0,
        costCentre: costCentreRows?.length || 0
    }
);

}

console.log(">>> CALLING saveVoucherExecutionData");

fs.writeFileSync(
    `./logs/VOUCHER-00-before-execution-${sync_batch_id}.json`,
    JSON.stringify({
        stage: "SAVE_VOUCHERS_TO_EXECUTION",

        sync_batch_id,

        vouchersCount:
            vouchers.length,

        allVoucherRowsCount:
            allVoucherRows.length,

        allVoucherGuidsCount:
            allVoucherGuids.length,

        firstVoucherGuid:
            allVoucherRows[0]?.guid || null,

        lastVoucherGuid:
            allVoucherRows[allVoucherRows.length - 1]?.guid || null

    }, null, 2)
);

const executionResult =
    await saveVoucherExecutionData({

        company_code,

        tally_owner,

        sync_batch_id,

        syncMode,

        executionMode,

        orphanGuids:
            orphanGuids || {},

        repairVoucherGuids:
            repairVoucherGuids || [],

        rowsToSave,

        allVoucherRows,

        allVoucherGuids,

        changedVoucherGuids:
             changedVoucherRows,

        voucherGuids,

        childRepairTables,

        ledgerRows,

        inventoryRows,

        stockVoucherRows,

        billAllocationRows,

         costCentreRows,

        STOCK_DEBUG_FILE

    });

    fs.writeFileSync(
    "./logs/voucher-execution-result.json",
    JSON.stringify(
        {
            success:
                executionResult?.success,

            hasReconciliation:
                !!executionResult?.reconciliation,

            missingCount:
                executionResult?.reconciliation?.missingGuids?.length || 0,

            missingGuids:
                executionResult?.reconciliation?.missingGuids || []

        },
        null,
        2
    )
);

    success =
    executionResult.success;

  const missingVoucherGuids =
    (executionResult?.reconciliation?.missingGuids || [])
        .map(row => row.guid)
        .filter(Boolean);

const changedVoucherGuids =
    (executionResult?.reconciliation?.alterChanged || [])
        .map(row => row.guid)
        .filter(Boolean);

if (
    missingVoucherGuids.length > 0 ||
    changedVoucherGuids.length > 0
) {

    console.log(
        "VOUCHER RECOVERY REQUIRED:",
        {
            missing: missingVoucherGuids.length,
            changed: changedVoucherGuids.length
        }
    );

    return {
        status: "WAITING_FOR_MISSING_VOUCHERS",

        missingVoucherGuids,

        changedVoucherGuids,

        executionResult
    };
}

}

/*
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
*/

if (isChildReconciliation) {

    return {

        total: vouchers.length,

        execution_status: success,

        childReconciliation: true

    };

}

await BatchStatusManager.updateFields({

    batch_id: sync_batch_id,

    fields: {

        incremental_post_completed: true

    }

});

console.log(
    "INCREMENTAL POST COMPLETED UPDATED"
);
/*
    fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "SAVE_EXECUTION_COMPLETED",
        success,
        rowsToSave: rowsToSave.length
    }) + "\n"
);
*/

 /*   
allVoucherGuids = [];
*/
//temp code
/*
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
    */
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
await BatchStatusManager.updateFields({

    batch_id: sync_batch_id,

    fields: {

        guid_scan_completed: true,

        total_vouchers: vouchers.length,

        current_module: "GUID_SCAN"

    }

});

const data = null;

const error = null;

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
   
/*
    fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "GUID_SCAN_UPDATED"
    }) + "\n"
);


fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "ENGINE_RECONCILIATION_COMPLETED"
    }) + "\n"
);
*/
console.log(
    "Voucher reconciliation already completed by Sync Engine."
);

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

    /*
fs.appendFileSync(
    "./logs/api-flow.jsonl",
    JSON.stringify({
        stage: "BATCH_COMPLETED"
    }) + "\n"
);
*/

  return {

    total: vouchers.length,

    execution_status: success

};

}

/*080826
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

*/

module.exports = {

    saveVouchers,
    
    //saveVoucherGuids,

    saveVoucherExecutionData,

     saveVoucher,

 /*   deleteVoucherLedgers,

    deleteVoucherInventory,

    deleteStockVouchers,

    deleteBillAllocations,

    deleteCostCentres,

    saveCostCentres,

    saveVoucherHeaders,

    saveVoucherLedgers,

    saveVoucherInventory,

    saveStockVouchers
*/
};
