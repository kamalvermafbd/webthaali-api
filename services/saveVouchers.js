const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const VoucherIntegrityService = require("./VoucherIntegrityService");
const fs = require("fs");

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

VOUCHER_SAVED: "VOUCHER_SAVED"

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

    for (const row of rowsToSave) {

        await addSyncValidationLog({

            sync_batch_id,

            company_code,

            tally_owner,

            voucher_guid: row.guid,

            voucher_number: row.voucher_number,

            voucher_type: row.voucher_type,

            action: VALIDATION_ACTION.VOUCHER_SAVED,

            validator_name: "saveVouchers",

            reason: "Voucher Header Saved"

        });

    }

    return rowsToSave.length;

}

async function saveVoucherLedgers({

    ledgerRows

}) {

    if (ledgerRows.length === 0) {

        return;

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

async function saveVoucherInventory({

    inventoryRows

}) {

    if (inventoryRows.length === 0) {

        return;

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

    STOCK_DEBUG_FILE

}) {

    if (stockVoucherRows.length === 0) {

        return;

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

    await saveVoucherLedgers({

        ledgerRows

    });

    await deleteVoucherInventory({

        company_code,

        tally_owner,

        voucherGuids

    });

    await saveVoucherInventory({

        inventoryRows

    });

    await saveStockVouchers({

        stockVoucherRows,

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

async function addSyncValidationLog({

    sync_batch_id,

    company_code,

    tally_owner,

    voucher_guid,

    voucher_number = null,

    voucher_type = null,

    action,

    validator_name,

    reason,

    remarks = null

}) {

    const { data: existing } = await supabase

    .from("sync_exe_queue")

    .select("id, reason")

    .eq("sync_id", sync_batch_id)

    .eq("voucher_guid", voucher_guid)

    .maybeSingle();

if (!existing) {
fs.appendFileSync(
    "./logs/queue-debug.jsonl",
    JSON.stringify({
        stage: "QUEUE_INSERT",
        sync_batch_id,
        voucher_guid,
        action
    }) + "\n"
);
    const { error } = await supabase

        .from("sync_exe_queue")

        .insert({

            sync_id: sync_batch_id,

            company_code,

            tally_owner,

            voucher_guid,

            voucher_number,

            voucher_type,

            action,

            validator_name,

            reason,

            remarks

        });

    if (error) {

        console.error(
            "Failed to write queue:",
            error.message
        );

    }

}
else {

    const newReason =
        existing.reason
            ? existing.reason + "\n" + reason
            : reason;

    const { error } = await supabase

        .from("sync_exe_queue")

        .update({

            reason: newReason

        })

        .eq("id", existing.id);

    if (error) {

        console.error(
            "Failed to update queue:",
            error.message
        );

    }

}

}

const STOCK_DEBUG_FILE =
    "./logs/stock-movement-debug.jsonl";

const ENABLE_QUEUE_EXECUTION = false;

console.log("===== SAVEVOUCHERS.JS LOADED =====");

fs.appendFileSync(
    "./logs/test.log",
    "SAVEVOUCHERS.JS LOADED\n"
);

async function saveVouchers({
    company_code,
    tally_owner,
    sync_batch_id,
    vouchers = []
}) {

    // =========================
// CLEAR PREVIOUS QUEUE
// =========================

await supabase
    .from("sync_exe_queue")
    .delete()
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);
    
    if (!Array.isArray(vouchers) || vouchers.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

   const now = new Date().toISOString();

const runId =
    `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const VALIDATION_STAGE = {

    NEW_VOUCHER: "NEW_VOUCHER",

    ALTER_ID: "ALTER_ID",

    INTEGRITY: "INTEGRITY"

};

const voucherRows = [];

const ledgerRows = [];

const inventoryRows = [];

const stockVoucherRows = [];


const existingVoucherMap =
    await loadExistingVoucherMap({

        company_code,

        tally_owner

    });




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


    for (const voucher of vouchers) {

        const header = voucher.header || {};

       if (!header.guid?.trim()) {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Voucher "${header.voucherNumber}" because GUID is missing.`
            );

            continue;

        }



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
const shouldSkip =
    await processValidationResult({

        integrityResult,

        header,

        sync_batch_id,

        company_code,

        tally_owner,

        newVoucherRows,

        changedVoucherRows,

        unchangedVoucherGuids

    });

if (shouldSkip) {

    continue;

}

/// new code ends//

/* remove today 23.07.26

const existingAlterId =
    existingVoucherMap.get(header.guid.trim());

if (existingAlterId === undefined) {

    newVoucherRows.push(header.guid.trim());

} else if (
    existingAlterId === Number(header.alterid)
) {

    unchangedVoucherGuids.push(header.guid.trim());

} else {

    changedVoucherRows.push(header.guid.trim());

}

if (
    existingAlterId !== undefined &&
    existingAlterId === Number(header.alterid)
) {

    continue;

}
*/

        voucherRows.push({

            company_code,
            tally_owner,

            guid: header.guid.trim(),

           masterid: header.masterid ?? null,

            alterid: header.alterid ?? null,

            voucher_type: header.voucherType ?? null,

            voucher_number: header.voucherNumber ?? null,

            voucher_date: header.voucherDate || null,

            effective_date: header.effectiveDate || null,

            reference: header.reference || null,

            reference_date: header.referenceDate || null,

            party_ledger: header.partyLedger || null,

            narration: header.narration || null,

            gstin: header.gstin || null,

            place_of_supply: header.placeOfSupply || null,

            buyer_name: header.buyerName || null,

            buyer_address: header.buyerAddress || null,

            gst_registration_type:
                header.gstRegistrationType || null,

            persisted_view:
                header.persistedView || null,

            is_invoice:
                header.isInvoice === "Yes",

            is_optional:
                header.isOptional === "Yes",

            is_cancelled:
                header.isCancelled === "Yes",

            is_deleted: false,

            last_synced_at: now,

            sync_batch_id,

            updated_at: now

        });




    for (const ledger of (voucher.ledgers || [])) {

            ledgerRows.push({

                voucher_guid: header.guid.trim(),

                company_code,

                tally_owner,

                ledger_name:
                    ledger.ledgerName?.trim() || null,

                ledger_masterid:
                    ledger.ledgerMasterId ?? null,

                    ledger_guid:
             ledger.ledgerGuid ?? null,

            ledger_alterid:
                ledger.ledgerAlterId ?? null,
/*
            ledger_parent_name:
                ledger.ledgerParentName ?? null,

            ledger_parent_guid:
                ledger.ledgerParentGuid ?? null,

            ledger_parent_masterid:
                ledger.ledgerParentMasterId ?? null,

            ledger_parent_alterid:
                ledger.ledgerParentAlterId ?? null,
*/
                amount:
                    ledger.amount ?? null,

                debit:
                    ledger.debit ?? null,

                credit:
                    ledger.credit ?? null,
/*
               is_deemed_positive:
                    ledger.isDeemedPositive === "Yes",

                is_party_ledger:
                    ledger.isPartyLedger === "Yes",

                is_last_deemed_positive:
                    ledger.isLastDeemedPositive === "Yes",

                remove_zero_entries:
                    ledger.removeZeroEntries === "Yes",
*/
                bill_allocations:
                    ledger.billAllocations ?? [],

                costcentre_allocations:
                    ledger.costCentreAllocations ?? []

            });

        }



    for (const item of (voucher.inventory || [])) {

     const gstRates = item.gstRates ?? [];

    const cgstRate =
        gstRates.find(r => r.dutyHead === "CGST")?.rate ?? null;

    const sgstRate =
        gstRates.find(r => r.dutyHead === "SGST/UTGST")?.rate ?? null;

    const igstRate =
        gstRates.find(r => r.dutyHead === "IGST")?.rate ?? null;

        const hsnCode =
            item.hsnCode || null;

          console.log({
    stock: item.stockItem,
    taxable: item.taxableAmount,
    cgst: item.cgstAmount,
    sgst: item.sgstAmount,
    igst: item.igstAmount
});

if (
    item.inventoryNode ===
    "ALLINVENTORYENTRIES.LIST"
) {

    inventoryRows.push({

                voucher_guid: header.guid.trim(),

                company_code,

                tally_owner,

                stock_item:
                    item.stockItem?.trim() || null,
          

                actual_qty:
                    item.actualQty || null,

                actual_qty_value:
                    item.actualQtyValue || null,

                billed_qty:
                    item.billedQty || null,

                billed_qty_value:
                    item.billedQtyValue || null,

                unit:
                    item.unit?.trim() || null,

                rate:
                    item.rate || null,

                rate_value:
                    item.rateValue || null,

                amount:
                    item.amount ?? null,

             hsn_code: hsnCode,

                discount:
                    item.discount ?? null,

                godown:
                    item.godown?.trim() || null,

                batch_id:
                    item.batchId ?? null,

                batches:
                    item.batches ?? [],

                accounting:
                    item.accounting ?? [],

                stock_guid:
                    item.stockGuid ?? null,

                stock_masterid:
                    item.stockMasterIdResolved ?? null,

                stock_alterid:
                    item.stockAlterId ?? null,
                /*
                voucher_master_id:
                    item.voucherMasterId ?? null,

                voucher_alter_id:
                    item.voucherAlterId ?? null,

                voucher_date:
                    item.voucherDate ?? null,

                voucher_type:
                    item.voucherType ?? null,

                    */

                transaction_type:
                    item.transactionType ?? null,

                ledger_name:
                    item.ledgerName ?? null,

                ledger_guid:
                    item.ledgerGuid ?? null,

                ledger_master_id:
                    item.ledgerMasterId ?? null,

                ledger_alter_id:
                    item.ledgerAlterId ?? null,
                /*
                ledger_parent_name:
                    item.ledgerParentName ?? null,

                ledger_parent_guid:
                    item.ledgerParentGuid ?? null,

                ledger_parent_master_id:
                    item.ledgerParentMasterId ?? null,

                ledger_parent_alter_id:
                    item.ledgerParentAlterId ?? null,  
                    */

                party_name:
                    item.partyName ?? null,

                party_guid:
                    item.partyGuid ?? null,

                party_master_id:
                    item.partyMasterId ?? null,

                party_alter_id:
                    item.partyAlterId ?? null,
                /*
                party_parent_name:
                    item.partyParentName ?? null,

                party_parent_guid:
                    item.partyParentGuid ?? null,

                party_parent_master_id:
                    item.partyParentMasterId ?? null,

                party_parent_alter_id:
                    item.partyParentAlterId ?? null,
                    */

                cgst_rate: cgstRate,

                sgst_rate: sgstRate,

                igst_rate: igstRate,


                gst_rate:
                    igstRate ??
                    ((cgstRate || 0) + (sgstRate || 0)),


               cgst_amount:
                    item.cgstAmount ?? 0,

                sgst_amount:
                    item.sgstAmount ?? 0,

                igst_amount:
                    item.igstAmount ?? 0,

                taxable_amount:
                    item.taxableAmount ?? 0,

              gst_rates: item.gstRates ?? [],

                costcentre_allocations:
                    item.costCentreAllocations ?? []

            });
        }

 else {

fs.appendFileSync(
    "./logs/test.log",
    JSON.stringify({
        stage: "ELSE_BLOCK",
        inventoryNode: item.inventoryNode,
        movementType: item.movementType,
        stock: item.stockItem
    }) + "\n"
);
fs.appendFileSync(
        STOCK_DEBUG_FILE,
        JSON.stringify({
            voucher: header.voucherNumber,
            guid: header.guid,
            inventoryNode: item.inventoryNode,
            movementType: item.movementType,
            stockItem: item.stockItem,
            godown: item.godown,
            batchName: item.batchName,
            batchId: item.batchId,
            raw: item.raw
        }) + "\n"
    );

    fs.appendFileSync(
    STOCK_DEBUG_FILE,
    JSON.stringify({
        stage: "FINAL_ITEM",
        inventoryNode: item.inventoryNode,
        movementType: item.movementType,
        stockItem: item.stockItem,
        itemGodown: item.godown,
        itemBatchName: item.batchName,
        itemBatchId: item.batchId,
        batches: item.batches
    }) + "\n"
);

    stockVoucherRows.push({

        voucher_guid: header.guid.trim(),

        company_code,

        tally_owner,

        stock_guid:
            item.stockGuid ?? null,

        stock_masterid:
            item.stockMasterIdResolved ?? null,

        stock_alterid:
            item.stockAlterId ?? null,

        stock_item:
            item.stockItem?.trim() || null,

        movement_type:
            item.movementType ?? null,

        actual_qty:
            item.actualQty ?? null,

        actual_qty_value:
            item.actualQtyValue ?? null,

        billed_qty:
            item.billedQty ?? null,

        billed_qty_value:
            item.billedQtyValue ?? null,

        unit:
            item.unit?.trim() || null,

        rate:
            item.rate ?? null,

        rate_value:
            item.rateValue ?? null,

        amount:
            item.amount ?? null,

        godown:
            item.godown?.trim() || null,

        batch_name:
            item.batchName ?? null,

        batch_id:
            item.batchId ?? null,

        inventory_node:
            item.inventoryNode ?? null,

        xml_payload:
            item.raw ?? null,

            voucher_type_name:
    header.voucherTypeName ?? null,

voucher_type:
    header.voucherType ?? null,

voucher_number:
    header.voucherNumber ?? null,

voucher_date:
    header.voucherDate || null,

effective_date:
    header.effectiveDate || null,

reference:
    header.reference || null,

narration:
    header.narration || null,

party_ledger_name:
    header.partyLedger || null,

party_gstin:
    header.gstin || null,

place_of_supply:
    header.placeOfSupply || null,

gst_registration_type:
    header.gstRegistrationType || null,

persisted_view:
    header.persistedView || null,

is_invoice:
    header.isInvoice === "Yes",

is_cancelled:
    header.isCancelled === "Yes",

is_optional:
    header.isOptional === "Yes",

is_deleted:
    header.isDeleted === "Yes",

ledger_name:
    item.ledgerName ?? null,

discount:
    item.discount ?? null,

additional_amount:
    item.additionalAmount ?? null,

batch_rate:
    item.batchRate ?? null,

batch_rate_value:
    item.batchRateValue ?? null,

batch_amount:
    item.batchAmount ?? null,

    });

}

        }

    }


        let success = 0;

        let rowsToSave = [];

let voucherGuids = [];

let ledgerVoucherGuids = [];

let inventoryVoucherGuids = [];

const debugData = {};



    if (
        ENABLE_QUEUE_EXECUTION &&
        voucherRows.length > 0
    ) {

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
    await saveVoucherHeaders({

        rowsToSave,

        sync_batch_id,

        company_code,

        tally_owner

    });

        const { count: ledgerBefore } = await supabase
    .from("tally_voucher_ledgers")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_LEDGER_DELETE",
        totalRows: ledgerBefore
    }) + "\n"
);

await deleteVoucherLedgers({

    company_code,

    tally_owner,

    voucherGuids

});


const { count: ledgerAfter } = await supabase
    .from("tally_voucher_ledgers")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "AFTER_LEDGER_DELETE",
        remainingRows: ledgerAfter
    }) + "\n"
);



    fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "DELETE_STOCK_INPUT",
        voucherGuidsLength: voucherGuids.length,
        voucherGuids: voucherGuids
    }) + "\n"
);    



await deleteStockVouchers({

    company_code,

    tally_owner,

    voucherGuids

});



const { count: stockCount } = await supabase
    .from("tally_stock_vouchers")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "AFTER_STOCK_DELETE",
        remainingRows: stockCount
    }) + "\n"
);


await saveVoucherLedgers({

    ledgerRows

});


const { count: inventoryBefore } = await supabase
    .from("tally_voucher_inventory")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_INVENTORY_DELETE",
        totalRows: inventoryBefore
    }) + "\n"
);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "DELETE_INPUT",
        voucherGuidsLength: voucherGuids.length,
        voucherGuids: voucherGuids
    }) + "\n"
);



await deleteVoucherInventory({

    company_code,

    tally_owner,

    voucherGuids

});


const { count: inventoryAfter } = await supabase
    .from("tally_voucher_inventory")
    .select("*", { count: "exact", head: true })
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "AFTER_INVENTORY_DELETE",
        remainingRows: inventoryAfter
    }) + "\n"
);


await saveVoucherInventory({

    inventoryRows

});
fs.appendFileSync(
    "./logs/inventory-save-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_SAVE_INVENTORY",
        totalRows: inventoryRows.length,
        firstRow: inventoryRows[0]
    }) + "\n"
);          

fs.appendFileSync(
    STOCK_DEBUG_FILE,
    JSON.stringify({
        stage: "before_insert",
        totalRows: stockVoucherRows.length,
        rows: stockVoucherRows
    }) + "\n"
);

fs.appendFileSync(
    "./logs/delete-debug.jsonl",
    JSON.stringify({
        stage: "BEFORE_STOCK_INSERT",
        stockVoucherRows: stockVoucherRows.length
    }) + "\n"
);

        

await saveStockVouchers({

    stockVoucherRows,

    STOCK_DEBUG_FILE

});



}

   

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

    deleteVoucherLedgers,

    deleteVoucherInventory,

    deleteStockVouchers,

    saveVoucher,

    saveVoucherHeaders,

    saveVoucherLedgers,

    saveVoucherInventory,

    saveStockVouchers

};