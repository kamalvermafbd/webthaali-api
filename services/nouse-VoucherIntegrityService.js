/**
 * ============================================================
 * VoucherIntegrityService
 * ------------------------------------------------------------
 * PURPOSE
 * ------------------------------------------------------------
 * Central validation engine for all voucher related operations.
 *
 * This service MUST be reused everywhere:
 *
 * 1. Sync Engine
 * 2. Trial Balance
 * 3. Balance Sheet
 * 4. Profit & Loss
 * 5. Ledger Report
 * 6. Stock Summary
 * 7. Dashboard
 * 8. GST Reports
 * 9. Any future accounting report
 *
 * NOTE
 * ------------------------------------------------------------
 * Never duplicate validation logic anywhere else.
 * All validation rules should be added only in this file.
 * ============================================================
 */

const { createClient } = require("@supabase/supabase-js");
const fs = require("fs");
const investigator = require("../logs/voucherIntegrityInvestigator");

console.log("INTEGRITY_DEBUG =", process.env.INTEGRITY_DEBUG);

const debugFile =
    "logs/inventory-row-match.jsonl";
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

class VoucherIntegrityService {

    constructor(){

    this.ledgerMasterCache = new Map();
    this.stockMasterCache = new Map();

}

    /**
     * ========================================================
     * Main Entry Point
     * ========================================================
     *
     * This method executes all validations.
     *
     * Return Example:
     *
     * {
     *    valid: true,
     *    requiresRepair: false,
     *    reasons: []
     * }
     *
     * {
     *    valid: false,
     *    requiresRepair: true,
     *    reasons: [
     *       "Ledger Count Mismatch",
     *       "Stock GUID Missing"
     *    ]
     * }
     */
    async validateVoucher({

    company_code,

    tally_owner,

    parsedVoucher,
    runId

}) {

    if(process.env.INTEGRITY_DEBUG === "true"){
    fs.appendFileSync(
        "logs/processed-vouchers.jsonl",
        JSON.stringify({
            guid: parsedVoucher.header.guid,
            voucherNumber: parsedVoucher.header.voucherNumber,
            voucherType: parsedVoucher.header.voucherType
        }) + "\n"
    );
}
    //--------------------------------------------------
    // Load Existing Voucher
    //--------------------------------------------------

    const dbData =
    await this.loadDbData({

        company_code,

        tally_owner,

        guid:
            parsedVoucher?.header?.guid

    });
    
    

//--------------------------------------------------
// Voucher Not Found
//--------------------------------------------------

if (!dbData) {

    return this.buildResponse({

        action: "INSERT",

        validation: this.buildValidValidation(),

        dbData: null

    });

}

//--------------------------------------------------
// AlterId validation has been moved to saveVouchers.
// VoucherIntegrityService now performs only
// integrity validation.
    //--------------------------------------------------
    // Run Validators
    //--------------------------------------------------

    const validation =
        await this.runValidators({

            parsedVoucher,

            dbData

        });

    if (process.env.INTEGRITY_DEBUG === "true") {

    investigator.investigate({

        parsedVoucher,

        dbData

    });

}

    //--------------------------------------------------
    // Decide Action
    //--------------------------------------------------

    const action =
        this.decideAction({

           // parsedVoucher,

         //   dbData,

            validation

        });




    //--------------------------------------------------
    // Standard Response
    //--------------------------------------------------

    return this.buildResponse({

        action,

        validation,

        dbData

    });

}

/**
 * ============================================
 * Load Existing Voucher
 * ============================================
 */
async loadDbData({

    company_code,

    tally_owner,

    guid

}) {

    //--------------------------------------------------
    // Validate Input
    //--------------------------------------------------

    if (!guid) {
        return null;
    }

    //--------------------------------------------------
    // Load Voucher Header
    //--------------------------------------------------

    const { data: header, error: headerError } =
        await supabase

            .from("tally_vouchers")

            .select("*")

            .eq("company_code", company_code)

            .eq("tally_owner", tally_owner)

            .eq("guid", guid)

            .maybeSingle();

    if (headerError) {

        throw new Error(
            "Failed to load voucher header : " +
            headerError.message
        );

    }

    

    //--------------------------------------------------
    // Voucher Not Found
    //--------------------------------------------------

    if (!header) {
        return null;
    }

    //--------------------------------------------------
    // Load Voucher Ledgers
    //--------------------------------------------------

    const {

        data: ledgers,

        error: ledgerError

    } = await supabase

        .from("tally_voucher_ledgers")

        .select("*")

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .eq("voucher_guid", guid);

    if (ledgerError) {

        throw new Error(
            "Failed to load voucher ledgers : " +
            ledgerError.message
        );

    }

    //--------------------------------------------------
    // Load Voucher Inventory
    //--------------------------------------------------

    const {

        data: inventory,

        error: inventoryError

    } = await supabase

        .from("tally_voucher_inventory")

        .select("*")

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .eq("voucher_guid", guid);

    if (inventoryError) {

        throw new Error(
            "Failed to load voucher inventory : " +
            inventoryError.message
        );

    }

    const {

    data: stockVouchers,

    error: stockVoucherError

} = await supabase

    .from("tally_stock_vouchers")

    .select("*")

    .eq("company_code", company_code)

    .eq("tally_owner", tally_owner)

    .eq("voucher_guid", guid);

if (stockVoucherError) {

    throw new Error(
        "Failed to load stock vouchers : " +
        stockVoucherError.message
    );

}

//--------------------------------------------------
// Load Ledger Masters
//--------------------------------------------------
let ledgerMasters =
    this.ledgerMasterCache.get(
        `${company_code}_${tally_owner}`
    );

if (!ledgerMasters) {

    const { data, error } = await supabase
        .from("tally_sync_ledgers")
        .select("*")
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .eq("is_deleted", false);

    if(error) throw error;

    ledgerMasters = data || [];

    this.ledgerMasterCache.set(
        `${company_code}_${tally_owner}`,
        ledgerMasters
    );
}

//--------------------------------------------------
// Load Stock Masters
//--------------------------------------------------
let stockMasters =
    this.stockMasterCache.get(
        `${company_code}_${tally_owner}`
    );

if (!stockMasters) {

    const { data, error } = await supabase
        .from("tally_sync_stocks")
        .select("*")
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .eq("is_deleted", false);

    if(error) throw error;

    stockMasters = data || [];

    this.stockMasterCache.set(
        `${company_code}_${tally_owner}`,
        stockMasters
    );
}

    //--------------------------------------------------
    // Return Complete DB Snapshot
    //--------------------------------------------------

const inventoryRows =
    inventory || [];

const stockVoucherRows =
    stockVouchers || [];


const ledgerMap = new Map(

    (ledgers || []).map(item => [

        item.ledger_guid,

        item

    ])

);

const inventoryMap = inventoryRows;
/*
const stockVoucherMap = new Map(
    stockVoucherRows.map(item => [
        [
            item.stock_guid,
            item.inventory_node,
            item.batch_id || ""
        ].join("|"),
        item
    ])
);
*/
const ledgerMasterMap = new Map(

    (ledgerMasters || []).map(item => [

        item.guid,

        item

    ])

);

const stockMasterMap = new Map(

    (stockMasters || []).map(item => [

        item.guid,

        item

    ])

);
return {

    header,

    ledgers: ledgers || [],

    ledgerMap,

    ledgerMasterMap,

    stockMasterMap,

    inventory: inventoryRows,

    stockVouchers: stockVoucherRows,

    inventoryMap,

   // stockVoucherMap

};


}


/**
 * ============================================
 * Run All Validators
 * ============================================
 */
async runValidators({

    parsedVoucher,

    dbData

}) {

    const reasons = [];

   const validators = [

   // --------------------------------------------------
// Stage 1 : Reverse Validation (DB → XML Cleanup)
// --------------------------------------------------
this.validateReverseData,

// --------------------------------------------------
// Stage 2 : Voucher Integrity Validation
// --------------------------------------------------
    this.validateHeader,

    this.validateMasters,

    this.validateLedgers,

    this.validateInventory,

    this.validateStockVouchers,

    this.validateParty,

    this.validateTotals,

    this.validateCounts,

    this.validateNullFields,

    this.validateDuplicates,

    this.validateFuture

];

    for (const validator of validators) {

    const before = reasons.length;

    await validator.call(
        this,
        parsedVoucher,
        dbData,
        reasons
    );

    if (reasons.length > before) {

        console.log(
            validator.name,
            reasons.slice(before)
        );

    }

}

    return {

        valid:
            reasons.length === 0,

        requiresRepair:
            reasons.length > 0,

        reasons

    };

}


/**
 * ============================================
 * Decide Action
 * ============================================
 */
decideAction({

  //  parsedVoucher,

  //  dbData,

    validation

}) {


    //--------------------------------------------------
    // Voucher Invalid
    //--------------------------------------------------

    if (validation.requiresRepair) {

        return "FORCE_UPDATE";

    }

   
    //--------------------------------------------------
    // Nothing Changed
    //--------------------------------------------------

    return "SKIP";

}

buildValidValidation() {

    return {

        valid: true,
        requiresRepair: false,
        reasons: []

    };

}

/**
 * ============================================
 * Standard Response
 * ============================================
 */
buildResponse({

    action,

    validation,

    dbData

}) {

    return {

    action,

    validation,

    valid:
        validation.valid,

    requiresRepair:
        validation.requiresRepair,

    reasons:
        validation.reasons,

    dbData

};

}

normalizeValue(value, field) {

    if (value === null || value === undefined) {
        return "";
    }

    if (
        field === "voucherDate" ||
        field === "effectiveDate"
    ) {

        const str = String(value).trim();

        if (/^\d{8}$/.test(str)) {

            return (
                str.substring(0, 4) + "-" +
                str.substring(4, 6) + "-" +
                str.substring(6, 8)
            );

        }

        return str;

    }

    return String(value).trim();

}


inventoryFields = [

    ["stockItem", "stock_item", "Stock Name"],
    ["stockMasterIdResolved", "stock_masterid", "Stock MasterId"],
    ["stockAlterId", "stock_alterid", "Stock AlterId"],

    ["actualQty", "actual_qty", "Actual Qty"],
    ["actualQtyValue", "actual_qty_value", "Actual Qty Value"],

    ["billedQty", "billed_qty", "Billed Qty"],
    ["billedQtyValue", "billed_qty_value", "Billed Qty Value"],

    ["unit", "unit", "Unit"],

    ["rate", "rate", "Rate"],
    ["rateValue", "rate_value", "Rate Value"],

    ["amount", "amount", "Amount"],

    ["hsnCode", "hsn_code", "HSN Code"],

    ["discount", "discount", "Discount"],

    ["godown", "godown", "Godown"],
    ["partyName", "party_name", "Party Name"],

["ledgerName", "ledger_name", "Ledger Name"],

["transactionType", "transaction_type", "Transaction Type"],

    ["partyGuid", "party_guid", "Party GUID"],
    ["partyMasterId", "party_master_id", "Party MasterId"],
    ["partyAlterId", "party_alter_id", "Party AlterId"],

    ["ledgerGuid", "ledger_guid", "Ledger GUID"],
    ["ledgerMasterId", "ledger_master_id", "Ledger MasterId"],
    ["ledgerAlterId", "ledger_alter_id", "Ledger AlterId"],

    ["cgstRate", "cgst_rate", "CGST Rate"],

["sgstRate", "sgst_rate", "SGST Rate"],

["igstRate", "igst_rate", "IGST Rate"],

["cgstAmount", "cgst_amount", "CGST Amount"],

["sgstAmount", "sgst_amount", "SGST Amount"],

["igstAmount", "igst_amount", "IGST Amount"],

["taxableAmount", "taxable_amount", "Taxable Amount"],

];

stockVoucherFields = [

    ["stockItem", "stock_item", "Stock Name"],

    ["stockMasterIdResolved", "stock_masterid", "Stock MasterId"],

    ["stockAlterId", "stock_alterid", "Stock AlterId"],

    ["movementType", "movement_type", "Movement Type"],

    ["actualQty", "actual_qty", "Actual Qty"],

    ["actualQtyValue", "actual_qty_value", "Actual Qty Value"],

    ["billedQty", "billed_qty", "Billed Qty"],

    ["billedQtyValue", "billed_qty_value", "Billed Qty Value"],

    ["unit", "unit", "Unit"],

    ["rate", "rate", "Rate"],

    ["rateValue", "rate_value", "Rate Value"],

    ["amount", "amount", "Amount"],

    ["godown", "godown", "Godown"],

    ["batchName", "batch_name", "Batch Name"],

    ["inventoryNode", "inventory_node", "Inventory Node"],
    ["ledgerName", "ledger_name", "Ledger Name"],

["discount", "discount", "Discount"],

["additionalAmount", "additional_amount", "Additional Amount"],

["batchRate", "batch_rate", "Batch Rate"],

["batchRateValue", "batch_rate_value", "Batch Rate Value"],

["batchAmount", "batch_amount", "Batch Amount"],

];

compareField(dbValue, parsedValue) {

    if (dbValue === null || dbValue === undefined) {
        dbValue = "";
    }

    if (parsedValue === null || parsedValue === undefined) {
        parsedValue = "";
    }

    // Number compare
    if (
        !isNaN(dbValue) &&
        !isNaN(parsedValue)
    ) {

        return Number(dbValue) === Number(parsedValue);

    }

    return (
        String(dbValue).trim() ===
        String(parsedValue).trim()
    );

}
ledgerFields = [

    ["ledgerName", "ledger_name", "Ledger Name"],

    ["ledgerMasterId", "ledger_masterid", "Ledger MasterId"],
    ["ledgerAlterId", "ledger_alterid", "Ledger AlterId"],

    ["amount", "amount", "Amount"],
    ["debit", "debit", "Debit"],
    ["credit", "credit", "Credit"]

];

    /**
     * ========================================================
     * Header Validation
  

     */
   async validateHeader(
    parsedVoucher,
    dbData,
    reasons
) {

    if (!dbData) {
    return;
}

const parsedHeader = parsedVoucher.header || {};
const dbHeader = dbData.header || {};
const fields = [

    ["voucherType", "voucher_type"],
    ["voucherNumber", "voucher_number"],
    ["voucherDate", "voucher_date"],
    ["effectiveDate", "effective_date"],
    ["partyLedger", "party_ledger"],
    ["reference", "reference"]

];


for (const [parsedField, dbField] of fields) {

    console.log({
    parsedField,
    parsedValue: parsedHeader[parsedField],
    dbField,
    dbValue: dbHeader[dbField]
});


 const parsedValue =
    this.normalizeValue(
        parsedHeader[parsedField],
        parsedField
    );

const dbValue =
    this.normalizeValue(
        dbHeader[dbField],
        parsedField
    );  

    if (parsedValue !== dbValue) {

        reasons.push(
            `Header mismatch : ${parsedField}`
        );

    }

}

}

/**
 * ========================================================
 * Master Validation
 * ========================================================
 *
 * Validates voucher references against master tables.
 */
async validateMasters(
    parsedVoucher,
    dbData,
    reasons
) {

    if (!dbData) {
        return;
    }

 //--------------------------------------------------
// Voucher Ledger Validation
//--------------------------------------------------

for (const ledger of (parsedVoucher.ledgers || [])) {

    const master =
        dbData.ledgerMasterMap.get(
            ledger.ledgerGuid
        );

    if (!master) {

        reasons.push(
            `Ledger Master Missing : ${ledger.ledgerName}`
        );

        continue;

    }

    if (
        Number(master.master_id) !==
        Number(ledger.ledgerMasterId)
    ) {

        reasons.push(
            `Ledger MasterId Mismatch : ${ledger.ledgerName}`
        );

    }

    if (
        Number(master.alter_id) !==
        Number(ledger.ledgerAlterId)
    ) {

        reasons.push(
            `Ledger AlterId Mismatch : ${ledger.ledgerName}`
        );

    }

}



 //--------------------------------------------------
// Inventory Stock Validation
//--------------------------------------------------

for (const item of (parsedVoucher.inventory || [])) {

    const master =
        dbData.stockMasterMap.get(
            item.stockGuid
        );

    if (!master) {

        reasons.push(
            `Stock Master Missing : ${item.stockItem}`
        );

        continue;

    }

    if (
        Number(master.masterid) !==
        Number(item.stockMasterIdResolved)
    ) {

        reasons.push(
            `Stock MasterId Mismatch : ${item.stockItem}`
        );

    }

    if (
        Number(master.alterid) !==
        Number(item.stockAlterId)
    ) {

        reasons.push(
            `Stock AlterId Mismatch : ${item.stockItem}`
        );

    }

}

//--------------------------------------------------
// Party Ledger Validation
//--------------------------------------------------

for (const item of (parsedVoucher.inventory || [])) {

    if (!item.partyGuid) {
        continue;
    }

    const master =
        dbData.ledgerMasterMap.get(
            item.partyGuid
        );

    if (!master) {

        reasons.push(
            `Party Master Missing : ${item.partyName}`
        );

        continue;

    }

    if (
        Number(master.master_id) !==
        Number(item.partyMasterId)
    ) {

        reasons.push(
            `Party MasterId Mismatch : ${item.partyName}`
        );

    }

    if (
        Number(master.alter_id) !==
        Number(item.partyAlterId)
    ) {

        reasons.push(
            `Party AlterId Mismatch : ${item.partyName}`
        );

    }

}

 //--------------------------------------------------
// Accounting Ledger Validation
//--------------------------------------------------

for (const item of (parsedVoucher.inventory || [])) {

    if (!item.ledgerGuid) {
        continue;
    }

    const master =
        dbData.ledgerMasterMap.get(
            item.ledgerGuid
        );

    if (!master) {

        reasons.push(
            `Accounting Ledger Missing : ${item.ledgerName}`
        );

        continue;

    }

    if (
        Number(master.master_id) !==
        Number(item.ledgerMasterId)
    ) {

        reasons.push(
            `Accounting Ledger MasterId Mismatch : ${item.ledgerName}`
        );

    }

    if (
        Number(master.alter_id) !==
        Number(item.ledgerAlterId)
    ) {

        reasons.push(
            `Accounting Ledger AlterId Mismatch : ${item.ledgerName}`
        );

    }

}

}

/**
 * ========================================================
 * Ledger Validation
 * ========================================================
 */
async validateLedgers(
    parsedVoucher,
    dbData,
    reasons
) {

     if (!dbData) {
        return;
    }

    for (const parsedLedger of (parsedVoucher.ledgers || [])) {

    const dbLedger =
    dbData.ledgerMap.get(
        parsedLedger.ledgerGuid
    );

    if (!dbLedger) {
        reasons.push(
            `Ledger Missing : ${parsedLedger.ledgerName}`
        );
        continue;
    }

    for (const [parsedField, dbField, label] of this.ledgerFields) {

    if (
        !this.compareField(
            dbLedger[dbField],
            parsedLedger[parsedField]
        )
    ) {

        reasons.push(
            `${label} Mismatch : ${parsedLedger.ledgerGuid}`
        );

    }

}

   

}

}


    /**
     * ========================================================
     * Inventory Validation
     * ========================================================
     */
  async validateInventory(
    parsedVoucher,
    dbData,
    reasons
) {
     if (!dbData) {
        return;
    }

    for (

    const parsedItem of

    (parsedVoucher.inventory || []).filter(

        item =>

            item.inventoryNode ===

            "ALLINVENTORYENTRIES.LIST"

    )

) {

const dbItem =
    dbData.inventoryMap.find(item =>

        item.stock_guid ===
            parsedItem.stockGuid &&

        Number(item.amount) ===
            Number(parsedItem.amount) &&

        String(item.godown || "").trim() ===
            String(parsedItem.godown || "").trim()

    );
if(process.env.INTEGRITY_DEBUG === "true"){
    fs.appendFileSync(
        debugFile,
    JSON.stringify({

        voucher_guid:
            parsedVoucher.header.guid,

        stock_guid:
            parsedItem.stockGuid,

        stock_item:
            parsedItem.stockItem,

        parsed: {
            amount: parsedItem.amount,
            godown: parsedItem.godown,
            cgst_rate: parsedItem.cgstRate,
            sgst_rate: parsedItem.sgstRate,
            igst_rate: parsedItem.igstRate
        },

        matched_db: dbItem
            ? {
                amount: dbItem.amount,
                godown: dbItem.godown,
                cgst_rate: dbItem.cgst_rate,
                sgst_rate: dbItem.sgst_rate,
                igst_rate: dbItem.igst_rate
            }
            : null

    }) + "\n"
);
}

    if (!dbItem) {

        reasons.push(
            `Inventory Missing : ${parsedItem.stockItem}`
        );

        continue;
    }

for (const [parsedField, dbField, label] of this.inventoryFields) {

    const skipGSTValidation =
[
    "Consumption Voucher View",
    "Multi Consumption Voucher View"
].includes(dbItem.persisted_view);

if (
    skipGSTValidation &&
    [
        "cgst_rate",
        "sgst_rate",
        "igst_rate",
        "cgst_amount",
        "sgst_amount",
        "igst_amount",
        "taxable_amount"
    ].includes(dbField)
) {
    continue;
}

    if (
        !this.compareField(
            dbItem[dbField],
            parsedItem[parsedField]
        )
    ) {

        reasons.push(
            `${label} Mismatch : ${parsedItem.stockGuid}`
        );

    }

}

 
  

}

}


/**
 * ========================================================
 * Stock Voucher Validation
 * ========================================================
 */
async validateStockVouchers(
    parsedVoucher,
    dbData,
    reasons
) {

    if (!dbData) {
        return;
    }

    const parsedStockVouchers =
    (parsedVoucher.inventory || []).filter(
        item =>
            item.inventoryNode !==
            "ALLINVENTORYENTRIES.LIST"
    );


    const stockVoucherMap = new Map(
    dbData.stockVouchers.map(item => [
        [
            item.stock_guid,
            item.inventory_node,
            item.batch_id || "",
            Number(item.amount || 0),
            String(item.godown || "").trim()
        ].join("|"),
        item
    ])
);


for (const parsedItem of parsedStockVouchers) {

    if (process.env.INTEGRITY_DEBUG === "true") {

    fs.appendFileSync(
        "logs/stock-voucher-match.jsonl",
        JSON.stringify({

            voucher_guid: parsedVoucher.header.guid,

            parsed: {
                stock_item: parsedItem.stockItem,
                stock_guid: parsedItem.stockGuid,
                inventory_node: parsedItem.inventoryNode,
                batch_id: parsedItem.batchId || "",
                amount: parsedItem.amount,
                godown: parsedItem.godown
            },

            candidates: dbData.stockVouchers.map(item => ({
                id: item.id,
                stock_item: item.stock_item,
                stock_guid: item.stock_guid,
                inventory_node: item.inventory_node,
                batch_id: item.batch_id || "",
                amount: item.amount,
                godown: item.godown,

                map_key: [
                    item.stock_guid,
                    item.inventory_node,
                    item.batch_id || ""
                ].join("|")
            }))

        }) + "\n"
    );

}

/* 300726
const stockVoucherMap = new Map(
    dbData.stockVouchers.map(item => [
        [
            item.stock_guid,
            item.inventory_node,
            item.batch_id || "",
            Number(item.amount || 0),
            String(item.godown || "").trim()
        ].join("|"),
        item
    ])
);

*/
/*
  const dbItem =
    dbData.stockVouchers.find(item =>

        item.stock_guid ===
            parsedItem.stockGuid &&

        item.inventory_node ===
            parsedItem.inventoryNode &&

        Number(item.amount) ===
            Number(parsedItem.amount) &&

        String(item.godown || "").trim() ===
            String(parsedItem.godown || "").trim()

    );
*/

const dbItem =
    stockVoucherMap.get(
        [
            parsedItem.stockGuid,
            parsedItem.inventoryNode,
            parsedItem.batchId || "",
            Number(parsedItem.amount || 0),
            String(parsedItem.godown || "").trim()
        ].join("|")
    );


    if (process.env.INTEGRITY_DEBUG === "true") {
   fs.appendFileSync(
    "logs/stock-voucher-match.jsonl",
    JSON.stringify({

       matched: dbItem
    ? {
        id: dbItem.id,
        stock_item: dbItem.stock_item,
        stock_guid: dbItem.stock_guid,
        inventory_node: dbItem.inventory_node,
        batch_id: dbItem.batch_id || "",
        amount: dbItem.amount,
        godown: dbItem.godown,

        map_key: [
            dbItem.stock_guid,
            dbItem.inventory_node,
            dbItem.batch_id || ""
        ].join("|")
    }
    : null

    }) + "\n"
);
    }

    if (!dbItem) {

        reasons.push(
            `Stock Voucher Missing : ${parsedItem.stockItem}`
        );

        continue;

    }

    for (const [parsedField, dbField, label] of this.stockVoucherFields) {

    if (
        !this.compareField(
            dbItem[dbField],
            parsedItem[parsedField]
        )
    ) {

        reasons.push(
            `${label} Mismatch : ${parsedItem.stockGuid}`
        );

    }

}

}
    // TODO:
    // Validate INVENTORYENTRIESIN.LIST
    // Validate INVENTORYENTRIESOUT.LIST
    // Compare movement type
    // Compare source/destination godown
    // Compare batches
}

/**
 * ========================================================
 * Reverse Validation (DB → XML)
 * ========================================================
 *
 * Validates that every database record still exists in
 * the latest Tally XML snapshot. Detects orphan Voucher,
 * Ledger and Inventory records created due to corruption
 * or deleted vouchers in Tally.
 */
async validateReverseData(
    parsedVoucher,
    dbData,
    reasons
) {

    if (!dbData) {
        return;
    }

    //--------------------------------------------------
    // Reverse Voucher Validation
    //--------------------------------------------------
    //
    // Purpose:
    // Verify that the voucher still exists in the latest
    // Tally XML snapshot.
    //
    // NOTE:
    // Voucher level reverse validation is handled by the
    // Sync Engine before VoucherIntegrityService.
    //
    // TODO:
    // Nothing required here.
    //
    //--------------------------------------------------


    //--------------------------------------------------
    // Reverse Ledger Validation
    //--------------------------------------------------
    //
    // Purpose:
    // Detect ledger records that exist in DB but are no
    // longer present in the latest Tally XML.
    //
    // TODO:
    // Compare DB Ledgers with Parsed Ledgers.
    // Add reason for every orphan ledger.
    //
    //--------------------------------------------------

    const parsedLedgerMap = new Map(

    (parsedVoucher.ledgers || []).map(item => [

        item.ledgerGuid,

        item

    ])

);

    for (const dbLedger of (dbData.ledgers || [])) {

        const parsedLedger =
    parsedLedgerMap.get(
        dbLedger.ledger_guid
    );

        if (!parsedLedger) {

            reasons.push(
                `Orphan Ledger : ${dbLedger.ledger_name}`
            );

        }

    }


    //--------------------------------------------------
// Reverse Inventory Validation
//--------------------------------------------------
//
// Purpose:
// Detect inventory records that exist in DB but are
// no longer present in the latest Tally XML.
//
// TODO:
// Compare DB Inventory with Parsed Inventory.
// Add reason for every orphan inventory.
//
//--------------------------------------------------

  const parsedInventoryRows =
    (parsedVoucher.inventory || []).filter(
        item =>
            item.inventoryNode ===
            "ALLINVENTORYENTRIES.LIST"
    );



const parsedInventoryMap = new Map(

    parsedInventoryRows.map(item => [

        [
            item.stockGuid,
            Number(item.amount || 0),
            String(item.godown || "").trim()
        ].join("|"),

        item

    ])

);


for (const dbItem of (dbData.inventory || [])) {

const parsedItem =
    parsedInventoryMap.get(

        [
            dbItem.stock_guid,
            Number(dbItem.amount || 0),
            String(dbItem.godown || "").trim()

        ].join("|")

    );

    if (!parsedItem) {

        reasons.push(
            `Orphan Inventory : ${dbItem.stock_item}`
        );

    }

}

const parsedStockVouchers =
    (parsedVoucher.inventory || []).filter(
        item =>
            item.inventoryNode !==
            "ALLINVENTORYENTRIES.LIST"
    );

const parsedStockVoucherMap = new Map(

    parsedStockVouchers.map(item => [

        [

            item.stockGuid,
            item.inventoryNode,
            item.batchId || "",
            Number(item.amount || 0),
            String(item.godown || "").trim()

        ].join("|"),

        item

    ])

);
  
for (const dbItem of (dbData.stockVouchers || [])) {

 const key = [

    dbItem.stock_guid,
    dbItem.inventory_node,
    dbItem.batch_id || "",
    Number(dbItem.amount || 0),
    String(dbItem.godown || "").trim()

].join("|");

const parsedItem =
    parsedStockVoucherMap.get(key);


    if (!parsedItem) {

        reasons.push(
            `Orphan Stock Voucher : ${dbItem.stock_item}`
        );

    }

}


    //--------------------------------------------------
    // Reverse Future Validation
    //--------------------------------------------------
    //
    // Reserved for future reverse validations:
    //
    // - Accounting Allocation
    // - Cost Centre Allocation
    // - Bill Allocation
    // - Batch Allocation
    // - GST Allocation
    //
    //--------------------------------------------------

}

    /**
     * ========================================================
     * Party Validation
     * ========================================================
     */
   async validateParty(
    parsedVoucher,
    dbData,
    reasons
) {
     if (!dbData) {
        return;
    }


    

}


    /**
     * ========================================================
     * Accounting Validation
     * ========================================================
     */
   async validateTotals(
    parsedVoucher,
    dbData,
    reasons
) {
     if (!dbData) {
        return;
    }

    let parsedDebit = 0;
let parsedCredit = 0;

let dbDebit = 0;
let dbCredit = 0;


for (const ledger of (parsedVoucher.ledgers || [])) {

    parsedDebit += Number(ledger.debit || 0);
    parsedCredit += Number(ledger.credit || 0);

}

for (const ledger of (dbData.ledgers || [])) {

    dbDebit += Number(ledger.debit || 0);
    dbCredit += Number(ledger.credit || 0);

}

if (parsedDebit !== dbDebit) {

    reasons.push(
        `Total Debit Mismatch : Parsed=${parsedDebit}, DB=${dbDebit}`
    );

}

if (parsedCredit !== dbCredit) {

    reasons.push(
        `Total Credit Mismatch : Parsed=${parsedCredit}, DB=${dbCredit}`
    );

}


const skipLedgerBalanceValidation =
[
    "Consumption Voucher View",
    "Multi Consumption Voucher View"
].includes(dbData.header.persisted_view);

if (
    !skipLedgerBalanceValidation &&
    parsedDebit !== parsedCredit
) {

    reasons.push(
        `Parsed Voucher Not Balanced : Debit=${parsedDebit}, Credit=${parsedCredit}, Difference=${parsedDebit - parsedCredit}`
    );

}

if (
    !skipLedgerBalanceValidation &&
    dbDebit !== dbCredit
) {

    reasons.push(
        `Database Voucher Not Balanced : Debit=${dbDebit}, Credit=${dbCredit}, Difference=${dbDebit - dbCredit}`
    );

}


let parsedInventoryTotal = 0;
let dbInventoryTotal = 0;

for (

    const item of

    (parsedVoucher.inventory || []).filter(

        item =>

            item.inventoryNode ===

            "ALLINVENTORYENTRIES.LIST"

    )

) {

    parsedInventoryTotal += Math.abs(
        Number(item.amount || 0)
    );

}

for (const item of (dbData.inventory || [])) {

    dbInventoryTotal += Math.abs(
        Number(item.amount || 0)
    );

}

if (parsedInventoryTotal !== dbInventoryTotal) {

    reasons.push(
        `Inventory Total Mismatch : Parsed=${parsedInventoryTotal}, DB=${dbInventoryTotal}`
    );

}


}


    /**
     * ========================================================
     * Count Validation
     * ========================================================
     *
     * Example:
     *
     * Tally Ledger Count = 7
     * DB Ledger Count = 6
     *
     * => Force Repair
     */
async validateCounts(
    parsedVoucher,
    dbData,
    reasons
) {

    if (!dbData) {
        return;
    }

    const parsedLedgerCount =
        parsedVoucher.ledgers?.length || 0;

    const dbLedgerCount =
        dbData.ledgers?.length || 0;

    if (parsedLedgerCount !== dbLedgerCount) {

        reasons.push(
            `Ledger Count Mismatch : Parsed=${parsedLedgerCount}, DB=${dbLedgerCount}`
        );

    }
const parsedInventoryCount =
    (parsedVoucher.inventory || []).filter(
        item =>
            item.inventoryNode ===
            "ALLINVENTORYENTRIES.LIST"
    ).length;

const dbInventoryCount =
    dbData.inventory?.length || 0;

if (parsedInventoryCount !== dbInventoryCount) {

    reasons.push(
        `Inventory Count Mismatch : Parsed=${parsedInventoryCount}, DB=${dbInventoryCount}`
    );

}

const parsedStockVoucherCount =
    (parsedVoucher.inventory || []).filter(
        item =>
            item.inventoryNode !==
            "ALLINVENTORYENTRIES.LIST"
    ).length;

const dbStockVoucherCount =
    dbData.stockVouchers?.length || 0;

if (parsedStockVoucherCount !== dbStockVoucherCount) {

    reasons.push(
        `Stock Voucher Count Mismatch : Parsed=${parsedStockVoucherCount}, DB=${dbStockVoucherCount}`
    );

}


}

    /**
     * ========================================================
     * NULL Validation
     * ========================================================
     *
     * Checks:
     *
     * Ledger GUID
     * Stock GUID
     * Party GUID
     * Master IDs
     * Alter IDs
     * Names
     */
  async validateNullFields(
    parsedVoucher,
    dbData,
    reasons
) {

     if (!dbData) {
        return;
    }

const requiredHeaderFields = [
    "guid",
    "masterid",
    "alterid",
    "voucher_number",
    "voucher_type",
    
];

for (const field of requiredHeaderFields) {

    if (
        dbData.header[field] === null ||
        dbData.header[field] === undefined ||
        String(dbData.header[field]).trim() === ""
    ) {

        reasons.push(
            `Header Field Missing : ${field}`
        );

    }

}

const requiredLedgerFields = [

    "ledger_guid",
    "ledger_masterid",
    "ledger_alterid"

];

for (const ledger of dbData.ledgers || []) {

    for (const field of requiredLedgerFields) {

        if (
            ledger[field] === null ||
            ledger[field] === undefined ||
            String(ledger[field]).trim() === ""
        ) {

            reasons.push(
                `Ledger Field Missing : ${field}`
            );

        }

    }

}

for (const item of (dbData.inventory || [])) {

    const requiredInventoryFields = [

        "stock_guid",
        "stock_masterid",
        "stock_alterid"

    ];

    if (item.transaction_type === "Yes") {

        requiredInventoryFields.push(

            "ledger_guid",
            "ledger_master_id",
            "ledger_alter_id",

            "party_guid",
            "party_master_id",
            "party_alter_id"

        );

    }

    for (const field of requiredInventoryFields) {

        if (

            item[field] === null ||

            item[field] === undefined ||

            String(item[field]).trim() === ""

        ) {

            reasons.push(
                `Inventory Field Missing : ${field}`
            );

        }

    }

}
const requiredStockVoucherFields = [

    "stock_guid",
    "stock_masterid",
    "stock_alterid",

    "movement_type",

    "inventory_node"

];

for (const item of (dbData.stockVouchers || [])) {

    for (const field of requiredStockVoucherFields) {

        if (
            item[field] === null ||
            item[field] === undefined ||
            String(item[field]).trim() === ""
        ) {

            reasons.push(
                `Stock Voucher Field Missing : ${field}`
            );

        }

    }

}



}


    /**
     * ========================================================
     * Duplicate Validation
     * ========================================================
     *
     * Detect accidental duplicate inserts.
     */
   async validateDuplicates(
    parsedVoucher,
    dbData,
    reasons
) {
     if (!dbData) {
        return;
    }

    const ledgerGuids = new Set();

    for (const ledger of (parsedVoucher.ledgers || [])) {

    if (!ledger.ledgerGuid) {
        continue;
    }

    if (ledgerGuids.has(ledger.ledgerGuid)) {

        reasons.push(
            `Duplicate Ledger GUID : ${ledger.ledgerGuid}`
        );

    }

    ledgerGuids.add(ledger.ledgerGuid);

}

const stockKeys = new Set();

for (const item of (parsedVoucher.inventory || [])) {

    if (!item.stockGuid) {
        continue;
    }

    const key = [

    item.stockGuid,

    item.inventoryNode,

    item.batchId || "",

    Number(item.amount || 0)

].join("|");

    if (stockKeys.has(key)) {

        reasons.push(
            `Duplicate Inventory : ${key}`
        );

    }

    stockKeys.add(key);

}

const dbStockVoucherKeys = new Set();

for (const item of (dbData.stockVouchers || [])) {

 const key = [

    item.stock_guid,

    item.inventory_node,

    item.batch_id || "",

    Number(item.amount || 0)

].join("|");

    if (dbStockVoucherKeys.has(key)) {

        reasons.push(
            `Duplicate Stock Voucher : ${key}`
        );

    }

    dbStockVoucherKeys.add(key);

}



}


    /**
     * ========================================================
     * Future Plug-in Validation
     * ========================================================
     *
     * Any future accounting validation should be added here.
     *
     * Example:
     *
     * validateGST()
     * validateCostCentre()
     * validateBillAllocation()
     * validateBankEntries()
     */
   async validateFuture(
    parsedVoucher,
    dbData,
    reasons
) {

     if (!dbData) {
        return;
    }

}

}

module.exports = new VoucherIntegrityService();