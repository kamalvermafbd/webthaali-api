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
const fs = require("fs");

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

class VoucherIntegrityService {

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
    
    fs.appendFileSync(
    `./validator-start-${runId}.txt`,
    `${parsedVoucher.header.guid}\n`
);

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
// AlterId Changed
//--------------------------------------------------

if (

    Number(parsedVoucher.header.alterid) !==
    Number(dbData.header.alterid)

) {

    return this.buildResponse({

        action: "UPDATE",

        validation: this.buildValidValidation(),

        dbData

    });

}

    //--------------------------------------------------
    // Run Validators
    //--------------------------------------------------

    const validation =
        await this.runValidators({

            parsedVoucher,

            dbData

        });

        fs.appendFileSync(
    `./validator-end-${runId}.txt`,
    `${parsedVoucher.header.guid}\n`
);

    //--------------------------------------------------
    // Decide Action
    //--------------------------------------------------

    const action =
        this.decideAction({

           // parsedVoucher,

         //   dbData,

            validation

        });


        

fs.appendFileSync(
    `./voucher-integrity-debug-${runId}.json`,
    JSON.stringify(
        {
            generatedAt: new Date().toISOString(),
            guid: parsedVoucher?.header?.guid,
            voucherNumber: parsedVoucher?.header?.voucherNumber,
            action,
            reasons: validation.reasons,
            parsedLedgerCount:
                parsedVoucher?.ledgers?.length || 0,
            dbLedgerCount:
                dbData?.ledgers?.length || 0
        }
    ) + "\n"
);


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

    fs.appendFileSync(
    "./load-header-debug.txt",
    `${guid} | headerFound=${!!header}\n`
        );

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

    //--------------------------------------------------
    // Return Complete DB Snapshot
    //--------------------------------------------------

    return {

        header,

        ledgers: ledgers || [],

        inventory: inventory || []

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

        this.validateHeader,

        this.validateLedgers,

        this.validateInventory,

        this.validateParty,

        this.validateTotals,

        this.validateCounts,

        this.validateNullFields,

        this.validateDuplicates,

        this.validateFuture

    ];

    for (const validator of validators) {

        await validator.call(
            this,
            parsedVoucher,
            dbData,
            reasons
        );

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
    ["ledgerParentGuid", "ledger_parent_guid", "Ledger Parent GUID"],
["ledgerParentMasterId", "ledger_parent_master_id", "Ledger Parent MasterId"],
["ledgerParentAlterId", "ledger_parent_alter_id", "Ledger Parent AlterId"],

["partyGuid", "party_guid", "Party GUID"],
["partyMasterId", "party_master_id", "Party MasterId"],
["partyAlterId", "party_alter_id", "Party AlterId"],

["partyParentName", "party_parent_name", "Party Parent Name"],
["partyParentGuid", "party_parent_guid", "Party Parent GUID"],
["partyParentMasterId", "party_parent_master_id", "Party Parent MasterId"],
["partyParentAlterId", "party_parent_alter_id", "Party Parent AlterId"],


["ledgerGuid", "ledger_guid", "Ledger GUID"],
["ledgerMasterId", "ledger_master_id", "Ledger MasterId"],
["ledgerAlterId", "ledger_alter_id", "Ledger AlterId"],

["voucherMasterId", "voucher_master_id", "Voucher MasterId"],
["voucherAlterId", "voucher_alter_id", "Voucher AlterId"],

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

    ["ledgerParentName", "ledger_parent_name", "Ledger Parent Name"],
    ["ledgerParentGuid", "ledger_parent_guid", "Ledger Parent GUID"],
    ["ledgerParentMasterId", "ledger_parent_masterid", "Ledger Parent MasterId"],
    ["ledgerParentAlterId", "ledger_parent_alterid", "Ledger Parent AlterId"],

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

    const dbLedger = (dbData.ledgers || []).find(
        l =>
            l.ledger_guid === parsedLedger.ledgerGuid
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

    for (const parsedItem of (parsedVoucher.inventory || [])) {

    const dbItem = (dbData.inventory || []).find(
        i =>
            i.stock_guid === parsedItem.stockGuid
    );

    if (!dbItem) {

        reasons.push(
            `Inventory Missing : ${parsedItem.stockName}`
        );

        continue;
    }

for (const [parsedField, dbField, label] of this.inventoryFields) {

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

 fs.appendFileSync(
    "./inventory-debug.txt",
    JSON.stringify({
        voucherGuid: parsedVoucher.header.guid,
        stockGuid: parsedItem.stockGuid,
        parsedName: parsedItem.stockItem,
        parsedItem,
        dbName: dbItem.stock_item
    }, null, 2) + "\n\n"
);
  








}

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

if (parsedDebit !== parsedCredit) {

    reasons.push(
        `Parsed Voucher Not Balanced`
    );

}

if (dbDebit !== dbCredit) {

    reasons.push(
        `Database Voucher Not Balanced`
    );

}
let parsedInventoryTotal = 0;
let dbInventoryTotal = 0;

for (const item of (parsedVoucher.inventory || [])) {

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
    parsedVoucher.inventory?.length || 0;

const dbInventoryCount =
    dbData.inventory?.length || 0;

if (parsedInventoryCount !== dbInventoryCount) {

    reasons.push(
        `Inventory Count Mismatch : Parsed=${parsedInventoryCount}, DB=${dbInventoryCount}`
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
    "ledger_alterid",

    "ledger_parent_guid",
    "ledger_parent_masterid",
    "ledger_parent_alterid"

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

const requiredInventoryFields = [

    "stock_guid",
    "stock_masterid",
    "stock_alterid",

    "ledger_guid",
    "ledger_master_id",
    "ledger_alter_id",

    "party_guid",
    "party_master_id",
    "party_alter_id",

    "voucher_master_id",
    "voucher_alter_id",

    "ledger_parent_guid",
    "ledger_parent_master_id",
    "ledger_parent_alter_id",

    "party_parent_guid",
    "party_parent_master_id",
    "party_parent_alter_id"

];

for (const item of dbData.inventory || []) {

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

const stockGuids = new Set();

for (const item of (parsedVoucher.inventory || [])) {

    if (!item.stockGuid) {
        continue;
    }

    if (stockGuids.has(item.stockGuid)) {

        reasons.push(
            `Duplicate Stock GUID : ${item.stockGuid}`
        );

    }

    stockGuids.add(item.stockGuid);

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