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

            parsedVoucher,

            dbData,

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

    parsedVoucher,

    dbData,

    validation

}) {

    //--------------------------------------------------
    // No Existing Voucher
    //--------------------------------------------------

    if (!dbData) {

        return "INSERT";

    }

    //--------------------------------------------------
    // Voucher Invalid
    //--------------------------------------------------

    if (validation.requiresRepair) {

        return "FORCE_UPDATE";

    }

    //--------------------------------------------------
    // AlterID Changed
    //--------------------------------------------------

    if (

        Number(parsedVoucher.header.alterid) !==
        Number(dbData.header.alterid)

    ) {

        return "UPDATE";

    }

    //--------------------------------------------------
    // Nothing Changed
    //--------------------------------------------------

    return "SKIP";

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

    if (
        (dbLedger.ledger_name || "").trim() !==
        (parsedLedger.ledgerName || "").trim()
    ) {

        reasons.push(
            `Ledger Name Mismatch : ${parsedLedger.ledgerGuid}`
        );
    }

    if (
        Number(dbLedger.amount) !==
        Number(parsedLedger.amount)
    ) {

        reasons.push(
            `Ledger Amount Mismatch : ${parsedLedger.ledgerGuid}`
        );
    }

    if (
    (dbLedger.ledger_parent_guid || "").trim() !==
    (parsedLedger.ledgerParentGuid || "").trim()
) {
    reasons.push(
        `Ledger Parent GUID Mismatch : ${parsedLedger.ledgerGuid}`
    );
}

if (
    (dbLedger.ledger_parent_name || "").trim() !==
    (parsedLedger.ledgerParentName || "").trim()
) {
    reasons.push(
        `Ledger Parent Name Mismatch : ${parsedLedger.ledgerGuid}`
    );
}

if (
    String(dbLedger.ledger_masterid || "").trim() !==
    String(parsedLedger.ledgerMasterId || "").trim()
) {
    reasons.push(
        `Ledger MasterId Mismatch : ${parsedLedger.ledgerGuid}`
    );
}

if (
    String(dbLedger.ledger_parent_alterid || "").trim() !==
    String(parsedLedger.ledgerParentAlterId || "").trim()
) {
    reasons.push(
        `Ledger Parent AlterId Mismatch : ${parsedLedger.ledgerGuid}`
    );
}


if (
    Number(dbLedger.debit) !==
    Number(parsedLedger.debit)
) {
    reasons.push(
        `Ledger Debit Mismatch : ${parsedLedger.ledgerGuid}`
    );
}

if (
    Number(dbLedger.credit) !==
    Number(parsedLedger.credit)
) {
    reasons.push(
        `Ledger Credit Mismatch : ${parsedLedger.ledgerGuid}`
    );
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

}

}

module.exports = new VoucherIntegrityService();