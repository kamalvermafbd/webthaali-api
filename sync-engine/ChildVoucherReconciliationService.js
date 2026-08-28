const {
    VOUCHER_CHILD_RECONCILIATION_VIEWS,
    VOUCHER_CHILD_RECONCILIATION_MAP
} = require("./constants");

const { createClient } =
    require("@supabase/supabase-js");


const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);


// ======================================================
// CHILD VOUCHER RECONCILIATION SERVICE
// ======================================================
//
// Phase-2:
// Parent VOUCHER reconciliation complete hone ke baad
// configured child reconciliation views check karega.
//
// IMPORTANT:
// Views constants.js se dynamically aayenge.
// Future mein new reconciliation view add karne par
// is file mein code change nahi karna padega.
// ======================================================


async function getReconciliationRows({
    viewName,
    company_code,
    tally_owner
}) {

    if (!viewName) {

        throw new Error(
            "viewName missing in child reconciliation"
        );

    }

    let query =
        supabase
            .from(viewName)
            .select("*");

    if (company_code) {

        query =
            query.eq(
                "company_code",
                company_code
            );

    }

    if (tally_owner) {

        query =
            query.eq(
                "tally_owner",
                tally_owner
            );

    }

    const {
        data,
        error
    } = await query;

    if (error) {

        throw new Error(
            `Child reconciliation view failed (${viewName}): ${error.message}`
        );

    }

    return data || [];

}


async function checkChildVoucherReconciliation({
    company_code,
    tally_owner
}) {

    const views =
        VOUCHER_CHILD_RECONCILIATION_VIEWS || [];

    if (!views.length) {

        return {

            success: true,

            clean: true,

            totalMissing: 0,

            totalExtra: 0,

            views: []

        };

    }


    const results = [];

    let totalMissing = 0;
    let totalExtra = 0;
    let totalAmountMismatch = 0;
    let totalStockMismatch = 0;
    let totalInventoryCountMismatch = 0;


    for (const viewName of views) {

        const rows =
            await getReconciliationRows({

                viewName,

                company_code,

                tally_owner

            });


        const missingRows =
            rows.filter(
                row =>
                    row.status === "MISSING"
            );

        const amountMismatchRows =
            rows.filter(
                row =>
                    row.status === "AMOUNT_MISMATCH"
            );

        const extraRows =
            rows.filter(
                row =>
                    row.status === "EXTRA"
            );

            // 27.08.26
// RECO STATUS COLLECTION
// View-specific repair action yahan nahi hoga.
// View -> targetTable mapping constants.js se aayegi.

        const stockMismatchRows =
            viewName === "stock_voucher_rows_reco"
                ? rows.filter(
                    row =>
                        row.status === "MISMATCH"
                )
                : [];
                
        const inventoryCountMismatchRows =
            viewName === "voucher_inventory_count_reco_view"
                ? rows.filter(
                    row =>
                        row.status === "MISMATCH"
                )
                : [];



        totalMissing +=
            missingRows.length;

        totalExtra +=
            extraRows.length;

        totalAmountMismatch +=
            amountMismatchRows.length;

        totalStockMismatch +=
             stockMismatchRows.length;

        totalInventoryCountMismatch +=
             inventoryCountMismatchRows.length;
    

    const childTable =
            VOUCHER_CHILD_RECONCILIATION_MAP[
                viewName
            ];

            // 27.08.26
// Generic repair mapping:
// Every configured reconciliation view must resolve
// to exactly one target child table.

        if (!childTable) {

            throw new Error(
                `Child reconciliation table mapping missing for view: ${viewName}`
            );

        }


        

        results.push({
            view: viewName,
            table: childTable,
            total: rows.length,
            missing: missingRows.length,
            extra: extraRows.length,
            amountMismatch: amountMismatchRows.length,
            missingRows,
            extraRows,
            amountMismatchRows,
            stockMismatchRows,
            inventoryCountMismatchRows
        });

        // 27.08.26
// GENERIC REPAIR PLAN
// Current reconciliation result is converted into
// view -> GUID -> targetTable -> action mapping.

        console.log(
            "CHILD RECONCILIATION:",
            viewName,
            "| MISSING:",
            missingRows.length,
            "| EXTRA:",
            extraRows.length
        );

    }


    const clean =
        totalMissing === 0 &&
        totalExtra === 0 &&
        totalAmountMismatch === 0 &&
        totalStockMismatch === 0 &&
        totalInventoryCountMismatch === 0;
        // 27.08.26 GENERIC REPAIR PLAN
// One combined plan from all configured reconciliation views.
// Each entry retains source view + target table + action.

const genericRepairPlan = results.flatMap(item => [

    ...(item.missingRows || []).map(row => ({
        guid: (
            row.voucher_guid ||
            row.guid
        )?.trim(),
        sourceView: item.view,
        targetTable: item.table,
        status: "MISSING",
        action: "INSERT"
    })),

    ...(item.extraRows || []).map(row => ({
        guid: (
            row.voucher_guid ||
            row.guid
        )?.trim(),
        sourceView: item.view,
        targetTable: item.table,
        status: "EXTRA",
        action: "DELETE"
    })),

    ...(item.amountMismatchRows || []).map(row => ({
        guid: (
            row.voucher_guid ||
            row.guid
        )?.trim(),
        sourceView: item.view,
        targetTable: item.table,
        status: "AMOUNT_MISMATCH",
        action: "REPLACE"
    })),

    ...(item.stockMismatchRows || []).map(row => ({
        guid: (
            row.voucher_guid ||
            row.guid
        )?.trim(),
        sourceView: item.view,
        targetTable: item.table,
        status: "MISMATCH",
        action: "REPLACE"
    }))

    ,

    ...(item.inventoryCountMismatchRows || []).map(row => ({
        guid: (
            row.voucher_guid ||
            row.guid
        )?.trim(),
        sourceView: item.view,
        targetTable: item.table,
        status: "MISMATCH",
        action: "REPLACE"
    }))

]).filter(item => item.guid);


    console.log(
        "===================================="
    );

    console.log(
        "CHILD VOUCHER RECONCILIATION"
    );

    console.log(
        "Views:",
        views.length
    );

    console.log(
        "Total Missing:",
        totalMissing
    );

    console.log(
        "Total Extra:",
        totalExtra
    );

    console.log(
        "Total Amount Mismatch:",
        totalAmountMismatch
    );

    console.log(
        "CLEAN:",
        clean
    );

    console.log(
        "===================================="
    );


const missingByChildTable = {};

const extraByChildTable = {};

const amountMismatchByChildTable = {};

const stockMismatchByChildTable = {};

const inventoryCountMismatchByChildTable = {};

// 27.08.26 OLD REPAIR PLAN BUILDER
// Kept temporarily for rollback/reference.
// Generic repair-plan flow will replace this block.

const repairPlan = [];

for (const item of results) {

    if (item.missingRows.length > 0) {

        missingByChildTable[item.table] =
            item.missingRows;

    }

    if (item.extraRows.length > 0) {

        extraByChildTable[item.table] =
            item.extraRows;

    }

    if (item.amountMismatchRows?.length > 0) {
        amountMismatchByChildTable[item.table] =
            item.amountMismatchRows;
    }

    if (item.stockMismatchRows?.length > 0) {
        stockMismatchByChildTable[item.table] =
            item.stockMismatchRows;
    }   

    if (item.inventoryCountMismatchRows?.length > 0) {
        inventoryCountMismatchByChildTable[item.table] =
            item.inventoryCountMismatchRows;
    }
    // 27.08.26 OLD MISSING REPAIR PLAN
// Kept for rollback/reference.

    for (const row of item.missingRows || []) {

    repairPlan.push({
        guid:
            (
                row.voucher_guid ||
                row.guid
            )?.trim(),

        sourceView:
            item.view,

        status:
            "MISSING",

        targetTable:
            item.table,

        action:
            "INSERT"
    });

}

// 27.08.26 OLD EXTRA REPAIR PLAN - KEEP FOR ROLLBACK

for (const row of item.extraRows || []) {

    repairPlan.push({
        guid:
            (
                row.voucher_guid ||
                row.guid
            )?.trim(),

        sourceView:
            item.view,

        status:
            "EXTRA",

        targetTable:
            item.table,

        action:
            "DELETE"
    });

}

// 27.08.26 OLD AMOUNT MISMATCH REPAIR PLAN
// Kept for rollback/reference.

for (const row of item.amountMismatchRows || []) {

    repairPlan.push({
        guid:
            (
                row.voucher_guid ||
                row.guid
            )?.trim(),

        sourceView:
            item.view,

        status:
            "AMOUNT_MISMATCH",

        targetTable:
            item.table,

        action:
            "REPLACE"
    });

}

// 27.08.26 OLD STOCK MISMATCH REPAIR PLAN - KEEP FOR ROLLBACK

for (const row of item.stockMismatchRows || []) {

    repairPlan.push({
        guid:
            (
                row.voucher_guid ||
                row.guid
            )?.trim(),

        sourceView:
            item.view,

        status:
            "MISMATCH",

        targetTable:
            item.table,

        action:
            "REPLACE"
    });

}


for (const row of item.inventoryCountMismatchRows || []) {

    repairPlan.push({
        guid:
            (
                row.voucher_guid ||
                row.guid
            )?.trim(),

        sourceView:
            item.view,

        status:
            "MISMATCH",

        targetTable:
            item.table,

        action:
            "REPLACE"
    });

}

}

    return {

            success: true,

            clean,

            totalMissing,

            totalExtra,

            totalAmountMismatch,

            totalStockMismatch,

            totalInventoryCountMismatch,

            views: results,

            repairPlan,

            // 27.08.26 GENERIC REPAIR PLAN
            genericRepairPlan,

            missingByChildTable,

            amountMismatchByChildTable,

            stockMismatchByChildTable,

            inventoryCountMismatchByChildTable,

            extraByChildTable

        };

}


// ======================================================
// PHASE-2 ENTRY POINT
// ======================================================

async function runChildVoucherReconciliation({
    company_code,
    tally_owner,
    sync_batch_id
}) {

    console.log(
        "===================================="
    );

    console.log(
        "CHILD VOUCHER RECONCILIATION START"
    );

    console.log(
        "Company:",
        company_code
    );

    console.log(
        "Tally Owner:",
        tally_owner
    );

    console.log(
        "Sync Batch:",
        sync_batch_id
    );

    console.log(
        "Configured Views:",
        VOUCHER_CHILD_RECONCILIATION_VIEWS
    );

    console.log(
        "===================================="
    );


    const result =
        await checkChildVoucherReconciliation({

            company_code,

            tally_owner

        });


    if (!result.success) {

        throw new Error(
            "Child voucher reconciliation failed"
        );

    }


    if (!result.clean) {

        console.log(
            "CHILD RECONCILIATION NOT CLEAN"
        );

        return {

            completed: false,

            ...result

        };

    }


    console.log(
        "CHILD VOUCHER RECONCILIATION CLEAN"
    );


    return {

        completed: true,

        ...result

    };

}


module.exports = {

    runChildVoucherReconciliation,

    checkChildVoucherReconciliation

};