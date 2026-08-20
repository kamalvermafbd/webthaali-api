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


        const extraRows =
            rows.filter(
                row =>
                    row.status === "EXTRA"
            );


        totalMissing +=
            missingRows.length;

        totalExtra +=
            extraRows.length;
    

    const childTable =
            VOUCHER_CHILD_RECONCILIATION_MAP[
                viewName
            ];

        if (!childTable) {

            throw new Error(
                `Child reconciliation table mapping missing for view: ${viewName}`
            );

        }


        

        results.push({

            view: viewName,

            table:
                childTable,

            total:
                rows.length,

            missing:
                missingRows.length,

            extra:
                extraRows.length,

            missingRows,

            extraRows

        });


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
        totalExtra === 0;


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
        "CLEAN:",
        clean
    );

    console.log(
        "===================================="
    );


    const missingByChildTable = {};

const extraByChildTable = {};

for (const item of results) {

    if (item.missingRows.length > 0) {

        missingByChildTable[item.table] =
            item.missingRows;

    }

    if (item.extraRows.length > 0) {

        extraByChildTable[item.table] =
            item.extraRows;

    }

}

    return {

            success: true,

            clean,

            totalMissing,

            totalExtra,

            views: results,

            missingByChildTable,

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