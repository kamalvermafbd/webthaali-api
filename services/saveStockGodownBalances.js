const { createClient } =
    require("@supabase/supabase-js");


const supabase =
    createClient(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_SERVICE_KEY
    );


// ======================================================
// SAVE STOCK GODOWN BALANCES
// ======================================================

async function saveStockGodownBalances({

    company_code,
    tally_owner,
    stockGodownBalances = [],
    sync_batch_id = null

}) {

    if (!company_code) {

        throw new Error(
            "company_code missing in saveStockGodownBalances"
        );

    }

    if (!tally_owner) {

        throw new Error(
            "tally_owner missing in saveStockGodownBalances"
        );

    }

    if (!Array.isArray(stockGodownBalances)) {

        throw new Error(
            "stockGodownBalances must be an array"
        );

    }

    if (stockGodownBalances.length === 0) {

        return {

            success: true,

            inserted: 0,

            updated: 0,

            total: 0

        };

    }


    // ==================================================
    // 1. COLLECT STOCK + GODOWN NAMES
    // ==================================================

    const stockNames = [
        ...new Set(

            stockGodownBalances

                .map(
                    row =>
                        String(
                            row.stock_name ||
                            row.stockName ||
                            row.stockItemName ||
                            ""
                        ).trim()
                )

                .filter(Boolean)

        )
    ];


    const godownNames = [
        ...new Set(

            stockGodownBalances

                .map(
                    row =>
                        String(
                            row.godown_name ||
                            row.godownName ||
                            ""
                        ).trim()
                )

                .filter(Boolean)

        )
    ];


    // ==================================================
    // 2. LOAD STOCK GUIDS
    // ==================================================

    const {
        data: stockRows,
        error: stockError
    } = await supabase

        .from("tally_sync_stocks")

        .select(
            "guid,name"
        )

        .eq(
            "company_code",
            company_code
        )

        .eq(
            "tally_owner",
            tally_owner
        )

       .in(
            "name",
            stockNames
        );


    if (stockError) {

        throw new Error(
            `Failed to load stock GUIDs: ${stockError.message}`
        );

    }


    const stockGuidMap =
        new Map();


    for (const row of stockRows || []) {

        const name =
                String(
                    row.name || ""
                ).trim();

        const guid =
            String(
                row.guid || ""
            ).trim();


        if (
            name &&
            guid
        ) {

            stockGuidMap.set(
                name,
                guid
            );

        }

    }


    // ==================================================
    // 3. LOAD GODOWN GUIDS
    // ==================================================

    const {
        data: godownRows,
        error: godownError
    } = await supabase

        .from("tally_sync_godowns")

        .select(
                "guid,name"
            )

        .eq(
            "company_code",
            company_code
        )

        .eq(
            "tally_owner",
            tally_owner
        )

       .in(
            "name",
            godownNames
        );


    if (godownError) {

        throw new Error(
            `Failed to load godown GUIDs: ${godownError.message}`
        );

    }


    const godownGuidMap =
        new Map();


    for (const row of godownRows || []) {

       const name =
                String(
                    row.name || ""
                ).trim();

        const guid =
            String(
                row.guid || ""
            ).trim();


        if (
            name &&
            guid
        ) {

            godownGuidMap.set(
                name,
                guid
            );

        }

    }


    // ==================================================
    // 4. BUILD FINAL DB ROWS
    // ==================================================

    const rows =
        stockGodownBalances.map(row => {

            const stockName =
                String(
                    row.stock_name ||
                    row.stockName ||
                    row.stockItemName ||
                    ""
                ).trim();


            const godownName =
                String(
                    row.godown_name ||
                    row.godownName ||
                    ""
                ).trim();


            const stockGuid =
                String(
                    row.stock_guid ||
                    row.stockGuid ||
                    stockGuidMap.get(
                        stockName
                    ) ||
                    ""
                ).trim();


            const godownGuid =
                row.godown_guid ||
                row.godownGuid ||
                godownGuidMap.get(
                    godownName
                ) ||
                null;


            return {

                company_code,

                tally_owner,

                stock_guid:
                    stockGuid,

                stock_name:
                    stockName,

                godown_guid:
                    godownGuid,

                godown_name:
                    godownName,

                closing_quantity:
                    Number(
                        row.closing_quantity ??
                        row.closingQuantity ??
                        0
                    ),

                unit:
                    row.unit ||
                    null,

                sync_batch_id

            };

        });


    // ==================================================
    // 5. VALIDATE
    // ==================================================

    const invalidRows =
        rows.filter(row =>

            !row.stock_guid ||
            !row.stock_name ||
            !row.godown_name

        );


    if (invalidRows.length > 0) {

        console.error(
            "INVALID STOCK GODOWN ROW SAMPLE:",
            invalidRows[0]
        );

        throw new Error(
            `Invalid stock godown rows: ${invalidRows.length}`
        );

    }


    // ==================================================
    // 6. SAVE
    // ==================================================

    const {
        data,
        error
    } =
        await supabase

            .from(
                "tally_stock_godown_balances"
            )

            .upsert(

                rows,

                {

                    onConflict:
                        "company_code,tally_owner,stock_guid,godown_name"

                }

            )

            .select();

    if (error) {

    console.error(
        "SAVE STOCK GODOWN BALANCES ERROR:",
        error
    );

    throw error;

}        

const {
    data: dbRows,
    error: dbError
} = await supabase
    .from("db_stock_view")
    .select(`
        company_code,
        tally_owner,
        stock_guid,
        stock_name,
        godown,
        unit,
        db_closing_quantity
    `)
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

if (dbError) {
    throw dbError;
}

const dbMap = new Map();

for (const row of dbRows || []) {

    const key = [
        row.stock_guid,
        row.godown,
        row.unit || ""
    ].join("|");

    dbMap.set(
        key,
        Number(row.db_closing_quantity || 0)
    );
}

const reconciliationRows =
    rows.map(row => {

        const key = [
            row.stock_guid,
            row.godown_name,
            row.unit || ""
        ].join("|");

        const dbStock =
            Number(
                dbMap.get(key) || 0
            );

        const tallyStock =
            Number(
                row.closing_quantity || 0
            );

        return {

            ...row,

            db_stock_quantity:
                dbStock,

            balance_difference:
                tallyStock - dbStock

        };

    });

   const {
    error: reconciliationError
} = await supabase
    .from("tally_stock_godown_balances")
    .upsert(
        reconciliationRows,
        {
            onConflict:
                "company_code,tally_owner,stock_guid,godown_name"
        }
    );

if (reconciliationError) {
    throw reconciliationError;
}

    console.log(
        "STOCK GODOWN BALANCES SAVED:",
        data?.length || 0
    );


    return {

        success: true,

        inserted:
            data?.length || 0,

        updated: 0,

        total:
            rows.length

    };

}


module.exports = {
    saveStockGodownBalances
};