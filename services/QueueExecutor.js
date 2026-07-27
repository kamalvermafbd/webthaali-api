const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

const {
    buildVoucherRow,
    buildLedgerRows,
    buildInventoryRows,
    buildStockVoucherRows
} = require("./voucherRowBuilder");

const {
    importVoucherByGuid
} = require("../tally/importVoucherByGuid");

const {
    importCompany
} = require("../tally/companyImportService");

const {
    importGroups
} = require("../tally/groupImportService");

const {
    importLedgers
} = require("../tally/ledgerImportService");

const {
    importStocks
} = require("../tally/stockImportService");

const {
    buildTallyLookups
} = require("../tally/tallyLookups");

const {

    deleteVoucherLedgers,

    deleteVoucherInventory,

    deleteStockVouchers,

    saveVoucher,

    saveVoucherLedgers,

    saveVoucherInventory,

    saveStockVouchers

} = require("./saveVouchers");

async function loadQueue({

    sync_batch_id,

    company_code,

    tally_owner

}) {

    const { data, error } = await supabase

        .from("sync_exe_queue")

        .select("*")

        .eq("sync_id", sync_batch_id)

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .in("execution_status", [

    "PENDING",

    "STARTED",

    "FAILED"

])

.order("id", { ascending: true });

    if (error) {

        throw new Error(

            "Failed to load queue : " +

            error.message

        );

    }

    return data || [];

}

function groupQueueByVoucher(queueRows) {

    const map = new Map();

    for (const row of queueRows) {

        if (!map.has(row.voucher_guid)) {

            map.set(row.voucher_guid, []);

        }

        map.get(row.voucher_guid).push(row);

    }

    return map;

}


async function loadVoucherDataFromTally({

    company_code,

    tally_owner,

    voucher_guid

}) {

  const {
    data: companyData,
    error: companyError
} = await supabase
    .from("company")
    .select("*")
    .eq("company_code", company_code)
    .single();

if (companyError || !companyData) {
    throw new Error("Company not found");
}

const company =
    tally_owner === "CA"
        ? companyData.ca_tally_company
        : companyData.client_tally_company;

if (!company) {
    throw new Error("Tally company not mapped");
}

const companyInfo =
    await importCompany({
        company
    });

    const groups =
    await importGroups({
        company
    });

const ledgers =
    await importLedgers({
        company,
        booksBeginningFrom:
            companyInfo.booksBeginningFrom
    });

const stocks =
    await importStocks({
        company
    });

const lookups =
    buildTallyLookups({
        groups,
        ledgers,
        stocks
    });

const parsedVoucher =
    await importVoucherByGuid({
        company,
        voucherGuid: voucher_guid,
        lookups
    });


if (!parsedVoucher) {
    throw new Error(
        `Voucher not found in Tally : ${voucher_guid}`
    );
}

const now = new Date().toISOString();

return {
    voucher: buildVoucherRow({

        header: parsedVoucher.header,

        company_code,

        tally_owner,

        sync_batch_id: null,

        now

    }),

    ledgers: buildLedgerRows({

    voucher: parsedVoucher,

    company_code,

    tally_owner

}),

inventory: buildInventoryRows({

    voucher: parsedVoucher,

    company_code,

    tally_owner

}),

stockVouchers: buildStockVoucherRows({

    voucher: parsedVoucher,

    company_code,

    tally_owner

})

};


}

async function updateQueueStage({

    queueIds,

    stage,

    status,

    extra = {}

}) {

    const { error } = await supabase

        .from("sync_exe_queue")

       .update({

    [stage]: status,

    ...extra

})

        .in("id", queueIds);

    if (error) {

        throw new Error(

            `Failed to update ${stage} : ` +

            error.message

        );

    }

}

async function executeQueue({

    sync_batch_id,

    company_code,

    tally_owner

}) {

   const queueRows =
    await loadQueue({

        sync_batch_id,

        company_code,

        tally_owner

    });

console.log(
    "Queue Rows :",
    queueRows.length
);

const voucherGroups =
    groupQueueByVoucher(queueRows);

console.log(
    "Voucher Groups :",
    voucherGroups.size
);

for (const [

    voucherGuid,

    rows

] of voucherGroups) {

    const queueRow = rows[0];

const queueIds = rows.map(r => r.id);

    try {

   

    console.log({

    voucherGuid,

    mark_for_delete: queueRow.mark_for_delete,

    mark_for_insert: queueRow.mark_for_insert,

    rows: rows.length

});

    if (

    !queueRow.mark_for_delete &&

    !queueRow.mark_for_insert

) {

    console.log(

        "Skipping :",

        voucherGuid

    );

    continue;

}

console.log(

    "Ready To Execute :",

    voucherGuid

);


if (

    queueRow.execution_status === "PENDING" ||

    queueRow.execution_status === "FAILED"

) {

    await updateQueueStage({

    queueIds,

    stage: "execution_status",

    status: "STARTED",

    extra: {

        started_at: new Date().toISOString(),

        completed_at: null,

        error_message: null

    }

});

console.log(

    "Marked STARTED :",

    voucherGuid

);

}

if (

    (!queueRow.mark_for_delete ||
        queueRow.delete_status === "COMPLETED") &&

    (!queueRow.mark_for_insert ||

        (

            queueRow.header_status === "COMPLETED" &&
            queueRow.ledger_status === "COMPLETED" &&
            queueRow.inventory_status === "COMPLETED" &&
            queueRow.stock_status === "COMPLETED"

        ))

) {

    console.log(

        "Already Fully Processed :",

        voucherGuid

    );

    continue;

}

let voucherData = null;

if (queueRow.mark_for_insert) {

    // TODO:
    // Load fresh voucher data from Tally.
    // This will be implemented after the
    // Tally fetch layer is completed.

    voucherData =
        await loadVoucherDataFromTally({
            company_code,
            tally_owner,
            voucher_guid: voucherGuid
        });

}

if (
    queueRow.mark_for_delete &&
    queueRow.delete_status !== "COMPLETED"
) {

    await deleteVoucherLedgers({

        company_code,

        tally_owner,

        voucherGuids: [

            voucherGuid

        ]

    });

    await deleteVoucherInventory({

        company_code,

        tally_owner,

        voucherGuids: [

            voucherGuid

        ]

    });

    await deleteStockVouchers({

        company_code,

        tally_owner,

        voucherGuids: [

            voucherGuid

        ]

    });

    console.log(

        "Voucher Deleted :",

        voucherGuid

    );

    await updateQueueStage({

    queueIds: queueIds,

    stage: "delete_status",

    status: "COMPLETED"

});

queueRow.delete_status = "COMPLETED";

}

if (queueRow.mark_for_insert) {

    if (queueRow.header_status !== "COMPLETED") {

        await saveVoucher({

            voucher: voucherData.voucher,

            sync_batch_id,

            company_code,

            tally_owner

        });


        console.log(

        "Voucher Header Saved :",

        voucherGuid

    );

        await updateQueueStage({

        queueIds: queueIds,

        stage: "header_status",

        status: "COMPLETED"

    });

    queueRow.header_status = "COMPLETED";

    }
    if (queueRow.ledger_status !== "COMPLETED") {

        await saveVoucherLedgers({

    ledgerRows: voucherData.ledgers,
    sync_batch_id

});

       console.log(

        "Voucher Ledgers Saved :",

        voucherGuid

    );

        await updateQueueStage({

        queueIds: queueIds,

        stage: "ledger_status",

        status: "COMPLETED"

    });

    queueRow.ledger_status = "COMPLETED";

    }

       if (queueRow.inventory_status !== "COMPLETED") {

        await saveVoucherInventory({

    inventoryRows: voucherData.inventory,
    sync_batch_id

});

    console.log(

        "Voucher Inventory Saved :",

        voucherGuid

    );

      await updateQueueStage({

        queueIds: queueIds,

        stage: "inventory_status",

        status: "COMPLETED"

    });
    queueRow.inventory_status = "COMPLETED";

    }

       if (queueRow.stock_status !== "COMPLETED") {

        await saveStockVouchers({

    stockVoucherRows: voucherData.stockVouchers,
    sync_batch_id,
    STOCK_DEBUG_FILE: "./logs/stock-movement-debug.jsonl"

});

    console.log(

        "Stock Vouchers Saved :",

        voucherGuid

    );

    await updateQueueStage({

        queueIds: queueIds,

        stage: "stock_status",

        status: "COMPLETED"

    });

    queueRow.stock_status = "COMPLETED";

    }

}

await updateQueueStage({

    queueIds,

    stage: "execution_status",

    status: "COMPLETED",

    extra: {

        completed_at: new Date().toISOString(),

        error_message: null

    }

});

console.log(

    "Queue Completed :",

    voucherGuid

);
}
catch (err) {

    await updateQueueStage({

    queueIds,

    stage: "execution_status",

    status: "FAILED",

    extra: {

        retry_count: (queueRow.retry_count || 0) + 1,

        error_message: err.message

    }

});

throw err;

    }

}
// TODO:
// 2. Group by voucher
// 3. Execute INSERT / UPDATE / FORCE_UPDATE
// 4. Mark completed
    // 2. Group by voucher
    // 3. Execute INSERT / UPDATE / FORCE_UPDATE
    // 4. Mark completed

}

module.exports = {

    executeQueue

};