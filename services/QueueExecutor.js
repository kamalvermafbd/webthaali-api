const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

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

function getVoucherAction(rows) {

    if (!rows || rows.length === 0) {

        return null;

    }

    return rows[0].action;

}

function shouldExecuteAction(action) {

    return [

        "INSERT",

        "UPDATE",

        "FORCE_UPDATE"

    ].includes(action);

}

async function loadVoucher({

    company_code,

    tally_owner,

    voucher_guid

}) {

    const { data, error } = await supabase

        .from("tally_vouchers")

        .select("*")

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner)

        .eq("guid", voucher_guid)

        .maybeSingle();

    if (error) {

        throw new Error(

            "Failed to load voucher : " +

            error.message

        );

    }

    return data;

}

async function loadVoucherData({

    company_code,

    tally_owner,

    voucher_guid

}) {

    const [

        voucher,

        ledgers,

        inventory,

        stockVouchers

    ] = await Promise.all([

        loadVoucher({

            company_code,

            tally_owner,

            voucher_guid

        }),

        supabase

            .from("tally_voucher_ledgers")

            .select("*")

            .eq("company_code", company_code)

            .eq("tally_owner", tally_owner)

            .eq("voucher_guid", voucher_guid),

        supabase

            .from("tally_voucher_inventory")

            .select("*")

            .eq("company_code", company_code)

            .eq("tally_owner", tally_owner)

            .eq("voucher_guid", voucher_guid),

        supabase

            .from("tally_stock_vouchers")

            .select("*")

            .eq("company_code", company_code)

            .eq("tally_owner", tally_owner)

            .eq("voucher_guid", voucher_guid)

    ]);
if (ledgers.error) {

    throw new Error(

        "Failed to load voucher ledgers : " +

        ledgers.error.message

    );

}

if (inventory.error) {

    throw new Error(

        "Failed to load voucher inventory : " +

        inventory.error.message

    );

}

if (stockVouchers.error) {

    throw new Error(

        "Failed to load stock vouchers : " +

        stockVouchers.error.message

    );

}

return {

    voucher,

    ledgers: ledgers.data || [],

    inventory: inventory.data || [],

    stockVouchers: stockVouchers.data || []

};

}


async function updateQueueStage({

    queueId,

    stage,

    status

}) {

    const { error } = await supabase

        .from("sync_exe_queue")

        .update({

            [stage]: status

        })

        .eq("id", queueId);

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

    const { error: startError } = await supabase

        .from("sync_exe_queue")

        .update({

    execution_status: "STARTED",

started_at: new Date().toISOString(),

completed_at: null,

retry_count: 0,

error_message: null

})

        .eq("id", queueRow.id);

    if (startError) {

        throw new Error(

            "Failed to mark queue STARTED : " +

            startError.message

        );

    }

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

    voucherData =
        await loadVoucherData({

            company_code,

            tally_owner,

            voucher_guid: voucherGuid

        });

    if (!voucherData.voucher) {

        throw new Error(

            `Voucher not found : ${voucherGuid}`

        );

    }

    console.log({

        voucher: voucherData.voucher.guid,

        ledgers: voucherData.ledgers.length,

        inventory: voucherData.inventory.length,

        stockVouchers: voucherData.stockVouchers.length

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

    queueId: queueRow.id,

    stage: "delete_status",

    status: "COMPLETED"

});

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

        queueId: queueRow.id,

        stage: "header_status",

        status: "COMPLETED"

    });

    }
    if (queueRow.ledger_status !== "COMPLETED") {

        await saveVoucherLedgers({

            ledgerRows: voucherData.ledgers

        });

       console.log(

        "Voucher Ledgers Saved :",

        voucherGuid

    );

        await updateQueueStage({

        queueId: queueRow.id,

        stage: "ledger_status",

        status: "COMPLETED"

    });

    }

       if (queueRow.inventory_status !== "COMPLETED") {

        await saveVoucherInventory({

            inventoryRows: voucherData.inventory

        });

    console.log(

        "Voucher Inventory Saved :",

        voucherGuid

    );

      await updateQueueStage({

        queueId: queueRow.id,

        stage: "inventory_status",

        status: "COMPLETED"

    });

    }

       if (queueRow.stock_status !== "COMPLETED") {

        await saveStockVouchers({

            stockVoucherRows: voucherData.stockVouchers

        });

    console.log(

        "Stock Vouchers Saved :",

        voucherGuid

    );

                  await updateQueueStage({

        queueId: queueRow.id,

        stage: "stock_status",

        status: "COMPLETED"

    });

    }

}

const { error: completedError } = await supabase

    .from("sync_exe_queue")

   .update({

    execution_status: "COMPLETED",

    completed_at: new Date().toISOString(),

    error_message: null

})

    .eq("id", queueRow.id);

if (completedError) {

    throw new Error(

        "Failed to mark queue COMPLETED : " +

        completedError.message

    );

}

console.log(

    "Queue Completed :",

    voucherGuid

);

}
catch (err) {

        const { error: failedError } = await supabase

            .from("sync_exe_queue")

            .update({

                execution_status: "FAILED",

                retry_count: (queueRow.retry_count || 0) + 1,

                error_message: err.message

            })

            .eq("id", queueRow.id);

        if (failedError) {

            throw new Error(

                "Failed to mark queue FAILED : " +

                failedError.message

            );

        }

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