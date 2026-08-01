// services/sync/SyncChunkProcessor.js

const {
    saveVoucherChunk
} = require("./chunks/saveVoucherChunk");


async function processSyncChunk({

    chunkProcessor,

    rows,

    context = {}

}) {


    if (
        !chunkProcessor ||
        !rows ||
        !Array.isArray(rows)
    ) {

        throw new Error(
            "Invalid sync chunk payload"
        );

    }



    switch (chunkProcessor) {


        // =========================
        // TRANSACTIONS
        // =========================

  case "VOUCHER":

    await saveVoucherChunk(
        rows,
        context
    );

    break;



        case "VOUCHER_LEDGER":

            // Future:
            // saveVoucherLedgerChunk(rows, context)

            break;



        case "VOUCHER_INVENTORY":

            // Future:
            // saveVoucherInventoryChunk(rows, context)

            break;




        // =========================
        // MASTERS
        // =========================


        case "GROUP":

            // Future:
            // saveGroupChunk(rows, context)

            break;



        case "LEDGER":

            // Future:
            // saveLedgerChunk(rows, context)

            break;



        case "UNIT":

            // Future:
            // saveUnitChunk(rows, context)

            break;



        case "GODOWN":

            // Future:
            // saveGodownChunk(rows, context)

            break;



        case "COST_CENTRE":

            // Future:
            // saveCostCentreChunk(rows, context)

            break;



        case "STOCK_GROUP":

            // Future:
            // saveStockGroupChunk(rows, context)

            break;



        case "STOCK":

            // Future:
            // saveStockChunk(rows, context)

            break;



        default:

            throw new Error(
                `Unknown chunk processor: ${chunkProcessor}`
            );

    }



    return {

        success:true,

        processor:chunkProcessor,

        processed:rows.length

    };


}



module.exports = {

    processSyncChunk

};