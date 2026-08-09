const BatchManager =
    require("./BatchManager");

const SnapshotManager =
require("./SnapshotManager");

const {
    buildVoucherOperations
} = require("./VoucherOperationBuilder");

const {
    MODULE_TYPE,

    ENTITY_TYPE,

    TABLES
} = require("./constants");
class VoucherExecutionService {

    async execute({

        company_code,

        tally_owner,

        sync_batch_id,

        rowsToSave,

        allVoucherGuids,

        voucherGuids,

        ledgerRows,

        inventoryRows,

        stockVoucherRows,

        billAllocationRows,

        costCentreRows

    }) {
        console.log(">>> VoucherExecutionService START");

        const operations =
            buildVoucherOperations({

                company_code,

                tally_owner,

                sync_batch_id,

                voucherGuids,

                voucherRows: rowsToSave,

                ledgerRows,

                inventoryRows,

                stockVoucherRows,

                billAllocationRows,

                costCentreRows

            });

        const execution =
            await BatchManager.execute({

                operations

            });
/*
    console.log(">>> BatchManager FINISHED");

    const snapshotRows =

    SnapshotManager.buildSnapshotRows({

        sync_batch_id,

        company_code,

        tally_owner,

        module: MODULE_TYPE.VOUCHER,

        entity_type: ENTITY_TYPE.VOUCHER,

        rows: rowsToSave

    });

    console.log("SNAPSHOT ROWS:", snapshotRows.length);
    console.dir(snapshotRows[0], { depth: null });

    await SnapshotManager.saveSnapshotRows({

        rows: snapshotRows

    });

    return execution;
*/

    console.log(">>> BatchManager FINISHED");

   

    return await BatchManager.postExecution({

        batch_id:
            sync_batch_id,

        module:
            MODULE_TYPE.VOUCHER,

        entity:
            ENTITY_TYPE.VOUCHER,

       table:
            TABLES.VOUCHERS,

        company_code,

        tally_owner,

        sync_batch_id,

        
        validation: null,

        execution

    });


    }

}

module.exports =
    new VoucherExecutionService();