const BatchQueue =
    require("./BatchQueue");

const BatchExecutor =
    require("./BatchExecutor");

const BatchStatusManager =
    require("./BatchStatusManager");

const ValidationPipeline =
    require("./ValidationPipeline");

const SnapshotManager =
    require("./SnapshotManager");

const ReconciliationManager =
    require("./ReconciliationManager");

const RetryManager =
    require("./RetryManager");

const MasterOperationBuilder =
    require("./MasterOperationBuilder");

const {

    ENTITY_METADATA,

    MODULE_TYPE

} = require("./constants");

class BatchManager {


// ----------------------------------
// Execute Prebuilt Operations
// ----------------------------------

async execute({

    operations = []

}) {

    if (!Array.isArray(operations)) {

        throw new Error(
            "operations must be an array"
        );

    }

    BatchQueue.enqueueMany(

    operations

    );

    const queuedOperations =

        BatchQueue.getAll();

    let successCount = 0;

    let failedCount = 0;

    const failedOperations = [];

    for (const operation of queuedOperations) {

        try {

            await BatchExecutor.execute(
                operation
            );

            successCount++;

        }

        catch (error) {

            console.error(
                "BATCH EXECUTION ERROR:",
                error
            );

            failedCount++;

            failedOperations.push({

                operation,

                error: error.message

            });

        }

    }

    try {

    return {

        success:

        failedCount === 0,

        successCount,

        failedCount,

        failedOperations

    };

}

finally {

    BatchQueue.clear();

}

}



    // ----------------------------------
    // Run Batch
    // ----------------------------------

    async run({

        batch_id,
       
        entity,

        company_code,

        tally_owner,

        sync_batch_id,

        rows = [],

        options = {}

    }) {

        const metadata =

    ENTITY_METADATA[entity];

if (!metadata) {

    throw new Error(

        `Unsupported entity : ${entity}`

    );

}

const {

    module,

    table,

    builder,

    inputKey

} = metadata;

    if (!batch_id) {

            throw new Error(

                "batch_id is required"

            );

        }


        if (!module) {

            throw new Error(

                "module is required"

            );

        }

        if (!table) {

            throw new Error(

                "table is required"

            );

        }

        if (!company_code) {

            throw new Error(

                "company_code is required"

            );

        }

        if (!tally_owner) {

            throw new Error(

                "tally_owner is required"

            );

        }

        if (!sync_batch_id) {

            throw new Error(

                "sync_batch_id is required"

            );

        }

        if (!Array.isArray(rows)) {

            throw new Error(

                "rows must be an array"

            );

        }

        await BatchStatusManager.markStarted({

            batch_id

        });

        await BatchStatusManager.updateModule({

            batch_id,

            module,

            entity,

            action: "PROCESSING"

        });


        const dbRows =

            rows.map(row =>

                builder.build({

                    company_code,

                    tally_owner,

                    sync_batch_id,

                    [inputKey]: row

                })

            );

        const validation =

            await ValidationPipeline.validate({

                table,

                company_code,

                tally_owner,

                rows: dbRows

            });

        if (

            validation.newRows.length === 0 &&

            validation.changedRows.length === 0

        ) {

            await BatchStatusManager.updateModule({

            batch_id,

            module,

            entity,

            action: "COMPLETED"

        });

        await BatchStatusManager.markReconciliationCompleted({

            batch_id

        });

            await BatchStatusManager.markCompleted({

                batch_id,

                module,

                processed:

                    validation.unchangedRows.length,

                failed: 0

            });

            return {

                success: true,

                message:

                    "No changes detected",

                validation

            };

        }

            let operations = [];

        if (module === MODULE_TYPE.MASTER) {

            operations =

                MasterOperationBuilder.buildOperation({

                    entity,

                    table,

                    company_code,

                    tally_owner,

                    sync_batch_id,

                    rows: [

                        ...validation.newRows,

                        ...validation.changedRows

                    ],

                    options

                });   




        }

       else if (module === MODULE_TYPE.VOUCHER) {

    throw new Error(

        "Voucher batches must be executed via saveVoucherExecutionData()."

    );

}

        else {

            throw new Error(

                "Unsupported module : " +

                module

            );

        }


       const execution =

    await this.execute({

        operations

    });

    const retryResult =

    execution.failedCount === 0

        ? {

            success: true,

            retried: 0

        }

        : await RetryManager.retry({

            batch_id,

           operations:

                execution.failedOperations.map(

                    item => item.operation

                )

        });

        if (!retryResult.success) {

            await BatchStatusManager.markFailed({

                batch_id

            });

            return {

                success: false,

                execution,

                retryResult

            };

        }

        
        await SnapshotManager.removeMissingGuids({

            sync_batch_id,

            company_code,

            tally_owner,

            module,

            entity_type:

                entity

        });


    const reconciliation =

        await ReconciliationManager.reconcile({

            table,

            company_code,

            tally_owner,

            module,

            entity_type:

                entity,

            sync_batch_id

        });


        let reconciliationOperations = [];

        reconciliationOperations.push(

            ...MasterOperationBuilder.buildAlterUpdateOperations({

                table,

                company_code,

                tally_owner,

                sync_batch_id,

                alterChanged:

                    reconciliation.alterChanged

            })

        );

        reconciliationOperations.push(

            ...MasterOperationBuilder.buildSoftDeleteOperations({

                table,

                company_code,

                tally_owner,

                sync_batch_id,

                extraGuids:

                    reconciliation.extraGuids

            })

        );

       const reconciliationExecution =

            reconciliationOperations.length === 0

                ? {

                    success: true,

                    failedCount: 0

                }

                : await this.execute({

                    operations:

                        reconciliationOperations

                });

        if (!reconciliationExecution.success) {

            await BatchStatusManager.markFailed({

                batch_id

            });

           return {

                success: false,

                validation,

                execution,

                retryResult,

                reconciliation,

                reconciliationExecution

            };

        }

        await BatchStatusManager.markReconciliationCompleted({

            batch_id

        });

        await BatchStatusManager.updateModule({

            batch_id,

            module,

            entity,

            action: "COMPLETED"

        });

        await BatchStatusManager.markCompleted({

            batch_id,

              module,

            processed:

                execution.successCount || 0,

            failed:

                execution.failedCount || 0

        });


        return {

            success: true,

            validation,

            execution,

            retryResult,

            reconciliation

        };



    }

}

module.exports =
    new BatchManager();
