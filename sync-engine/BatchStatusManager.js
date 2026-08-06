// ======================================
// TODO (Resume Engine)
// ======================================
//
// 1. Merge payload.connector instead of overwrite.
// 2. Auto increment retry_count.
// 3. Add loadBatch() helper for Resume Engine.
//



const { createClient } =
    require("@supabase/supabase-js");

const {

    BATCH_STATUS,

    TABLES

} = require("./constants");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

class BatchStatusManager {

    // ----------------------------------
    // Generic Update
    // ----------------------------------

    async updateFields({

        batch_id,

        fields = {}

    }) {

        if (!batch_id) {

            throw new Error(

                "batch_id is required"

            );

        }

        if (

            Object.keys(fields).length === 0

        ) {

            return true;

        }

        const {

            error

        } = await supabase

            .from(TABLES.SYNC_BATCHES)

            .update({

                ...fields,

                last_activity_at:
                    new Date().toISOString()

            })

            .eq(

                "batch_id",

                batch_id

            );

        if (error) {

            throw new Error(

                "Failed to update sync_batches : " +

                error.message

            );

        }

        return true;

    }

        // ----------------------------------
    // Update Batch Status
    // ----------------------------------

    async updateStatus({

        batch_id,

        status

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                batch_status: status

            }

        });

    }

    // ----------------------------------
    // Update Current Module
    // ----------------------------------

    async updateModule({

        batch_id,

        module,

        entity = null,

        action = null,

        operation = null

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                current_module: module,

                current_entity: entity,

                current_action: action,

                current_operation: operation

            }

        });

    }

    // ----------------------------------
    // Update Progress
    // ----------------------------------

    async updateProgress({

        batch_id,

        progress

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                sync_progress: progress

            }

        });

    }

        // ----------------------------------
    // Mark Started
    // ----------------------------------

    async markStarted({

        batch_id

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                batch_status:

                    BATCH_STATUS.RUNNING,

                started_at:

                    new Date().toISOString()

            }

        });

    }

    // ----------------------------------
    // Mark Completed
    // ----------------------------------

    async markCompleted({

        batch_id,

        module,

        processed = 0,

        failed = 0

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                batch_status:

                    BATCH_STATUS.COMPLETED,

                processed_vouchers:

                    processed,

                failed_vouchers:

                    failed,

                completed_at:

                    new Date().toISOString(),

                    last_successful_module: module

            }

        });

    }

    // ----------------------------------
    // Mark Failed
    // ----------------------------------

    async markFailed({

        batch_id,

        error

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                batch_status:

                    BATCH_STATUS.FAILED,

                error_message:

                    error?.message ||

                    String(error)

            }

        });

    }

        // ----------------------------------
    // Update Connector State
    // ----------------------------------

    // TODO
// Merge existing payload before update.
// Do not overwrite payload.connector.

    async updateConnectorState({

        batch_id,

        connectorState

    }) {

        return this.updateFields({

            batch_id,

            fields: {

            }

        });

    }


    
    // ----------------------------------
    // Increment Retry Count
    // ----------------------------------

    // TODO
// Auto increment retry_count from database.
// Current implementation expects retry_count from caller.


    async incrementRetry({

        batch_id,

        retry_count

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                retry_count

            }

        });

    }

    // ----------------------------------
    // Mark Reconciliation Completed
    // ----------------------------------

    async markReconciliationCompleted({

        batch_id

    }) {

        return this.updateFields({

            batch_id,

            fields: {

                reconciliation_completed: true,

                reconciliation_completed_at:

                    new Date().toISOString()

            }

        });

    }


}

module.exports =
    new BatchStatusManager();