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
    // Load Batch
    // ----------------------------------

        async loadBatch({

            batch_id

        }) {

            if (!batch_id) {

                throw new Error(

                    "batch_id is required"

                );

            }

            const {

                data,

                error

            } = await supabase

                .from(TABLES.SYNC_BATCHES)

                .select("*")

                .eq(

                    "batch_id",

                    batch_id

                )

                .single();

            if (error) {

                throw new Error(

                    "Failed to load batch : " +

                    error.message

                );

            }

            return data;

        }


        // ----------------------------------
        // Load Running Batch
        // ----------------------------------

        async loadRunningBatch({

            company_code,

            tally_owner

        }) {

            const {

                data,

                error

            } = await supabase

                .from(TABLES.SYNC_BATCHES)

                .select("*")

                .eq(

                    "company_code",

                    company_code

                )

                .eq(

                    "tally_owner",

                    tally_owner

                )

                .eq(

                    "batch_status",

                    BATCH_STATUS.RUNNING

                )

                .order(

                    "created_at",

                    {

                        ascending: false

                    }

                )

                .limit(1);

            if (error) {

                throw new Error(

                    "Failed to load running batch : " +

                    error.message

                );

            }

            return data?.[0] || null;

        }

        async claimOrCreateHttpBatch({

            company_code,

            tally_owner,

            request_id

        }) {

            if (!company_code || !tally_owner || !request_id) {

                throw new Error(
                    "company_code, tally_owner, and request_id are required"
                );

            }

            const {

                data,

                error

            } = await supabase.rpc(

                "claim_or_create_http_sync_batch",

                {

                    p_company_code: company_code,

                    p_tally_owner: tally_owner,

                    p_request_id: request_id

                }

            );

            if (error) {

                throw new Error(
                    "Failed to claim HTTP sync batch : " +
                    error.message
                );

            }

            const result = Array.isArray(data) ? data[0] : data;

            if (!result || !result.batch_id ||
                typeof result.claimed !== "boolean" ||
                typeof result.reason !== "string" ||
                (result.worker_id !== null &&
                 typeof result.worker_id !== "string")) {

                throw new Error(
                    "Invalid HTTP sync batch claim result"
                );

            }

            return {

                batch_id: result.batch_id,

                claimed: result.claimed,

                reason: result.reason,

                worker_id: result.worker_id

            };

        }

        async releaseHttpBatch({

            batch_id,

            worker_id

        }) {

            if (!batch_id || !worker_id) {

                throw new Error(
                    "batch_id and worker_id are required"
                );

            }

            const {

                data,

                error

            } = await supabase

                .from(TABLES.SYNC_BATCHES)

                .update({

                    worker_status: "PENDING",

                    worker_id: null,

                    locked_at: null,

                    heartbeat_at: null

                })

                .eq("batch_id", batch_id)

                .eq("worker_id", worker_id)

                .eq("worker_status", "HTTP_RUNNING")

                .select("batch_id")

                .maybeSingle();

            if (error) {

                throw new Error(
                    "Failed to release HTTP sync batch : " +
                    error.message
                );

            }

            return {

                released: Boolean(data?.batch_id),

                batch_id: data?.batch_id || null

            };

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

                current_operation: operation,

                last_successful_module: module

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

                    typeof error === "string"

                        ? error

                        : error?.message || "Unknown Error"
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
