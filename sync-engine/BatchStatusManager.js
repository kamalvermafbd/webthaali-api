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

    TABLES,

     SYNC_STAGE,

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
    execution_claim_token,
    fields = {}
}) {

        if (!batch_id) {

            throw new Error(

                "batch_id is required"

            );

        }

        if (
            typeof execution_claim_token !== "string" ||
            !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                execution_claim_token
            )
        ) {
            const error = new Error(
                "updateFields requires a valid execution claim token"
            );
            error.code = "EXECUTION_CLAIM_LOST";
            throw error;
        }
        if (

            Object.keys(fields).length === 0

        ) {

            return true;

        }

       const { data, error } = await supabase
    .from(TABLES.SYNC_BATCHES)
    .update({
        ...fields,
        last_activity_at: new Date().toISOString()
    })
    .eq("batch_id", batch_id)
.eq("execution_claim_token", execution_claim_token)
.neq("batch_status", "COMPLETED")
.neq("batch_status", "CLOSED")
.or(
    "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
)
    .select("batch_id")
    .maybeSingle();

        if (error) {

            throw new Error(

                "Failed to update sync_batches : " +

                error.message

            );

        }

    if (!data) {
    const error = new Error(
        `Batch update rejected: ownership or state mismatch, batch_id=${batch_id}`
    );
    error.code = "EXECUTION_CLAIM_LOST";
    throw error;
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

        // ----------------------------------
// Check Previous Completed Sync
// ----------------------------------

async hasCompletedSync({

    company_code,
    tally_owner,
    exclude_batch_id = null

}) {

    let query = supabase

        .from(TABLES.SYNC_BATCHES)

        .select("batch_id")

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
            BATCH_STATUS.COMPLETED
        )

        .order(
            "completed_at",
            {
                ascending: false
            }
        )

        .limit(1);

    if (exclude_batch_id) {

        query = query.neq(
            "batch_id",
            exclude_batch_id
        );

    }

    const {
        data,
        error
    } = await query;

    if (error) {

        throw new Error(

            "Failed to check previous completed sync : " +

            error.message

        );

    }

    return Boolean(
        data?.length
    );

}

        async claimOrCreateHttpBatch({

    company_code,

    tally_owner,

    request_id,

    sync_mode,

    sync_period

}) {

          if (
            !company_code ||
            !tally_owner ||
            !request_id ||
            !sync_mode
        ) {

            throw new Error(
                "company_code, tally_owner, request_id, and sync_mode are required"
            );

        }

        if (
            sync_mode === "PERIODIC" &&
            !sync_period
        ) {

            throw new Error(
                "sync_period is required for PERIODIC sync"
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

                    p_request_id: request_id,

                    p_sync_mode: sync_mode,

                    p_sync_period: sync_period

                }

            );

            if (error) {

                throw new Error(
                    "Failed to claim HTTP sync batch : " +
                    error.message
                );

            }

            const result = Array.isArray(data) ? data[0] : data;

           if (
    !result ||
    typeof result.claimed !== "boolean" ||
    typeof result.reason !== "string" ||
    (
        !result.batch_id &&
        !(
            result.claimed === false &&
            result.reason === "RECONCILE_IN_PROGRESS" &&
            result.batch_id === null &&
            result.worker_id === null
        )
    ) ||
    (
        result.worker_id !== null &&
        typeof result.worker_id !== "string"
    )
) {
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
           .or(
    "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
)
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
    execution_claim_token,
    status
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
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
    execution_claim_token,
    module,
    entity = null,
    action = null,
    operation = null
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
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
    execution_claim_token,
    progress
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
        fields: {
            sync_progress: progress
        }
    });
}

        // ----------------------------------
    // Mark Started
    // ----------------------------------

    async markStarted({
    batch_id,
    execution_claim_token
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
      fields: {
            batch_status: BATCH_STATUS.PROCESSING,
            started_at: new Date().toISOString()
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
    failed = 0,
    execution_claim_token
}) {
    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        throw new Error(
            "markCompleted requires a valid execution claim token"
        );
    }
        console.log("BATCH MARK COMPLETED:", {
    batch_id,
    module,
    processed,
    failed
});

      const { data, error } = await supabase
    .from(TABLES.SYNC_BATCHES)
   .update({
    batch_status: BATCH_STATUS.COMPLETED,
    worker_status: "COMPLETED",
    processed_vouchers: processed,
    failed_vouchers: failed,
    completed_at: new Date().toISOString(),
    current_module: module,
    last_successful_module: module,
    error_message: null,
    last_activity_at: new Date().toISOString()
})
    .eq("batch_id", batch_id)
.eq("batch_status", "PROCESSING")
.eq("worker_status", "RUNNING")
.eq("execution_claim_token", execution_claim_token)
.or(
    "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
)
    .select("batch_id")
    .maybeSingle();

if (error) {
    throw new Error(
        "Failed to mark batch completed: " + error.message
    );
}

if (!data) {
    console.warn(
        "BATCH COMPLETION SKIPPED — STATE CHANGED:",
        batch_id
    );
}

return Boolean(data);

    }

  // ----------------------------------
// Mark Failed
// ----------------------------------

async markFailed({
    batch_id,
    error,
    execution_claim_token
}) {
    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        throw new Error(
            "markFailed requires a valid execution claim token"
        );
    }

    console.log("🔥 BATCH MARK FAILED:", {
        batch_id,
        error:
            typeof error === "string"
                ? error
                : error?.message || "Unknown Error"
    });


const originalError =
    typeof error === "string"
        ? error
        : error?.message || "Unknown Error";

const isUncertainOutcome =
    error?.outcomeUnknown === true ||
    error?.code === "GET_MASTERS_TRANSFER_OUTCOME_UNKNOWN" ||
    error?.durableClaimUnconfirmed === true ||
    error?.durableVerificationUnconfirmed === true ||
    (
        error?.requestDispatched === true &&
        error?.connectorResponseReceived !== true
    );

if (isUncertainOutcome) {
    console.error(
        "BATCH FAILURE BLOCKED — CONNECTOR OUTCOME UNCERTAIN:",
        batch_id,
        originalError
    );

    return false;
}

const finalErrorMessage = originalError;

const { data, error: updateError } = await supabase
    .from(TABLES.SYNC_BATCHES)
   .update({
    batch_status: BATCH_STATUS.FAILED,
    worker_status: "FAILED",
    current_stage: "FAILED",
    error_message: finalErrorMessage
})
       .eq("batch_id", batch_id)
.eq("execution_claim_token", execution_claim_token)
.neq("batch_status", "COMPLETED")
.neq("batch_status", "CLOSED")
.neq("batch_status", "FAILED")
.or(
            "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
        )
    .select("batch_id")
    .maybeSingle();

if (updateError) {
    throw new Error("Failed to mark batch failed: " + updateError.message);
}

if (!data) {
    console.log("BATCH FAILURE UPDATE SKIPPED — ALREADY COMPLETED/CLOSED:", batch_id);
}

return Boolean(data);


}

// ----------------------------------
// Mark HTTP Outcome Unknown
// ----------------------------------

async markHttpOutcomeUnknown({
    batch_id,
    error,
    execution_claim_token
}) {
    if (!batch_id) {
        throw new Error("batch_id is required");
    }

    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        throw new Error(
            "markHttpOutcomeUnknown requires a valid execution claim token"
        );
    }

    const errorMessage =
        typeof error === "string"
            ? error
            : error?.message || "HTTP connection lost";

    const { data, error: updateError } = await supabase
        .from(TABLES.SYNC_BATCHES)
       .update({
    error_message:
        `HTTP_OUTCOME_UNKNOWN: ${errorMessage}`,
    last_activity_at: new Date().toISOString()
})
       .eq("batch_id", batch_id)
.eq("batch_status", "PROCESSING")
.eq("worker_status", "RUNNING")
.eq("execution_claim_token", execution_claim_token)
.or(
    "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
)
        .select("batch_id")
        .maybeSingle();

    if (updateError) {
        throw new Error(
            "Failed to mark HTTP outcome unknown: " +
            updateError.message
        );
    }

    if (!data) {
    throw new Error(
        `HTTP_OUTCOME_UNKNOWN_MARK_NOT_PERSISTED: batch_id=${batch_id}`
    );
}

return true;
}
    // ----------------------------------
// Mark Waiting For Connector
// ----------------------------------

async markWaitingConnector({
    batch_id,
    execution_claim_token
}) {
    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        throw new Error(
            "markWaitingConnector requires a valid execution claim token"
        );
    }

    const { data, error } = await supabase.rpc(
        "mark_sync_batch_waiting_connector_if_not_started",
        {
            p_batch_id: batch_id,
            p_execution_claim_token: execution_claim_token
        }
    );

    if (error) {
        throw new Error(
            "Failed to mark batch waiting: " + error.message
        );
    }

    if (data !== true) {
        console.warn(
            "[BATCH STATUS] WAITING_CONNECTOR TRANSITION REJECTED",
            { batch_id }
        );

        return false;
    }

    console.log(
        "[BATCH STATUS] WAITING_CONNECTOR TRANSITION VERIFIED",
        { batch_id }
    );

    return true;
}

// ----------------------------------
// Mark Connector Offline Timeout Failed
// ----------------------------------

async markConnectorOfflineTimeoutFailed({
    batch_id,
    execution_claim_token
}) {
    if (
        typeof execution_claim_token !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            execution_claim_token
        )
    ) {
        throw new Error(
            "markConnectorOfflineTimeoutFailed requires a valid execution claim token"
        );
    }

    let data;
let error;

try {
    const response = await supabase.rpc(
        "mark_sync_batch_waiting_connector_if_not_started",
        {
            p_batch_id: batch_id,
            p_execution_claim_token: execution_claim_token,
            p_fail_on_timeout: true
        }
    );

    data = response.data;
    error = response.error;
} catch (rpcError) {
    error = rpcError;
}

   if (error) {
    console.error(
        "CONNECTOR OFFLINE TIMEOUT RPC ERROR — VERIFYING DATABASE:",
        batch_id,
        error
    );

    let verifiedBatch;

    try {
        verifiedBatch = await this.loadBatch({
            batch_id
        });
    } catch (verifyError) {
        throw new Error(
            "CONNECTOR_OFFLINE_TIMEOUT_OUTCOME_UNKNOWN: " +
            "RPC failed and database verification failed: " +
            verifyError.message
        );
    }

    const timeoutConfirmed =
        verifiedBatch?.batch_id === batch_id &&
        typeof verifiedBatch.execution_claim_token === "string" &&
        verifiedBatch.execution_claim_token.toLowerCase() ===
            execution_claim_token.toLowerCase() &&
        verifiedBatch.batch_status === "FAILED" &&
        verifiedBatch.worker_status === "FAILED" &&
        verifiedBatch.current_stage === "FAILED" &&
        verifiedBatch.error_message === "CONNECTOR_OFFLINE_TIMEOUT" &&
        verifiedBatch.connector_retry_ready === false;

    if (timeoutConfirmed) {
        console.log(
            "CONNECTOR OFFLINE TIMEOUT — DATABASE FAILURE VERIFIED:",
            batch_id
        );

        return true;
    }

    throw new Error(
        "CONNECTOR_OFFLINE_TIMEOUT_OUTCOME_UNKNOWN: " +
        "RPC failed and expected terminal state was not verified"
    );
}

    if (data !== true) {
        console.warn(
            "[BATCH STATUS] CONNECTOR OFFLINE TIMEOUT TRANSITION REJECTED",
            { batch_id }
        );

        return false;
    }

    console.log(
        "[BATCH STATUS] CONNECTOR OFFLINE TIMEOUT FAILED VERIFIED",
        { batch_id }
    );

    return true;
}
        // ----------------------------------
    // Update Connector State
    // ----------------------------------

    // TODO
// Merge existing payload before update.
// Do not overwrite payload.connector.

  async updateConnectorState({
    batch_id,
    execution_claim_token,
    connectorState
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
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
    execution_claim_token,
    retry_count
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
        fields: {
            retry_count
        }
    });
}
// ----------------------------------
// Reset Connector Retry Readiness
// ----------------------------------

async resetConnectorRetryReadiness({ batch_id }) {
    if (!batch_id) {
        throw new Error("batch_id is required");
    }

    const { data, error } = await supabase
        .from(TABLES.SYNC_BATCHES)
        .update({
            connector_retry_ready: false
        })
        .eq("batch_id", batch_id)
        .eq("batch_status", "FAILED")
        .eq("worker_status", "FAILED")
        .like("error_message", "CONNECTOR_LOSS:%")
        .eq("connector_retry_ready", true)
        .select("batch_id")
        .maybeSingle();

    if (error) {
        throw new Error(
            "Failed to reset connector retry readiness: " +
            error.message
        );
    }

    return Boolean(data);
}
    // ----------------------------------
    // Mark Reconciliation Completed
    // ----------------------------------

  async markReconciliationCompleted({
    batch_id,
    execution_claim_token
}) {

    return this.updateFields({
        batch_id,
        execution_claim_token,
        fields: {
            reconciliation_completed: true,
            reconciliation_completed_at:
                new Date().toISOString()
        }
    });
}
// ----------------------------------
// Verify HTTP Execution Termination
// ----------------------------------

async verifyHttpExecutionTermination({
    batch_id,
    execution_claim_token,
    invocation_id
}) {
    const uuidPattern =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    if (
        typeof batch_id !== "string" ||
        !batch_id.trim() ||
        typeof execution_claim_token !== "string" ||
        !uuidPattern.test(execution_claim_token) ||
        typeof invocation_id !== "string" ||
        !uuidPattern.test(invocation_id)
    ) {
        return {
            verified: false,
            reason: "INVALID_VERIFICATION_ARGUMENTS"
        };
    }

    try {
        const { data, error } = await supabase.rpc(
            "assess_sync_http_settlement",
            {
                p_batch_id: batch_id,
                p_execution_claim_token: execution_claim_token
            }
        );

        if (error) {
            console.error(
                "HTTP EXECUTION VERIFICATION RPC FAILED:",
                batch_id,
                error
            );

            return {
                verified: false,
                reason: "VERIFICATION_RPC_FAILED"
            };
        }

        const result = Array.isArray(data)
            ? data[0]
            : data;

        const verified =
            result?.decision === "TERMINATED" &&
            result?.execution_state === "TERMINATED" &&
            typeof result?.invocation_id === "string" &&
            result.invocation_id.toLowerCase() ===
                invocation_id.toLowerCase() &&
            Boolean(result?.terminated_at) &&
            result?.terminal_outcome ===
                "HANDLER_COMPLETED_SUCCESS";

        return {
            verified,
            reason: verified
                ? "HTTP_EXECUTION_TERMINATED"
                : "HTTP_EXECUTION_TERMINATION_UNVERIFIED"
        };

    } catch (error) {
        console.error(
            "HTTP EXECUTION VERIFICATION UNCERTAIN:",
            batch_id,
            error
        );

        return {
            verified: false,
            reason: "VERIFICATION_RPC_UNCERTAIN"
        };
    }
}

}


module.exports =
    new BatchStatusManager();
