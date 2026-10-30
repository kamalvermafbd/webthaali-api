require("dotenv").config();

const {
    createClient
} = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

function isExecutionClaimToken(value) {
    return (
        typeof value === "string" &&
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    );
}


async function lockJob(job, workerConfig) {

    const batchDbId = job?.data?.batch_db_id;

    if (!batchDbId || !workerConfig?.worker_name) {
        throw new Error("Invalid batch ID or worker configuration");
    }

      const retryClaimToken = job?.data?.retry_claim_token ?? null;

    const { data: claimResult, error: claimError } =
        await supabase.rpc("claim_sync_batch_fifo_v2", {
            p_batch_db_id: batchDbId,
            p_worker_name: workerConfig.worker_name,
            p_retry_claim_token: retryClaimToken
        });

    if (claimError) {
        throw new Error(
            `FIFO batch claim failed: ${claimError.message}`
        );
    }

    const result = Array.isArray(claimResult)
        ? claimResult[0]
        : claimResult;

    if (!result || typeof result.claimed !== "boolean") {
        throw new Error("Invalid FIFO batch claim result");
    }

    if (!result.claimed) {
        const reason = String(
            result.reason || "UNKNOWN"
        ).trim().toUpperCase();

        job.claimRejectionReason = reason;

        console.log("FIFO BATCH CLAIM DECLINED:", {
            batch_db_id: batchDbId,
            reason
        });

        return null;
    }

    const executionClaimToken = result.execution_claim_token;

    if (!isExecutionClaimToken(executionClaimToken)) {
        throw new Error(
            "Successful FIFO claim returned no valid execution token"
        );
    }

    const { data: lockedBatch, error: readError } =
        await supabase
            .from("sync_batches")
            .select("*")
            .eq("id", batchDbId)
            .eq("batch_status", "PROCESSING")
            .eq("worker_status", "RUNNING")
            .eq("worker_id", workerConfig.worker_name)
            .eq("execution_claim_token", executionClaimToken)
            .single();

    if (readError || !lockedBatch) {
        throw new Error(
            `Claimed batch read failed: ${
                readError?.message || "Batch not found"
            }`
        );
    }

    return lockedBatch;
}
async function startHeartbeat(
    jobId,
    workerConfig,
    executionClaimToken
) {
    if (!isExecutionClaimToken(executionClaimToken)) {
        throw new Error(
            "Heartbeat requires a valid execution claim token"
        );
    }

    console.log(
        "HEARTBEAT STARTED:",
        jobId
    );

    const heartbeat =
        setInterval(
            async () => {

                const now =
                    new Date();

            const { data: heartbeatRow, error } =
    await supabase
        .from("sync_batches")
        .update({
            heartbeat_at: now,
            last_activity_at: now
        })
        .eq("id", jobId)
        .eq("batch_status", "PROCESSING")
        .eq("worker_status", "RUNNING")
        .eq("worker_id", workerConfig.worker_name)
        .eq("execution_claim_token", executionClaimToken)
        .select("id")
        .maybeSingle();

if (error) {
    console.error("Heartbeat update error:", error);
} else if (!heartbeatRow) {
    console.error(
        "Heartbeat ownership lost or batch no longer running:",
        {
            batch_db_id: jobId,
            worker_name: workerConfig.worker_name
        }
    );
}

                const { error: workerHeartbeatError } =
                    await supabase
                        .from("workers")
                        .update({
                            runtime_heartbeat_at:
                                now
                        })
                        .eq(
                            "runtime_id",
                            workerConfig.runtime_id
                        )
                        .eq(
                            "is_active",
                            true
                        );

                if (workerHeartbeatError) {

                    console.error(
                        "Worker runtime heartbeat update error:",
                        workerHeartbeatError
                    );
                }

            },
            30 * 1000
        );

    return heartbeat;
}

async function recoverStaleBatches(workerConfig) {

    const heartbeatTimeout =
        workerConfig.heartbeat_timeout_seconds || 120;

    const staleBefore =
        new Date(
            Date.now() -
            heartbeatTimeout * 1000
        ).toISOString();

    /*
     * ---------------------------------------------------------
     * 1. STALE RUNNING BATCH
     *
     * PROCESSING + RUNNING + stale heartbeat
     * = worker execution is considered failed.
     *
     * IMPORTANT:
     * Do NOT put it back into PENDING.
     * We are NOT using Resume Engine.
     * ---------------------------------------------------------
     */
/* 271026
    const {
        data: staleRunning,
        error: staleRunningError
    } = await supabase
        .from("sync_batches")
        .update({

            batch_status:
                "FAILED",

            worker_status:
                "FAILED",

            worker_id:
                null,

            locked_at:
                null,

            started_at:
                null,

            heartbeat_at:
                null,

            completed_at:
                new Date().toISOString(),

            current_stage:
                "FAILED",

            error_message:
                "Batch heartbeat timeout"

        })
        .eq(
            "worker_id",
            workerConfig.worker_name
        )
        .eq(
            "batch_status",
            "PROCESSING"
        )
        .eq(
            "worker_status",
            "RUNNING"
        )
      .lt(
    "heartbeat_at",
    staleBefore
)
.or(
    "error_message.is.null,and(error_message.not.match.^CONNECTOR_OUTCOME_UNKNOWN:,error_message.not.match.^HTTP_OUTCOME_UNKNOWN:)"
)
.select(
    "id, batch_id"
);
    if (staleRunningError) {

        throw new Error(
            "Stale running batch failure update failed: " +
            staleRunningError.message
        );

    }
*/

// Heartbeat timeout alone cannot prove remote HTTP execution stopped.
// Keep RUNNING batch ownership until fenced recovery is available.
const staleRunning = [];

    /*
     * ---------------------------------------------------------
     * 2. WAITING CONNECTOR TIMEOUT
     *
     * Connector was offline and batch was moved to:
     *
     * PENDING + WAITING_CONNECTOR
     *
     * If it remains there beyond the timeout,
     * mark it FAILED.
     *
     * This query intentionally does NOT depend on worker_id
     * because WAITING_CONNECTOR has worker_id = NULL.
     * ---------------------------------------------------------
     */
/* 211026
    const connectorWaitTimeoutSeconds = 90;

    const connectorStaleBefore =
        new Date(
            Date.now() -
            connectorWaitTimeoutSeconds * 1000
        ).toISOString();

    const {
        data: staleConnectorBatches,
        error: connectorError
    } = await supabase
        .from("sync_batches")
        .update({

            batch_status:
                "FAILED",

            worker_status:
                "FAILED",

            worker_id:
                null,

            locked_at:
                null,

            heartbeat_at:
                null,

            completed_at:
                new Date().toISOString(),

            current_stage:
                "FAILED",

            error_message:
                "Connector did not reconnect within timeout"

        })
        .eq(
            "batch_status",
            "PENDING"
        )
        .eq(
            "worker_status",
            "PENDING"
        )
        .eq(
            "current_stage",
            "WAITING_CONNECTOR"
        )
       .lt(
            "waiting_connector_since",
            connectorStaleBefore
        )
        .select(
            "id, batch_id"
        );

    if (connectorError) {

        throw new Error(
            "Waiting connector batch failure update failed: " +
            connectorError.message
        );

    }
*/
    
      /*
       * ---------------------------------------------------------
       * 2B. STALE PENDING INIT RESERVATION
       *
       * Batch reserved but worker never started it.
       * Release the stale reservation for normal dispatch.
       * ---------------------------------------------------------
       */

      const initReservationStaleBefore =
          new Date(Date.now() - 180 * 1000).toISOString();

      const {
          data: staleInitBatches,
          error: staleInitError
      } = await supabase
          .from("sync_batches")
          .update({
              worker_id: null,
              locked_at: null,
              heartbeat_at: null
          })
          .eq("worker_id", workerConfig.worker_name)
          .eq("batch_status", "PENDING")
          .eq("worker_status", "PENDING")
          .eq("current_stage", "INIT")
         .is("started_at", null)
            .is("heartbeat_at", null)
            .is("execution_claim_token", null)
            .lt("locked_at", initReservationStaleBefore)
            .select("id, batch_id");
      if (staleInitError) {
          throw new Error(
              "Stale INIT reservation recovery failed: " +
              staleInitError.message
          );
      }

      if (staleInitBatches?.length) {
          console.log(
              "STALE INIT RESERVATIONS RELEASED:",
              JSON.stringify(staleInitBatches)
          );
      }



    /*
     * ---------------------------------------------------------
     * 3. CLEAN TERMINAL BATCH STATE
     *
     * FAILED batch must never occupy a worker slot.
     * ---------------------------------------------------------
     */

    const {
        data: terminalBatches,
        error: terminalError
    } = await supabase
        .from("sync_batches")
        .update({

            worker_status:
                "FAILED",

            worker_id:
                null,

            locked_at:
                null,

            heartbeat_at:
                null

        })
        .eq(
            "worker_id",
            workerConfig.worker_name
        )
        .eq(
            "batch_status",
            "FAILED"
        )
       .eq(
            "worker_status",
            "RUNNING"
        )
        .is("execution_claim_token", null)
        .select(
            "id, batch_id"
        );

    if (terminalError) {

        throw new Error(
            "Terminal batch cleanup failed: " +
            terminalError.message
        );

    }


    /*
     * ---------------------------------------------------------
     * LOG
     * ---------------------------------------------------------
     */

      if (
        staleRunning?.length ||
        
        staleInitBatches?.length ||
        terminalBatches?.length
    ) {

        console.log(
            "WORKER STALE STATE HANDLED:",
            JSON.stringify(
                {
                    staleRunning,
                    
                    staleInitBatches,
                    terminalBatches
                },
                null,
                2
            )
        );

    }


      return {
        staleRunning: staleRunning || [],

        staleInitBatches: staleInitBatches || [],
        terminalBatches: terminalBatches || []
    };

}


module.exports = {
    lockJob,
    startHeartbeat,
    recoverStaleBatches
};