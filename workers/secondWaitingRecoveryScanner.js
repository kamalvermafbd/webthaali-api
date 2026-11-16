
"use strict";

// Conservative scanner:
// Never redispatch an already-assigned reservation.
// Enable only after coordinated rollout and crash tests.

const { createClient } = require("@supabase/supabase-js");

const {
    dispatchRecoveredWaitingBatch
} = require("./secondQueueExistingBatch");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

let running = false;

async function scanWaitingRecovery() {
    if (running) return;

    running = true;

    try {
        const { data: batches, error } = await supabase
            .from("sync_batches")
            .select("*")
            .eq("waiting_recovery_pending", true)
            .eq("batch_status", "PENDING")
            .eq("worker_status", "PENDING")
            .eq("current_stage", "PENDING")
            .order("last_activity_at", {
                ascending: true
            })
            .limit(25);

        if (error) {
            throw error;
        }

        for (const batch of batches || []) {

            // Reserved batches require reconciliation.
            // Never infer that a missing Bull job means
            // the previous execution never started.

            if (batch.worker_id != null) {
                console.warn(
                    "[WAITING RECOVERY] RESERVED_REQUIRES_RECONCILIATION",
                    {
                        batch_id: batch.batch_id,
                        dispatch_generation: batch.dispatch_generation,
                        worker_id: batch.worker_id
                    }
                );

                continue;
            }

            // Unreserved eligibility checks.
            if (
                batch.batch_closed === true ||
                batch.cleanup_claim_token != null ||
                batch.retry_claim_token != null ||
                Number(batch.operation_count || 0) !== 0
            ) {
                continue;
            }

            try {
                await dispatchRecoveredWaitingBatch(batch);
            } catch (err) {
                console.error(
                    "[WAITING RECOVERY] DISPATCH_DEFERRED",
                    batch.batch_id,
                    err.message
                );
            }
        }

    } finally {
        running = false;
    }
}

function startWaitingRecoveryScanner() {

    if (
        process.env.ENABLE_WAITING_RECOVERY_SCANNER !== "true"
    ) {
        return null;
    }

    const run = () => {
        void scanWaitingRecovery().catch(err => {
            console.error(
                "[WAITING RECOVERY] SCAN_FAILED",
                err.message
            );
        });
    };

    setImmediate(run);

    const timer = setInterval(run, 60000);

    timer.unref();

    return timer;
}

module.exports = {
    scanWaitingRecovery,
    startWaitingRecoveryScanner
};
