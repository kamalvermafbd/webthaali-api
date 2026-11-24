
"use strict";

// Reconcile waiting recovery handoffs using the existing reservation.
// Assigned rows retain their worker and dispatch generation.
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
        let lastId = null;

        for (;;) {
            let query = supabase
                .from("sync_batches")
                .select("*")
                .eq("waiting_recovery_pending", true)
                .eq("batch_status", "PENDING")
                .eq("worker_status", "PENDING")
                .eq("current_stage", "PENDING")
                .order("id", { ascending: true })
                .limit(25);

            if (lastId !== null) {
                query = query.gt("id", lastId);
            }

            const { data: batches, error } = await query;

            if (error) {
                throw error;
            }

            if (!batches || batches.length === 0) {
                break;
            }

            for (const batch of batches) {
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

            lastId = batches[batches.length - 1].id;

            if (batches.length < 25) {
                break;
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
