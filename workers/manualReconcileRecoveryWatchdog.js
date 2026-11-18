
"use strict";

require("dotenv").config();

const { createClient } = require("@supabase/supabase-js");

const {
    createManualReconcileAutoRecovery
} = require("../services/manualReconciliation/manualReconcileAutoRecovery");

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_KEY;

if (!supabaseUrl || !serviceRoleKey) {
    throw new Error(
        "MANUAL_RECOVERY_SUPABASE_CONFIG_MISSING"
    );
}

const supabase = createClient(
    supabaseUrl,
    serviceRoleKey,
    {
        auth: {
            persistSession: false,
            autoRefreshToken: false
        }
    }
);

const watchdog = createManualReconcileAutoRecovery({
    supabase,
    intervalMs: 10000,
    staleAfterMs: 120000,
    batchSize: 25,
    excludedCompanyCodes: ["C0053"],
    leaseSeconds: 120,
    onResult: result => {
        console.log("[MANUAL RECOVERY WATCHDOG]", {
            attemptId: result.attemptId || null,
            recovered: result.recovered === true,
            reason: result.reason || null
        });
    }
});

function shutdown() {
    console.log("[MANUAL RECOVERY WATCHDOG] Stopping");
    watchdog.stop();
}

process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);

console.log("[MANUAL RECOVERY WATCHDOG] Initialized");

// Explicit activation gate.
if (process.env.MANUAL_RECONCILE_AUTO_RECOVERY_ENABLED === "true") {
    watchdog.start();
    console.log("[MANUAL RECOVERY WATCHDOG] Started");
} else {
    console.log("[MANUAL RECOVERY WATCHDOG] Disabled");
}
