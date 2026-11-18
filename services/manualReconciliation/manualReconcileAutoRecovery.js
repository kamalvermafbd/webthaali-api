
"use strict";

const {
    recoverManualReconciliation
} = require("./recoverManualReconciliation");

const {
    inspectManualReconcileRecovery
} = require("./inspectManualReconcileRecovery");

const UUID =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const ELIGIBLE = new Set([
    "CHECKPOINT_PRESENT_RELEASE_VERIFIED",
    "FINALIZATION_RECORDED"
]);

function createManualReconcileAutoRecovery({
    supabase,
    intervalMs = 60000,
    staleAfterMs = 120000,
    batchSize = 25,
    excludedCompanyCodes = ["C0053"],
    onResult = () => {}
}) {
    if (
        !supabase ||
        typeof supabase.from !== "function" ||
        typeof supabase.rpc !== "function" ||
        !Number.isSafeInteger(intervalMs) ||
        intervalMs < 10000 ||
        !Number.isSafeInteger(staleAfterMs) ||
        staleAfterMs < 60000 ||
        !Number.isSafeInteger(batchSize) ||
        batchSize < 1 ||
        batchSize > 100 ||
        !Array.isArray(excludedCompanyCodes) ||
        excludedCompanyCodes.some(
            value => typeof value !== "string"
        ) ||
        typeof onResult !== "function"
    ) {
        throw new TypeError(
            "MANUAL_RECOVERY_WATCHDOG_CONFIG_INVALID"
        );
    }

    let timer = null;
    let running = null;
    let stopped = true;

    async function scan() {
        const cutoff = new Date(
            Date.now() - staleAfterMs
        ).toISOString();

        const { data, error } = await supabase
            .from("manual_reconcile_status")
            .select(
                "id,company_code,tally_owner,updated_at"
            )
            .eq("status", "RUNNING")
            .lt("updated_at", cutoff)
            .order("updated_at", {
                ascending: true
            })
            .limit(batchSize);

        if (error) {
            throw error;
        }

        if (!Array.isArray(data)) {
            throw new Error(
                "MANUAL_RECOVERY_SCAN_INVALID"
            );
        }

        const results = [];

        for (const row of data) {
            if (
                excludedCompanyCodes.includes(
                    row.company_code
                )
            ) {
                continue;
            }

            const {
                data: attempts,
                error: attemptError
            } = await supabase
                .from("manual_reconcile_attempts")
                .select("attempt_id")
                .eq("reservation_id", row.id)
                .eq("company_code", row.company_code)
                .eq("tally_owner", row.tally_owner)
                .limit(2);

            if (attemptError) {
                throw attemptError;
            }

            if (
                !Array.isArray(attempts) ||
                attempts.length !== 1 ||
                !UUID.test(
                    attempts[0].attempt_id || ""
                )
            ) {
                const item = {
                    reservationId: row.id,
                    recovered: false,
                    reason: "ATTEMPT_IDENTITY_AMBIGUOUS"
                };

                results.push(item);
                onResult(item);
                continue;
            }

            const attemptId =
                attempts[0].attempt_id;

            try {
                const inspection =
                    await inspectManualReconcileRecovery({
                        supabase,
                        attemptId
                    });

                if (
                    !ELIGIBLE.has(
                        inspection.state
                    )
                ) {
                    const item = {
                        attemptId,
                        recovered: false,
                        reason: inspection.state
                    };

                    results.push(item);
                    onResult(item);
                    continue;
                }

                const outcome =
                    await recoverManualReconciliation({
                        supabase,
                        attemptId
                    });

                const item = {
                    attemptId,
                    ...outcome
                };

                results.push(item);
                onResult(item);

            } catch (error) {
                const item = {
                    attemptId,
                    recovered: false,
                    reason: "RECOVERY_ERROR",
                    errorCode: error.message
                };

                results.push(item);
                onResult(item);
            }
        }

        return results;
    }

    function runOnce() {
        if (!running) {
            running = scan().finally(() => {
                running = null;
            });
        }

        return running;
    }

    function start() {
        if (!stopped) {
            return;
        }

        stopped = false;

        timer = setInterval(() => {
            runOnce().catch(error => {
                onResult({
                    recovered: false,
                    reason: "WATCHDOG_SCAN_ERROR",
                    errorCode: error.message
                });
            });
        }, intervalMs);

        timer.unref?.();
    }

    function stop() {
        stopped = true;

        if (timer) {
            clearInterval(timer);
        }

        timer = null;
    }

    return {
        start,
        stop,
        runOnce
    };
}

module.exports = {
    createManualReconcileAutoRecovery
};

