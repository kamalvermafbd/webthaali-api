
"use strict";

const { randomUUID } = require("crypto");

const {
    recoverManualReconciliation
} = require("./recoverManualReconciliation");

const {
    inspectManualReconcileRecovery
} = require("./inspectManualReconcileRecovery");

const UUID =
    /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

const ELIGIBLE = new Set([
    "CHECKPOINT_PRESENT_RELEASE_VERIFIED",
    "FINALIZATION_RECORDED"
]);

function createManualReconcileAutoRecovery({
    supabase,
    intervalMs = 60000,
    staleAfterMs = 120000,
    batchSize = 25,
    excludedCompanyCodes = [],
    onResult = () => {},
    claimedBy = `manual-recovery-${randomUUID()}`,
    leaseSeconds = 120
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
        excludedCompanyCodes.some(v => typeof v !== "string") ||
        typeof onResult !== "function" ||
        typeof claimedBy !== "string" ||
        !claimedBy.trim() ||
        claimedBy.length > 200 ||
        !Number.isSafeInteger(leaseSeconds) ||
        leaseSeconds < 30 ||
        leaseSeconds > 600
    ) {
        throw new TypeError(
            "MANUAL_RECOVERY_WATCHDOG_CONFIG_INVALID"
        );
    }

    const excluded = new Set(excludedCompanyCodes);

    let timer = null;
    let running = null;
    let stopped = true;

    function report(item) {
        try {
            onResult(item);
        } catch (_) {
            // Logging must not change recovery.
        }
    }

    async function rpcBoolean(name, args) {
        const response = await supabase.rpc(name, args);

        if (
            response?.error ||
            typeof response?.data !== "boolean"
        ) {
            throw new Error(
                `MANUAL_RECOVERY_${name}_UNCERTAIN`
            );
        }

        return response.data;
    }

    async function recoverWithLease(attemptId) {
        const claimToken = randomUUID();

        const args = {
            p_attempt_id: attemptId,
            p_claim_token: claimToken
        };

        let acquired = false;
        let leaseLost = false;
        let renewing = false;
        let heartbeat = null;
        let recoveryStarted = false;
        let recoveryConfirmed = false;

        try {
            acquired = await rpcBoolean(
                "claim_manual_reconcile_auto_recovery_v1",
                {
                    ...args,
                    p_claimed_by: claimedBy,
                    p_lease_seconds: leaseSeconds
                }
            );

            if (!acquired) {
                return {
                    attemptId,
                    recovered: false,
                    reason: "LEASE_NOT_ACQUIRED"
                };
            }

            // Lease is orchestration ownership,
            // not a remote Tally execution fence.
            heartbeat = setInterval(() => {
                if (renewing || leaseLost) return;

                renewing = true;

                rpcBoolean(
                    "renew_manual_reconcile_auto_recovery_v1",
                    {
                        ...args,
                        p_lease_seconds: leaseSeconds
                    }
                )
                    .then(ok => {
                        if (!ok) leaseLost = true;
                    })
                    .catch(() => {
                        leaseLost = true;
                    })
                    .finally(() => {
                        renewing = false;
                    });
            }, Math.floor(leaseSeconds * 1000 / 3));

            heartbeat.unref?.();

            const inspection =
                await inspectManualReconcileRecovery({
                    supabase,
                    attemptId
                });

            if (leaseLost) {
                return {
                    attemptId,
                    recovered: false,
                    reason: "LEASE_LOST"
                };
            }

            if (!ELIGIBLE.has(inspection.state)) {
                return {
                    attemptId,
                    recovered: false,
                    reason: inspection.state
                };
            }

            // Renew before entering recovery.
            const stillOwned = await rpcBoolean(
                "renew_manual_reconcile_auto_recovery_v1",
                {
                    ...args,
                    p_lease_seconds: leaseSeconds
                }
            );

            if (!stillOwned || leaseLost) {
                return {
                    attemptId,
                    recovered: false,
                    reason: "LEASE_LOST"
                };
            }
recoveryStarted = true;
            const outcome =
                await recoverManualReconciliation({
                    supabase,
                    attemptId
                });

            if (leaseLost) {
                return {
                    attemptId,
                    recovered: false,
                    reason:
                        "LEASE_LOST_OUTCOME_REQUIRES_INSPECTION"
                };
            }
recoveryConfirmed =
    outcome?.recovered === true &&
    (
        outcome?.reason === "CLOSED" ||
        outcome?.reason === "ALREADY_CLOSED"
    );
            return {
                attemptId,
                ...outcome
            };

        } catch (error) {
            return {
                attemptId,
                recovered: false,
                reason: "RECOVERY_ERROR",
                errorCode:
                    error?.message || "UNKNOWN"
            };

   } finally {
    if (heartbeat) {
        clearInterval(heartbeat);
    }

    // Once recovery execution begins, preserve the claim
    // unless terminal success is explicitly confirmed.
    // An uncertain mutation must never free the attempt
    // for another automatic recovery execution.
    const safeToRelease =
        acquired &&
        !leaseLost &&
        (
            !recoveryStarted ||
            recoveryConfirmed
        );

    if (safeToRelease) {
        try {
            await rpcBoolean(
                "release_manual_reconcile_auto_recovery_v1",
                args
            );
        } catch (_) {
            report({
                attemptId,
                recovered: false,
                reason: "LEASE_RELEASE_UNCERTAIN"
            });
        }
    } else if (acquired) {
        report({
            attemptId,
            recovered: false,
            reason: "LEASE_RETAINED_FOR_VERIFICATION"
        });
    }
}
    }

    async function scan() {
        const cutoff = new Date(
            Date.now() - staleAfterMs
        ).toISOString();

        const { data, error } = await supabase
            .from("manual_reconcile_status")
            .select(
                "id,company_code,tally_owner," +
                "operation_token,updated_at"
            )
            .eq("status", "RUNNING")
            .lt("updated_at", cutoff)
            .order("updated_at", {
                ascending: true
            })
            .limit(batchSize);

        if (error || !Array.isArray(data)) {
            throw new Error(
                "MANUAL_RECOVERY_SCAN_FAILED"
            );
        }

        const results = [];

        for (const row of data) {
            if (
                stopped ||
                excluded.has(row.company_code)
            ) {
                continue;
            }

            try {
                const {
                    data: attempts,
                    error: attemptError
                } = await supabase
                    .from("manual_reconcile_attempts")
                    .select(
                        "attempt_id,operation_token"
                    )
                    .eq("reservation_id", row.id)
                    .eq("company_code", row.company_code)
                    .eq("tally_owner", row.tally_owner)
                    .eq(
                        "operation_token",
                        row.operation_token
                    )
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
                        reason:
                            "ATTEMPT_IDENTITY_AMBIGUOUS"
                    };

                    results.push(item);
                    report(item);
                    continue;
                }

                const item = await recoverWithLease(
                    attempts[0].attempt_id
                );

                results.push(item);
                report(item);

            } catch (error) {
                const item = {
                    reservationId: row.id,
                    recovered: false,
                    reason: "RECOVERY_ERROR",
                    errorCode:
                        error?.message || "UNKNOWN"
                };

                results.push(item);
                report(item);
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
        if (!stopped) return;

        stopped = false;

        timer = setInterval(() => {
            runOnce().catch(() => {
                report({
                    recovered: false,
                    reason: "WATCHDOG_SCAN_ERROR"
                });
            });
        }, intervalMs);

        
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
