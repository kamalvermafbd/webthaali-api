
"use strict";

const { randomUUID } = require("crypto");
const http = require("http");
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
async function dispatchFailureRecovery(attemptId, dispatchToken) {
    const token =
        process.env.MANUAL_RECONCILE_INTERNAL_RECOVERY_TOKEN;

    const endpoint =
        process.env.MANUAL_RECONCILE_RECOVERY_INTERNAL_URL;

    if (
        typeof token !== "string" ||
        token.length < 32 ||
        typeof endpoint !== "string"
    ) {
        throw new Error("FAILURE_DISPATCH_CONFIG_MISSING");
    }

    const url = new URL(endpoint);

    if (
        url.protocol !== "http:" ||
        url.hostname !== "127.0.0.1" ||
        !url.port ||
        url.pathname !== "/reconcileRecovery" ||
        url.search ||
        url.hash ||
        url.username ||
        url.password
    ) {
        throw new Error("FAILURE_DISPATCH_URL_INVALID");
    }

   const body = JSON.stringify({
    attempt_id: attemptId,
    recovery_type: "FAILURE",
    dispatch_token: dispatchToken
});

    return new Promise((resolve, reject) => {
        const request = http.request(
            url,
            {
                method: "POST",
                headers: {
                    "content-type": "application/json",
                    "content-length": Buffer.byteLength(body),
                    "x-manual-recovery-token": token
                },
                agent: false
            },
            response => {
                let responseBody = "";

                response.on("data", chunk => {
                    responseBody += chunk;

                    if (responseBody.length > 16384) {
                        request.destroy(
                            new Error("FAILURE_RESPONSE_TOO_LARGE")
                        );
                    }
                });

                response.on("end", () => {
                    let result;

                    try {
                        result = JSON.parse(responseBody);
                    } catch (_) {
                        reject(
                            new Error("FAILURE_RESPONSE_UNVERIFIED")
                        );
                        return;
                    }

                    if (
                        response.statusCode === 200 &&
                        result?.success === true &&
                        result?.recovered === true &&
                        result?.attempt_id === attemptId
                    ) {
                        resolve({
                            recovered: true,
                            reason: result.recovery_state
                        });
                        return;
                    }

                    resolve({
                        recovered: false,
                        reason: "FAILURE_OUTCOME_REQUIRES_INSPECTION"
                    });
                });
            }
        );

        request.setTimeout(15000, () => {
            request.destroy(
                new Error("FAILURE_DISPATCH_TIMEOUT")
            );
        });

        request.on("error", reject);
        request.end(body);
    });
}
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

        if (inspection.state === "EXECUTION_UNCERTAIN") {
    if (
        process.env.MANUAL_RECONCILE_AUTO_FAILURE_DISPATCH_ENABLED
        !== "true"
    ) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_DISABLED"
        };
    }

    const endpoint =
        process.env.MANUAL_RECONCILE_RECOVERY_INTERNAL_URL;

    const token =
        process.env.MANUAL_RECONCILE_INTERNAL_RECOVERY_TOKEN;

    if (
        typeof endpoint !== "string" ||
        typeof token !== "string" ||
        token.length < 32
    ) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_CONFIG_MISSING"
        };
    }

    // Validate endpoint before taking permanent claim.
    let url;

    try {
        url = new URL(endpoint);
    } catch (_) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_URL_INVALID"
        };
    }

    if (
        url.protocol !== "http:" ||
        url.hostname !== "127.0.0.1" ||
        !url.port ||
        url.pathname !== "/reconcileRecovery" ||
        url.search ||
        url.hash ||
        url.username ||
        url.password
    ) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_URL_INVALID"
        };
    }

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

    // From this point, uncertain SQL/HTTP outcomes must
    // retain the recovery lease for verification.
    recoveryStarted = true;

    const dispatchToken = randomUUID();

    const claimed = await rpcBoolean(
        "claim_manual_reconcile_failure_dispatch_v1",
        {
            p_attempt_id: attemptId,
            p_dispatch_token: dispatchToken,
            p_claimed_by: claimedBy
        }
    );

    if (!claimed) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_ALREADY_CLAIMED"
        };
    }

    // Durable transition before network dispatch.
    const { data: marked, error: markError } =
        await supabase
            .from("manual_reconcile_failure_dispatches")
            .update({
                dispatch_state: "DISPATCH_STARTED"
            })
            .eq("attempt_id", attemptId)
            .eq("dispatch_token", dispatchToken)
            .eq("dispatch_state", "CLAIMED")
            .select("attempt_id");

    if (
        markError ||
        !Array.isArray(marked) ||
        marked.length !== 1
    ) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_MARK_UNCERTAIN"
        };
    }

    if (leaseLost) {
        return {
            attemptId,
            recovered: false,
            reason: "LEASE_LOST_BEFORE_DISPATCH"
        };
    }

    try {
    const outcome = await dispatchFailureRecovery(
    attemptId,
    dispatchToken
);

        if (
            outcome.recovered === true &&
            !leaseLost
        ) {
            recoveryConfirmed = true;

            return {
                attemptId,
                recovered: true,
                reason: outcome.reason
            };
        }

        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_OUTCOME_REQUIRES_INSPECTION"
        };

    } catch (_) {
        return {
            attemptId,
            recovered: false,
            reason: "FAILURE_DISPATCH_OUTCOME_UNCERTAIN"
        };
    }
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
