"use strict";

// =====================================================
// CENTRAL CONNECTOR QUARANTINE POLICY
// =====================================================

const OPERATION_TYPES = Object.freeze({
    FIRST_SYNC: "FIRST_SYNC",
    NORMAL_SYNC: "NORMAL_SYNC",
    MANUAL_RECONCILIATION: "MANUAL_RECONCILIATION",
    BILLEY_TO_TALLY: "BILLEY_TO_TALLY",
    UNKNOWN: "UNKNOWN"
});

// =====================================================
// CENTRAL CONFIGURATION
// All quarantine freshness policies are managed here.
// =====================================================

const QUARANTINE_POLICY = Object.freeze({
    FIRST_SYNC: 5 * 60 * 1000,
    NORMAL_SYNC: 5 * 60 * 1000,
    MANUAL_RECONCILIATION: 5 * 60 * 1000,
    BILLEY_TO_TALLY: 5 * 60 * 1000,
    UNKNOWN: 5 * 60 * 1000
});

// =====================================================
// NORMALIZE OPERATION TYPE
// =====================================================

function normalizeOperationType(operationType) {
    const type = String(operationType || "")
        .trim()
        .toUpperCase();

    return Object.prototype.hasOwnProperty.call(
        OPERATION_TYPES,
        type
    )
        ? OPERATION_TYPES[type]
        : OPERATION_TYPES.UNKNOWN;
}

// =====================================================
// RESOLVE COORDINATOR OPERATION TYPE
// =====================================================

function resolveOperationType(options) {
    if (options?.releaseEvidence === true) {
        return OPERATION_TYPES.MANUAL_RECONCILIATION;
    }

    return normalizeOperationType(
        options?.operationType
    );
}

// =====================================================
// GET QUARANTINE POLICY
// =====================================================

function getQuarantinePolicy(operationType) {
    const type = normalizeOperationType(operationType);

    return Object.freeze({
        operationType: type,
        freshnessMs: QUARANTINE_POLICY[type]
    });
}

// =====================================================
// EVALUATE QUARANTINE FRESHNESS
// =====================================================

function evaluateQuarantine(
    quarantine,
    nowMs = Date.now()
) {
    if (quarantine === null) {
        return Object.freeze({
            quarantined: false,
            state: "NOT_QUARANTINED",
            fresh: false,
            stale: false,
            recoveryEligible: false,
            requiresOwnershipCheck: true,
            recoveryAuthorized: false
        });
    }

    const policy = getQuarantinePolicy(
        quarantine?.operation_type
    );

    const quarantinedAt = Date.parse(
        quarantine?.quarantined_at || ""
    );

    if (
        !Number.isFinite(nowMs) ||
        !Number.isFinite(quarantinedAt) ||
        quarantinedAt > nowMs
    ) {
        return Object.freeze({
            quarantined: true,
            state: "INVALID_QUARANTINE_STATE",
            operationType: policy.operationType,
            fresh: false,
            stale: false,
            recoveryEligible: false,
            operationAllowed: false,
            recoveryAuthorized: false
        });
    }

    const ageMs = nowMs - quarantinedAt;
    const stale = ageMs >= policy.freshnessMs;

    return Object.freeze({
        quarantined: true,

        state: stale
            ? "STALE_QUARANTINE"
            : "FRESH_QUARANTINE",

        operationType: policy.operationType,

        ageMs,

        freshnessMs: policy.freshnessMs,

        remainingMs: Math.max(
            0,
            policy.freshnessMs - ageMs
        ),

        fresh: !stale,
        stale,

        recoveryEligible: stale,

        // Age alone must never authorize a new operation.
        operationAllowed: false,

        // Remote settlement must be verified separately.
        recoveryAuthorized: false
    });
}
// =====================================================
// FIRST SYNC / NORMAL SYNC CLASSIFICATION
// =====================================================

async function resolveSyncOperationType({
    supabase,
    companyCode,
    tallyOwner,
    batchId
}) {
    const company = String(companyCode || "").trim();
    const owner = String(tallyOwner || "").trim().toUpperCase();
    const currentBatchId = String(batchId || "").trim();

    if (
        !supabase ||
        !company ||
        !["CA", "USER"].includes(owner) ||
        !currentBatchId
    ) {
        return OPERATION_TYPES.UNKNOWN;
    }

    try {
        const { data, error } = await supabase
            .from("sync_batches")
            .select("batch_id")
            .eq("company_code", company)
            .eq("tally_owner", owner)
            .neq("batch_id", currentBatchId)
            .in("batch_status", ["COMPLETED", "CLOSED"])
            .limit(1);

        if (error || !Array.isArray(data)) {
            throw error || new Error(
                "INVALID_SYNC_HISTORY_RESPONSE"
            );
        }

        return data.length > 0
            ? OPERATION_TYPES.NORMAL_SYNC
            : OPERATION_TYPES.FIRST_SYNC;

    } catch (error) {
        console.error(
            "[QUARANTINE MANAGER] Sync classification failed",
            {
                company_code: company,
                tally_owner: owner,
                batch_id: currentBatchId,
                error: error?.message || String(error)
            }
        );

        return OPERATION_TYPES.UNKNOWN;
    }
}
// =====================================================
// EXPORTS
// =====================================================

module.exports = {
    OPERATION_TYPES,
    QUARANTINE_POLICY,
    normalizeOperationType,
    resolveOperationType,
    getQuarantinePolicy,
    evaluateQuarantine,
    resolveSyncOperationType
};