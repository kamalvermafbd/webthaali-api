const { createClient } =
require("@supabase/supabase-js");

const {
    TABLES
} = require("./constants");

const BatchStatusManager =
require("./BatchStatusManager");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function readVerifiedCleanupProgress({
    sync_batch_id,
    cleanupClaimToken
}) {
    const { data, error } = await supabase.rpc(
        "get_sync_batch_cleanup_progress",
        {
            p_batch_id: sync_batch_id,
            p_claim_token: cleanupClaimToken
        }
    );

    if (error) {
        throw new Error(
            `Cleanup progress verification failed: ${error.message}`
        );
    }

    if (!Array.isArray(data) || data.length !== 1) {
        throw new Error(
            "Cleanup progress verification returned invalid rows"
        );
    }

    const progress = data[0];

    if (
        progress.claim_matches !== true ||
        progress.batch_closed !== true ||
        progress.cleanup_status !== "RUNNING" ||
        !Number.isInteger(progress.cleanup_completed_steps) ||
        progress.cleanup_completed_steps < 0 ||
        progress.cleanup_completed_steps > 15
    ) {
        throw new Error(
            "Cleanup progress ownership or state invalid"
        );
    }

    return progress.cleanup_completed_steps;
}

// ======================================================
// VERIFIED CLEANUP RECOVERY STATE
// ======================================================

async function readVerifiedCleanupRecoveryState({
    sync_batch_id,
    cleanupClaimToken
}) {
    const { data, error } = await supabase.rpc(
        "get_sync_batch_cleanup_recovery_state",
        {
            p_batch_id: sync_batch_id,
            p_claim_token: cleanupClaimToken
        }
    );

    if (error) {
        throw new Error(
            `Cleanup recovery verification failed: ${error.message}`
        );
    }

    if (!Array.isArray(data) || data.length !== 1) {
        throw new Error(
            "Cleanup recovery returned invalid rows"
        );
    }

    const state = data[0];

    if (
        state.claim_matches !== true ||
        state.batch_closed !== true ||
        state.cleanup_status !== "RUNNING" ||
        !Number.isInteger(state.cleanup_completed_steps) ||
        state.cleanup_completed_steps < 0 ||
        state.cleanup_completed_steps > 15
    ) {
        throw new Error(
            "Cleanup recovery ownership or state invalid"
        );
    }

    const counts = state.cleanup_deleted_counts;

    if (
        !counts ||
        typeof counts !== "object" ||
        Array.isArray(counts)
    ) {
        throw new Error(
            "Cleanup recovery deleted counts invalid"
        );
    }

    const expectedTables = [
        ...CLEANUP_TABLES,
        TABLES.OPENING_BALANCE_ALLOCATIONS,
        TABLES.STOCK_OPENING_BALANCES
    ];

    for (let i = 0; i < expectedTables.length; i++) {
        const table = expectedTables[i];

        const hasCount =
            Object.prototype.hasOwnProperty.call(counts, table);

        if (i < state.cleanup_completed_steps) {
            if (
                !hasCount ||
                typeof counts[table] !== "number" ||
                !Number.isSafeInteger(counts[table]) ||
                counts[table] < 0
            ) {
                throw new Error(
                    `Cleanup recovery count invalid for ${table}`
                );
            }
        } else if (hasCount) {
            throw new Error(
                `Cleanup recovery unexpected count for ${table}`
            );
        }
    }

    if (Object.keys(counts).length !== state.cleanup_completed_steps) {
        throw new Error(
            "Cleanup recovery counts do not match progress"
        );
    }

    return {
        completedSteps: state.cleanup_completed_steps,
        deletedCounts: counts
    };
}

// ======================================================
// TABLES
// ======================================================

const CLEANUP_TABLES = [

    TABLES.GROUPS,

    TABLES.STOCK_GROUPS,

    TABLES.LEDGERS,

    TABLES.STOCKS,

    TABLES.UNITS,

    TABLES.GODOWNS,

    TABLES.COST_CENTRES,

    TABLES.VOUCHERS,

    TABLES.VOUCHER_LEDGERS,

    TABLES.VOUCHER_INVENTORY,

    TABLES.STOCK_VOUCHERS,

    TABLES.COST_CENTRE_ALLOCATIONS,

    TABLES.BILL_ALLOCATIONS

];

class CleanupManager {

    // ======================================================
    // HARD DELETE
    // ======================================================

    async cleanup({

    company_code,

    tally_owner,

    sync_batch_id,

    ownerInstanceId

}) {

       if (

    !company_code ||

    !tally_owner ||

    !sync_batch_id ||

    typeof ownerInstanceId !== "string" ||

    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(ownerInstanceId)

) {
            throw new Error(
                "Cleanup parameters missing"
            );

        }

        //--------------------------------------------------
        // Verify Batch
        //--------------------------------------------------

const batch = await BatchStatusManager.loadBatch({
    batch_id: sync_batch_id
});
        if (
    !batch ||
    batch.company_code !== company_code ||
    batch.tally_owner !== tally_owner
) {
    throw new Error(
        `Cleanup batch ownership mismatch: ${sync_batch_id}`
    );
}

    if (
    batch.batch_status !== "CLOSED" ||
    batch.worker_status !== "COMPLETED" ||
    batch.batch_closed !== true ||
    batch.reconciliation_completed !== true ||
    batch.ledger_reconciliation_completed !== true ||
    batch.stock_reconciliation_completed !== true
) {

            return {

                success: false,

                skipped: true,

                reason:
                    "Sync batch reconciliation not completed"

            };

        }

              //--------------------------------------------------
        // Acquire Cleanup Ownership
        //--------------------------------------------------

        let cleanupClaims;
        let claimError;
        let cleanupClaimToken;
            let recoveryState = null;

        try {
           const response = await supabase.rpc(
    "claim_sync_batch_cleanup",
    {
        p_batch_id: sync_batch_id,
        p_owner_instance_id: ownerInstanceId
    }
);

            cleanupClaims = response.data;
            claimError = response.error;

        } catch (cause) {
            claimError = {
                message: cause?.message ||
                    "UNKNOWN_CLEANUP_CLAIM_EXCEPTION"
            };
        }
        if (claimError) {
            throw new Error(
                `Cleanup claim failed: ${claimError.message}`
            );
        }

               if (
            !Array.isArray(cleanupClaims) ||
            cleanupClaims.length !== 1 ||
            !cleanupClaims[0] ||
            typeof cleanupClaims[0] !== "object"
        ) {
            throw new Error(
                "Cleanup claim result invalid; " +
                "database outcome uncertain; " +
                "do not retry without verification"
            );
        }

        const cleanupClaim = cleanupClaims[0];

        if (cleanupClaim.claimed === false) {

    if (cleanupClaim.reason !== "CLEANUP_ALREADY_CLAIMED") {
        throw new Error(
            `Cleanup claim rejected: ${
                cleanupClaim.reason || "UNKNOWN"
            }`
        );
    }

    let recoveryClaims;
    let recoveryError;

    try {
        const response = await supabase.rpc(
            "recover_sync_batch_cleanup",
            {
                p_batch_id: sync_batch_id,
                p_owner_instance_id: ownerInstanceId
            }
        );

        recoveryClaims = response.data;
        recoveryError = response.error;

    } catch (cause) {
        throw new Error(
            `Cleanup recovery outcome uncertain: ${
                cause?.message || "UNKNOWN"
            }`
        );
    }

    if (recoveryError) {
        throw new Error(
            `Cleanup recovery outcome uncertain: ${recoveryError.message}`
        );
    }

    if (
        !Array.isArray(recoveryClaims) ||
        recoveryClaims.length !== 1 ||
        recoveryClaims[0]?.recovered !== true ||
        typeof recoveryClaims[0]?.claim_token !== "string"
    ) {
        throw new Error(
            `Cleanup recovery not confirmed: ${
                recoveryClaims?.[0]?.reason || "INVALID_RESULT"
            }`
        );
    }

    cleanupClaimToken = recoveryClaims[0].claim_token;

    recoveryState = await readVerifiedCleanupRecoveryState({
        sync_batch_id,
        cleanupClaimToken
    });
}

       if (cleanupClaim.claimed === true) {
    if (
        typeof cleanupClaim.claim_token !== "string" ||
        cleanupClaim.claim_token.length === 0
    ) {
        throw new Error(
            "Cleanup claim token invalid; " +
            "database outcome uncertain; " +
            "do not retry without verification"
        );
    }

    cleanupClaimToken = cleanupClaim.claim_token;
}

if (
    typeof cleanupClaimToken !== "string" ||
    cleanupClaimToken.length === 0
) {
    throw new Error(
        "Cleanup ownership not confirmed"
    );
}

        //--------------------------------------------------
        // Cleanup
        //--------------------------------------------------

        const cleanedTables = [];

const completedSteps = recoveryState?.completedSteps ?? 0;

if (recoveryState) {
    for (const table of CLEANUP_TABLES.slice(0, completedSteps)) {
        cleanedTables.push({
            table,
            deleted: recoveryState.deletedCounts[table]
        });
    }
}

for (const table of CLEANUP_TABLES.slice(completedSteps)) {

                   let deletedCount;
            let error;

            try {
                const response = await supabase.rpc(
                    "fenced_sync_cleanup_delete",
                    {
                        p_batch_id: sync_batch_id,
                        p_claim_token: cleanupClaimToken,
                        p_table_name: table
                    }
                );

                deletedCount = response.data;
                error = response.error;

            } catch (cause) {
                error = {
                    message: cause?.message ||
                        "UNKNOWN_CLEANUP_RPC_EXCEPTION"
                };
            }

                     if (error) {

                let verificationDetail = "UNVERIFIED";

                try {
                    const committedSteps =
                        await readVerifiedCleanupProgress({
                            sync_batch_id,
                            cleanupClaimToken
                        });

                    verificationDetail =
                        `COMMITTED_STEPS_${committedSteps}`;

                } catch (verificationError) {
                    verificationDetail =
                        `VERIFICATION_FAILED: ${verificationError.message}`;
                }

                throw new Error(
                    `Cleanup failed for ${table}: ${error.message}; ` +
                    `database progress: ${verificationDetail}; ` +
                    `cleanup ownership retained for recovery`
                );
            }

            if (
                typeof deletedCount !== "number" ||
                !Number.isSafeInteger(deletedCount) ||
                deletedCount < 0
            ) {
                throw new Error(
                    `Cleanup result invalid for ${table}`
                );
            }

            cleanedTables.push({
                table,
                deleted: deletedCount
            });

        }

             //--------------------------------------------------
        // Opening Balance Allocation Cleanup
        //--------------------------------------------------
if (completedSteps < 14) {
        let openingDeletedCount;
        let openingCleanupError;

        try {
            const response = await supabase.rpc(
                "fenced_sync_cleanup_delete",
                {
                    p_batch_id: sync_batch_id,
                    p_claim_token: cleanupClaimToken,
                    p_table_name: TABLES.OPENING_BALANCE_ALLOCATIONS
                }
            );

            openingDeletedCount = response.data;
            openingCleanupError = response.error;

        } catch (cause) {
            openingCleanupError = {
                message: cause?.message ||
                    "UNKNOWN_OPENING_BALANCE_CLEANUP_EXCEPTION"
            };
        }

               if (openingCleanupError) {

            let verificationDetail = "UNVERIFIED";

            try {
                const committedSteps =
                    await readVerifiedCleanupProgress({
                        sync_batch_id,
                        cleanupClaimToken
                    });

                verificationDetail =
                    `COMMITTED_STEPS_${committedSteps}`;

            } catch (verificationError) {
                verificationDetail =
                    `VERIFICATION_FAILED: ${verificationError.message}`;
            }

            throw new Error(
                `Opening balance cleanup failed: ${openingCleanupError.message}; ` +
                `database progress: ${verificationDetail}; ` +
                `cleanup ownership retained for recovery`
            );
        }

        if (
            typeof openingDeletedCount !== "number" ||
            !Number.isSafeInteger(openingDeletedCount) ||
            openingDeletedCount < 0
        ) {
            throw new Error(
                "Opening balance cleanup result invalid"
            );
        }

        cleanedTables.push({
            table: TABLES.OPENING_BALANCE_ALLOCATIONS,
            deleted: openingDeletedCount
        });
        } else {
    cleanedTables.push({
        table: TABLES.OPENING_BALANCE_ALLOCATIONS,
        deleted: recoveryState.deletedCounts[
            TABLES.OPENING_BALANCE_ALLOCATIONS
        ]
    });
}

        // --------------------------------------------------
        // Stock Opening Balance Cleanup
        // --------------------------------------------------
if (completedSteps < 15) {
                     let stockOpeningDeletedCount;
        let stockOpeningCleanupError;

        try {
            const response = await supabase.rpc(
                "fenced_sync_cleanup_delete",
                {
                    p_batch_id: sync_batch_id,
                    p_claim_token: cleanupClaimToken,
                    p_table_name: TABLES.STOCK_OPENING_BALANCES
                }
            );

            stockOpeningDeletedCount = response.data;
            stockOpeningCleanupError = response.error;

        } catch (cause) {
            stockOpeningCleanupError = {
                message: cause?.message ||
                    "UNKNOWN_STOCK_OPENING_CLEANUP_EXCEPTION"
            };
        }

        if (stockOpeningCleanupError) {

            let verificationDetail = "UNVERIFIED";

            try {
                const committedSteps =
                    await readVerifiedCleanupProgress({
                        sync_batch_id,
                        cleanupClaimToken
                    });

                verificationDetail =
                    `COMMITTED_STEPS_${committedSteps}`;

            } catch (verificationError) {
                verificationDetail =
                    `VERIFICATION_FAILED: ${verificationError.message}`;
            }

            throw new Error(
                `Stock opening balance cleanup failed: ${stockOpeningCleanupError.message}; ` +
                `database progress: ${verificationDetail}; ` +
                `cleanup ownership retained for recovery`
            );
        }

        if (
            typeof stockOpeningDeletedCount !== "number" ||
            !Number.isSafeInteger(stockOpeningDeletedCount) ||
            stockOpeningDeletedCount < 0
        ) {
            throw new Error(
                "Stock opening balance cleanup result invalid"
            );
        }

        cleanedTables.push({
            table: TABLES.STOCK_OPENING_BALANCES,
            deleted: stockOpeningDeletedCount
        });

        } else {
    cleanedTables.push({
        table: TABLES.STOCK_OPENING_BALANCES,
        deleted: recoveryState.deletedCounts[
            TABLES.STOCK_OPENING_BALANCES
        ]
    });
}
                //--------------------------------------------------
        // Complete Verified Cleanup
        //--------------------------------------------------

              let cleanupCompleted;
        let completionError;

        try {
            const response = await supabase.rpc(
                "complete_sync_batch_cleanup",
                {
                    p_batch_id: sync_batch_id,
                    p_claim_token: cleanupClaimToken
                }
            );

            cleanupCompleted = response.data;
            completionError = response.error;

        } catch (cause) {
            completionError = {
                message: cause?.message ||
                    "UNKNOWN_CLEANUP_COMPLETION_EXCEPTION"
            };
        }

                    if (completionError || cleanupCompleted !== true) {

            let verifiedBatch;

            try {
                const { data, error } = await supabase
                    .from("sync_batches")
                    .select(
                        "company_code, tally_owner, batch_status, worker_status, " +
                        "batch_closed, reconciliation_completed, " +
                        "ledger_reconciliation_completed, " +
                        "stock_reconciliation_completed, " +
                        "cleanup_status, cleanup_completed_steps, " +
                        "cleanup_claim_token, cleanup_completed_at, " +
                        "cleanup_deleted_counts"
                    )
                    .eq("batch_id", sync_batch_id)
                    .single();

                if (error || !data) {
                    throw new Error(
                        error?.message || "Batch not found"
                    );
                }

                verifiedBatch = data;

            } catch (verificationError) {
                throw new Error(
                    `Cleanup completion outcome uncertain: ${
                        completionError?.message ||
                        "INVALID_COMPLETION_RESULT"
                    }; verification failed: ${
                        verificationError.message
                    }`
                );
            }

            const completionCommitted =
                verifiedBatch.company_code === company_code &&
                verifiedBatch.tally_owner === tally_owner &&
                verifiedBatch.batch_status === "CLOSED" &&
                verifiedBatch.worker_status === "COMPLETED" &&
                verifiedBatch.batch_closed === true &&
                verifiedBatch.reconciliation_completed === true &&
                verifiedBatch.ledger_reconciliation_completed === true &&
                verifiedBatch.stock_reconciliation_completed === true &&
                verifiedBatch.cleanup_status === "COMPLETED" &&
                verifiedBatch.cleanup_completed_steps === 15 &&
                verifiedBatch.cleanup_claim_token === null &&
                verifiedBatch.cleanup_completed_at !== null;

            if (!completionCommitted) {
                throw new Error(
                    `Cleanup completion not confirmed: ${
                        completionError?.message ||
                        "INVALID_COMPLETION_RESULT"
                    }; cleanup ownership retained if still claimed`
                );
            }

            const durableCounts =
                verifiedBatch.cleanup_deleted_counts;

            if (
                !durableCounts ||
                typeof durableCounts !== "object" ||
                Array.isArray(durableCounts) ||
                Object.keys(durableCounts).length !==
                    cleanedTables.length ||
                cleanedTables.some(
                    ({ table, deleted }) =>
                        !Object.prototype.hasOwnProperty.call(
                            durableCounts,
                            table
                        ) ||
                        durableCounts[table] !== deleted
                )
            ) {
                throw new Error(
                    "Cleanup completion committed but durable counts mismatch"
                );
            }

            // Completion and counts verified despite lost RPC response.
        }

              

                // Verify durable cleanup counts before reporting success.
        const { data: finalBatch, error: finalError } =
            await supabase
                .from("sync_batches")
                .select(
                    "cleanup_status, cleanup_completed_steps, cleanup_claim_token, cleanup_deleted_counts"
                )
                .eq("batch_id", sync_batch_id)
                .single();

        if (
            finalError ||
            !finalBatch ||
            finalBatch.cleanup_status !== "COMPLETED" ||
            finalBatch.cleanup_completed_steps !== 15 ||
            finalBatch.cleanup_claim_token !== null
        ) {
            throw new Error(
                "Final cleanup verification failed"
            );
        }

        const durableCounts = finalBatch.cleanup_deleted_counts;

        if (
            !durableCounts ||
            typeof durableCounts !== "object" ||
            Array.isArray(durableCounts) ||
            Object.keys(durableCounts).length !== cleanedTables.length ||
            cleanedTables.some(
                ({ table, deleted }) =>
                    !Object.prototype.hasOwnProperty.call(
                        durableCounts,
                        table
                    ) ||
                    durableCounts[table] !== deleted
            )
        ) {
            throw new Error(
                "Final cleanup deleted counts mismatch"
            );
        }
        //--------------------------------------------------
        // Result
        //--------------------------------------------------

        return {

            success: true,

            sync_batch_id,

            cleanedTables

        };

    }

}

module.exports =
new CleanupManager();