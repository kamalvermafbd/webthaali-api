const crypto = require("crypto");

const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");


const {
    dispatchBatch
} = require("./queueExistingBatch");


async function startSync({
    company_code,
    tally_owner,
    sync_mode,
    sync_period
}) {

    company_code =
        String(company_code || "").trim();

    tally_owner =
        String(tally_owner || "")
            .trim()
            .toUpperCase();

    sync_mode =
        String(sync_mode || "")
            .trim()
            .toUpperCase();

    sync_period =
        String(sync_period || "")
            .trim()
            .toUpperCase();


    // =========================================
    // VALIDATION
    // =========================================

    if (!company_code) {
        throw new Error(
            "company_code missing"
        );
    }

    if (
        tally_owner !== "USER" &&
        tally_owner !== "CA"
    ) {
        throw new Error(
            "Invalid tally_owner"
        );
    }


    // =========================================
    // 1. CLAIM / CREATE BATCH
    // =========================================

    const claimResult =
        await BatchStatusManager
            .claimOrCreateHttpBatch({

                company_code,

                tally_owner,

                request_id:
                    crypto.randomUUID(),

                sync_mode,

                sync_period

            });


    // =========================================
// EXISTING ACTIVE BATCH
// =========================================

if (
    claimResult.claimed !== true
) {

    /*
     * Recover only an orphan PENDING batch.
     *
     * PENDING + no worker means previous dispatch
     * did not complete. Reuse this same batch.
     */

    if (
        claimResult.reason === "ALREADY_ACTIVE" &&
        !claimResult.worker_id
    ) {

        const existingBatch =
            await BatchStatusManager.loadBatch({
                batch_id: claimResult.batch_id
            });

        if (
            existingBatch &&
            existingBatch.batch_status === "PENDING" &&
            !existingBatch.worker_id
        ) {
/*
            const connectorResult =
                await pairConnectorForBatch({
                    company_code,
                    tally_owner
                });

            if (!connectorResult?.success) {

                throw new Error(
                    connectorResult?.error ||
                    "Connector pairing failed"
                );
            }
            */

            const dispatchResult =
                await dispatchBatch(existingBatch);

            return {

                success: true,

                queued: true,

                already_active: false,

                recovered: true,

                batch_id:
                    existingBatch.batch_id,

                status: "QUEUED",

                worker:
                    dispatchResult.worker.worker_name,

                worker_type:
                    dispatchResult.worker.worker_type,

                server_id:
                    dispatchResult.worker.server_id,

                queue:
                    dispatchResult.worker.queue_name

            };
        }
    }

    /*
     * PROCESSING / RUNNING or already assigned:
     * do not touch it.
     */

    return {

        success: false,

        queued: false,

        already_active: true,

        reason:
            claimResult.reason,

        batch_id:
            claimResult.batch_id,

        worker_id:
            claimResult.worker_id || null

    };

}


    const batch_id =
        claimResult.batch_id;


    // =========================================
    // 2. LOAD CREATED BATCH
    // =========================================

    const batch =
        await BatchStatusManager.loadBatch({

            batch_id

        });


    if (!batch) {

        throw new Error(
            `Batch not found: ${batch_id}`
        );

    }


    // =========================================
    // 3. EXISTING CONNECTOR PAIRING
    // =========================================
/*
    const connectorResult =
        await pairConnectorForBatch({

            company_code,

            tally_owner

        });


    if (!connectorResult?.success) {

        throw new Error(
            connectorResult?.error ||
            "Connector pairing failed"
        );

    }

*/
    // =========================================
    // 4. DISPATCH FLOW
    // =========================================

    const dispatchResult =
        await dispatchBatch(batch);


    // =========================================
    // FINAL RESPONSE
    // =========================================

    return {

        success: true,

        queued: true,

        already_active: false,

        batch_id,

        status: "QUEUED",

        worker:
            dispatchResult.worker.worker_name,

        worker_type:
            dispatchResult.worker.worker_type,

        server_id:
            dispatchResult.worker.server_id,

        queue:
            dispatchResult.worker.queue_name

    };

}


module.exports = {
    startSync
};