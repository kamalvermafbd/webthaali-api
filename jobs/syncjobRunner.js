const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");

const { Agent } = require("undici");

const syncAgent = new Agent({
    headersTimeout: 0,
    bodyTimeout: 0
});

async function runJob(job, serverUrl) {

    switch (job.job_type) {

        case "TALLY_SYNC": {

            console.log(
                "Running TALLY_SYNC",
                job.batch_id
            );

            const MAX_ATTEMPTS = 3;
            const RETRY_DELAY_MS = 3000;

            let lastError = null;

            for (
                let attempt = 1;
                attempt <= MAX_ATTEMPTS;
                attempt++
            ) {

              try {

    let retryBatch;

try {
    retryBatch =
        await BatchStatusManager.loadBatch({
            batch_id: job.batch_id
        });
} catch (lookupError) {
    console.error(
        "⛔ BATCH LOOKUP FAILED — HTTP DISPATCH BLOCKED:",
        job.batch_id,
        lookupError
    );

    return;
}

if (!retryBatch) {
    console.error(
        "⛔ BATCH NOT FOUND — HTTP DISPATCH BLOCKED:",
        job.batch_id
    );

    return;
}

    if (
        typeof retryBatch?.error_message === "string" &&
        retryBatch.error_message.startsWith(
            "CONNECTOR_OUTCOME_UNKNOWN:"
        )
    ) {
        console.error(
            "⛔ UNCERTAIN BATCH — HTTP RETRY BLOCKED:",
            job.batch_id
        );

        return;
    }

    console.log(
        `TALLY_SYNC ATTEMPT ${attempt}/${MAX_ATTEMPTS}:`,
        job.batch_id
    );

let response;

const syncServerUrl =
    process.env.BILLEY_SYNC_API_URL ||
    "https://sync-api.billey.in";


response = await fetch(
    `${syncServerUrl}/getMasters` +
    `?company_code=${encodeURIComponent(job.company_code)}` +
    `&tally_owner=${encodeURIComponent(job.tally_owner)}` +
    `&sync_mode=${encodeURIComponent(job.sync_mode)}` +
    `&sync_period=${encodeURIComponent(job.sync_period)}` +
    `&worker_batch_id=${encodeURIComponent(job.batch_id)}`,
    {
        dispatcher: syncAgent
    }
);  

                    console.log(
                        "TALLY_SYNC HTTP STATUS:",
                        response.status
                    );

                    console.log(
                        "TALLY_SYNC HTTP RESPONSE:",
                        await response.clone().text()
                    );

                    if (!response.ok) {

                        const text =
                            await response.text();

                        let errorData = null;

                        try {
                            errorData = JSON.parse(text);
                        } 
                        
                        catch {
                            // Response is not JSON
                        }

                        if (
                            errorData?.error_code === "CONNECTOR_OUTCOME_UNKNOWN"
                        ) {
                            console.error(
                                "⛔ UNCERTAIN CONNECTOR OUTCOME — RETRY BLOCKED:",
                                job.batch_id
                            );

                            return;
                        }
                       if (
                            errorData?.error === "Connector offline" ||
                            errorData?.error === "TALLY_OFFLINE" ||
                            text.includes("Connector offline") ||
                            text.includes("TALLY_OFFLINE")
                        ) {

                            await BatchStatusManager.markWaitingConnector({
                                batch_id: job.batch_id
                            });

                            console.log(
                                "CONNECTOR OFFLINE — BATCH WAITING:",
                                job.batch_id
                            );

                            return;
                        }

                        throw new Error(
                            `getMasters failed: ${response.status} ${text}`
                        );
                    }

                    const responseText =
                        await response.text();

                    let responseData = null;

                    try {
                        responseData = JSON.parse(responseText);
                    } catch {
                        throw new Error(
                            `getMasters returned invalid JSON: ${responseText}`
                        );
                    }

                  if (responseData?.success === false) {

                    if (
                        responseData?.error_code === "CONNECTOR_OUTCOME_UNKNOWN"
                    ) {
                        console.error(
                            "⛔ UNCERTAIN CONNECTOR OUTCOME — RETRY BLOCKED:",
                            job.batch_id
                        );

                        return;
                    }

                    throw new Error(
                        responseData.error ||
                        "getMasters returned success:false"
                    );
                }

                    console.log(
                        "TALLY_SYNC REQUEST COMPLETED:",
                        job.batch_id
                    );

                    return;

                }
                catch (error) {

    lastError = error;

    const errorMessage =
        error?.message || String(error);

    if (
    errorMessage.includes("Connector disconnected") ||
    errorMessage.includes("transport close") ||
    errorMessage.includes("Connector offline") ||
     errorMessage.includes("TALLY_OFFLINE") ||
    errorMessage.includes("ECONNREFUSED")
) {
        await BatchStatusManager.markWaitingConnector({
            batch_id: job.batch_id
        });

        console.log(
            "🔌 CONNECTOR DISCONNECTED — BATCH WAITING:",
            job.batch_id
        );

        return;
    }

    console.error(
        `TALLY_SYNC ATTEMPT ${attempt} FAILED:`,
        error?.message || error
    );
              if (
                        attempt < MAX_ATTEMPTS
                    ) {

                        await new Promise(
                            resolve =>
                                setTimeout(
                                    resolve,
                                    RETRY_DELAY_MS
                                )
                        );

                    }

                }

            }

            let currentBatch = null;

            try {
                currentBatch = await BatchStatusManager.loadBatch({
                    batch_id: job.batch_id
                });
            } catch (statusError) {
                console.error(
                    "BATCH STATUS CHECK FAILED:",
                    job.batch_id,
                    statusError.message
                );
            }

            if (
                currentBatch?.batch_status === "CLOSED" &&
                currentBatch?.batch_closed === true &&
                currentBatch?.reconciliation_completed === true
            ) {
                console.log(
                    "BATCH ALREADY CLOSED — SKIPPING FAILURE UPDATE:",
                    job.batch_id
                );

                return;
            }

            if (
    typeof currentBatch?.error_message === "string" &&
    currentBatch.error_message.startsWith(
        "CONNECTOR_OUTCOME_UNKNOWN:"
    )
) {
    console.error(
        "⛔ UNCERTAIN BATCH — FAILURE OVERWRITE BLOCKED:",
        job.batch_id
    );

    return;
}

            await BatchStatusManager.markFailed({

                batch_id: job.batch_id,

                error:
                    lastError?.message ||
                    "TALLY_SYNC failed after maximum retries"

            });

            throw lastError ||
                new Error(
                    "TALLY_SYNC failed after maximum retries"
                );
        }


        case "INVOICE_GENERATE":

            console.log(
                "Running INVOICE_GENERATE",
                job.batch_id
            );

            break;


        case "REPORT_GENERATE":

            console.log(
                "Running REPORT_GENERATE",
                job.batch_id
            );

            break;


        default:

            throw new Error(
                "Unknown job type: "
                + job.job_type
            );

    }

}


module.exports = {
    runJob
};