const BatchStatusManager =
    require("../sync-engine/BatchStatusManager");

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

                    console.log(
                        `TALLY_SYNC ATTEMPT ${attempt}/${MAX_ATTEMPTS}:`,
                        job.batch_id
                    );

                    const controller =
                        new AbortController();

                    const timeout =
                        setTimeout(
                            () => controller.abort(),
                            5 * 60 * 1000
                        );

                    let response;

                    try {

                        response = await fetch(
                            `${serverUrl}/getMasters` +
                            `?company_code=${encodeURIComponent(job.company_code)}` +
                            `&tally_owner=${encodeURIComponent(job.tally_owner)}` +
                            `&sync_mode=PERIODIC` +
                            `&sync_period=SIX_MONTHS` +
                            `&worker_batch_id=${encodeURIComponent(job.batch_id)}`,
                            {
                                signal: controller.signal
                            }
                        );

                    } finally {

                        clearTimeout(timeout);

                    }

                    if (!response.ok) {

                        const text =
                            await response.text();

                        throw new Error(
                            `getMasters failed: ${response.status} ${text}`
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