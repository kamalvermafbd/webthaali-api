const BatchExecutor =
    require("./BatchExecutor");

const BatchStatusManager =
    require("./BatchStatusManager");


// ======================================
// RETRY POLICY
// ======================================
//
// Retry only temporary/network errors.
// Maximum 3 attempts.
// Exponential backoff: 2s → 4s → 8s.
//

function isRetryableError(error) {

    const message =
        String(
            error?.message ||
            error ||
            ""
        ).toLowerCase();

    return (
        message.includes("fetch failed") ||
        message.includes("connecttimeouterror") ||
        message.includes("connect timeout") ||
        message.includes("und_err_connect_timeout") ||
        message.includes("network")
    );
}

class RetryManager {

// ----------------------------------
// Retry Failed Operations
// ----------------------------------
async retry({

    batch_id,

    operations = []

}) {

    if (!batch_id) {

        throw new Error(
            "batch_id is required"
        );

    }

    if (!Array.isArray(operations)) {

        throw new Error(
            "operations must be an array"
        );

    }

    if (operations.length === 0) {

        return {

            retried: 0,

            failed: 0,

            skipped: true

        };

    }

    const MAX_RETRIES = 3;

    const BACKOFF_MS = [
        2000,
        4000,
        8000
    ];

    let retried = 0;

    let failed = 0;

    const failedOperations = [];

    for (const operation of operations) {

        let operationSuccess = false;

        let lastError = null;

        for (
            let attempt = 1;
            attempt <= MAX_RETRIES;
            attempt++
        ) {

            try {

                await BatchExecutor.execute(
                    operation
                );

                retried++;

                operationSuccess = true;

                break;

            }
            catch (error) {

                lastError = error;

                const retryable =
                    isRetryableError(error);

                console.error(
                    `RETRY ATTEMPT ${attempt}/${MAX_RETRIES}`,
                    {
                        retryable,
                        error:
                            error?.message ||
                            String(error)
                    }
                );

                if (!retryable) {

                    console.error(
                        "NON-RETRYABLE ERROR. RETRY STOPPED."
                    );

                    break;

                }

                if (
                    attempt < MAX_RETRIES
                ) {

                    const delay =
                        BACKOFF_MS[
                            attempt - 1
                        ];

                    console.log(
                        `RETRYING IN ${delay}ms`
                    );

                    await new Promise(
                        resolve =>
                            setTimeout(
                                resolve,
                                delay
                            )
                    );

                }

            }

        }

        if (!operationSuccess) {

            failed++;

            failedOperations.push({

                operation,

                error:
                    lastError?.message ||
                    String(lastError)

            });

        }

    }

    await BatchStatusManager.incrementRetry({

        batch_id,

        retry_count:
            retried

    });

    return {

        retried,

        failed,

        failedOperations,

        success:
            failed === 0

    };

}



}

module.exports =
    new RetryManager();