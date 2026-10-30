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
    execution_claim_token,
    operations = []
}) {

    if (!batch_id) {

        throw new Error(
            "batch_id is required"
        );

    }

    if (
    typeof execution_claim_token !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    execution_claim_token
)
) {
    throw new Error(
        "RetryManager.retry requires a valid execution claim token"
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

        if (operation.operation === "INSERT") {
    failed++;
    failedOperations.push({
        operation,
        error: "INSERT retry skipped: execution outcome may be uncertain"
    });
    continue;
}

        let operationSuccess = false;

        let lastError = null;

        for (
            let attempt = 1;
            attempt <= MAX_RETRIES;
            attempt++
        ) {

            try {

             await BatchExecutor.execute({
                ...operation,
                batch_id,
                execution_claim_token
            });

                retried++;

                operationSuccess = true;

                break;

            }
            catch (error) {

                lastError = error;

                const isFencedInsert =
                operation.operation === "INSERT";

            const isClaimLost =
                error?.code === "EXECUTION_CLAIM_LOST" ||
                String(error?.message || "").includes(
                    "EXECUTION_CLAIM_LOST"
                );

            if (isClaimLost) {
                throw error;
            }

            const retryable =
                !isFencedInsert &&
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