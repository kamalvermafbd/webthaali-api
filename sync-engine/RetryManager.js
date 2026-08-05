const BatchExecutor =
    require("./BatchExecutor");

const BatchStatusManager =
    require("./BatchStatusManager");


// ======================================
// TODO
// ======================================
//
// 1. Retry only retryable operations.
// 2. Add max retry limit.
// 3. Add exponential backoff.
//

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

        let retried = 0;

        let failed = 0;

                for (const operation of operations) {

            try {

                await BatchExecutor.execute(

                    operation

                );

                retried++;

            }

           catch (error) {

                failed++;

            operation.retry_error =

                error?.message ||

                String(error);

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

            success:

                failed === 0

        };

}



}

module.exports =
    new RetryManager();