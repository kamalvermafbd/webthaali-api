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

        
let offlineSince = null;

for (
    let attempt = 1;
    ;
    attempt++
) {

    let httpDispatchStarted = false;
let httpResponseReceived = false;

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

  return {
    outcome: "BATCH_LOOKUP_FAILED",
    skipped: true,
    sync_completed: false,
    batch_id: job.batch_id
};
}

if (!retryBatch) {
    console.error(
        "⛔ BATCH NOT FOUND — HTTP DISPATCH BLOCKED:",
        job.batch_id
    );

    return {
    outcome: "BATCH_NOT_FOUND",
    skipped: true,
    sync_completed: false,
    batch_id: job.batch_id
};
}

if (
    typeof job.execution_claim_token !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        job.execution_claim_token
    ) ||
    String(retryBatch.execution_claim_token || "").toLowerCase() !==
        job.execution_claim_token.toLowerCase() ||
    retryBatch.batch_status !== "PROCESSING" ||
    retryBatch.worker_status !== "RUNNING"
) {
    console.error(
        "⛔ STALE OR INVALID EXECUTION CLAIM — HTTP DISPATCH BLOCKED:",
        job.batch_id
    );

    return {
        outcome: "STALE_EXECUTION_CLAIM",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id
    };
}

   if (
    typeof retryBatch?.error_message === "string" &&
    (
        retryBatch.error_message.startsWith(
            "CONNECTOR_OUTCOME_UNKNOWN:"
        ) ||
        retryBatch.error_message.startsWith(
            "HTTP_OUTCOME_UNKNOWN:"
        )
    )
) {
      console.error(
    "⛔ UNCERTAIN BATCH — HTTP RETRY BLOCKED:",
    job.batch_id
);

return {
    outcome: "PREVIOUS_EXECUTION_OUTCOME_UNKNOWN",
    skipped: true,
    sync_completed: false,
    batch_id: job.batch_id
};
    }

    console.log(
    `TALLY_SYNC ATTEMPT ${attempt}:`,
    job.batch_id
);
let response;


const syncServerUrl =
    process.env.BILLEY_SYNC_API_URL ||
    "https://sync-api.billey.in";


const requestUrl =
    `${syncServerUrl}/getMasters` +
    `?company_code=${encodeURIComponent(job.company_code)}` +
    `&tally_owner=${encodeURIComponent(job.tally_owner)}` +
    `&sync_mode=${encodeURIComponent(job.sync_mode)}` +
    `&sync_period=${encodeURIComponent(job.sync_period)}` +
    `&worker_batch_id=${encodeURIComponent(job.batch_id)}`;

httpDispatchStarted = true;
response = await fetch(requestUrl, {
    dispatcher: syncAgent,
    headers: {
        "X-Execution-Claim-Token": job.execution_claim_token
    }
});
httpResponseReceived = true;

                    console.log(
                        "TALLY_SYNC HTTP STATUS:",
                        response.status
                    );

                  let responseBody;

try {
    responseBody = await response.text();
} catch (bodyError) {
    const errorMessage =
        bodyError?.message || String(bodyError);

   try {
    await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `HTTP response body read failed: ${errorMessage}`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ HTTP OUTCOME UNKNOWN — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: Response body read failed: ${errorMessage}`
);
}

console.log(
    "TALLY_SYNC HTTP RESPONSE:",
    responseBody
);

                    if (!response.ok) {

    const text = responseBody;

    let errorData = null;

    try {
        errorData = JSON.parse(text);
    } catch {
        // Response is not JSON
    }

    if (
        response.status === 409 &&
        errorData?.success === false &&
        errorData?.error_code ===
            "HTTP_EXECUTION_IN_PROGRESS"
    ) {
        console.log(
            "HTTP EXECUTION ALREADY OWNED — DUPLICATE REQUEST SKIPPED:",
            job.batch_id
        );

        return {
            outcome: "HTTP_EXECUTION_IN_PROGRESS",
            skipped: true,
            sync_completed: false,
            batch_id: job.batch_id,
            http_status: response.status
        };
    }

        if (
        response.status === 503 &&
        errorData?.success === false &&
        errorData?.error_code ===
            "HTTP_EXECUTION_GUARD_UNAVAILABLE"
    ) {
      console.error(
    "HTTP EXECUTION GUARD UNAVAILABLE — ACQUISITION UNCONFIRMED:",
    job.batch_id
);

        return {
            outcome: "HTTP_EXECUTION_GUARD_UNAVAILABLE",
            skipped: true,
            sync_completed: false,
            batch_id: job.batch_id,
            http_status: response.status
        };
    }

   if (
    (response.status === 409 || response.status === 503) &&
    errorData?.success === false &&
    errorData?.error_code === "HTTP_EXECUTION_NOT_STARTED"
) {
    if (
        response.status === 503 &&
        errorData.reason === "CONNECTOR_OFFLINE" &&
        errorData.batch_id === job.batch_id
    ) {

                       if (offlineSince === null) {
            offlineSince = Date.now();
        }

        const remainingMs =
            30000 - (Date.now() - offlineSince);

        if (remainingMs > 0) {
            console.log(
                "CONNECTOR OFFLINE — RECOVERY WAIT:",
                job.batch_id,
                Math.ceil(remainingMs / 1000)
            );

            await new Promise(resolve =>
                setTimeout(resolve, Math.min(2000, remainingMs))
            );

            continue;
        }

      console.log("🔎 OFFLINE TIMEOUT TOKEN DIAGNOSTIC:", {
    batch_id: job.batch_id,
    token_type: typeof job.execution_claim_token,
    token_length: job.execution_claim_token?.length,
    token_valid_uuid:
        typeof job.execution_claim_token === "string" &&
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            job.execution_claim_token
        )
});
        const failedMarked =
            await BatchStatusManager.markConnectorOfflineTimeoutFailed({
                batch_id: job.batch_id,
                execution_claim_token: job.execution_claim_token
            });

      if (failedMarked !== true) {
    console.error(
        "⛔ CONNECTOR OFFLINE TIMEOUT — ATOMIC FAILURE REJECTED:",
        job.batch_id
    );

    let verifiedBatch;

    try {
        verifiedBatch = await BatchStatusManager.loadBatch({
            batch_id: job.batch_id
        });
    } catch (verifyError) {
        throw new Error(
            "CONNECTOR_OFFLINE_TIMEOUT_OUTCOME_UNKNOWN: " +
            "Transition rejected and database verification failed: " +
            verifyError.message
        );
    }

    const timeoutConfirmed =
        verifiedBatch?.batch_id === job.batch_id &&
        typeof verifiedBatch.execution_claim_token === "string" &&
        verifiedBatch.execution_claim_token.toLowerCase() ===
            job.execution_claim_token.toLowerCase() &&
        verifiedBatch.batch_status === "FAILED" &&
        verifiedBatch.worker_status === "FAILED" &&
        verifiedBatch.current_stage === "FAILED" &&
        verifiedBatch.error_message === "CONNECTOR_OFFLINE_TIMEOUT" &&
        verifiedBatch.connector_retry_ready === false;

    if (timeoutConfirmed) {
        return {
            outcome: "CONNECTOR_OFFLINE_TIMEOUT",
            skipped: true,
            sync_completed: false,
            batch_id: job.batch_id,
            http_status: response.status
        };
    }

    throw new Error(
        "CONNECTOR_OFFLINE_TIMEOUT_TRANSITION_REJECTED: " +
        "Expected terminal state not verified for batch " +
        job.batch_id
    );
}

        return {
            outcome: "CONNECTOR_OFFLINE_TIMEOUT",
            skipped: true,
            sync_completed: false,
            batch_id: job.batch_id,
            http_status: response.status
        };
    }

    return {
        outcome: "HTTP_EXECUTION_NOT_STARTED",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status,
        reason: errorData.reason
    };
}
if (
    response.status === 503 &&
    errorData?.success === false &&
    [
        "HTTP_LIFECYCLE_START_UNVERIFIED",
        "HTTP_EXECUTION_TERMINATION_UNVERIFIED"
    ].includes(errorData?.error_code)
) {
    const marked =
        await BatchStatusManager.markHttpOutcomeUnknown({
            batch_id: job.batch_id,
            error: errorData.error_code,
            execution_claim_token: job.execution_claim_token
        });

    if (marked !== true) {
        throw new Error(
            `HTTP_OUTCOME_UNKNOWN: ${errorData.error_code} mark not confirmed`
        );
    }

    return {
        outcome: errorData.error_code,
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}
                if (
    errorData?.error_code === "CONNECTOR_OUTCOME_UNKNOWN"
) {
    const marked =
        await BatchStatusManager.markHttpOutcomeUnknown({
            batch_id: job.batch_id,
            error: "CONNECTOR_OUTCOME_UNKNOWN",
            execution_claim_token: job.execution_claim_token
        });

    if (marked !== true) {
        throw new Error(
            "CONNECTOR_OUTCOME_UNKNOWN_MARK_NOT_CONFIRMED"
        );
    }

    console.error(
        "⛔ UNCERTAIN CONNECTOR OUTCOME — DURABLE MARK VERIFIED:",
        job.batch_id
    );

    return {
        outcome: "CONNECTOR_OUTCOME_UNKNOWN",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}
                    if (
    errorData?.success === false &&
    (
        errorData?.error === "Connector offline" ||
        errorData?.error === "TALLY_OFFLINE" ||
        errorData?.error_code === "TALLY_OFFLINE"
    )
) {
    console.warn(
        "CONNECTOR OFFLINE — EXECUTION NOT VERIFIED AS NOT_STARTED:",
        job.batch_id
    );

    return {
        outcome: "CONNECTOR_OFFLINE_EXECUTION_UNVERIFIED",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}

                  if (
    response.status === 503 &&
    errorData?.success === false &&
    errorData?.error_code ===
        "DURABLE_CONNECTOR_OPERATION_CLAIM_UNCONFIRMED"
) {
    const marked =
        await BatchStatusManager.markHttpOutcomeUnknown({
            batch_id: job.batch_id,
            error: "DURABLE_CONNECTOR_OPERATION_CLAIM_UNCONFIRMED",
            execution_claim_token: job.execution_claim_token
        });
//check
    if (marked !== true) {
        throw new Error(
            "DURABLE_CLAIM_UNCONFIRMED_MARK_NOT_CONFIRMED"
        );
    }

    return {
        outcome: "DURABLE_CONNECTOR_OPERATION_CLAIM_UNCONFIRMED",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}

if (
    response.status === 500 &&
    errorData?.success === false &&
    errorData?.error_code === "MASTER_SAVE_FAILED"
) {
    let failedBatch = null;

    if (errorData.batch_id === job.batch_id) {
        try {
            failedBatch = await BatchStatusManager.loadBatch({
                batch_id: job.batch_id
            });
        } catch (lookupError) {
            console.error(
                "MASTER SAVE FAILURE VERIFICATION READ FAILED:",
                job.batch_id,
                lookupError
            );
        }
    }

    const storedError = failedBatch?.error_message;

    if (
        failedBatch?.batch_id === job.batch_id &&
        typeof failedBatch.execution_claim_token === "string" &&
        failedBatch.execution_claim_token.toLowerCase() ===
            job.execution_claim_token.toLowerCase() &&
        failedBatch.batch_status === "FAILED" &&
        failedBatch.worker_status === "FAILED" &&
        typeof storedError === "string" &&
        !storedError.startsWith("CONNECTOR_OUTCOME_UNKNOWN:") &&
        !storedError.startsWith("HTTP_OUTCOME_UNKNOWN:")
    ) {
        return {
            outcome: "MASTER_SAVE_FAILED",
            skipped: true,
            sync_completed: false,
            batch_id: job.batch_id,
            http_status: response.status
        };
    }

    // Verification failed: preserve existing unknown-outcome handling.
}

try {
    await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `Unrecognized HTTP failure ${response.status}: ${text}`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ UNRECOGNIZED HTTP FAILURE — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: getMasters failed with unrecognized HTTP ${response.status}`
);
                    }

                   const responseText = responseBody;

                    let responseData = null;

                  try {
    responseData = JSON.parse(responseText);
} catch (parseError) {
    try {
        await BatchStatusManager.markHttpOutcomeUnknown({
            batch_id: job.batch_id,
            error: `HTTP success response contained invalid JSON: ${
                parseError?.message || String(parseError)
            }`,
            execution_claim_token: job.execution_claim_token
        });
    } catch (markError) {
        console.error(
            "⛔ INVALID HTTP JSON — DATABASE MARK FAILED:",
            job.batch_id,
            markError
        );
    }

    throw new Error(
        `HTTP_OUTCOME_UNKNOWN: getMasters returned invalid JSON (HTTP ${response.status})`
    );
}

                 if (responseData?.success !== true) {

  if (
    responseData?.error_code === "CONNECTOR_OUTCOME_UNKNOWN"
) {
    const marked =
        await BatchStatusManager.markHttpOutcomeUnknown({
            batch_id: job.batch_id,
            error: "CONNECTOR_OUTCOME_UNKNOWN",
            execution_claim_token: job.execution_claim_token
        });

    if (marked !== true) {
        throw new Error(
            "CONNECTOR_OUTCOME_UNKNOWN_MARK_NOT_CONFIRMED"
        );
    }

    console.error(
        "⛔ UNCERTAIN CONNECTOR OUTCOME — DURABLE MARK VERIFIED:",
        job.batch_id
    );

    return {
        outcome: "CONNECTOR_OUTCOME_UNKNOWN",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}
   try {
    await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `HTTP success:false response: ${
            responseData?.error ||
            "getMasters did not return success:true"
        }`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ HTTP SUCCESS FALSE — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: getMasters returned success:false (HTTP ${response.status})`
);
}

           const httpVerification =
    await BatchStatusManager.verifyHttpExecutionTermination({
        batch_id: job.batch_id,
        execution_claim_token: job.execution_claim_token,
        invocation_id: responseData?.http_execution_invocation_id
    });

if (httpVerification.verified !== true) {
    console.error(
        "HTTP EXECUTION TERMINATION NOT VERIFIED:",
        job.batch_id,
        httpVerification.reason
    );

    await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `HTTP_EXECUTION_TERMINATION_UNVERIFIED: ${httpVerification.reason}`,
        execution_claim_token: job.execution_claim_token
    });

    return {
        outcome: "HTTP_EXECUTION_TERMINATION_UNVERIFIED",
        skipped: true,
        sync_completed: false,
        batch_id: job.batch_id,
        http_status: response.status
    };
}

console.log(
    "TALLY_SYNC HTTP EXECUTION VERIFIED:",
    job.batch_id
);

return {
    outcome: "HTTP_RESPONSE_RECEIVED",
    skipped: false,
    sync_completed: false,
    batch_id: job.batch_id,
    http_status: response.status,
    response_success: true,
    http_execution_verified: true
};

                }
                catch (error) {

    const errorMessage =
        error?.message || String(error);

    if (!httpDispatchStarted) {
    console.error(
        "⛔ TALLY_SYNC PRE-DISPATCH FAILURE — HTTP NOT STARTED:",
        job.batch_id,
        errorMessage
    );

    throw error;
}

        if (errorMessage.startsWith("HTTP_OUTCOME_UNKNOWN:")) {
    console.error(
        "⛔ HTTP OUTCOME UNKNOWN — AUTOMATIC RETRY BLOCKED:",
        job.batch_id,
        errorMessage
    );

    throw error;
}

const transportErrorCode =
    error?.cause?.code || error?.code || "";

if (
    error?.name === "TypeError" &&
    errorMessage === "fetch failed"
) {
   let marked = false;

try {
    marked = await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `Fetch failed with unknown transport outcome: ${
            error?.cause?.message || errorMessage
        }`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ FETCH OUTCOME UNKNOWN — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

console.error(
    "⛔ FETCH OUTCOME UNKNOWN — AUTOMATIC RETRY BLOCKED:",
    job.batch_id,
    marked
);

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: ${errorMessage}`
);
}

if (
    [
        "UND_ERR_SOCKET",
        "UND_ERR_HEADERS_TIMEOUT",
        "UND_ERR_BODY_TIMEOUT",
        "ETIMEDOUT",
        "ECONNABORTED",
        "EPIPE"
    ].includes(transportErrorCode)
) {
   let marked = false;

try {
    marked = await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `${transportErrorCode}: ${errorMessage}`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ HTTP TRANSPORT OUTCOME UNKNOWN — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

console.error(
    "⛔ HTTP TRANSPORT OUTCOME UNKNOWN — RETRY BLOCKED:",
    job.batch_id,
    transportErrorCode,
    marked
);

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: ${transportErrorCode}: ${errorMessage}`
);
}

 if (
    errorMessage.includes("socket hang up") ||
    error?.code === "ECONNRESET" ||
    error?.cause?.code === "ECONNRESET" ||
    String(error?.cause?.message || "").includes("socket hang up")
) {
    console.error(
        "⛔ HTTP CONNECTION LOST — SERVER EXECUTION MAY STILL BE ACTIVE:",
        job.batch_id,
        errorMessage
    );

  let marked = false;

try {
    marked = await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: errorMessage,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ HTTP CONNECTION LOST — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

console.warn(
    "HTTP OUTCOME UNKNOWN — DATABASE MARK RESULT:",
    job.batch_id,
    marked
);

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: ${errorMessage}`
);
}

  

  console.error(
    "⛔ TALLY_SYNC ERROR — AUTOMATIC RETRY DISABLED:",
    job.batch_id,
    errorMessage
);

if (httpResponseReceived) {
    // Server response received: preserve the known HTTP/application error.
    throw error;
}

let marked = false;

try {
    marked = await BatchStatusManager.markHttpOutcomeUnknown({
        batch_id: job.batch_id,
        error: `Unclassified transport failure: ${errorMessage}`,
        execution_claim_token: job.execution_claim_token
    });
} catch (markError) {
    console.error(
        "⛔ UNCLASSIFIED TRANSPORT FAILURE — DATABASE MARK FAILED:",
        job.batch_id,
        markError
    );
}

console.warn(
    "HTTP OUTCOME UNKNOWN — DATABASE MARK RESULT:",
    job.batch_id,
    marked
);

throw new Error(
    `HTTP_OUTCOME_UNKNOWN: ${errorMessage}`
);
                }

            }

        
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