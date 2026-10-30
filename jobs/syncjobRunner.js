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

            const MAX_ATTEMPTS = 1;

            for (
                let attempt = 1;
                attempt <= MAX_ATTEMPTS;
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
        `TALLY_SYNC ATTEMPT ${attempt}/${MAX_ATTEMPTS}:`,
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
                        errorData?.error_code === "CONNECTOR_OUTCOME_UNKNOWN"
                    ) {
                        console.error(
                            "⛔ UNCERTAIN CONNECTOR OUTCOME — RETRY BLOCKED:",
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

                            const waitingMarked =
                                await BatchStatusManager.markWaitingConnector({
                                    batch_id: job.batch_id,
                                    execution_claim_token: job.execution_claim_token
                                });

                            console.log(
                                waitingMarked === true
                                    ? "CONNECTOR OFFLINE — BATCH WAITING:"
                                    : "CONNECTOR OFFLINE — WAITING TRANSITION DEFERRED:",
                                job.batch_id
                            );
                           return {
    outcome: waitingMarked === true
        ? "CONNECTOR_WAITING"
        : "CONNECTOR_WAITING_TRANSITION_DEFERRED",
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
    console.error(
        "⛔ UNCERTAIN CONNECTOR OUTCOME — RETRY BLOCKED:",
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

            console.log(
    "TALLY_SYNC REQUEST COMPLETED:",
    job.batch_id
);

return {
    outcome: "HTTP_RESPONSE_RECEIVED",
    skipped: false,
    sync_completed: false,
    batch_id: job.batch_id,
    http_status: response.status,
    response_success: responseData?.success === true
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