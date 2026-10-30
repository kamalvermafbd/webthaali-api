
"use strict";

const crypto = require("crypto");

const {
    sendChunkedToConnector
} = require("./sendChunkedToConnector");

async function readDurableClaimByOrdinal({
    supabase,
    batchId,
    executionClaimToken,
    operationOrdinal,
    event,
    companyCode,
    tallyOwner,
    connectorId,
    tallyCompanyGuid,
    tallyCompanyName,
    payloadFingerprint
}) {
    let response;

    try {
        response = await supabase.rpc(
            "read_sync_connector_operation_by_ordinal",
            {
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken,
                p_operation_ordinal: operationOrdinal
            }
        );
    } catch {
        return null;
    }

    if (response?.error) {
        return null;
    }

    const rows = response?.data;

    if (!Array.isArray(rows) || rows.length !== 1) {
        return null;
    }

    const operation = rows[0];

    if (
        !operation.request_id ||
        operation.operation_name !== event ||
        operation.operation_ordinal !== operationOrdinal ||
        operation.company_code !== companyCode ||
        operation.tally_owner !== tallyOwner ||
        operation.connector_id !== String(connectorId) ||
        operation.tally_company_guid !== String(tallyCompanyGuid) ||
        operation.tally_company_name !== String(tallyCompanyName) ||
        operation.payload_fingerprint !== payloadFingerprint
    ) {
        return null;
    }

    if (
        ![
            "PREPARED",
            "DISPATCH_MAY_HAVE_OCCURRED",
            "RESULT_VERIFIED",
            "OUTCOME_UNKNOWN"
        ].includes(operation.operation_state)
    ) {
        return null;
    }

    return operation;
}

async function confirmDurableVerification({
    supabase,
    requestId,
    batchId,
    executionClaimToken,
    operationOrdinal,
    event,
    companyCode,
    tallyOwner,
    connectorId,
    tallyCompanyGuid,
    tallyCompanyName,
    payloadFingerprint,
    expectedResultPayload
}) {
    let response;

    try {
        response = await supabase.rpc(
            "read_sync_connector_operation",
            {
                p_request_id: requestId,
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken
            }
        );
    } catch {
        return false;
    }

    if (response?.error) {
        return false;
    }

    const rows = response?.data;

    if (!Array.isArray(rows) || rows.length !== 1) {
        return false;
    }

    const operation = rows[0];

    if (
        operation.request_id !== requestId ||
        operation.operation_state !== "RESULT_VERIFIED" ||
        operation.operation_name !== event ||
        operation.operation_ordinal !== operationOrdinal ||
        operation.company_code !== companyCode ||
        operation.tally_owner !== tallyOwner ||
        operation.connector_id !== String(connectorId) ||
        operation.tally_company_guid !== String(tallyCompanyGuid) ||
        operation.tally_company_name !== String(tallyCompanyName) ||
        operation.payload_fingerprint !== payloadFingerprint ||
        !operation.result_verified_at
    ) {
        return false;
    }

    const savedResult = operation.result_payload;

    if (
        !savedResult ||
        savedResult.requestId !== requestId ||
        savedResult.success !== true
    ) {
        return false;
    }

    // Compare JSON content without relying on object key ordering.
    const sameJson = (a, b) => {
        if (a === b) return true;

        if (
            a === null ||
            b === null ||
            typeof a !== "object" ||
            typeof b !== "object"
        ) {
            return false;
        }

        if (Array.isArray(a) !== Array.isArray(b)) {
            return false;
        }

        const keysA = Object.keys(a).sort();
        const keysB = Object.keys(b).sort();

        if (
            keysA.length !== keysB.length ||
            keysA.some((key, index) => key !== keysB[index])
        ) {
            return false;
        }

        return keysA.every(key => sameJson(a[key], b[key]));
    };

    return sameJson(savedResult, expectedResultPayload);
}

function createUncertainError(message, cause, options = {}) {
    const error = new Error(message);

    if (cause) {
        error.cause = cause;
    }

    error.requestDispatched =
        options.requestDispatched !== false;

    error.connectorResponseReceived = false;

    error.durableVerificationUnconfirmed =
        options.durableVerificationUnconfirmed !== false;

    if (options.dispatchMarkUnconfirmed === true) {
        error.dispatchMarkUnconfirmed = true;
    }

    return error;
}

async function executeDurableConnectorOperation(options) {
    const {
        supabase,
        socket,
        event,
        payload,
        connectorId,
        batchId,
        executionClaimToken,
        operationOrdinal,
        companyCode,
        tallyOwner,
        tallyCompanyGuid,
        tallyCompanyName
    } = options;

    if (
        !supabase ||
        !socket ||
        !event ||
        !payload ||
        !connectorId ||
        !batchId ||
        !executionClaimToken ||
        !companyCode ||
        !tallyOwner ||
        !tallyCompanyGuid ||
        !tallyCompanyName ||
        !Number.isSafeInteger(operationOrdinal) ||
        operationOrdinal < 1
    ) {
        throw new Error(
            "INVALID_DURABLE_CONNECTOR_OPERATION_CONTEXT"
        );
    }

    const payloadFingerprint = crypto
        .createHash("sha256")
        .update(JSON.stringify(payload))
        .digest("hex");

    let claim;
    let claimError;

    try {
        const response = await supabase.rpc(
            "claim_sync_connector_operation",
            {
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken,
                p_operation_ordinal: operationOrdinal,
                p_operation_name: event,
                p_company_code: companyCode,
                p_tally_owner: tallyOwner,
                p_connector_id: String(connectorId),
                p_tally_company_guid: String(tallyCompanyGuid),
                p_tally_company_name: String(tallyCompanyName),
                p_payload_fingerprint: payloadFingerprint
            }
        );

        claim = response.data;
        claimError = response.error;
    } catch (cause) {
    const existingOperation = await readDurableClaimByOrdinal({
        supabase,
        batchId,
        executionClaimToken,
        operationOrdinal,
        event,
        companyCode,
        tallyOwner,
        connectorId,
        tallyCompanyGuid,
        tallyCompanyName,
        payloadFingerprint
    });

    const error = new Error(
        "DURABLE_CONNECTOR_OPERATION_CLAIM_UNCONFIRMED"
    );

    error.cause = cause;
    error.durableClaimUnconfirmed = true;

    if (existingOperation) {
        error.durableOperationState =
            existingOperation.operation_state;

        error.durableOperationRequestId =
            existingOperation.request_id;
    }

    throw error;
}

  if (claimError) {
    const existingOperation = await readDurableClaimByOrdinal({
        supabase,
        batchId,
        executionClaimToken,
        operationOrdinal,
        event,
        companyCode,
        tallyOwner,
        connectorId,
        tallyCompanyGuid,
        tallyCompanyName,
        payloadFingerprint
    });

    const error = new Error(
        "DURABLE_CONNECTOR_OPERATION_CLAIM_UNCONFIRMED: " +
        claimError.message
    );

    error.cause = claimError;
    error.durableClaimUnconfirmed = true;

    if (existingOperation) {
        error.durableOperationState =
            existingOperation.operation_state;

        error.durableOperationRequestId =
            existingOperation.request_id;
    }

    throw error;
}

    if (
    !Array.isArray(claim) ||
    claim.length !== 1 ||
    claim[0]?.dispatch_allowed !== true ||
    claim[0]?.operation_state !== "PREPARED" ||
    !claim[0]?.request_id
) {
    const existingOperation = await readDurableClaimByOrdinal({
        supabase,
        batchId,
        executionClaimToken,
        operationOrdinal,
        event,
        companyCode,
        tallyOwner,
        connectorId,
        tallyCompanyGuid,
        tallyCompanyName,
        payloadFingerprint
    });

    const error = new Error(
        "DURABLE_CONNECTOR_OPERATION_CLAIM_NOT_ELIGIBLE"
    );

    error.durableClaimUnconfirmed = true;

    if (existingOperation) {
        error.durableOperationState =
            existingOperation.operation_state;

        error.durableOperationRequestId =
            existingOperation.request_id;
    }

    throw error;
}
const confirmedClaim = await readDurableClaimByOrdinal({
    supabase,
    batchId,
    executionClaimToken,
    operationOrdinal,
    event,
    companyCode,
    tallyOwner,
    connectorId,
    tallyCompanyGuid,
    tallyCompanyName,
    payloadFingerprint
});

if (
    !confirmedClaim ||
    confirmedClaim.request_id !== claim[0].request_id ||
    confirmedClaim.operation_state !== "PREPARED"
) {
    const error = new Error(
        "DURABLE_CONNECTOR_OPERATION_CLAIM_IDENTITY_UNCONFIRMED"
    );

    error.durableClaimUnconfirmed = true;

    throw error;
}
    const requestId = claim[0].request_id;

  const beforeDispatch = async () => {
    let data;
    let error;

    try {
        const response = await supabase.rpc(
            "mark_sync_connector_operation_dispatch",
            {
                p_request_id: requestId,
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken
            }
        );

        data = response.data;
        error = response.error;

    } catch (cause) {
        throw createUncertainError(
            "DURABLE_CONNECTOR_DISPATCH_MARK_UNCONFIRMED",
            cause,
            {
                requestDispatched: false,
                durableVerificationUnconfirmed: false,
                dispatchMarkUnconfirmed: true
            }
        );
    }

    if (error || data !== true) {
        throw createUncertainError(
            "DURABLE_CONNECTOR_DISPATCH_MARK_FAILED: " +
            (error?.message || "Dispatch not authorized"),
            error,
            {
                requestDispatched: false,
                durableVerificationUnconfirmed: false,
                dispatchMarkUnconfirmed: true
            }
        );
    }
};

const verifyAfterTransfer = async (result, signal) => {
    if (signal.aborted) {
        throw createUncertainError(
            "DURABLE_CONNECTOR_LOCK_LOST_BEFORE_VERIFICATION"
        );
    }

    if (
        result?.success !== true ||
        result?.requestId !== requestId
    ) {
        throw createUncertainError(
            "DURABLE_CONNECTOR_RESULT_INVALID"
        );
    }

       let verified;
    let verifyError;

    try {
        const response = await supabase.rpc(
            "verify_sync_connector_operation_result",
            {
                p_request_id: requestId,
                p_batch_id: batchId,
                p_execution_claim_token: executionClaimToken,
                p_result_payload: {
                    requestId: result.requestId,
                    success: result.success,
                    summary: result.summary || {}
                }
            }
        );

        verified = response.data;
        verifyError = response.error;
    } catch (cause) {
        const confirmed = await confirmDurableVerification({
            supabase,
            requestId,
            batchId,
            executionClaimToken,
            operationOrdinal,
            event,
            companyCode,
            tallyOwner,
            connectorId,
            tallyCompanyGuid,
            tallyCompanyName,
            payloadFingerprint,
            expectedResultPayload: {
                requestId: result.requestId,
                success: result.success,
                summary: result.summary || {}
            }
        });

        if (confirmed === true) {
            return result;
        }

        throw createUncertainError(
            "DURABLE_CONNECTOR_RESULT_VERIFICATION_UNCONFIRMED",
            cause
        );
    }

    if (verifyError || verified !== true) {
        const confirmed = await confirmDurableVerification({
            supabase,
            requestId,
            batchId,
            executionClaimToken,
            operationOrdinal,
            event,
            companyCode,
            tallyOwner,
            connectorId,
            tallyCompanyGuid,
            tallyCompanyName,
            payloadFingerprint,
            expectedResultPayload: {
                requestId: result.requestId,
                success: result.success,
                summary: result.summary || {}
            }
        });

        if (confirmed === true) {
            return result;
        }

        throw createUncertainError(
            "DURABLE_CONNECTOR_RESULT_VERIFICATION_FAILED: " +
            (verifyError?.message || "Verification not confirmed"),
            verifyError
        );
    }

    return result;
};

    const result = await sendChunkedToConnector(
    socket,
    event,
    payload,
    connectorId,
    {
        batch_id: batchId,
        execution_claim_token: executionClaimToken,
        request_id: requestId,

        // Durable execution identity
        identity_scope: JSON.stringify([
            "billey-tally-v1",
            String(connectorId),
            String(companyCode),
            String(tallyOwner)
        ]),
        company_identity: `guid:${String(tallyCompanyGuid)}`,
        payload_fingerprint: `sha256:${payloadFingerprint}`,

        before_dispatch: beforeDispatch,
after_transfer: verifyAfterTransfer
    }
);

   

    return result;
}

module.exports = {
    executeDurableConnectorOperation
};
