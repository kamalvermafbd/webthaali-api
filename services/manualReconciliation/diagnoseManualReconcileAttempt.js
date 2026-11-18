
"use strict";

const {
    inspectManualReconcileRecovery,
    readManualReconcileRecoverySnapshot
} = require("./inspectManualReconcileRecovery");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function diagnoseManualReconcileAttempt({
    supabase,
    attemptId
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        throw new TypeError(
            "INVALID_MANUAL_RECONCILE_DIAGNOSTIC_INPUT"
        );
    }

    const id = attemptId.toLowerCase();

    const inspection =
        await inspectManualReconcileRecovery({
            supabase,
            attemptId: id
        });

    const snapshot =
        await readManualReconcileRecoverySnapshot(
            supabase,
            id
        );

    if (!snapshot.attempt) {
        return {
            attemptId: id,
            inspectionState: inspection.state,
            attemptFound: false,
            action: "REVIEW"
        };
    }

    const operations = snapshot.operations.map(
        operation => ({
            operationId: operation.operation_id ?? null,
            ordinal: operation.operation_ordinal ?? null,
            type: operation.operation_type ?? null,
            state: operation.state ?? null,
            sourceFetchOperationId:
                operation.source_fetch_operation_id ?? null
        })
    );

    const consistent =
        inspection.reservationStatus ===
            snapshot.status?.status &&
        inspection.executionBound ===
            Boolean(snapshot.execution) &&
        inspection.checkpointRecorded ===
            Boolean(snapshot.checkpoint) &&
        inspection.finalizationState ===
            (snapshot.finalization?.state || null) &&
        inspection.journal?.operationCount ===
            operations.length;

    return {
        attemptId: id,
        companyCode: snapshot.attempt.company_code,
        tallyOwner: snapshot.attempt.tally_owner,
        inspectionState: inspection.state,
        consistentAcrossReads: consistent,
        reservationStatus:
            snapshot.status?.status ?? null,
        executionBound:
            Boolean(snapshot.execution),
        checkpointRecorded:
            Boolean(snapshot.checkpoint),
        finalizationState:
            snapshot.finalization?.state ?? null,
        journal:
            inspection.journal ?? null,
        operations,
        action: !consistent
            ? "RETRY_DIAGNOSTIC"
            : inspection.state === "ALREADY_CLOSED"
                ? "NONE"
                : inspection.state === "EXECUTION_UNCERTAIN"
                    ? "VERIFY_OPERATION_AND_CONNECTOR_SETTLEMENT"
                    : (
                        inspection.state ===
                            "CHECKPOINT_PRESENT_RELEASE_VERIFIED" ||
                        inspection.state ===
                            "FINALIZATION_RECORDED"
                    )
                        ? "EXISTING_RECOVERY_ELIGIBLE_FOR_REVIEW"
                        : "REVIEW"
    };
}

module.exports = {
    diagnoseManualReconcileAttempt
};
