
"use strict";

const {
    readManualReconcileReleaseReceipt
} = require("../../socketio/connectorHealthStore");

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const VERIFIED = "RESULT_VERIFIED";

const OPERATION_TYPES = new Set([
    "TALLY_FETCH",
    "LEDGER_APPLY",
    "VOUCHER_FETCH",
    "VOUCHER_APPLY"
]);

function fail(reason) {
    throw new Error(`RECONCILE_RECOVERY_${reason}`);
}

function same(a, b) {
    return a === b;
}

function sameJson(a, b) {
    if (a === b) return true;

    if (Array.isArray(a) || Array.isArray(b)) {
        return (
            Array.isArray(a) &&
            Array.isArray(b) &&
            a.length === b.length &&
            a.every((item, index) => sameJson(item, b[index]))
        );
    }

    if (
        a &&
        b &&
        typeof a === "object" &&
        typeof b === "object"
    ) {
        const keysA = Object.keys(a);
        const keysB = Object.keys(b);

        return (
            keysA.length === keysB.length &&
            keysA.every(
                key =>
                    Object.prototype.hasOwnProperty.call(b, key) &&
                    sameJson(a[key], b[key])
            )
        );
    }

    return false;
}

async function readOne(supabase, table, field, value) {
    const { data, error } = await supabase
        .from(table)
        .select("*")
        .eq(field, value)
        .maybeSingle();

    if (error) {
        throw new Error(
            `RECONCILE_RECOVERY_READ_FAILED:${table}`,
            { cause: error }
        );
    }

    return data;
}

async function readOperations(supabase, attemptId) {
    const rows = [];
    const pageSize = 500;

    for (let offset = 0; ; offset += pageSize) {
        const { data, error } = await supabase
            .from("manual_reconcile_operations")
            .select(
                "operation_id, attempt_id, reservation_id, " +
                "operation_token, execution_id, operation_ordinal, " +
                "operation_type, state, source_fetch_operation_id"
            )
            .eq("attempt_id", attemptId)
            .order("operation_ordinal", { ascending: true })
            .order("operation_id", { ascending: true })
            .range(offset, offset + pageSize - 1);

        if (error || !Array.isArray(data)) {
            throw new Error(
                "RECONCILE_RECOVERY_JOURNAL_READ_FAILED",
                { cause: error }
            );
        }

        rows.push(...data);

        if (data.length < pageSize) {
            return rows;
        }
    }
}

function inspectJournal(operations, attempt, execution) {
    const ids = new Set();
    const ordinals = new Set();
    const byId = new Map();
    const applyCounts = new Map();

    let unverifiedCount = 0;

    for (const operation of operations) {
        if (
            !UUID.test(operation.operation_id || "") ||
            !same(operation.attempt_id, attempt.attempt_id) ||
            !same(operation.reservation_id, attempt.reservation_id) ||
            !same(operation.operation_token, attempt.operation_token) ||
            !execution ||
            !same(operation.execution_id, execution.execution_id) ||
            !OPERATION_TYPES.has(operation.operation_type) ||
            !Number.isSafeInteger(operation.operation_ordinal) ||
            operation.operation_ordinal < 0 ||
            ids.has(operation.operation_id) ||
            ordinals.has(operation.operation_ordinal)
        ) {
            fail("JOURNAL_IDENTITY_INVALID");
        }

        ids.add(operation.operation_id);
        ordinals.add(operation.operation_ordinal);
        byId.set(operation.operation_id, operation);

        if (operation.state !== VERIFIED) {
            unverifiedCount++;
        }
    }

    for (const operation of operations) {
        const isLedgerApply =
            operation.operation_type === "LEDGER_APPLY";

        const isVoucherApply =
            operation.operation_type === "VOUCHER_APPLY";

        if (!isLedgerApply && !isVoucherApply) {
            continue;
        }

        const source = byId.get(
            operation.source_fetch_operation_id
        );

        const expectedType = isLedgerApply
            ? "TALLY_FETCH"
            : "VOUCHER_FETCH";

        if (
            !source ||
            source.operation_type !== expectedType ||
            (
                isVoucherApply &&
                source.operation_ordinal + 1 !==
                    operation.operation_ordinal
            )
        ) {
            fail("JOURNAL_APPLY_LINK_INVALID");
        }

        const count =
            (applyCounts.get(source.operation_id) || 0) + 1;

        applyCounts.set(source.operation_id, count);

        if (count > 1) {
            fail("JOURNAL_DUPLICATE_APPLY");
        }
    }

    let incompleteFetchCount = 0;

    for (const operation of operations) {
        if (
            operation.operation_type !== "TALLY_FETCH" &&
            operation.operation_type !== "VOUCHER_FETCH"
        ) {
            continue;
        }

        if (applyCounts.get(operation.operation_id) !== 1) {
            incompleteFetchCount++;
        }
    }

    return {
        operationCount: operations.length,
        verifiedCount:
            operations.length - unverifiedCount,
        unverifiedCount,
        incompleteFetchCount,
        allVerified:
            unverifiedCount === 0 &&
            incompleteFetchCount === 0
    };
}

function verifyIdentity(
    attempt,
    execution,
    checkpoint,
    finalization
) {
    if (
        !UUID.test(attempt.attempt_id || "") ||
        !UUID.test(attempt.operation_token || "") ||
        typeof attempt.company_code !== "string" ||
        !attempt.company_code.trim() ||
        !["USER", "CA"].includes(attempt.tally_owner)
    ) {
        fail("ATTEMPT_IDENTITY_INVALID");
    }

    for (const record of [
        execution,
        checkpoint,
        finalization
    ]) {
        if (!record) continue;

        if (
            !same(record.attempt_id, attempt.attempt_id) ||
            !same(record.reservation_id, attempt.reservation_id) ||
            !same(record.operation_token, attempt.operation_token)
        ) {
            fail("RECORD_IDENTITY_MISMATCH");
        }
    }

    if (checkpoint && !execution) {
        fail("CHECKPOINT_WITHOUT_EXECUTION");
    }

    if (finalization && (!checkpoint || !execution)) {
        fail("FINALIZATION_EVIDENCE_INCOMPLETE");
    }

    if (execution) {
        if (
            !UUID.test(execution.execution_id || "") ||
            !UUID.test(execution.owner_token || "") ||
            !UUID.test(execution.server_instance_id || "") ||
            !execution.connector_id ||
            !execution.socket_id
        ) {
            fail("EXECUTION_IDENTITY_INVALID");
        }
    }

    if (checkpoint) {
        if (
            !same(
                checkpoint.execution_id,
                execution.execution_id
            ) ||
            !same(
                checkpoint.connector_id,
                execution.connector_id
            ) ||
            !same(
                checkpoint.owner_token,
                execution.owner_token
            ) ||
            checkpoint.final_recheck_completed !== true ||
            !Array.isArray(
                checkpoint.remaining_mismatch_ledger_guids
            )
        ) {
            fail("CHECKPOINT_IDENTITY_MISMATCH");
        }
    }

    if (!finalization) return;

    const evidence = finalization.release_evidence;

    if (
        !["EVIDENCE_RECORDED", "CLOSED"].includes(
            finalization.state
        ) ||
        finalization.final_recheck_completed !== true ||
        finalization.connector_release_verified !== true ||
        !evidence ||
        typeof evidence !== "object" ||
        Array.isArray(evidence) ||
        evidence.redisReleaseConfirmed !== true ||
        !same(evidence.attemptId, attempt.attempt_id) ||
        !same(
            evidence.executionId,
            execution.execution_id
        ) ||
        !same(
            evidence.serverInstanceId,
            execution.server_instance_id
        ) ||
        !same(
            evidence.connectorId,
            execution.connector_id
        ) ||
        !same(
            evidence.socketId,
            execution.socket_id
        ) ||
        !same(
            evidence.ownerToken,
            execution.owner_token
        ) ||
        !same(
            finalization.connector_id,
            execution.connector_id
        ) ||
        !same(
            finalization.release_owner_token,
            execution.owner_token
        ) ||
        !sameJson(
            finalization.remaining_mismatch_ledger_guids,
            checkpoint.remaining_mismatch_ledger_guids
        )
    ) {
        fail("FINALIZATION_EVIDENCE_INVALID");
    }
}

async function inspectManualReconcileRecovery({
    supabase,
    attemptId
}) {
    if (
        !supabase ||
        typeof supabase.from !== "function" ||
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        fail("INPUT_INVALID");
    }

    const attempt = await readOne(
        supabase,
        "manual_reconcile_attempts",
        "attempt_id",
        attemptId
    );

    if (!attempt) {
        return {
            attemptId,
            state: "ATTEMPT_NOT_FOUND",
            recoveryRequiresVerification: true
        };
    }

    const [
        execution,
        checkpoint,
        finalization,
        status,
        operations,
        redisReceipt
    ] = await Promise.all([
        readOne(
            supabase,
            "manual_reconcile_executions",
            "attempt_id",
            attemptId
        ),
        readOne(
            supabase,
            "manual_reconcile_pre_release_checkpoints",
            "attempt_id",
            attemptId
        ),
        readOne(
            supabase,
            "manual_reconcile_finalizations",
            "attempt_id",
            attemptId
        ),
        readOne(
            supabase,
            "manual_reconcile_status",
            "id",
            attempt.reservation_id
        ),
        readOperations(supabase, attemptId),
        readManualReconcileReleaseReceipt(attemptId)
    ]);

    if (
        !status ||
        status.company_code !== attempt.company_code ||
        status.tally_owner !== attempt.tally_owner
    ) {
        fail("RESERVATION_SCOPE_INVALID");
    }

    verifyIdentity(
        attempt,
        execution,
        checkpoint,
        finalization
    );

    let redisReleaseReceiptVerified = false;

    if (redisReceipt) {
        if (
            !execution ||
            !checkpoint ||
            redisReceipt.attemptId !== attempt.attempt_id ||
            redisReceipt.executionId !== execution.execution_id ||
            redisReceipt.serverInstanceId !== execution.server_instance_id ||
            redisReceipt.connectorId !== execution.connector_id ||
            redisReceipt.socketId !== execution.socket_id ||
            redisReceipt.ownerToken !== execution.owner_token ||
            redisReceipt.redisReleaseConfirmed !== true
        ) {
            fail("REDIS_RELEASE_RECEIPT_IDENTITY_MISMATCH");
        }

        redisReleaseReceiptVerified = true;
    }

    const journal = inspectJournal(
        operations,
        attempt,
        execution
    );

    const reservationOwned =
        status.status === "RUNNING" &&
        status.operation_token === attempt.operation_token;

    let state = "RECOVERY_MANUAL_REVIEW_REQUIRED";

    if (finalization?.state === "CLOSED") {
        if (journal.allVerified) {
            state = "ALREADY_CLOSED";
        }
    } else if (!reservationOwned) {
        state = "RESERVATION_NOT_OWNED";
    } else if (
        finalization?.state === "EVIDENCE_RECORDED" &&
        journal.allVerified
    ) {
        state = "FINALIZATION_RECORDED";
    } else if (
        checkpoint &&
        journal.allVerified
    ) {
        state =
            "CHECKPOINT_PRESENT_RELEASE_UNVERIFIED";
    } else if (execution) {
        state = "EXECUTION_UNCERTAIN";
    } else if (operations.length === 0) {
        state = "ADMISSION_ONLY";
    }

    return {
        attemptId,
        state,
        reservationStatus: status.status,
        reservationOwned,
        executionBound: Boolean(execution),
        checkpointRecorded: Boolean(checkpoint),
        redisReleaseReceiptPresent: Boolean(redisReceipt),
        redisReleaseReceiptVerified,
        coordinatorCompletionVerified:
            finalization?.state === "EVIDENCE_RECORDED" ||
            finalization?.state === "CLOSED",
        finalizationState:
            finalization?.state || null,
        journal,
        recoveryRequiresVerification: true
    };
}

module.exports = {
    inspectManualReconcileRecovery
};
