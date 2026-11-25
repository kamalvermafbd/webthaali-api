
"use strict";

// Manual Reconciliation failure recovery only. Not used by Normal Sync.
// Redis primitives do NOT independently prove that SQL or Tally has settled.
const Redis = require("ioredis");

const redis = new Redis({
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: Number(process.env.REDIS_PORT || 6379),
    lazyConnect: true,
    connectTimeout: 3000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false
});

redis.on("error", error => {
    console.error("[MANUAL FAILURE RECOVERY REDIS]", error.message);
});

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function ready() {
    if (redis.status === "wait") await redis.connect();

    if (redis.status !== "ready") {
        throw new Error("MANUAL_FAILURE_REDIS_NOT_READY");
    }
}

function identity(input) {
    if (
        !input ||
        typeof input !== "object" ||
        Array.isArray(input)
    ) {
        throw new Error("MANUAL_FAILURE_IDENTITY_INVALID");
    }

    const {
        attemptId,
        executionId,
        serverInstanceId,
        connectorId,
        socketId,
        ownerToken
    } = input;

    if (
        ![attemptId, executionId, serverInstanceId, ownerToken]
            .every(v => typeof v === "string" && UUID.test(v)) ||
        ![connectorId, socketId]
            .every(v =>
                typeof v === "string" &&
                v.trim() &&
                v === v.trim()
            )
    ) {
        throw new Error("MANUAL_FAILURE_IDENTITY_INVALID");
    }

    return {
        attemptId,
        executionId,
        serverInstanceId,
        connectorId,
        socketId,
        ownerToken
    };
}


function keys(i) {
    return [
        `billey:connector:operation-lock:${i.connectorId}`,
        `billey:socket:operation-lock:${i.socketId}`,
        `billey:connector:operation-active:${i.connectorId}`,
        `billey:socket:operation-active:${i.socketId}`,
        `billey:connector:quarantine:${i.connectorId}`,
        `billey:socket:quarantine:${i.socketId}`,
        `billey:manual-reconcile:failure-stage:${i.attemptId}`,
        `billey:manual-reconcile:release-receipt:${i.attemptId}`,
        `billey:manual-reconcile:failure-unblock:${i.attemptId}`
    ];
}



async function stageManualReconcileFailureCleanup(input) {
    const i = identity(input);

    const authorization = input?.authorization;

    if (
        !authorization ||
        authorization.authorized !== true ||
        authorization.reason !== "REDIS_STAGE_AUTHORIZED" ||
        authorization.connector_id !== i.connectorId ||
        authorization.socket_id !== i.socketId ||
        authorization.owner_token !== i.ownerToken
    ) {
        throw new Error(
            "MANUAL_FAILURE_STAGE_AUTHORIZATION_INVALID"
        );
    }

    const receipt = {
        attemptId: i.attemptId,
        executionId: i.executionId,
        serverInstanceId: i.serverInstanceId,
        connectorId: i.connectorId,
        socketId: i.socketId,
        ownerToken: i.ownerToken,
        stage: "LOCKS_CLEARED_MARKERS_RETAINED",
        markersRetained: true
    };

    await ready();

    const result = await redis.eval(
        MANUAL_FAILURE_STAGE_LUA,
        8,
        ...keys(i).slice(0, 8),
        i.ownerToken,
        i.connectorId,
        i.socketId,
        JSON.stringify(receipt),
        i.attemptId,
        i.executionId,
        i.serverInstanceId
    );

    if (
        !Array.isArray(result) ||
        result.length !== 2
    ) {
        throw new Error(
            "MANUAL_FAILURE_STAGE_RESULT_INVALID"
        );
    }

    if (
        Number(result[0]) !== 1 ||
        result[1] !== "LOCKS_CLEARED_MARKERS_RETAINED"
    ) {
        return {
            staged: false,
            reason: String(result[1] || "STAGE_NOT_CONFIRMED")
        };
    }

    const stored =
        await readManualReconcileFailureCleanupReceipt(
            i.attemptId
        );

    if (
        !stored ||
        stored.attemptId !== i.attemptId ||
        stored.executionId !== i.executionId ||
        stored.serverInstanceId !== i.serverInstanceId ||
        stored.connectorId !== i.connectorId ||
        stored.socketId !== i.socketId ||
        stored.ownerToken !== i.ownerToken ||
        stored.stage !== receipt.stage ||
        stored.markersRetained !== true
    ) {
        throw new Error(
            "MANUAL_FAILURE_STAGE_RECEIPT_NOT_VERIFIED"
        );
    }

    return {
        staged: true,
        reason: "LOCKS_CLEARED_MARKERS_RETAINED",
        receipt: stored
    };
}


// Read-only receipt inspection. No Redis keys are deleted.
async function readManualReconcileFailureCleanupReceipt(attemptId) {
    if (
        typeof attemptId !== "string" ||
        !UUID.test(attemptId)
    ) {
        throw new Error("MANUAL_FAILURE_ATTEMPT_ID_INVALID");
    }

    await ready();

    const raw = await redis.get(
        `billey:manual-reconcile:failure-stage:${attemptId}`
    );

    if (raw === null) return null;

    let receipt;

    try {
        receipt = JSON.parse(raw);
    } catch (_) {
        throw new Error("MANUAL_FAILURE_STAGE_RECEIPT_CORRUPT");
    }

    if (
        !receipt ||
        receipt.attemptId !== attemptId ||
        receipt.stage !== "LOCKS_CLEARED_MARKERS_RETAINED" ||
        receipt.markersRetained !== true ||
        !UUID.test(receipt.executionId || "") ||
        !UUID.test(receipt.serverInstanceId || "") ||
        !UUID.test(receipt.ownerToken || "") ||
        typeof receipt.connectorId !== "string" ||
        !receipt.connectorId ||
        typeof receipt.socketId !== "string" ||
        !receipt.socketId
    ) {
        throw new Error("MANUAL_FAILURE_STAGE_RECEIPT_INVALID");
    }

    return Object.freeze(receipt);
}



async function readManualReconcileFailureUnblockReceipt(attemptId) {
    if (typeof attemptId !== "string" || !UUID.test(attemptId)) {
        throw new Error("MANUAL_FAILURE_ATTEMPT_ID_INVALID");
    }

    await ready();

    const raw = await redis.get(
        `billey:manual-reconcile:failure-unblock:${attemptId}`
    );

    if (raw === null) return null;

    let receipt;

    try {
        receipt = JSON.parse(raw);
    } catch (_) {
        throw new Error("MANUAL_FAILURE_UNBLOCK_RECEIPT_CORRUPT");
    }

    if (
        !receipt ||
        typeof receipt !== "object" ||
        Array.isArray(receipt) ||
        receipt.attemptId !== attemptId ||
        !UUID.test(receipt.executionId || "") ||
        !UUID.test(receipt.serverInstanceId || "") ||
        !UUID.test(receipt.ownerToken || "") ||
        typeof receipt.connectorId !== "string" ||
        !receipt.connectorId ||
        typeof receipt.socketId !== "string" ||
        !receipt.socketId ||
        receipt.stage !== "UNBLOCKED_AFTER_SQL_CLOSURE" ||
        receipt.markersCleared !== true
    ) {
        throw new Error("MANUAL_FAILURE_UNBLOCK_RECEIPT_INVALID");
    }

    return Object.freeze(receipt);
}

async function unblockManualReconcileAfterSqlClosure(input) {
    const i = identity(input);
    const supabase = input?.supabase;

    if (!supabase || typeof supabase.from !== "function") {
        throw new Error("MANUAL_FAILURE_SQL_CLIENT_INVALID");
    }

    // SQL closure must be confirmed before Redis mutation.
    const { data: recovery, error: recoveryError } =
        await supabase
            .from("manual_reconcile_failure_recovery_evidence")
         
            .select(
                "attempt_id,execution_id,connector_id," +
                "socket_id,owner_token," +
                "coordinator_server_instance_id," +
                "recovery_state,remote_execution_fenced," +
                "redis_cleanup_confirmed," +
                "redis_stage_receipt,redis_stage_confirmed_at," +
                "terminal_closed_at,coordinator_unsafe_detected"
            )

            .eq("attempt_id", i.attemptId)
            .single();

    if (recoveryError || !recovery) {
        throw new Error("MANUAL_FAILURE_SQL_CLOSURE_UNVERIFIED");
    }

    const receipt = recovery.redis_stage_receipt;

    if (
        recovery.attempt_id !== i.attemptId ||
        recovery.execution_id !== i.executionId ||
        recovery.connector_id !== i.connectorId ||
        recovery.socket_id !== i.socketId ||
        recovery.owner_token !== i.ownerToken ||

        recovery.coordinator_server_instance_id !== i.serverInstanceId ||
        recovery.coordinator_unsafe_detected !== false ||
        recovery.recovery_state !== "SETTLED" ||
        recovery.remote_execution_fenced !== true ||

        recovery.redis_cleanup_confirmed !== true ||
        !recovery.redis_stage_confirmed_at ||
        !recovery.terminal_closed_at ||
        !receipt ||
        typeof receipt !== "object" ||
        Array.isArray(receipt) ||
        receipt.attemptId !== i.attemptId ||
        receipt.executionId !== i.executionId ||
        receipt.serverInstanceId !== i.serverInstanceId ||
        receipt.connectorId !== i.connectorId ||
        receipt.socketId !== i.socketId ||
        receipt.ownerToken !== i.ownerToken ||
        receipt.stage !== "LOCKS_CLEARED_MARKERS_RETAINED" ||
        receipt.markersRetained !== true
    ) {
        throw new Error(
            "MANUAL_FAILURE_SQL_CLOSURE_IDENTITY_INVALID"
        );
    }

    const redisReceipt =
        await readManualReconcileFailureCleanupReceipt(i.attemptId);

    if (
        !redisReceipt ||
        Object.keys(receipt).length !==
            Object.keys(redisReceipt).length ||
        Object.keys(receipt).some(
            key => receipt[key] !== redisReceipt[key]
        )
    ) {
        throw new Error(
            "MANUAL_FAILURE_SQL_REDIS_RECEIPT_MISMATCH"
        );
    }

    const unblockReceipt = {
        attemptId: i.attemptId,
        executionId: i.executionId,
        serverInstanceId: i.serverInstanceId,
        connectorId: i.connectorId,
        socketId: i.socketId,
        ownerToken: i.ownerToken,
        stage: "UNBLOCKED_AFTER_SQL_CLOSURE",
        markersCleared: true
    };

    await ready();

    const result = await redis.eval(
        `
        local function sameJson(actual, expected)
            local okA, a = pcall(cjson.decode, actual)
            local okB, b = pcall(cjson.decode, expected)

            if not okA or not okB
               or type(a) ~= "table"
               or type(b) ~= "table" then
                return false
            end

            for k, v in pairs(b) do
                if a[k] ~= v then
                    return false
                end
            end

            for k, _ in pairs(a) do
                if b[k] == nil then
                    return false
                end
            end

            return true
        end

     
        -- Existing immutable receipt must not hide
        -- conflicting or recreated ownership markers.
        local previous = redis.call("GET", KEYS[9])

        if previous then
            if not sameJson(previous, ARGV[5]) then
                return {0, "UNBLOCK_RECEIPT_CONFLICT"}
            end

            for index = 1, 6 do
                if redis.call("EXISTS", KEYS[index]) == 1 then
                    return {0, "UNBLOCK_MARKERS_REAPPEARED"}
                end
            end

            return {1, "ALREADY_UNBLOCKED"}
        end


        local stage = redis.call("GET", KEYS[7])

        if not stage then
            return {0, "STAGE_RECEIPT_MISSING"}
        end

        if not sameJson(stage, ARGV[4]) then
            return {0, "STAGE_RECEIPT_MISMATCH"}
        end

        if redis.call("EXISTS", KEYS[8]) == 1 then
            return {0, "SUCCESS_RELEASE_CONFLICT"}
        end

        for index = 1, 2 do
            local value = redis.call("GET", KEYS[index])

            if value and value ~= ARGV[1] then
                return {0, "LOCK_OWNER_MISMATCH"}
            end
        end

        if redis.call("GET", KEYS[3]) ~= ARGV[1]
           or redis.call("GET", KEYS[4]) ~= ARGV[1] then
            return {0, "ACTIVE_OWNER_MISMATCH"}
        end

        for index = 5, 6 do
            local raw = redis.call("GET", KEYS[index])

            if not raw then
                return {0, "QUARANTINE_MISSING"}
            end

            local ok, q = pcall(cjson.decode, raw)

            if not ok or type(q) ~= "table"
               or q["connector_id"] ~= ARGV[2]
               or q["socket_id"] ~= ARGV[3]
               or q["owner_token"] ~= ARGV[1] then
                return {0, "QUARANTINE_IDENTITY_MISMATCH"}
            end
        end

        local ok, r = pcall(cjson.decode, ARGV[5])

        if not ok or type(r) ~= "table"
           or r["attemptId"] ~= ARGV[6]
           or r["executionId"] ~= ARGV[7]
           or r["serverInstanceId"] ~= ARGV[8]
           or r["connectorId"] ~= ARGV[2]
           or r["socketId"] ~= ARGV[3]
           or r["ownerToken"] ~= ARGV[1]
           or r["stage"] ~= "UNBLOCKED_AFTER_SQL_CLOSURE"
           or r["markersCleared"] ~= true then
            return {0, "UNBLOCK_RECEIPT_INVALID"}
        end

        -- Atomic: save receipt and delete owned keys.
        redis.call("SET", KEYS[9], ARGV[5])

        redis.call(
            "DEL",
            KEYS[1], KEYS[2], KEYS[3],
            KEYS[4], KEYS[5], KEYS[6]
        )

        return {1, "UNBLOCKED"}
        `,
        9,
        ...keys(i),
        i.ownerToken,
        i.connectorId,
        i.socketId,
        JSON.stringify(redisReceipt),
        JSON.stringify(unblockReceipt),
        i.attemptId,
        i.executionId,
        i.serverInstanceId
    );

    if (
        !Array.isArray(result) ||
        result.length !== 2 ||
        Number(result[0]) !== 1 ||
        !["UNBLOCKED", "ALREADY_UNBLOCKED"].includes(result[1])
    ) {
        return {
            unblocked: false,
            reason: String(result?.[1] || "UNBLOCK_UNVERIFIED")
        };
    }

    const stored =
        await readManualReconcileFailureUnblockReceipt(i.attemptId);

    if (
        !stored ||
        Object.keys(stored).length !==
            Object.keys(unblockReceipt).length ||
        Object.keys(unblockReceipt).some(
            key => stored[key] !== unblockReceipt[key]
        )
    ) {
        throw new Error(
            "MANUAL_FAILURE_UNBLOCK_RECEIPT_NOT_VERIFIED"
        );
    }

    return {
        unblocked: true,
        reason: result[1],
        receipt: stored
    };
}


// ============================================================
// MANUAL FAILURE RECOVERY — ATOMIC REDIS PREFLIGHT
// Read-only. Does not authorize or perform cleanup.
// ============================================================

async function inspectManualReconcileFailureRedis(input) {
    const i = identity(input);

    await ready();

    const result = await redis.eval(
        `
        local values = {}

        for index = 1, 8 do
            values[index] = redis.call("GET", KEYS[index])
        end

        local function ownerMatches(value)
            return value == ARGV[1]
        end

        local connectorActive = ownerMatches(values[3])
        local socketActive = ownerMatches(values[4])

        local connectorLock =
            values[1] == false or ownerMatches(values[1])

        local socketLock =
            values[2] == false or ownerMatches(values[2])

               local function quarantineMatches(raw)
            -- Failure staging requires both quarantine records.
            if not raw then
                return false
            end

            local ok, record = pcall(cjson.decode, raw)

            if not ok or type(record) ~= "table" then
                return false
            end

            return record["connector_id"] == ARGV[2]
                and record["socket_id"] == ARGV[3]
                and record["owner_token"] == ARGV[1]
        end

        local quarantinesMatch =
            quarantineMatches(values[5])
            and quarantineMatches(values[6])

        local stageExists = values[7] ~= false
        local successReceiptExists = values[8] ~= false
                -- Read-only quarantine diagnostics.
        -- Missing records and ownership conflicts are distinct.
        local connectorQuarantinePresent =
            values[5] ~= false

        local socketQuarantinePresent =
            values[6] ~= false

        local quarantineRepairCandidate =
            connectorActive
            and socketActive
            and connectorLock
            and socketLock
            and not stageExists
            and not successReceiptExists
            and not connectorQuarantinePresent
            and not socketQuarantinePresent
        local safeOwnership =
            connectorActive
            and socketActive
            and connectorLock
            and socketLock
            and quarantinesMatch
            and not stageExists
            and not successReceiptExists

            return {
                safeOwnership and 1 or 0,
                connectorActive and 1 or 0,
                socketActive and 1 or 0,
                connectorLock and 1 or 0,
                socketLock and 1 or 0,
                quarantinesMatch and 1 or 0,
                stageExists and 1 or 0,
                successReceiptExists and 1 or 0,
                connectorQuarantinePresent and 1 or 0,
                socketQuarantinePresent and 1 or 0,
                quarantineRepairCandidate and 1 or 0
            }
        `,
        8,
        ...keys(i).slice(0, 8),
        i.ownerToken,
        i.connectorId,
        i.socketId
    );

        if (!Array.isArray(result) || result.length !== 11) {
        throw new Error("MANUAL_FAILURE_REDIS_PREFLIGHT_INVALID");
    }

    const flags = result.map(Number);

    if (flags.some(value => value !== 0 && value !== 1)) {
        throw new Error("MANUAL_FAILURE_REDIS_PREFLIGHT_INVALID");
    }

    return Object.freeze({
        attemptId: i.attemptId,
        executionId: i.executionId,
        connectorId: i.connectorId,
        socketId: i.socketId,

        ownershipConsistent: flags[0] === 1,
        connectorActiveOwned: flags[1] === 1,
        socketActiveOwned: flags[2] === 1,
        connectorLockCompatible: flags[3] === 1,
        socketLockCompatible: flags[4] === 1,
        quarantinesCompatible: flags[5] === 1,
        stageReceiptExists: flags[6] === 1,
        successReceiptExists: flags[7] === 1,

        connectorQuarantinePresent: flags[8] === 1,
        socketQuarantinePresent: flags[9] === 1,
        quarantineRepairCandidate: flags[10] === 1,

        cleanupAuthorized: false
    });
}

// ============================================================
// MANUAL FAILURE RECOVERY — ATOMIC STAGING LUA
// Definition only. Not executed until durable authorization.
// ============================================================

const MANUAL_FAILURE_STAGE_LUA = `
    local connectorLock = redis.call("GET", KEYS[1])
    local socketLock = redis.call("GET", KEYS[2])
    local connectorActive = redis.call("GET", KEYS[3])
    local socketActive = redis.call("GET", KEYS[4])
    local connectorQuarantine = redis.call("GET", KEYS[5])
    local socketQuarantine = redis.call("GET", KEYS[6])
    local existingStage = redis.call("GET", KEYS[7])
    local successReceipt = redis.call("GET", KEYS[8])

    -- Existing stage requires separate receipt verification.
    -- Never overwrite an immutable staging receipt.
    if existingStage then
        return {0, "STAGE_ALREADY_EXISTS"}
    end

    if successReceipt then
        return {0, "SUCCESS_RELEASE_ALREADY_EXISTS"}
    end

    -- Persistent markers must belong to the expected owner.
    if connectorActive ~= ARGV[1]
       or socketActive ~= ARGV[1] then
        return {0, "ACTIVE_OWNER_MISMATCH"}
    end

    -- Locks may have expired, but any surviving lock must
    -- belong to the same owner.
    if (connectorLock and connectorLock ~= ARGV[1])
       or (socketLock and socketLock ~= ARGV[1]) then
        return {0, "LOCK_OWNER_MISMATCH"}
    end

       local function quarantineMatches(raw)
        -- Missing quarantine is not accepted for failure staging.
        -- Both persistent quarantine records must exist
        -- and belong to this exact execution owner.
        if not raw then
            return false
        end

        local ok, record = pcall(cjson.decode, raw)

        if not ok or type(record) ~= "table" then
            return false
        end

        return record["connector_id"] == ARGV[2]
           and record["socket_id"] == ARGV[3]
           and record["owner_token"] == ARGV[1]
    end

    if not quarantineMatches(connectorQuarantine)
       or not quarantineMatches(socketQuarantine) then
        return {0, "QUARANTINE_OWNER_MISMATCH"}
    end

    -- Receipt is supplied by trusted server integration.
    -- Lua validates its exact ownership identity.
    local ok, receipt = pcall(cjson.decode, ARGV[4])

    if not ok or type(receipt) ~= "table" then
        return {0, "STAGE_RECEIPT_INVALID"}
    end

    if receipt["attemptId"] ~= ARGV[5]
       or receipt["executionId"] ~= ARGV[6]
       or receipt["serverInstanceId"] ~= ARGV[7]
       or receipt["connectorId"] ~= ARGV[2]
       or receipt["socketId"] ~= ARGV[3]
       or receipt["ownerToken"] ~= ARGV[1]
       or receipt["stage"] ~= "LOCKS_CLEARED_MARKERS_RETAINED"
       or receipt["markersRetained"] ~= true then
        return {0, "STAGE_RECEIPT_IDENTITY_INVALID"}
    end

    -- Redis SET before DELETE: both are atomic inside Lua.
    redis.call("SET", KEYS[7], ARGV[4])

    -- Delete only volatile locks.
    -- Persistent active markers and quarantine remain.
    redis.call("DEL", KEYS[1], KEYS[2])

    return {1, "LOCKS_CLEARED_MARKERS_RETAINED"}
`;


module.exports = {
    stageManualReconcileFailureCleanup,
    readManualReconcileFailureCleanupReceipt,
    inspectManualReconcileFailureRedis,
    unblockManualReconcileAfterSqlClosure,
    readManualReconcileFailureUnblockReceipt
};
