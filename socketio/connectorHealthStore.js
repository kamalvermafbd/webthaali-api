
"use strict";

require("dotenv").config();

const Redis = require("ioredis");
const {
    OPERATION_TYPES,
    normalizeOperationType,
    evaluateQuarantine
} = require("./connectorQuarantineManager");
const REDIS_HOST =
    process.env.REDIS_HOST || "127.0.0.1";

const REDIS_PORT =
    Number(process.env.REDIS_PORT || 6379);

const HEALTH_KEY_PREFIX =
    "billey:connector:health:";

const DEFAULT_HEALTH_TTL_SECONDS = 180;

const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    lazyConnect: true,
    connectTimeout: 3000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false
});

redis.on("error", (error) => {
    console.error(
        "[CONNECTOR HEALTH STORE] Redis error:",
        error.message
    );
});

async function ensureConnected() {
    if (redis.status === "wait") {
        await redis.connect();
    }

    if (redis.status !== "ready") {
        throw new Error(
            `Redis is not ready. Current status: ${redis.status}`
        );
    }
}

function normalizeConnectorId(connectorId) {
    const id = String(connectorId || "").trim();

    if (!id) {
        throw new Error("connectorId is required");
    }

    return id;
}

function getHealthKey(connectorId) {
    return HEALTH_KEY_PREFIX +
        normalizeConnectorId(connectorId);
}

// ==========================================
// GLOBAL CONNECTOR RECOVERY QUARANTINE
// ==========================================

const CONNECTOR_QUARANTINE_PREFIX =
    "billey:connector:quarantine:";

function getConnectorQuarantineKey(connectorId) {
    return CONNECTOR_QUARANTINE_PREFIX +
        normalizeConnectorId(connectorId);
}

function getSocketQuarantineKey(socketId) {
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    return `billey:socket:quarantine:${sid}`;
}

// ==========================================
// PERSISTENT ACTIVE OPERATION MARKERS
// ==========================================

function getConnectorActiveOperationKey(connectorId) {
    return `billey:connector:operation-active:${normalizeConnectorId(connectorId)}`;
}

function getSocketActiveOperationKey(socketId) {
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    return `billey:socket:operation-active:${sid}`;
}
// ==========================================
// QUARANTINE CONNECTOR + SOCKET ATOMICALLY
// ==========================================

async function quarantineConnectorAndSocket(
    connectorId,
    socketId,
    reason,
    ownerToken = null,
    operationType = OPERATION_TYPES.UNKNOWN
)  {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

   const token = String(ownerToken || "").trim();

if (!token) {
    throw new Error("QUARANTINE_OWNER_TOKEN_REQUIRED");
}

const record = JSON.stringify({
    connector_id: id,
    socket_id: sid,
    owner_token: token,

    operation_type:
        normalizeOperationType(operationType),

    reason: String(reason || "OPERATION_STATE_UNKNOWN"),
    quarantined_at: new Date().toISOString()
});

await ensureConnected();
   const result = await redis.eval(
    `
        local connectorRecord = redis.call("GET", KEYS[1])
        local socketRecord = redis.call("GET", KEYS[2])
      -- Reject another owner's locks or persistent markers.
for i = 3, 6 do
    local currentOwner = redis.call("GET", KEYS[i])

    if currentOwner and currentOwner ~= ARGV[4] then
        return 0
    end
end

-- Both persistent markers must belong to this operation.
if redis.call("GET", KEYS[5]) ~= ARGV[4]
    or redis.call("GET", KEYS[6]) ~= ARGV[4] then
    return 0
end
        -- Never overwrite or partially create quarantine records.
    if connectorRecord or socketRecord then
    if not connectorRecord or not socketRecord
        or connectorRecord ~= socketRecord then
        return 0
    end

    local existing = cjson.decode(connectorRecord)

    if existing["connector_id"] ~= ARGV[2]
        or existing["socket_id"] ~= ARGV[3]
        or existing["owner_token"] ~= ARGV[4] then
        return 0
    end

        return 1
end

redis.call("SET", KEYS[1], ARGV[1])
        redis.call("SET", KEYS[2], ARGV[1])

        return 1
    `,
     6,
    getConnectorQuarantineKey(id),
    getSocketQuarantineKey(sid),
    `billey:connector:operation-lock:${id}`,
    `billey:socket:operation-lock:${sid}`,
    getConnectorActiveOperationKey(id),
    getSocketActiveOperationKey(sid),
    record,
    id,
    sid,
    token
);

if (Number(result) !== 1) {
    throw new Error(
        "QUARANTINE_CONFLICT: Connector/socket quarantine records are inconsistent"
    );
}

    return {
        connector_id: id,
        socket_id: sid,
        quarantined: true
    };
}

async function setConnectorHealth(
    connectorId,
    health,
    options = {}
) {
    if (
        !health ||
        typeof health !== "object" ||
        Array.isArray(health)
    ) {
        throw new Error(
            "health must be a valid object"
        );
    }

    const id = normalizeConnectorId(connectorId);

    const ttlSeconds =
        Number(
            options.ttlSeconds ||
            DEFAULT_HEALTH_TTL_SECONDS
        );

    if (
        !Number.isInteger(ttlSeconds) ||
        ttlSeconds <= 0
    ) {
        throw new Error(
            "ttlSeconds must be a positive integer"
        );
    }

   const safeHealth = Object.fromEntries(
    Object.entries(health).filter(
        ([key]) =>
            !key.startsWith("tally_queue_") &&
            key !== "socket_id" &&
            key !== "connection_status" &&
            key !== "disconnected_since_ms" &&
key !== "socket_lease_renewed_at_ms"
    )
);

const record = {
    ...safeHealth,
    connector_id: id,
    connection_status: "DISCONNECTED",
    shared_updated_at: new Date().toISOString()
};

    await ensureConnected();

    const result = await redis.set(
    getHealthKey(id),
    JSON.stringify(record),
    "EX",
    ttlSeconds,
    "NX"
);

if (result !== "OK") {
    throw new Error(
        "CONNECTOR_HEALTH_ALREADY_EXISTS: " +
        "Use an ownership-verified health updater"
    );
}

    return record;
}

async function getConnectorHealth(connectorId) {
    const id = normalizeConnectorId(connectorId);

    await ensureConnected();

    const value = await redis.get(
        getHealthKey(id)
    );

    if (!value) {
        return null;
    }

    try {
        return JSON.parse(value);
    } catch (error) {
        console.error(
            "[CONNECTOR HEALTH STORE] Invalid JSON for connector:",
            id
        );

        throw new Error(
            "Stored connector health record is invalid JSON"
        );
    }
}

async function updateConnectorHealthAtomic(
    connectorId,
    patch,
    options = {}
) {
    if (
        !patch ||
        typeof patch !== "object" ||
        Array.isArray(patch)
    ) {
        throw new Error("patch must be a valid object");
    }

    const id = normalizeConnectorId(connectorId);
    const ttlSeconds = Number(
        options.ttlSeconds || DEFAULT_HEALTH_TTL_SECONDS
    );

    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
        throw new Error("ttlSeconds must be a positive integer");
    }

    await ensureConnected();

    const key = getHealthKey(id);

    const result = await redis.eval(
        `
        local current = redis.call("GET", KEYS[1])
        local record = {}

        if current then
            record = cjson.decode(current)
        end

        local patch = cjson.decode(ARGV[1])

     for k, v in pairs(patch) do
    -- Generic updates must not modify queue proof,
    -- connector ownership or disconnect evidence.
    if string.sub(k, 1, 12) ~= "tally_queue_"
        and k ~= "socket_id"
        and k ~= "connection_status"
        and k ~= "connector_id"
        and k ~= "shared_updated_at"
        and k ~= "disconnected_since_ms"
and k ~= "socket_lease_renewed_at_ms" then
        record[k] = v
    end
end

        record["connector_id"] = ARGV[2]
        record["shared_updated_at"] = ARGV[3]

        local updated = cjson.encode(record)

        redis.call("SET", KEYS[1], updated, "EX", ARGV[4])

        return updated
        `,
        1,
        key,
        JSON.stringify(patch),
        id,
        new Date().toISOString(),
        String(ttlSeconds)
    );

    return JSON.parse(result);
}

async function deleteConnectorHealth(connectorId) {
    normalizeConnectorId(connectorId);

    throw new Error(
        "CONNECTOR_HEALTH_DELETE_REQUIRES_VERIFIED_CLEANUP"
    );
}

async function ping() {
    await ensureConnected();
    return redis.ping();
}

async function closeConnectorHealthStore() {
    if (redis.status === "end") {
        return;
    }

    if (redis.status === "wait") {
        redis.disconnect();
        return;
    }

    await redis.quit();
}



async function claimConnectorSocketOwnership(
    connectorId,
    socketId,
    options = {}
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    const ttlSeconds = Number(
        options.ttlSeconds || DEFAULT_HEALTH_TTL_SECONDS
    );

    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
        throw new Error("ttlSeconds must be a positive integer");
    }

    await ensureConnected();

    const previousHealthRaw = await redis.get(getHealthKey(id));

    let previousSid = "";

    if (previousHealthRaw !== null) {
        let previousHealth;

        try {
            previousHealth = JSON.parse(previousHealthRaw);
        } catch {
            throw new Error("CONNECTOR_HEALTH_MALFORMED");
        }

        if (
            !previousHealth ||
            typeof previousHealth !== "object" ||
            Array.isArray(previousHealth)
        ) {
            throw new Error("CONNECTOR_HEALTH_MALFORMED");
        }

        const storedSocket = previousHealth.socket_id;

        if (storedSocket !== undefined && storedSocket !== null) {
            if (
                typeof storedSocket !== "string" ||
                storedSocket !== storedSocket.trim()
            ) {
                throw new Error("CONNECTOR_HEALTH_SOCKET_INVALID");
            }

            previousSid = storedSocket;
        }
    }

    const previousKeySid = previousSid || sid;

const result = await redis.eval(
    `
-- Do not change ownership while connector/socket
-- locks, quarantine or active markers exist.

if redis.call("EXISTS", KEYS[2]) == 1 then
    return "REJECTED:CONNECTOR_OPERATION_LOCK"
end

if redis.call("EXISTS", KEYS[3]) == 1 then
    return "REJECTED:CONNECTOR_ACTIVE_MARKER"
end

if redis.call("EXISTS", KEYS[4]) == 1 then
    return "REJECTED:CONNECTOR_QUARANTINE"
end

if redis.call("EXISTS", KEYS[5]) == 1 then
    return "REJECTED:SOCKET_OPERATION_LOCK"
end

if redis.call("EXISTS", KEYS[6]) == 1 then
    return "REJECTED:SOCKET_QUARANTINE"
end

if redis.call("EXISTS", KEYS[7]) == 1 then
    return "REJECTED:SOCKET_ACTIVE_MARKER"
end

    local current = redis.call("GET", KEYS[1])
    local record = {}

    if current then
        record = cjson.decode(current)

        local currentSocket = record["socket_id"]
        local currentStatus = record["connection_status"]

       if currentSocket
    and currentSocket ~= ""
    and currentSocket ~= ARGV[2]
    and currentStatus ~= "DISCONNECTED" then

    local now = redis.call("TIME")

    local nowMs =
        tonumber(now[1]) * 1000 +
        math.floor(tonumber(now[2]) / 1000)

    local renewedAt =
        record["socket_lease_renewed_at_ms"]

    local leaseExpired =
        type(renewedAt) == "number"
        and renewedAt > 0
        and renewedAt <= nowMs
        and (nowMs - renewedAt) >= 45000

    local operationStatus =
        record["operation_status"]

    local activeOperation =
        record["active_operation"]

    local noActiveOperation =
        activeOperation == nil
        or activeOperation == cjson.null

    local safeToConsiderTransfer =
        currentStatus == "CONNECTED"
        and leaseExpired
        and operationStatus ~= "BUSY"
        and noActiveOperation

    if not safeToConsiderTransfer then
        return "REJECTED_HEALTH:" .. cjson.encode({
            reason = "OTHER_CONNECTED_SOCKET",
            health = {
                socket_id = currentSocket,
                connection_status = currentStatus,
                operation_status = operationStatus,
                shared_updated_at =
                    record["shared_updated_at"]
            }
        })
    end
end
    end

   

local previousSocket = record["socket_id"]

if previousSocket == nil or previousSocket == cjson.null then
    previousSocket = ""
end

if type(previousSocket) ~= "string" then
    return "REJECTED:INVALID_PREVIOUS_SOCKET"
end

-- Prevent transfer if ownership changed after the JS pre-read.
if previousSocket ~= ARGV[5] then
    return "REJECTED:STALE_PREVIOUS_SOCKET"
end

-- All Redis keys must be supplied explicitly in KEYS.
if previousSocket ~= "" and previousSocket ~= ARGV[2] then

    if redis.call("EXISTS", KEYS[8]) == 1 then
        return "REJECTED:PREVIOUS_SOCKET_OPERATION_LOCK"
    end

    if redis.call("EXISTS", KEYS[9]) == 1 then
        return "REJECTED:PREVIOUS_SOCKET_QUARANTINE"
    end

    if redis.call("EXISTS", KEYS[10]) == 1 then
        return "REJECTED:PREVIOUS_SOCKET_ACTIVE_MARKER"
    end
end



record["connector_id"] = ARGV[1]
record["socket_id"] = ARGV[2]
record["connection_status"] = "CONNECTED"
record["disconnected_since_ms"] = cjson.null

if previousSocket ~= ARGV[2] then
    -- A new socket cannot inherit the old socket's queue proof.
    record["operation_status"] = "UNKNOWN"
    record["tally_status"] = "UNKNOWN"
    record["tally_queue_idle"] = false
    record["tally_queue_generation"] = cjson.null
    record["tally_queue_connector_epoch"] = cjson.null
    record["tally_queue_reported_at"] = cjson.null
    record["tally_queue_reported_at_ms"] = cjson.null
end

local leaseTime = redis.call("TIME")

record["socket_lease_renewed_at_ms"] =
    tonumber(leaseTime[1]) * 1000 +
    math.floor(tonumber(leaseTime[2]) / 1000)

record["shared_updated_at"] = ARGV[3]

    local updated = cjson.encode(record)

    redis.call("SET", KEYS[1], updated, "EX", ARGV[4])

    return updated
   
    `,
    10,
    getHealthKey(id),
    `billey:connector:operation-lock:${id}`,
    getConnectorActiveOperationKey(id),
    getConnectorQuarantineKey(id),
    `billey:socket:operation-lock:${sid}`,
    getSocketQuarantineKey(sid),
    getSocketActiveOperationKey(sid),
  
    `billey:socket:operation-lock:${previousKeySid}`,
    getSocketQuarantineKey(previousKeySid),
    getSocketActiveOperationKey(previousKeySid),
    id,
    sid,
    new Date().toISOString(),
    String(ttlSeconds),
    previousSid

);


    if (result === 0) {
    return {
        claimed: false,
        health: null,
        reason: "UNSPECIFIED"
    };
}

if (
    typeof result === "string" &&
    result.startsWith("REJECTED_HEALTH:")
) {
    const payload = JSON.parse(
        result.slice("REJECTED_HEALTH:".length)
    );

    return {
        claimed: false,
        health: payload.health || null,
        reason: payload.reason || "UNSPECIFIED"
    };
}

if (
    typeof result === "string" &&
    result.startsWith("REJECTED:")
) {
    return {
        claimed: false,
        health: null,
        reason: result.slice("REJECTED:".length)
    };
}

return {
    claimed: true,
    health: JSON.parse(result)
};
}


async function updateConnectorHealthIfSocketOwner(
    connectorId,
    socketId,
    patch,
    options = {}
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
        throw new Error("patch must be a valid object");
    }

    const ttlSeconds = Number(
        options.ttlSeconds || DEFAULT_HEALTH_TTL_SECONDS
    );

    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
        throw new Error("ttlSeconds must be a positive integer");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        local current = redis.call("GET", KEYS[1])

        if not current then
            return 0
        end

       
        local record = cjson.decode(current)

        if record["socket_id"] ~= ARGV[1] then
            return 0
        end

      local patch = cjson.decode(ARGV[2])

-- A disconnected socket cannot update shared health again.
-- Reconnection must first pass through claimConnectorSocketOwnership.
if record["connection_status"] == "DISCONNECTED"
    and patch["connection_status"] ~= "DISCONNECTED" then
    return 0
end

local becameDisconnected =
    patch["connection_status"] == "DISCONNECTED"
    and record["connection_status"] ~= "DISCONNECTED"

for k, v in pairs(patch) do
  if k ~= "connector_id"
    and k ~= "socket_id"
    and k ~= "shared_updated_at"
    and k ~= "disconnected_since_ms"
    and k ~= "socket_lease_renewed_at_ms"
    and string.sub(k, 1, 12) ~= "tally_queue_" then
        record[k] = v
    end
end

if becameDisconnected then
    local now = redis.call("TIME")
    record["disconnected_since_ms"] =
        tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

record["connector_id"] = ARGV[3]
record["shared_updated_at"] = ARGV[4]

        local updated = cjson.encode(record)

        redis.call("SET", KEYS[1], updated, "EX", ARGV[5])

        return updated
        `,
        1,
        getHealthKey(id),
        sid,
        JSON.stringify(patch),
        id,
        new Date().toISOString(),
        String(ttlSeconds)
    );

    if (result === 0) {
        return { updated: false, health: null };
    }

    return {
        updated: true,
        health: JSON.parse(result)
    };
}


async function updateConnectorTallyQueueStatus(
    connectorId,
    socketId,
    queueStatus
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    const status = queueStatus;

    if (
        !status ||
        typeof status !== "object" ||
        Array.isArray(status) ||
        typeof status.idle !== "boolean" ||
        !Number.isSafeInteger(status.pendingOperations) ||
        status.pendingOperations < 0 ||
        !Number.isSafeInteger(status.runningOperations) ||
        status.runningOperations < 0 ||
        !Number.isSafeInteger(status.operationGeneration) ||
        status.operationGeneration < 0 ||
        (
    status.connectorEpoch != null &&
    (
        typeof status.connectorEpoch !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            status.connectorEpoch
        )
    )
) ||
      (
    status.idle === true &&
    (
        status.pendingOperations !== 0 ||
        status.runningOperations !== 0
    )
)
    ) {
        throw new Error("Invalid Tally queue status");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        local raw = redis.call("GET", KEYS[1])

        if not raw then
            return "REJECTED:NO_HEALTH"
        end

        local health = cjson.decode(raw)

        if health["connector_id"] ~= ARGV[1]
            or health["socket_id"] ~= ARGV[2]
            or health["connection_status"] ~= "CONNECTED" then
            return "REJECTED:NOT_CONNECTED_OWNER"
        end

      local incoming = cjson.decode(ARGV[3])
local oldGeneration = health["tally_queue_generation"]

local oldEpoch = health["tally_queue_connector_epoch"]
local incomingEpoch = incoming["connectorEpoch"]

if type(oldEpoch) == "string" then
    if type(incomingEpoch) ~= "string"
        or incomingEpoch ~= oldEpoch then
        return "REJECTED:CONNECTOR_EPOCH_CONFLICT"
    end
end

        if type(oldGeneration) == "number" then
            if incoming["operationGeneration"] < oldGeneration then
                return "REJECTED:STALE_GENERATION"
            end

         if incoming["operationGeneration"] == oldGeneration then
    if health["tally_queue_idle"] ~= incoming["idle"]
        or health["tally_queue_pending_operations"] ~= incoming["pendingOperations"]
        or health["tally_queue_running_operations"] ~= incoming["runningOperations"] then
        return "REJECTED:GENERATION_CONFLICT"
    end

    -- Identical heartbeat from the currently verified socket owner.
    -- Renew freshness using Redis server time.
    local redisTime = redis.call("TIME")
    local reportedAtMs =
        tonumber(redisTime[1]) * 1000 +
        math.floor(tonumber(redisTime[2]) / 1000)
health["tally_queue_connector_epoch"] =
    incoming["connectorEpoch"] or cjson.null
    health["tally_queue_reported_at"] = ARGV[4]
    health["tally_queue_reported_at_ms"] = reportedAtMs
    health["shared_updated_at"] = ARGV[4]

    redis.call(
        "SET",
        KEYS[1],
        cjson.encode(health),
        "KEEPTTL"
    )

    return "UNCHANGED"
end
        end

      local redisTime = redis.call("TIME")
local reportedAtMs =
    tonumber(redisTime[1]) * 1000 +
    math.floor(tonumber(redisTime[2]) / 1000)

health["tally_queue_idle"] = incoming["idle"]
health["tally_queue_pending_operations"] = incoming["pendingOperations"]
health["tally_queue_running_operations"] = incoming["runningOperations"]
health["tally_queue_generation"] = incoming["operationGeneration"]
health["tally_queue_connector_epoch"] =
    incoming["connectorEpoch"] or cjson.null
health["tally_queue_reported_at"] = ARGV[4]
health["tally_queue_reported_at_ms"] = reportedAtMs
health["shared_updated_at"] = ARGV[4]

        redis.call("SET", KEYS[1], cjson.encode(health), "KEEPTTL")

        return "UPDATED"
        `,
        1,
        getHealthKey(id),
        id,
        sid,
        JSON.stringify(status),
        new Date().toISOString()
    );

    return {
        updated: result === "UPDATED",
        unchanged: result === "UNCHANGED",
        reason: result.startsWith("REJECTED:")
            ? result.slice("REJECTED:".length)
            : null
    };
}
 // Renew health-key TTL only if this socket still owns a CONNECTED record.
async function renewConnectorSocketLease(
    connectorId,
    socketId,
    options = {}
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    const ttlSeconds = Number(
        options.ttlSeconds || DEFAULT_HEALTH_TTL_SECONDS
    );

    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
        throw new Error("ttlSeconds must be a positive integer");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        local current = redis.call("GET", KEYS[1])

        if not current then
            return 0
        end

        local record = cjson.decode(current)

        if record["socket_id"] ~= ARGV[1] then
            return 0
        end

        if record["connection_status"] ~= "CONNECTED" then
            return 0
        end

local now = redis.call("TIME")

record["socket_lease_renewed_at_ms"] =
    tonumber(now[1]) * 1000 +
    math.floor(tonumber(now[2]) / 1000)

redis.call(
    "SET",
    KEYS[1],
    cjson.encode(record),
    "EX",
    ARGV[2]
)

return 1
        `,
        1,
        getHealthKey(id),
        sid,
        String(ttlSeconds)
    );

    return {
        renewed: Number(result) === 1,
        connector_id: id
    };
}



async function acquireConnectorOperationLock(
    connectorId,
    ownerToken,
    options = {}
) {
    const id = normalizeConnectorId(connectorId);
    const token = String(ownerToken || "").trim();

    if (!token) {
        throw new Error("ownerToken is required");
    }

    const ttlMs = Number(options.ttlMs || 120000);

    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
        throw new Error("ttlMs must be a positive integer");
    }

    await ensureConnected();

   const result = await redis.eval(
    `
    if redis.call("EXISTS", KEYS[1]) == 1
        or redis.call("EXISTS", KEYS[2]) == 1
        or redis.call("EXISTS", KEYS[3]) == 1 then
        return 0
    end

    redis.call(
        "SET",
        KEYS[1],
        ARGV[1],
        "PX",
        ARGV[2]
    )

    return 1
    `,
    3,
    `billey:connector:operation-lock:${id}`,
    getConnectorQuarantineKey(id),
    getConnectorActiveOperationKey(id),
    token,
    String(ttlMs)
);

return Number(result) === 1;
}

async function releaseConnectorOperationLock(
    connectorId,
    ownerToken
) {
    const id = normalizeConnectorId(connectorId);
    const token = String(ownerToken || "").trim();

    if (!token) {
        throw new Error("ownerToken is required");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end

        return 0
        `,
        1,
        `billey:connector:operation-lock:${id}`,
        token
    );

    return result === 1;
}


async function renewConnectorOperationLock(
    connectorId,
    ownerToken,
    ttlMs = 120000
) {
    if (!connectorId || !ownerToken) {
        throw new Error(
            "connectorId and ownerToken are required"
        );
    }

    const ttl = Number(ttlMs);

if (!Number.isInteger(ttl) || ttl <= 0) {
    throw new Error("ttlMs must be a positive integer");
}


const key =
    `billey:connector:operation-lock:${normalizeConnectorId(connectorId)}`;


    
const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("PEXPIRE", KEYS[1], ARGV[2])
    end
    return 0
`;

await ensureConnected();

const result = await redis.eval(

        script,
        1,
        key,
        ownerToken,
        String(ttl)
    );

    return Number(result) === 1;
}


async function getConnectorOperationStatus(connectorId) {
    const watchdog = await getConnectorWatchdogStatus(connectorId);

    return {
        connector_id: watchdog.connector_id,
        status: watchdog.operation_status,
        busy: watchdog.busy,
        ttl_ms: watchdog.lock_ttl_ms
    };
}

async function getConnectorWatchdogStatus(connectorId) {
    const id = normalizeConnectorId(connectorId);

    await ensureConnected();

    const health = await getConnectorHealth(id);

    const socketId = String(health?.socket_id || "").trim();

    const connectorLockKey =
        `billey:connector:operation-lock:${id}`;

    const keys = [
        connectorLockKey,
        getConnectorQuarantineKey(id),
        getConnectorActiveOperationKey(id)
    ];

    if (socketId) {
        keys.push(
            `billey:socket:operation-lock:${socketId}`,
            getSocketQuarantineKey(socketId),
            getSocketActiveOperationKey(socketId)
        );
    }

    const values = await redis.mget(...keys);

    const connectorLock = values[0];
    const connectorQuarantine = values[1];
    const connectorActive = values[2];

    const socketLock = socketId ? values[3] : null;
    const socketQuarantine = socketId ? values[4] : null;
    const socketActive = socketId ? values[5] : null;

    const quarantined =
        connectorQuarantine !== null ||
        socketQuarantine !== null;

    const activeOperation =
        connectorActive !== null ||
        socketActive !== null;

   const connectorLocked = connectorLock !== null;
const socketLocked = socketLock !== null;

const locked = connectorLocked || socketLocked;


const inconsistentGlobalLock =
    Boolean(socketId) &&
    (
               // A connector lock requires its matching socket lock.
        // A socket-only lock may belong to another connector alias.
        (
            connectorLocked && !socketLocked
        ) ||

        // Both locks exist but their owners differ.
        (
            connectorLocked &&
            socketLocked &&
            connectorLock !== socketLock
        ) ||

       // Any existing lock must have its corresponding
        // persistent active-operation marker.
        (
            (connectorLocked && connectorActive === null) ||
            (socketLocked && socketActive === null)
        ) ||

        // Connector lock and marker ownership mismatch.
        (
            connectorActive !== null &&
            connectorLocked &&
            connectorActive !== connectorLock
        ) ||

        // Socket lock and marker ownership mismatch.
        (
            socketActive !== null &&
            socketLocked &&
            socketActive !== socketLock
        ) ||

        // Persistent markers disagree.
        (
            connectorActive !== null &&
            socketActive !== null &&
            connectorActive !== socketActive
        )
    );

const connected =
    health?.connection_status === "CONNECTED";
const missingSocketMapping =
    !socketId &&
    (
        connectorLocked ||
        connectorActive !== null
    );

const inconsistentConnectorState =
    (connectorLocked && connectorActive === null) ||
    (!connectorLocked && connectorActive !== null) ||
    (
        connectorLocked &&
        connectorActive !== null &&
        connectorLock !== connectorActive
    );

const redisTime = await redis.time();

const nowMs =
    Number(redisTime[0]) * 1000 +
    Math.floor(Number(redisTime[1]) / 1000);

const reportedAtMs =
    health?.tally_queue_reported_at_ms;

const queueVerifiedIdle =
    health?.tally_queue_idle === true &&
    Number.isSafeInteger(health?.tally_queue_generation) &&
    health.tally_queue_generation >= 0 &&
    Number.isSafeInteger(reportedAtMs) &&
    reportedAtMs >= 0 &&
    reportedAtMs <= nowMs &&
    nowMs - reportedAtMs <= 30000;

const operationStatus =
    quarantined ||
    missingSocketMapping ||
    inconsistentConnectorState ||
    (activeOperation && !locked) ||
    inconsistentGlobalLock
        ? "QUARANTINED"
        : locked || activeOperation
            ? "BUSY"
            : connected &&
              socketId &&
              health?.tally_status === "ONLINE" &&
              queueVerifiedIdle
                ? "FREE"
                : "UNKNOWN";

    const ttlMs = connectorLock !== null
        ? await redis.pttl(connectorLockKey)
        : 0;

    return {
        connector_id: id,

        connection_status: connected
            ? "CONNECTED"
            : "DISCONNECTED",

        operation_status: operationStatus,

        busy: operationStatus !== "FREE",

        available:
            connected && operationStatus === "FREE",

        quarantined:
            operationStatus === "QUARANTINED",

        active_operation: activeOperation,

        lock_ttl_ms: ttlMs,

        socket_id: socketId || null,

        last_updated_at:
            health?.shared_updated_at || null
    };
}

// ==========================================
// GLOBAL SOCKET OPERATION LOCK
// ==========================================

async function acquireSocketOperationLock(
    socketId,
    ownerToken,
    options = {}
) {
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    if (!token) {
        throw new Error("ownerToken is required");
    }

    const ttlMs = Number(options.ttlMs || 120000);

    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
        throw new Error("ttlMs must be a positive integer");
    }

    await ensureConnected();
const result = await redis.eval(
    `
    if redis.call("EXISTS", KEYS[1]) == 1
        or redis.call("EXISTS", KEYS[2]) == 1
        or redis.call("EXISTS", KEYS[3]) == 1 then
        return 0
    end

    redis.call(
        "SET",
        KEYS[1],
        ARGV[1],
        "PX",
        ARGV[2]
    )

    return 1
    `,
    3,
    `billey:socket:operation-lock:${sid}`,
    getSocketQuarantineKey(sid),
    getSocketActiveOperationKey(sid),
    token,
    String(ttlMs)
);

return Number(result) === 1;
}

// ==========================================
// RELEASE GLOBAL SOCKET OPERATION LOCK
// ==========================================

async function releaseSocketOperationLock(
    socketId,
    ownerToken
) {
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    if (!token) {
        throw new Error("ownerToken is required");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end

        return 0
        `,
        1,
        `billey:socket:operation-lock:${sid}`,
        token
    );

    return Number(result) === 1;
}

// ==========================================
// RENEW GLOBAL SOCKET OPERATION LOCK
// ==========================================

async function renewSocketOperationLock(
    socketId,
    ownerToken,
    ttlMs = 120000
) {
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();
    const ttl = Number(ttlMs);

    if (!sid) {
        throw new Error("socketId is required");
    }

    if (!token) {
        throw new Error("ownerToken is required");
    }

    if (!Number.isInteger(ttl) || ttl <= 0) {
        throw new Error("ttlMs must be a positive integer");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("PEXPIRE", KEYS[1], ARGV[2])
        end

        return 0
        `,
        1,
        `billey:socket:operation-lock:${sid}`,
        token,
        String(ttl)
    );

    return Number(result) === 1;
}

// ==========================================
// GET GLOBAL SOCKET OPERATION STATUS
// ==========================================
async function getSocketOperationStatus(socketId) {
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    await ensureConnected();

    const key = `billey:socket:operation-lock:${sid}`;

    const values = await redis.mget(
        key,
        getSocketQuarantineKey(sid),
        getSocketActiveOperationKey(sid)
    );

    const socketLock = values[0];
const socketQuarantine = values[1];
const socketActive = values[2];

const locked = socketLock !== null;
const quarantined = socketQuarantine !== null;
const activeOperation = socketActive !== null;

const ownershipMismatch =
    locked &&
    activeOperation &&
    socketLock !== socketActive;
const status =
    quarantined ||
    (activeOperation && !locked) ||
    (locked && !activeOperation) ||
    ownershipMismatch
        ? "QUARANTINED"
        : locked || activeOperation
            ? "BUSY"
            : "FREE";

    const ttlMs = locked
        ? await redis.pttl(key)
        : 0;

    return {
        socket_id: sid,
        status,
        busy: status !== "FREE",
        ttl_ms: ttlMs
    };
}

// ==========================================
// ATOMIC CONNECTOR + SOCKET LOCK
// ==========================================

async function acquireConnectorAndSocketLock(
    connectorId,
    socketId,
    ownerToken,
    options = {}
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();
    const ttlMs = Number(options.ttlMs || 120000);

    if (!sid || !token) {
        throw new Error("socketId and ownerToken are required");
    }

    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
        throw new Error("ttlMs must be a positive integer");
    }

    await ensureConnected();

    const connectorKey =
        `billey:connector:operation-lock:${id}`;

    const socketKey =
        `billey:socket:operation-lock:${sid}`;

    const result = await redis.eval(
    `
    -- Only the currently connected socket may acquire this connector.
local healthRaw = redis.call("GET", KEYS[7])

if not healthRaw then
    return 0
end

local health = cjson.decode(healthRaw)

if health["connector_id"] ~= ARGV[3]
    or health["socket_id"] ~= ARGV[4]
    or health["connection_status"] ~= "CONNECTED"
    or health["tally_status"] ~= "ONLINE" then
    return 0
end

    -- Require fresh, verified Tally queue-idle proof.
    if health["tally_queue_idle"] ~= true then
        return 0
    end

    local generation = health["tally_queue_generation"]

    if type(generation) ~= "number"
        or generation < 0 then
        return 0
    end

    local reportedAt = health["tally_queue_reported_at"]

    if type(reportedAt) ~= "string" then
        return 0
    end

    -- Freshness must be checked inside Redis, atomically
    -- with the operation lock acquisition.
    local redisTime = redis.call("TIME")
    local nowMs =
        tonumber(redisTime[1]) * 1000 +
        math.floor(tonumber(redisTime[2]) / 1000)

    local reportedAtMs = tonumber(
        health["tally_queue_reported_at_ms"]
    )

    if not reportedAtMs
        or reportedAtMs > nowMs
        or nowMs - reportedAtMs > 30000 then
        return 0
    end

    -- Reject existing connector/socket locks.
    if redis.call("EXISTS", KEYS[1]) == 1
        or redis.call("EXISTS", KEYS[2]) == 1 then
        return 0
    end

    -- Reject quarantined connector/socket.
    if redis.call("EXISTS", KEYS[3]) == 1
        or redis.call("EXISTS", KEYS[4]) == 1 then
        return 0
    end

    -- Reject unfinished operations, even after lease expiry.
    if redis.call("EXISTS", KEYS[5]) == 1
        or redis.call("EXISTS", KEYS[6]) == 1 then
        return 0
    end

    -- Expiring leases.
    redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
    redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[2])

    -- Persistent operation markers: no TTL.
    redis.call("SET", KEYS[5], ARGV[1])
    redis.call("SET", KEYS[6], ARGV[1])

    return 1
    `,
   7,
connectorKey,
socketKey,
getConnectorQuarantineKey(id),
getSocketQuarantineKey(sid),
getConnectorActiveOperationKey(id),
getSocketActiveOperationKey(sid),
getHealthKey(id),
token,
String(ttlMs),
id,
sid
);

    return Number(result) === 1;
}

// ==========================================
// ATOMIC CONNECTOR + SOCKET LOCK RELEASE
// ==========================================

async function releaseConnectorAndSocketLock(
    connectorId,
    socketId,
    ownerToken
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (!sid || !token) {
        throw new Error("socketId and ownerToken are required");
    }

    await ensureConnected();

    const connectorKey =
        `billey:connector:operation-lock:${id}`;

    const socketKey =
        `billey:socket:operation-lock:${sid}`;

    const result = await redis.eval(
    `
    -- Verify ownership of both locks and markers.
    if redis.call("GET", KEYS[1]) ~= ARGV[1]
    or redis.call("GET", KEYS[2]) ~= ARGV[1]
    or redis.call("GET", KEYS[3]) ~= ARGV[1]
    or redis.call("GET", KEYS[4]) ~= ARGV[1] then
    return 0
end

if redis.call("EXISTS", KEYS[5]) == 1
    or redis.call("EXISTS", KEYS[6]) == 1 then
    return 0
end

    -- Atomically release locks and active markers.
    redis.call("DEL", KEYS[1])
    redis.call("DEL", KEYS[2])
    redis.call("DEL", KEYS[3])
    redis.call("DEL", KEYS[4])

    return 1
    `,
 6,
connectorKey,
socketKey,
getConnectorActiveOperationKey(id),
getSocketActiveOperationKey(sid),
getConnectorQuarantineKey(id),
getSocketQuarantineKey(sid),
token
);

    return Number(result) === 1;
}

// ==========================================
// SYNC CONNECTOR ATOMIC RELEASE RECEIPT
// FIRST_SYNC / NORMAL_SYNC
// ==========================================

function buildSyncConnectorReleaseReceipt({
    requestId,
    batchId,
    executionClaimToken,
    connectorId,
    socketId,
    ownerToken,
    operationType
}) {
    const UUID =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (
        typeof requestId !== "string" ||
        !UUID.test(requestId) ||
        typeof batchId !== "string" ||
        !batchId.trim() ||
        typeof executionClaimToken !== "string" ||
        !UUID.test(executionClaimToken) ||
        !id ||
        !sid ||
        !UUID.test(token) ||
        !["FIRST_SYNC", "NORMAL_SYNC"].includes(
            operationType
        )
    ) {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_IDENTITY_INVALID"
        );
    }

    return {
        requestId,
        batchId,
        executionClaimToken,
        connectorId: id,
        socketId: sid,
        ownerToken: token,
        operationType,
        redisReleaseConfirmed: true
    };
}


async function releaseSyncConnectorLockWithReceipt(
    context
) {
    const receipt =
        buildSyncConnectorReleaseReceipt(context);

    const id = receipt.connectorId;
    const sid = receipt.socketId;
    const token = receipt.ownerToken;

    await ensureConnected();

    const receiptKey =
        `billey:sync:release-receipt:${receipt.requestId}`;

    const result = await redis.eval(
        `
        local existing = redis.call("GET", KEYS[7])

        -- Receipt replay must never modify current
        -- connector or socket ownership.
        if existing then
            if existing == ARGV[2] then
                return 2
            end

            return -1
        end

        -- Verify both volatile locks and both
        -- persistent active operation markers.
        if redis.call("GET", KEYS[1]) ~= ARGV[1]
            or redis.call("GET", KEYS[2]) ~= ARGV[1]
            or redis.call("GET", KEYS[3]) ~= ARGV[1]
            or redis.call("GET", KEYS[4]) ~= ARGV[1] then

            return 0
        end

        -- Quarantined ownership requires the
        -- existing authorized recovery mechanism.
        if redis.call("EXISTS", KEYS[5]) == 1
            or redis.call("EXISTS", KEYS[6]) == 1 then

            return 0
        end

        -- Record release evidence before deleting
        -- ownership keys in the same Lua execution.
        redis.call("SET", KEYS[7], ARGV[2])

        redis.call(
            "DEL",
            KEYS[1],
            KEYS[2],
            KEYS[3],
            KEYS[4]
        )

        return 1
        `,
        7,
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${sid}`,
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(sid),
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(sid),
        receiptKey,
        token,
        JSON.stringify(receipt)
    );

    const code = Number(result);

    if (code === -1) {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_RECEIPT_CONFLICT"
        );
    }

    if (code === 0) {
        return {
            released: false,
            replayed: false,
            receipt: null
        };
    }

    if (code !== 1 && code !== 2) {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_RESULT_INVALID"
        );
    }

    return {
        released: true,
        replayed: code === 2,
        receipt
    };
}


async function readSyncConnectorReleaseReceipt(
    context
) {
    const expected =
        buildSyncConnectorReleaseReceipt(context);

    await ensureConnected();

    const raw = await redis.get(
        `billey:sync:release-receipt:${expected.requestId}`
    );

    if (raw === null) {
        return null;
    }

    let receipt;

    try {
        receipt = JSON.parse(raw);
    } catch {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_RECEIPT_CORRUPT"
        );
    }

    // Verify the complete immutable execution
    // and connector ownership identity.
    if (
        !receipt ||
        receipt.requestId !== expected.requestId ||
        receipt.batchId !== expected.batchId ||
        receipt.executionClaimToken !==
            expected.executionClaimToken ||
        receipt.connectorId !== expected.connectorId ||
        receipt.socketId !== expected.socketId ||
        receipt.ownerToken !== expected.ownerToken ||
        receipt.operationType !== expected.operationType ||
        receipt.redisReleaseConfirmed !== true
    ) {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_RECEIPT_MISMATCH"
        );
    }

    return receipt;
}
// ==========================================
// VERIFIED SYNC QUARANTINE RECOVERY
// ==========================================

async function recoverSyncConnectorLockWithReceipt(context) {

    const receipt =
        buildSyncConnectorReleaseReceipt(context);

    const {
        recoveryAuthorized,
        connectorEpoch,
        operationGeneration,
        quarantinedAt
    } = context;

    const UUID =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    const quarantineTimeMs =
        Date.parse(quarantinedAt);

    if (
        recoveryAuthorized !== true ||
        typeof connectorEpoch !== "string" ||
        !UUID.test(connectorEpoch) ||
        !Number.isSafeInteger(operationGeneration) ||
        operationGeneration < 0 ||
        !Number.isFinite(quarantineTimeMs)
    ) {
        throw new Error(
            "SYNC_CONNECTOR_RECOVERY_EVIDENCE_INVALID"
        );
    }

    const id = receipt.connectorId;
    const sid = receipt.socketId;
    const token = receipt.ownerToken;

    await ensureConnected();

    const result = await redis.eval(
        `
        local existing = redis.call("GET", KEYS[7])

        -- Idempotent receipt replay.
        -- Never modify current ownership on replay.
        if existing then
            if existing == ARGV[4] then
                return 2
            end

            return -1
        end

        -- Both quarantine records must exist.
        local connectorRaw = redis.call("GET", KEYS[1])
        local socketRaw = redis.call("GET", KEYS[2])

        if not connectorRaw or not socketRaw then
            return 0
        end

        local connectorRecord = cjson.decode(connectorRaw)
        local socketRecord = cjson.decode(socketRaw)

        local function matches(record)
            return record["connector_id"] == ARGV[1]
                and record["socket_id"] == ARGV[2]
                and record["owner_token"] == ARGV[3]
                and record["operation_type"] == ARGV[5]
                and record["quarantined_at"] == ARGV[6]
        end

        if not matches(connectorRecord)
            or not matches(socketRecord) then
            return 0
        end

        -- Verify original persistent ownership.
        if redis.call("GET", KEYS[3]) ~= ARGV[3]
            or redis.call("GET", KEYS[4]) ~= ARGV[3] then
            return 0
        end

        -- Never recover while either lock exists.
        if redis.call("EXISTS", KEYS[5]) == 1
            or redis.call("EXISTS", KEYS[6]) == 1 then
            return 0
        end

        local redisTime = redis.call("TIME")

        local nowMs =
            tonumber(redisTime[1]) * 1000 +
            math.floor(tonumber(redisTime[2]) / 1000)

        -- Five-minute quarantine eligibility.
        if nowMs - tonumber(ARGV[7]) < 300000 then
            return 0
        end

        -- Verify original connector process and socket.
        local healthRaw = redis.call("GET", KEYS[8])

        if not healthRaw then
            return 0
        end

        local health = cjson.decode(healthRaw)

        if health["connector_id"] ~= ARGV[1]
            or health["socket_id"] ~= ARGV[2]
            or health["connection_status"] ~= "CONNECTED"
            or health["tally_queue_connector_epoch"] ~= ARGV[8] then
            return 0
        end

        -- Queue must still match the observed idle generation.
        if health["tally_queue_idle"] ~= true
            or health["tally_queue_pending_operations"] ~= 0
            or health["tally_queue_running_operations"] ~= 0
            or health["tally_queue_generation"] ~= tonumber(ARGV[9]) then
            return 0
        end

        local reportedAtMs =
            tonumber(health["tally_queue_reported_at_ms"])

        if not reportedAtMs
            or reportedAtMs > nowMs
            or nowMs - reportedAtMs > 15000 then
            return 0
        end

        -- Persist release receipt and remove quarantine
        -- ownership in one atomic Redis execution.
        redis.call("SET", KEYS[7], ARGV[4])

        redis.call(
            "DEL",
            KEYS[1],
            KEYS[2],
            KEYS[3],
            KEYS[4]
        )

        return 1
        `,
        8,
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(sid),
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(sid),
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${sid}`,
        `billey:sync:release-receipt:${receipt.requestId}`,
        getHealthKey(id),
        id,
        sid,
        token,
        JSON.stringify(receipt),
        receipt.operationType,
        quarantinedAt,
        String(quarantineTimeMs),
        connectorEpoch,
        String(operationGeneration)
    );

    const code = Number(result);

    if (code === -1) {
        throw new Error(
            "SYNC_CONNECTOR_RELEASE_RECEIPT_CONFLICT"
        );
    }

    if (code === 0) {
        return {
            recovered: false,
            replayed: false,
            receipt: null
        };
    }

    if (code !== 1 && code !== 2) {
        throw new Error(
            "SYNC_CONNECTOR_RECOVERY_RESULT_INVALID"
        );
    }

    return {
        recovered: true,
        replayed: code === 2,
        receipt
    };
}
 // ==========================================
 // MANUAL RECONCILIATION ATOMIC RELEASE RECEIPT
 // ==========================================

async function releaseManualReconcileLockWithReceipt({
    connectorId,
    socketId,
    ownerToken,
    attemptId,
    executionId,
    serverInstanceId
}) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

      const UUID =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    if (
        !sid ||
        !UUID.test(token) ||
        !UUID.test(attemptId || "") ||
        !UUID.test(executionId || "") ||
        !UUID.test(serverInstanceId || "")
    ) {
        throw new Error(
            "MANUAL_RECONCILE_RELEASE_IDENTITY_INVALID"
        );
    }

    await ensureConnected();

    const receipt = {
        attemptId,
        executionId,
        serverInstanceId,
        connectorId: id,
        socketId: sid,
        ownerToken: token,
        redisReleaseConfirmed: true
    };

    const receiptKey =
        `billey:manual-reconcile:release-receipt:${attemptId}`;

    const result = await redis.eval(
        `
        local existing = redis.call("GET", KEYS[7])

        -- Existing receipt is immutable.
        -- Replay must not modify any ownership keys.
        if existing then
            if existing == ARGV[2] then
                return 2
            end

            return -1
        end

        -- All four ownership values must match.
        if redis.call("GET", KEYS[1]) ~= ARGV[1]
            or redis.call("GET", KEYS[2]) ~= ARGV[1]
            or redis.call("GET", KEYS[3]) ~= ARGV[1]
            or redis.call("GET", KEYS[4]) ~= ARGV[1] then
            return 0
        end

        -- Never release quarantined ownership.
        if redis.call("EXISTS", KEYS[5]) == 1
            or redis.call("EXISTS", KEYS[6]) == 1 then
            return 0
        end

        -- Record the receipt and release ownership
        -- in one atomic Redis execution.
        redis.call("SET", KEYS[7], ARGV[2])

        redis.call(
            "DEL",
            KEYS[1],
            KEYS[2],
            KEYS[3],
            KEYS[4]
        )

        return 1
        `,
        7,
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${sid}`,
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(sid),
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(sid),
        receiptKey,
        token,
        JSON.stringify(receipt)
    );

    const code = Number(result);

    if (code === -1) {
        throw new Error(
            "MANUAL_RECONCILE_RELEASE_RECEIPT_CONFLICT"
        );
    }

    if (code === 0) {
        return {
            released: false,
            replayed: false,
            receipt: null
        };
    }

    if (code !== 1 && code !== 2) {
        throw new Error(
            "MANUAL_RECONCILE_RELEASE_RESULT_INVALID"
        );
    }

    return {
        released: true,
        replayed: code === 2,
        receipt
    };
}

async function readManualReconcileReleaseReceipt(attemptId) {
      const UUID =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

    if (!UUID.test(attemptId || "")) {
        throw new Error(
            "MANUAL_RECONCILE_RECEIPT_ATTEMPT_INVALID"
        );
    }

    await ensureConnected();

    const raw = await redis.get(
        `billey:manual-reconcile:release-receipt:${attemptId}`
    );

    if (raw === null) {
        return null;
    }

    let receipt;

    try {
        receipt = JSON.parse(raw);
    } catch {
        throw new Error(
            "MANUAL_RECONCILE_RECEIPT_CORRUPT"
        );
    }

    if (
        !receipt ||
        receipt.attemptId !== attemptId ||
        receipt.redisReleaseConfirmed !== true ||
        !UUID.test(receipt.executionId || "") ||
        !UUID.test(receipt.serverInstanceId || "") ||
        !UUID.test(receipt.ownerToken || "") ||
        typeof receipt.connectorId !== "string" ||
        !receipt.connectorId ||
        typeof receipt.socketId !== "string" ||
        !receipt.socketId
    ) {
        throw new Error(
            "MANUAL_RECONCILE_RECEIPT_INVALID"
        );
    }

    return receipt;
}

// ==========================================
// ATOMIC CONNECTOR + SOCKET LOCK RENEWAL
// ==========================================

async function renewConnectorAndSocketLock(
    connectorId,
    socketId,
    ownerToken,
    ttlMs = 120000
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();
    const ttl = Number(ttlMs);

    if (!sid || !token) {
        throw new Error("socketId and ownerToken are required");
    }

    if (!Number.isInteger(ttl) || ttl <= 0) {
        throw new Error("ttlMs must be a positive integer");
    }

    await ensureConnected();

    const connectorKey =
        `billey:connector:operation-lock:${id}`;

    const socketKey =
        `billey:socket:operation-lock:${sid}`;

    const result = await redis.eval(
        `
        if redis.call("GET", KEYS[1]) ~= ARGV[1]
    or redis.call("GET", KEYS[2]) ~= ARGV[1]
    or redis.call("GET", KEYS[3]) ~= ARGV[1]
    or redis.call("GET", KEYS[4]) ~= ARGV[1] then
    return 0
end

if redis.call("EXISTS", KEYS[5]) == 1
    or redis.call("EXISTS", KEYS[6]) == 1 then
    return 0
end

        redis.call("PEXPIRE", KEYS[1], ARGV[2])
        redis.call("PEXPIRE", KEYS[2], ARGV[2])

        return 1
        `,
   6,
connectorKey,
socketKey,
getConnectorActiveOperationKey(id),
getSocketActiveOperationKey(sid),
getConnectorQuarantineKey(id),
getSocketQuarantineKey(sid),
token,
String(ttl)
    );

    return Number(result) === 1;
}

// ============================================================
// VERIFIED CONNECTOR RECOVERY — ATOMIC REDIS CLEANUP
// ============================================================

async function clearVerifiedConnectorRecovery({
    connectorId,
    socketId,
    ownerToken,
    verified
}) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (!sid || !token) {
        throw new Error("socketId and ownerToken are required");
    }

    if (verified !== true) {
        throw new Error(
            "Recovery requires verified remote operation state"
        );
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        local connectorQuarantine = redis.call("GET", KEYS[1])
        local socketQuarantine = redis.call("GET", KEYS[2])

        if not connectorQuarantine then
            return 0
        end

        local record = cjson.decode(connectorQuarantine)

       if record["connector_id"] ~= ARGV[1]
    or record["socket_id"] ~= ARGV[2]
    or record["owner_token"] ~= ARGV[3] then
    return 0
end

if not socketQuarantine then
    return 0
end

local socketRecord = cjson.decode(socketQuarantine)

if socketRecord["connector_id"] ~= ARGV[1]
    or socketRecord["socket_id"] ~= ARGV[2]
    or socketRecord["owner_token"] ~= ARGV[3] then
    return 0
end

        if redis.call("GET", KEYS[3]) ~= ARGV[3]
            or redis.call("GET", KEYS[4]) ~= ARGV[3] then
            return 0
        end

        -- Never clear markers while a lock exists.
        if redis.call("EXISTS", KEYS[5]) == 1
            or redis.call("EXISTS", KEYS[6]) == 1 then
            return 0
        end

        redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[4])

        return 1
        `,
        6,
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(sid),
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(sid),
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${sid}`,
        id,
        sid,
        token
    );

    return {
        recovered: Number(result) === 1,
        connector_id: id
    };
}
async function inspectManualReconcileActiveOwnership({
    connectorId,
    socketId,
    ownerToken
}) {
    const id = normalizeConnectorId(connectorId);

    if (
        typeof socketId !== "string" ||
        !socketId.trim() ||
        typeof ownerToken !== "string" ||
        !ownerToken.trim()
    ) {
        throw new Error(
            "MANUAL_RECONCILE_OWNERSHIP_IDENTITY_INVALID"
        );
    }

    await ensureConnected();

    const values = await redis.mget(
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${socketId}`,
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(socketId),
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(socketId),
        getHealthKey(id)
    );

    if (!Array.isArray(values) || values.length !== 7) {
        throw new Error(
            "MANUAL_RECONCILE_OWNERSHIP_READ_INVALID"
        );
    }

    const [
        connectorLock,
        socketLock,
        connectorMarker,
        socketMarker,
        connectorQuarantine,
        socketQuarantine,
        rawHealth
    ] = values;

    const health = rawHealth
        ? JSON.parse(rawHealth)
        : null;

    const locksMatch =
        connectorLock === ownerToken &&
        socketLock === ownerToken;

    const markersMatch =
        connectorMarker === ownerToken &&
        socketMarker === ownerToken;

    const quarantined =
        connectorQuarantine !== null ||
        socketQuarantine !== null;

    const socketMatches =
        health?.socket_id === socketId;

    const connected =
        health?.connection_status === "CONNECTED";

    return Object.freeze({
        connectorId: id,
        socketId,
        locksMatch,
        markersMatch,
        quarantined,
        socketMatches,
        connected,

        activeOwnershipVerified:
            locksMatch &&
            markersMatch &&
            !quarantined &&
            socketMatches &&
            connected,

        remoteSettlementVerified: false,
        recoveryAuthorized: false
    });
}
// Read-only, atomic recovery inspection.
// This does NOT authorize remote settlement.
async function inspectConnectorRecoveryState(connectorId) {
    const id = normalizeConnectorId(connectorId);
    await ensureConnected();

    const raw = await redis.eval(
        `
        local quarantine = redis.call("GET", KEYS[1])
        local marker = redis.call("GET", KEYS[2])
        local lockPresent = redis.call("EXISTS", KEYS[3])
        local health = redis.call("GET", KEYS[4])

        return {
            quarantine or "",
            marker or "",
            tostring(lockPresent),
            health or ""
        }
        `,
        4,
        getConnectorQuarantineKey(id),
        getConnectorActiveOperationKey(id),
        `billey:connector:operation-lock:${id}`,
        getHealthKey(id)
    );

    let quarantine = null;
    let health = null;

    try {
        quarantine = raw[0] ? JSON.parse(raw[0]) : null;
        health = raw[3] ? JSON.parse(raw[3]) : null;
    } catch (error) {
        const failure = new Error(
            "CONNECTOR_RECOVERY_STATE_MALFORMED"
        );
        failure.cause = error;
        throw failure;
    }

    const oldSocketId =
        String(quarantine?.socket_id || "").trim();

    const ownerToken =
        String(quarantine?.owner_token || "").trim();

    const hasActiveMarker = raw[1] !== "";
    const markerMatchesOwner =
        Boolean(ownerToken && raw[1] === ownerToken);

    const lockPresent = raw[2] === "1";

        const quarantinePolicy =
        evaluateQuarantine(quarantine);

    return Object.freeze({
        connectorId: id,

        quarantined: quarantine !== null,

        quarantineState: quarantinePolicy.state,
        quarantineFresh: quarantinePolicy.fresh,
        quarantineStale: quarantinePolicy.stale,

        operationType: quarantinePolicy.operationType || null,

        quarantineAgeMs: quarantinePolicy.ageMs ?? null,
        quarantineFreshnessMs:
            quarantinePolicy.freshnessMs ?? null,

        quarantineRemainingMs:
            quarantinePolicy.remainingMs ?? null,

        recoveryEligible:
            quarantinePolicy.recoveryEligible === true,

        quarantinedAt:
            quarantine?.quarantined_at || null,

     oldSocketId: oldSocketId || null,
ownerToken: markerMatchesOwner ? ownerToken : null,
hasActiveMarker,
markerMatchesOwner,
lockPresent,
healthSocketId: health?.socket_id || null,

        remoteSettlementVerified: false,
        recoveryAuthorized: false
    });
}
async function recoverConnectorAfterReconnect({
    connectorId,
    oldSocketId,
    newSocketId,
    ownerToken,
       verified,
    allowVerifiedLegacyRecovery = false,
    ttlSeconds = DEFAULT_HEALTH_TTL_SECONDS
}) {
    const id = normalizeConnectorId(connectorId);
    const oldSid = String(oldSocketId || "").trim();
    const newSid = String(newSocketId || "").trim();
    const token = String(ownerToken || "").trim();

    if (!oldSid || !newSid || !token) {
        throw new Error(
            "oldSocketId, newSocketId and ownerToken are required"
        );
    }

    if (oldSid === newSid) {
        throw new Error(
            "Recovery requires a new socket ID"
        );
    }

    if (verified !== true) {
        throw new Error(
            "Recovery requires verified remote operation state"
        );
    }

    const ttl = Number(ttlSeconds);

    if (!Number.isInteger(ttl) || ttl <= 0) {
        throw new Error("ttlSeconds must be a positive integer");
    }

    await ensureConnected();

    const result = await redis.eval(
        `
        local quarantine = redis.call("GET", KEYS[2])

        if not quarantine then
            return 0
        end

        local record = cjson.decode(quarantine)

        if record["connector_id"] ~= ARGV[1]
            or record["socket_id"] ~= ARGV[2] then
            return 0
        end

        if record["owner_token"] ~= ARGV[4] then
            return 0
        end

            local oldSocketQuarantine =
            redis.call("GET", KEYS[3])

        local connectorMarker =
            redis.call("GET", KEYS[4])

        local oldSocketMarker =
            redis.call("GET", KEYS[5])

        -- Connector marker must match the verified operation.
        if connectorMarker ~= ARGV[4] then
            return 0
        end

        if oldSocketQuarantine then
            local socketRecord =
                cjson.decode(oldSocketQuarantine)

            if socketRecord["connector_id"] ~= ARGV[1]
                or socketRecord["socket_id"] ~= ARGV[2]
                or socketRecord["owner_token"] ~= ARGV[4]
                or oldSocketMarker ~= ARGV[4] then
                return 0
            end
        else
            -- Legacy orphan recovery is explicitly authorized.
            -- Never accept partially missing socket state.
            if ARGV[7] ~= "1"
                or oldSocketMarker then
                return 0
            end
        end

        -- Neither old nor new socket may have an active lock.
        for i = 6, 8 do
            if redis.call("EXISTS", KEYS[i]) == 1 then
                return 0
            end
        end

        -- Reject stale operation state on the new socket.
        if redis.call("EXISTS", KEYS[9]) == 1
            or redis.call("EXISTS", KEYS[10]) == 1 then
            return 0
        end

        local healthRaw = redis.call("GET", KEYS[1])
        local health = {}

        if healthRaw then
            health = cjson.decode(healthRaw)

            if health["socket_id"]
                and health["socket_id"] ~= ARGV[2] then
                return 0
            end
        end

        -- Cleanup and ownership transfer happen atomically.
        redis.call(
            "DEL",
            KEYS[2], KEYS[3], KEYS[4], KEYS[5]
        )

       health["connector_id"] = ARGV[1]
health["socket_id"] = ARGV[3]
health["connection_status"] = "CONNECTED"
health["disconnected_since_ms"] = cjson.null
health["operation_status"] = "UNKNOWN"
health["tally_status"] = "UNKNOWN"

-- Old socket queue status must not authorize new socket.
health["tally_queue_idle"] = false
health["tally_queue_generation"] = cjson.null
health["tally_queue_connector_epoch"] = cjson.null
health["tally_queue_reported_at"] = cjson.null
health["tally_queue_reported_at_ms"] = cjson.null

local leaseTime = redis.call("TIME")

health["socket_lease_renewed_at_ms"] =
    tonumber(leaseTime[1]) * 1000 +
    math.floor(tonumber(leaseTime[2]) / 1000)

health["shared_updated_at"] = ARGV[5]

        local updated = cjson.encode(health)

        redis.call(
            "SET", KEYS[1], updated, "EX", ARGV[6]
        )

        return updated
        `,
        10,
        getHealthKey(id),
        getConnectorQuarantineKey(id),
        getSocketQuarantineKey(oldSid),
        getConnectorActiveOperationKey(id),
        getSocketActiveOperationKey(oldSid),
        `billey:connector:operation-lock:${id}`,
        `billey:socket:operation-lock:${oldSid}`,
        `billey:socket:operation-lock:${newSid}`,
        getSocketQuarantineKey(newSid),
        getSocketActiveOperationKey(newSid),
        id,
        oldSid,
        newSid,
        token,
              new Date().toISOString(),
        String(ttl),
        allowVerifiedLegacyRecovery === true ? "1" : "0"
    );

    if (result === 0) {
        return {
            recovered: false,
            connector_id: id,
            health: null
        };
    }

    return {
        recovered: true,
        connector_id: id,
        health: JSON.parse(result)
    };
}

async function acquireHttpBatchExecutionGuard(batchId, ownerToken) {
    if (
        typeof batchId !== "string" ||
        !batchId.trim() ||
        typeof ownerToken !== "string" ||
        !ownerToken.trim()
    ) {
        throw new Error(
            "batchId and ownerToken must be nonempty strings"
        );
    }

    await ensureConnected();

    const key =
        `billey:sync:http-execution:${batchId.trim()}`;

    const result = await redis.set(
        key,
        ownerToken,
        "NX"
    );

    return result === "OK";
}

async function releaseHttpBatchExecutionGuard(batchId, ownerToken) {
    if (
        typeof batchId !== "string" ||
        !batchId.trim() ||
        typeof ownerToken !== "string" ||
        !ownerToken.trim()
    ) {
        throw new Error(
            "batchId and ownerToken must be nonempty strings"
        );
    }

    await ensureConnected();

    const key =
        `billey:sync:http-execution:${batchId.trim()}`;

    const result = await redis.eval(
        `
        if redis.call("GET", KEYS[1]) ~= ARGV[1] then
            return 0
        end

        return redis.call("DEL", KEYS[1])
        `,
        1,
        key,
        ownerToken
    );

    return Number(result) === 1;
}
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
    setConnectorHealth,
    getConnectorHealth,
    updateConnectorHealthAtomic,
    deleteConnectorHealth,
    ping,
    closeConnectorHealthStore,
    claimConnectorSocketOwnership,
    updateConnectorHealthIfSocketOwner,
    renewConnectorSocketLease,
    renewConnectorOperationLock,
 
    acquireConnectorOperationLock,

    releaseConnectorOperationLock,
    getConnectorOperationStatus,
    getConnectorWatchdogStatus,
    acquireSocketOperationLock,
    releaseSocketOperationLock,
    renewSocketOperationLock,
    getSocketOperationStatus,
    acquireConnectorAndSocketLock,
    releaseConnectorAndSocketLock,
        releaseSyncConnectorLockWithReceipt,
    readSyncConnectorReleaseReceipt,
    recoverSyncConnectorLockWithReceipt,
    releaseManualReconcileLockWithReceipt,
    readManualReconcileReleaseReceipt,
    renewConnectorAndSocketLock,
       quarantineConnectorAndSocket,
    clearVerifiedConnectorRecovery,
    inspectConnectorRecoveryState,
inspectManualReconcileActiveOwnership,
    recoverConnectorAfterReconnect,
    acquireHttpBatchExecutionGuard,
    updateConnectorTallyQueueStatus,
        releaseHttpBatchExecutionGuard,

    // Manual Reconciliation failure recovery
    stageManualReconcileFailureCleanup,
    readManualReconcileFailureCleanupReceipt,
    inspectManualReconcileFailureRedis,
    unblockManualReconcileAfterSqlClosure,
    readManualReconcileFailureUnblockReceipt
};