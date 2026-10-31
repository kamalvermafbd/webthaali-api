
"use strict";

require("dotenv").config();

const Redis = require("ioredis");

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
    ownerToken = null
) {
    const id = normalizeConnectorId(connectorId);
    const sid = String(socketId || "").trim();

    if (!sid) {
        throw new Error("socketId is required");
    }

    const record = JSON.stringify({
    connector_id: id,
    socket_id: sid,
    owner_token: ownerToken
        ? String(ownerToken).trim()
        : null,
    reason: String(reason || "OPERATION_STATE_UNKNOWN"),
    quarantined_at: new Date().toISOString()
});

    await ensureConnected();

   const result = await redis.eval(
    `
        local connectorRecord = redis.call("GET", KEYS[1])
        local socketRecord = redis.call("GET", KEYS[2])

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
  2,
getConnectorQuarantineKey(id),
getSocketQuarantineKey(sid),
record,
id,
sid,
ownerToken ? String(ownerToken).trim() : ""
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

    const record = {
        ...health,
        connector_id: id,
        shared_updated_at: new Date().toISOString()
    };

    await ensureConnected();

    await redis.set(
        getHealthKey(id),
        JSON.stringify(record),
        "EX",
        ttlSeconds
    );

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
            record[k] = v
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
    const id = normalizeConnectorId(connectorId);

    await ensureConnected();

    return redis.del(getHealthKey(id));
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
            return "REJECTED:OTHER_CONNECTED_SOCKET"
        end
    end

    record["connector_id"] = ARGV[1]
    record["socket_id"] = ARGV[2]
    record["connection_status"] = "CONNECTED"
    record["shared_updated_at"] = ARGV[3]

    local updated = cjson.encode(record)

    redis.call("SET", KEYS[1], updated, "EX", ARGV[4])

    return updated
    `,
    7,
getHealthKey(id),
`billey:connector:operation-lock:${id}`,
getConnectorActiveOperationKey(id),
getConnectorQuarantineKey(id),
`billey:socket:operation-lock:${sid}`,
getSocketQuarantineKey(sid),
getSocketActiveOperationKey(sid),
id,
sid,
new Date().toISOString(),
String(ttlSeconds)
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

        -- A delayed heartbeat must not revive a disconnected socket.
        -- Reconnection must first pass through claimConnectorSocketOwnership.
        if record["connection_status"] == "DISCONNECTED"
            and patch["connection_status"] == "CONNECTED" then
            return 0
        end


        for k, v in pairs(patch) do
            if k ~= "connector_id"
                and k ~= "socket_id"
                and k ~= "shared_updated_at" then
                record[k] = v
            end
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

        return redis.call("EXPIRE", KEYS[1], ARGV[2])
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
        // Connector and socket locks must always exist together.
(
    connectorLocked !== socketLocked
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

const operationStatus =
    quarantined ||
    missingSocketMapping ||
    inconsistentConnectorState ||
    (activeOperation && !locked) ||
    inconsistentGlobalLock
        ? "QUARANTINED"
        : locked || activeOperation
            ? "BUSY"
            : "FREE";

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
    or health["connection_status"] ~= "CONNECTED" then
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


async function recoverConnectorAfterReconnect({
    connectorId,
    oldSocketId,
    newSocketId,
    ownerToken,
    verified,
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

    if not oldSocketQuarantine then
        return 0
    end

    local socketRecord =
        cjson.decode(oldSocketQuarantine)

    if socketRecord["connector_id"] ~= ARGV[1]
        or socketRecord["socket_id"] ~= ARGV[2]
        or socketRecord["owner_token"] ~= ARGV[4] then
        return 0
    end

        -- Verify persistent markers belong to the old operation.
        if redis.call("GET", KEYS[4]) ~= ARGV[4]
            or redis.call("GET", KEYS[5]) ~= ARGV[4] then
            return 0
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
        health["operation_status"] = "FREE"
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
        String(ttl)
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
    renewConnectorAndSocketLock,
       quarantineConnectorAndSocket,
    clearVerifiedConnectorRecovery,
    recoverConnectorAfterReconnect,
    acquireHttpBatchExecutionGuard,
    releaseHttpBatchExecutionGuard
};
