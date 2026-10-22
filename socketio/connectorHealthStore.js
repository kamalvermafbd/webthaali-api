
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
                return 0
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
        1,
        getHealthKey(id),
        id,
        sid,
        new Date().toISOString(),
        String(ttlSeconds)
    );

    if (result === 0) {
        return { claimed: false, health: null };
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

    const result = await redis.set(
        `billey:connector:operation-lock:${id}`,
        token,
        "PX",
        ttlMs,
        "NX"
    );

    return result === "OK";
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

    const result = await redis.eval(
        script,
        1,
        key,
        ownerToken,
        String(ttl)
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
    renewConnectorOperationLock,
    acquireConnectorOperationLock,
    releaseConnectorOperationLock
};