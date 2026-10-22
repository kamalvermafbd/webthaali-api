
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

module.exports = {
    setConnectorHealth,
    getConnectorHealth,
    deleteConnectorHealth,
    ping,
    closeConnectorHealthStore
};
