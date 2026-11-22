
"use strict";

// Run ONLY against an isolated, disposable Redis instance.
// Never run this test against production Redis.

const assert = require("node:assert/strict");
const Redis = require("ioredis");
const { randomUUID } = require("node:crypto");

const host = process.env.REDIS_HOST || "127.0.0.1";
const port = Number(process.env.REDIS_PORT || 6389);

if (process.env.OWNERSHIP_TEST_ISOLATED_REDIS !== "YES") {
    throw new Error(
        "Set OWNERSHIP_TEST_ISOLATED_REDIS=YES only after verifying isolated Redis."
    );
}

if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("Invalid REDIS_PORT");
}

const store = require("./connectorHealthStore");

const redis = new Redis({
    host,
    port,
    lazyConnect: true,
    connectTimeout: 3000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false
});

const connector = `test-ownership-${randomUUID()}`;
const oldSocket = `test-old-${randomUUID()}`;
const newSocket = `test-new-${randomUUID()}`;

const healthKey = `billey:connector:health:${connector}`;
const oldLock = `billey:socket:operation-lock:${oldSocket}`;
const oldMarker = `billey:socket:operation-active:${oldSocket}`;
const oldQuarantine = `billey:socket:quarantine:${oldSocket}`;

const keys = [
    healthKey,
    oldLock,
    oldMarker,
    oldQuarantine
];

async function seed(status) {
    await redis.set(
        healthKey,
        JSON.stringify({
            connector_id: connector,
            socket_id: oldSocket,
            connection_status: status,
            tally_queue_idle: true,
            tally_queue_generation: 4,
            tally_status: "ONLINE"
        }),
        "EX",
        180
    );
}

async function rejectWith(key, reason) {
    await seed("DISCONNECTED");

    await redis.set(key, "test-owner", "EX", 180);

    const before = await redis.get(healthKey);

    const result =
        await store.claimConnectorSocketOwnership(
            connector,
            newSocket
        );

    assert.equal(result.claimed, false);
    assert.equal(result.reason, reason);

    assert.equal(
        await redis.get(healthKey),
        before,
        "Rejected claim must not change health"
    );

    assert.equal(
        await redis.exists(key),
        1,
        "Rejected claim must not delete protection"
    );

    await redis.del(key);

    console.log("PASS:", reason);
}

(async () => {
    try {
        await redis.connect();

        await rejectWith(
            oldLock,
            "PREVIOUS_SOCKET_OPERATION_LOCK"
        );

        await rejectWith(
            oldMarker,
            "PREVIOUS_SOCKET_ACTIVE_MARKER"
        );

        await rejectWith(
            oldQuarantine,
            "PREVIOUS_SOCKET_QUARANTINE"
        );

        await seed("CONNECTED");

        const connected =
            await store.claimConnectorSocketOwnership(
                connector,
                newSocket
            );

        assert.equal(connected.claimed, false);
        assert.equal(
            connected.reason,
            "OTHER_CONNECTED_SOCKET"
        );

        assert.equal(
            connected.health?.socket_id,
            oldSocket
        );

        assert.equal(
            JSON.parse(await redis.get(healthKey)).socket_id,
            oldSocket
        );

        console.log(
            "PASS: OTHER_CONNECTED_SOCKET with diagnostic owner"
        );

        await seed("DISCONNECTED");

        const transferred =
            await store.claimConnectorSocketOwnership(
                connector,
                newSocket
            );

        assert.equal(transferred.claimed, true);
        assert.equal(
            transferred.health.socket_id,
            newSocket
        );

        assert.equal(
            transferred.health.connection_status,
            "CONNECTED"
        );

        assert.equal(
            transferred.health.tally_queue_idle,
            false
        );

        assert.equal(
            transferred.health.tally_status,
            "UNKNOWN"
        );

        console.log(
            "PASS: safe transfer and queue proof reset"
        );

        console.log("ALL 5 TESTS PASSED");

    } finally {
        // Clean only unique temporary test keys.
        // Never FLUSHDB or FLUSHALL.

        if (redis.status === "ready") {
            await redis.del(...keys);
        }

        redis.disconnect();

        await store.closeConnectorHealthStore()
            .catch(() => {});
    }
})().catch(error => {
    console.error("TEST FAILED:", error);
    process.exitCode = 1;
});
