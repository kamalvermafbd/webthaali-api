
"use strict";

// ============================================================
// CONNECTOR SOCKET OWNERSHIP REGRESSION SUITE
// 20 TESTS - ISOLATED REDIS ONLY
// ============================================================

const assert = require("node:assert/strict");
const Redis = require("ioredis");
const { randomUUID } = require("node:crypto");

// Strict safety guard: never run against production Redis.
if (
    process.env.OWNERSHIP_TEST_ISOLATED_REDIS !== "YES" ||
    process.env.REDIS_HOST !== "127.0.0.1" ||
    process.env.REDIS_PORT !== "6389"
) {
    throw new Error(
        "ISOLATED_REDIS_REQUIRED: " +
        "Set REDIS_HOST=127.0.0.1, " +
        "REDIS_PORT=6389 and " +
        "OWNERSHIP_TEST_ISOLATED_REDIS=YES"
    );
}

const host = "127.0.0.1";
const port = 6389;

const store = require("./connectorHealthStore");

function createRedis() {
    return new Redis({
        host,
        port,
        lazyConnect: true,
        connectTimeout: 3000,
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false
    });
}

const redis = createRedis();

const connector = `test-ownership-${randomUUID()}`;
const oldSocket = `test-old-${randomUUID()}`;
const newSocket = `test-new-${randomUUID()}`;

const healthKey =
    `billey:connector:health:${connector}`;

const connectorLock =
    `billey:connector:operation-lock:${connector}`;

const connectorMarker =
    `billey:connector:operation-active:${connector}`;

const connectorQuarantine =
    `billey:connector:quarantine:${connector}`;

const oldLock =
    `billey:socket:operation-lock:${oldSocket}`;

const oldMarker =
    `billey:socket:operation-active:${oldSocket}`;

const oldQuarantine =
    `billey:socket:quarantine:${oldSocket}`;

const newLock =
    `billey:socket:operation-lock:${newSocket}`;

const newMarker =
    `billey:socket:operation-active:${newSocket}`;

const newQuarantine =
    `billey:socket:quarantine:${newSocket}`;

const keys = [
    healthKey,
    connectorLock,
    connectorMarker,
    connectorQuarantine,
    oldLock,
    oldMarker,
    oldQuarantine,
    newLock,
    newMarker,
    newQuarantine
];

let passed = 0;

function makeHealth({
    socketId = oldSocket,
    status = "DISCONNECTED",
    ...overrides
} = {}) {
    return {
        connector_id: connector,
        socket_id: socketId,
        connection_status: status,
        tally_queue_idle: true,
        tally_queue_generation: 4,
        tally_queue_pending_operations: 0,
        tally_queue_running_operations: 0,
        tally_queue_reported_at: "2026-01-01T00:00:00Z",
        tally_queue_reported_at_ms: 1767225600000,
        tally_status: "ONLINE",
        operation_status: "FREE",
        ...overrides
    };
}

async function seed(options = {}) {
    const health = makeHealth(options);

    await redis.set(
        healthKey,
        JSON.stringify(health),
        "EX",
        180
    );

    return health;
}

async function reset() {
    // Delete only keys belonging to this random test identity.
    await redis.del(...keys);
}

async function claim(socketId = newSocket, options) {
    return store.claimConnectorSocketOwnership(
        connector,
        socketId,
        options
    );
}

async function test(name, fn) {
    await reset();

    try {
        await fn();
        passed++;
        console.log(`PASS ${passed}/20: ${name}`);
    } catch (error) {
        console.error(`FAIL: ${name}`);
        throw error;
    } finally {
        await reset();
    }
}

async function expectRejectedKey(key, reason) {
    await seed();

    await redis.set(
        key,
        "test-owner",
        "EX",
        180
    );

    const before = await redis.get(healthKey);

    const result = await claim();

    assert.equal(result.claimed, false);
    assert.equal(result.reason, reason);

    assert.equal(
        await redis.get(healthKey),
        before,
        "Rejected claim must preserve health"
    );

    assert.equal(
        await redis.exists(key),
        1,
        "Rejected claim must preserve protection key"
    );
}

async function main() {
    await redis.connect();

    // Confirm this is the expected isolated Redis instance.
    assert.equal(await redis.ping(), "PONG");

    const configuredPort =
        await redis.config("GET", "port");

    assert.equal(
        Number(configuredPort[1]),
        6389,
        "Unexpected Redis port"
    );

    // ========================================================
    // TESTS 1-3: PREVIOUS SOCKET PROTECTION
    // ========================================================

    await test(
        "Previous socket operation lock",
        async () => {
            await expectRejectedKey(
                oldLock,
                "PREVIOUS_SOCKET_OPERATION_LOCK"
            );
        }
    );

    await test(
        "Previous socket active marker",
        async () => {
            await expectRejectedKey(
                oldMarker,
                "PREVIOUS_SOCKET_ACTIVE_MARKER"
            );
        }
    );

    await test(
        "Previous socket quarantine",
        async () => {
            await expectRejectedKey(
                oldQuarantine,
                "PREVIOUS_SOCKET_QUARANTINE"
            );
        }
    );

    // ========================================================
    // TESTS 4-6: CONNECTOR PROTECTION
    // ========================================================

    await test(
        "Connector operation lock",
        async () => {
            await expectRejectedKey(
                connectorLock,
                "CONNECTOR_OPERATION_LOCK"
            );
        }
    );

    await test(
        "Connector active marker",
        async () => {
            await expectRejectedKey(
                connectorMarker,
                "CONNECTOR_ACTIVE_MARKER"
            );
        }
    );

    await test(
        "Connector quarantine",
        async () => {
            await expectRejectedKey(
                connectorQuarantine,
                "CONNECTOR_QUARANTINE"
            );
        }
    );

    // ========================================================
    // TESTS 7-9: NEW SOCKET PROTECTION
    // ========================================================

    await test(
        "New socket operation lock",
        async () => {
            await expectRejectedKey(
                newLock,
                "SOCKET_OPERATION_LOCK"
            );
        }
    );

    await test(
        "New socket quarantine",
        async () => {
            await expectRejectedKey(
                newQuarantine,
                "SOCKET_QUARANTINE"
            );
        }
    );

    await test(
        "New socket active marker",
        async () => {
            await expectRejectedKey(
                newMarker,
                "SOCKET_ACTIVE_MARKER"
            );
        }
    );

    // ========================================================
    // TEST 10: CONNECTED OWNER MUST NOT BE REPLACED
    // ========================================================

    await test(
        "Other connected socket rejected",
        async () => {
            await seed({ status: "CONNECTED" });

            const before =
                await redis.get(healthKey);

            const result = await claim();

            assert.equal(result.claimed, false);
            assert.equal(
                result.reason,
                "OTHER_CONNECTED_SOCKET"
            );

            assert.equal(
                result.health?.socket_id,
                oldSocket
            );

            assert.equal(
                await redis.get(healthKey),
                before
            );
        }
    );

    // ========================================================
    // TEST 11: SAFE OWNERSHIP TRANSFER
    // ========================================================

    await test(
        "Disconnected socket safe transfer",
        async () => {
            await seed();

            const result = await claim();

            assert.equal(result.claimed, true);
            assert.equal(
                result.health.socket_id,
                newSocket
            );
            assert.equal(
                result.health.connection_status,
                "CONNECTED"
            );

            const stored =
                JSON.parse(await redis.get(healthKey));

            assert.equal(stored.socket_id, newSocket);
            assert.equal(
                stored.connection_status,
                "CONNECTED"
            );
        }
    );

    // ========================================================
    // TEST 12: SAME SOCKET RECLAIM
    // ========================================================

    await test(
        "Same socket ownership reclaim",
        async () => {
            await seed({
                socketId: oldSocket,
                status: "CONNECTED"
            });

            const result = await claim(oldSocket);

            assert.equal(result.claimed, true);
            assert.equal(
                result.health.socket_id,
                oldSocket
            );
            assert.equal(
                result.health.connection_status,
                "CONNECTED"
            );
        }
    );

    // ========================================================
    // TEST 13: MISSING HEALTH RECORD
    // ========================================================

    await test(
        "Missing health record initial claim",
        async () => {
            const result = await claim();

            assert.equal(result.claimed, true);
            assert.equal(
                result.health.socket_id,
                newSocket
            );
            assert.equal(
                result.health.tally_queue_idle,
                false
            );
        }
    );

    // ========================================================
    // TEST 14: NULL PREVIOUS SOCKET
    // ========================================================

    await test(
        "Null previous socket ownership",
        async () => {
            await seed({
                socketId: null,
                status: "DISCONNECTED"
            });

            const result = await claim();

            assert.equal(result.claimed, true);
            assert.equal(
                result.health.socket_id,
                newSocket
            );
        }
    );

    // ========================================================
    // TEST 15: MALFORMED HEALTH JSON
    // ========================================================

    await test(
        "Malformed health rejected",
        async () => {
            await redis.set(
                healthKey,
                "{invalid-json",
                "EX",
                180
            );

            await assert.rejects(
                () => claim(),
                /CONNECTOR_HEALTH_MALFORMED/
            );

            assert.equal(
                await redis.get(healthKey),
                "{invalid-json"
            );
        }
    );

    // ========================================================
    // TEST 16: INVALID SOCKET TYPE
    // ========================================================

    await test(
        "Invalid stored socket type rejected",
        async () => {
            await seed({
                socketId: 12345
            });

            const before =
                await redis.get(healthKey);

            await assert.rejects(
                () => claim(),
                /CONNECTOR_HEALTH_SOCKET_INVALID/
            );

            assert.equal(
                await redis.get(healthKey),
                before
            );
        }
    );

    // ========================================================
    // TEST 17: WHITESPACE SOCKET ID
    // ========================================================

    await test(
        "Whitespace stored socket rejected",
        async () => {
            await seed({
                socketId: " invalid-socket "
            });

            const before =
                await redis.get(healthKey);

            await assert.rejects(
                () => claim(),
                /CONNECTOR_HEALTH_SOCKET_INVALID/
            );

            assert.equal(
                await redis.get(healthKey),
                before
            );
        }
    );

    // ========================================================
    // TEST 18: DETERMINISTIC STALE SOCKET RACE
    // ========================================================

    await test(
        "STALE_PREVIOUS_SOCKET race guard",
        async () => {
            await seed();

            const competingSocket =
                `test-race-${randomUUID()}`;

            const competingHealth = {
                connector_id: connector,
                socket_id: competingSocket,
                connection_status: "DISCONNECTED",
                tally_queue_idle: false,
                tally_status: "UNKNOWN"
            };

            const raceRedis = createRedis();
            await raceRedis.connect();

            const originalEval = Redis.prototype.eval;
            let raceInjected = false;

            try {
                // Inject competing health after JS pre-read
                // but before the ownership Lua EVAL.
                Redis.prototype.eval =
                    async function (...args) {
                        if (
                            !raceInjected &&
                            typeof args[0] === "string" &&
                            args[0].includes(
                                "STALE_PREVIOUS_SOCKET"
                            )
                        ) {
                            raceInjected = true;

                            await raceRedis.set(
                                healthKey,
                                JSON.stringify(competingHealth),
                                "EX",
                                180
                            );
                        }

                        return originalEval.apply(
                            this,
                            args
                        );
                    };

                const result = await claim();

                assert.equal(raceInjected, true);
                assert.equal(result.claimed, false);
                assert.equal(
                    result.reason,
                    "STALE_PREVIOUS_SOCKET"
                );

                assert.deepEqual(
                    JSON.parse(
                        await redis.get(healthKey)
                    ),
                    competingHealth,
                    "Stale claim overwrote competing owner"
                );
            } finally {
                Redis.prototype.eval = originalEval;
                raceRedis.disconnect();
            }
        }
    );

    // ========================================================
    // TEST 19: QUEUE PROOF RESET ON TRANSFER
    // ========================================================

    await test(
        "All queue proof fields reset",
        async () => {
            await seed();

            const result = await claim();

            assert.equal(result.claimed, true);

            const health = result.health;

            assert.equal(
                health.operation_status,
                "UNKNOWN"
            );
            assert.equal(
                health.tally_status,
                "UNKNOWN"
            );
            assert.equal(
                health.tally_queue_idle,
                false
            );
            assert.equal(
                health.tally_queue_generation,
                null
            );
            assert.equal(
                health.tally_queue_reported_at,
                null
            );
            assert.equal(
                health.tally_queue_reported_at_ms,
                null
            );
        }
    );

    // ========================================================
    // TEST 20: HEALTH TTL ON SUCCESSFUL CLAIM
    // ========================================================

    await test(
        "Ownership health TTL applied",
        async () => {
            await seed();

            const result = await claim(
                newSocket,
                { ttlSeconds: 90 }
            );

            assert.equal(result.claimed, true);

            const ttl =
                await redis.ttl(healthKey);

            assert.ok(
                ttl > 0 && ttl <= 90,
                `Unexpected health TTL: ${ttl}`
            );

            assert.equal(
                result.health.connector_id,
                connector
            );
        }
    );

    assert.equal(passed, 20);

    console.log("");
    console.log("======================================");
    console.log("ALL 20 TESTS PASSED");
    console.log("ISOLATED REDIS OWNERSHIP SUITE PASSED");
    console.log("======================================");
}

(async () => {
    try {
        await main();
    } catch (error) {
        console.error("");
        console.error("TEST SUITE FAILED:", error);
        process.exitCode = 1;
    } finally {
        try {
            if (redis.status === "ready") {
                await reset();
            }
        } catch (cleanupError) {
            console.error(
                "TEST CLEANUP FAILED:",
                cleanupError
            );
            process.exitCode = 1;
        }

        redis.disconnect();

        try {
            await store.closeConnectorHealthStore();
        } catch (error) {
            console.error(
                "STORE CLOSE FAILED:",
                error
            );
            process.exitCode = 1;
        }
    }
})();
