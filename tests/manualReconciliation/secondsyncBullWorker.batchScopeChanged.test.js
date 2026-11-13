"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

test("active worker skips BATCH_SCOPE_CHANGED without dispatch or failure writes", async () => {
    const worker = {
        id: "isolated-worker",
        worker_name: "isolated-worker-name",
        queue_name: "isolated-queue",
        server_id: "isolated-server",
        concurrency: 1
    };
    const calls = {
        lock: 0, delayed: 0, heartbeat: 0, runJob: 0,
        database: 0, network: 0, exit: 0
    };
    let processor;
    let processing = false;
    const forbidden = name => () => {
        calls[name]++;
        throw new Error(`Unexpected ${name} call`);
    };
    const supabase = {
        from(table) {
            if (processing) return forbidden("database")();
            assert.equal(table, "workers");
            const query = {
                select() { return query; },
                eq() { return query; },
                async single() { return { data: { ...worker }, error: null }; },
                update: forbidden("database")
            };
            return query;
        },
        async rpc(name) {
            if (processing) return forbidden("database")();
            assert.equal(name, "claim_worker_runtime");
            return { data: [{ id: worker.id }], error: null };
        }
    };
    class MockWorker {
        constructor(queueName, callback) {
            assert.equal(queueName, worker.queue_name);
            assert.equal(processor, undefined);
            processor = callback;
        }
        on() { return this; }
        close = forbidden("network");
    }
    class MockDelayedError extends Error {}
    const dependencies = {
        dotenv: { config() {} },
        bullmq: { Worker: MockWorker, DelayedError: MockDelayedError },
        "@supabase/supabase-js": { createClient() { return supabase; } },
        crypto: { randomUUID() { return "isolated-runtime"; } },
        "../jobs/syncjobRunner": { runJob: forbidden("runJob") },
        "./workerLockService": {
            async lockJob(input, config) {
                calls.lock++;
                assert.equal(input.data.batch_db_id, 123);
                assert.equal(config.worker_name, worker.worker_name);
                input.claimRejectionReason = "BATCH_SCOPE_CHANGED";
                return null;
            },
            startHeartbeat: forbidden("heartbeat")
        }
    };
    const sandbox = {
        require(name) {
            assert.ok(Object.hasOwn(dependencies, name), `Unmocked import: ${name}`);
            return dependencies[name];
        },
        process: {
            argv: ["node", "secondsyncBullWorker.js", worker.id],
            env: {},
            once() {},
            exit: forbidden("exit")
        },
        console: { log() {}, error: forbidden("exit") },
        setInterval(callback, delay) {
            assert.equal(delay, 30000);
            // Never schedule or execute the worker runtime heartbeat.
            return { isolatedTimer: true };
        },
        clearInterval() {},
        setTimeout: forbidden("network"),
        fetch: forbidden("network")
    };
    const filename = path.resolve(__dirname, "../../workers/secondsyncBullWorker.js");
    const source = fs.readFileSync(filename, "utf8");
    await vm.runInNewContext(source, sandbox, { filename, timeout: 1000 });
    assert.equal(typeof processor, "function");
    processing = true;
    const result = await processor({
        id: "isolated-job",
        data: {
            batch_id: "isolated-batch",
            batch_db_id: 123,
            worker_name: worker.worker_name,
            worker_id: worker.id,
            server_id: worker.server_id,
            http_url: "https://must-never-be-called.invalid"
        },
        moveToDelayed: forbidden("delayed")
    }, "isolated-bullmq-token");

    // The result originates in another VM realm; compare its JSON representation.
    assert.deepEqual(JSON.parse(JSON.stringify(result)), {
        success: true,
        skipped: true,
        reason: "BATCH_SCOPE_CHANGED"
    });
    assert.deepEqual(calls, {
        lock: 1, delayed: 0, heartbeat: 0, runJob: 0,
        database: 0, network: 0, exit: 0
    });
});
