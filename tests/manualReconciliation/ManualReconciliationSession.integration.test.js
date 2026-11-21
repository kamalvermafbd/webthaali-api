
"use strict";

// LOCAL ONLY. In-memory boundaries; no Supabase, Redis, Tally or servers.
// Deliberately does not test Redis Lua atomicity or PostgreSQL transactions.
// Run this file alone: node --test --test-concurrency=1 <this file>
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { EventEmitter } = require("node:events");
const Module = require("node:module");
const path = require("node:path");
const fs = require("node:fs");

const ROOT = path.resolve(__dirname, "../..");
const local = relative => path.join(ROOT, relative);
const SESSION = local("services/manualReconciliation/ManualReconciliationSession.js");
const HEALTH = local("socketio/connectorHealthStore.js");
const REGISTRY = local("socketio/connectorRegistry.js");
const TOKEN = "11111111-1111-4111-8111-111111111111";
const BATCH = "22222222-2222-4222-8222-222222222222";
const CLAIM = "33333333-3333-4333-8333-333333333333";
const LEDGER = "44444444-4444-4444-8444-444444444444";
const COMPANY_GUID = "55555555-5555-4555-8555-555555555555";
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

const RPC = {
    admission: "start_manual_reconcile_claimed_v1",
    prepareAcquisition: "prepare_manual_reconcile_acquisition_v1",
    confirmAcquisition: "confirm_manual_reconcile_acquisition_v1",
    bind: "bind_manual_reconcile_execution_v1",
    prepare: "prepare_manual_reconcile_fetch_v2",
    dispatch: "mark_manual_reconcile_fetch_dispatch_v2",
    verify: "verify_manual_reconcile_fetch_result_v2",
    prepareApply: "prepare_manual_reconcile_apply_v2",
    apply: "apply_manual_reconcile_ledger_opening_v3",
    checkpoint: "record_manual_reconcile_pre_release_checkpoint_v2",
    record: "record_manual_reconcile_finalization_v2",
    close: "close_manual_reconcile_v2"
};

// No real sleeps. Advance deadline-by-deadline, allowing promise
// continuations between callbacks.
function installClock() {
    const originals = {
        setTimeout,
        clearTimeout,
        setInterval,
        clearInterval,
        now: Date.now
    };

    const timers = new Map();
    let now = 1700000000000;
    let serial = 0;

    function schedule(callback, delay, repeat, args) {
        assert.equal(typeof callback, "function");

        const duration = Math.max(1, Number(delay) || 1);
        const handle = {
            id: ++serial,
            unref() { return this; },
            ref() { return this; }
        };

        timers.set(handle, {
            callback,
            deadline: now + duration,
            duration,
            repeat,
            args
        });

        return handle;
    }

    global.setTimeout = (fn, ms, ...args) =>
        schedule(fn, ms, false, args);

    global.setInterval = (fn, ms, ...args) =>
        schedule(fn, ms, true, args);

    global.clearTimeout = global.clearInterval =
        handle => timers.delete(handle);

    Date.now = () => now;

    async function flush() {
        for (let i = 0; i < 300; i++) {
            await Promise.resolve();
        }
    }

    return {
        pending: () => timers.size,

        async until(predicate, label) {
            for (let i = 0; i < 3000; i++) {
                if (predicate()) return;
                await Promise.resolve();
            }

            assert.fail(
                "Controlled microtask budget exhausted: " + label
            );
        },

        async advance(ms) {
            const target = now + ms;
            let callbacks = 0;

            while (true) {
                let next = null;

                for (const entry of timers) {
                    if (
                        entry[1].deadline <= target &&
                        (
                            !next ||
                            entry[1].deadline < next[1].deadline
                        )
                    ) {
                        next = entry;
                    }
                }

                if (!next) break;

                assert.ok(
                    ++callbacks < 10000,
                    "fake clock must not spin"
                );

                const [handle, timer] = next;
                now = timer.deadline;

                if (timer.repeat) {
                    timer.deadline += timer.duration;
                } else {
                    timers.delete(handle);
                }

                timer.callback(...timer.args);
                await flush();
            }

            now = target;
            await flush();
        },

        restore() {
            timers.clear();

            global.setTimeout = originals.setTimeout;
            global.clearTimeout = originals.clearTimeout;
            global.setInterval = originals.setInterval;
            global.clearInterval = originals.clearInterval;
            Date.now = originals.now;
        }
    };
}

function scoped(filename) {
    return (
        filename.startsWith(
            local("services/manualReconciliation") + path.sep
        ) ||
        [
            SESSION,
            HEALTH,
            REGISTRY,
            local("socketio/connectorOperationCoordinator.js"),
            local("socketio/sendDurableReconcileRequest.js"),
            local("utils/reconciliations/fetchSqlVoucherEntries.js")
        ].includes(filename)
    );
}

async function withHarness(mode, body) {
    const savedCache = new Map(
        Object.entries(require.cache).filter(
            ([name]) => scoped(name)
        )
    );

    const originalLoad = Module._load;
    const originalConsoleError = console.error;
    const originalFetch =
        Object.getOwnPropertyDescriptor(global, "fetch");

    const events = [];
    const violations = [];

    const state = {
        emits: [],
        quarantines: [],
        errors: [],
        mismatchReads: 0,
        lock: null,
        binding: null,
        fetch: null,
        fetchState: null,
        apply: null,
        applied: false,
        checkpoint: false,
        recorded: false,
        closed: false,
        raceInjected: false
    };

    let clock;
    let socket;
    let pending;
    let sendRequest;
    let markDispatch;

    const fail = message => {
        violations.push(message);
        throw new Error(message);
    };

    const row = value => ({
        data: [value],
        error: null
    });

    function requireLock(connector, socketId, owner) {
        assert.deepEqual(state.lock, {
            connector,
            socketId,
            owner
        });
    }

    function checkExecution(args) {
        assert.ok(
            state.binding,
            "binding must precede journal calls"
        );

        assert.equal(
            args.p_attempt_id,
            state.binding.p_attempt_id
        );

        assert.equal(
            args.p_operation_token,
            TOKEN
        );

        assert.equal(
            args.p_execution_id,
            state.binding.p_execution_id
        );
    }

    try {
        clock = installClock();

        console.error = (...args) => {
            state.errors.push(args);
        };

        Object.defineProperty(global, "fetch", {
            configurable: true,
            writable: true,
            value: () => fail("Unexpected external fetch")
        });

        socket = new EventEmitter();

        const emitLocal =
            EventEmitter.prototype.emit.bind(socket);

        socket.id = "local-synthetic-socket";
        socket.connected = true;
        socket.companyGuids = [COMPANY_GUID];

        socket.protocolReceiver = {
            cancelCollection() {
                events.push("collection:cancel");
            }
        };

        socket.disconnect = force => {
            assert.equal(force, true);

            if (!socket.connected) return;

            socket.connected = false;
            events.push("socket:disconnect");

            emitLocal(
                "disconnect",
                "synthetic server disconnect"
            );
        };

        socket.emit = (event, payload) => {
            if (event !== "reconcileLedgerOpening") {
                return fail(
                    "Unexpected outbound event: " + event
                );
            }

            assert.equal(
                state.fetchState,
                "DISPATCH_MAY_HAVE_OCCURRED"
            );

            assert.equal(
                payload.requestId,
                state.fetch.p_request_id
            );

            state.emits.push(payload);
            events.push("socket:emit");

            if (mode !== "timeout") {
                queueMicrotask(() => {
                   emitLocal(
                    "reconcileLedgerOpeningResult",
                    {
                        requestId: payload.requestId,

                        attemptId: payload.attemptId,
                        executionId: payload.executionId,
                        operationId: payload.operationId,
                        operationOrdinal: payload.operationOrdinal,

                        success: true,

                        data: {
                            guid: LEDGER,
                            openingBalance: "100.00",
                            openingBalanceType: "CR"
                        }
                    }
                );
                });
            }

            return true;
        };

        const health = {
            async acquireConnectorAndSocketLock(
                connector,
                socketId,
                owner,
                options
            ) {
                assert.equal(state.lock, null);
                assert.equal(
                    connector,
                    "local-synthetic-connector"
                );
                assert.equal(socketId, socket.id);
                assert.match(owner, UUID);
                assert.equal(options.ttlMs, 120000);

                state.lock = {
                    connector,
                    socketId,
                    owner
                };

                events.push("redis:acquire");
                return true;
            },

            async renewConnectorAndSocketLock(
                connector,
                socketId,
                owner,
                ttl
            ) {
                requireLock(
                    connector,
                    socketId,
                    owner
                );

                assert.equal(ttl, 120000);
                events.push("redis:renew");

                return true;
            },

            async releaseConnectorAndSocketLock(
                connector,
                socketId,
                owner
            ) {
                requireLock(
                    connector,
                    socketId,
                    owner
                );

                assert.equal(
                    state.checkpoint,
                    true,
                    "checkpoint must precede release"
                );

                events.push("redis:release");

                if (mode === "release-failure") {
                    return false;
                }

                state.lock = null;
                events.push("redis:release:confirmed");

                return true;
            },

            async releaseManualReconcileLockWithReceipt(args) {
                requireLock(
                    args.connectorId,
                    args.socketId,
                    args.ownerToken
                );

                assert.equal(
                    state.checkpoint,
                    true,
                    "checkpoint must precede atomic release"
                );

                assert.equal(
                    args.attemptId,
                    state.binding.p_attempt_id
                );

                assert.equal(
                    args.executionId,
                    state.binding.p_execution_id
                );

                assert.equal(
                    args.serverInstanceId,
                    state.binding.p_server_instance_id
                );

                events.push("redis:release");

                if (mode === "release-failure") {
                    return {
                        released: false,
                        replayed: false,
                        receipt: null
                    };
                }

                const receipt = {
                    attemptId: args.attemptId,
                    executionId: args.executionId,
                    serverInstanceId: args.serverInstanceId,
                    connectorId: args.connectorId,
                    socketId: args.socketId,
                    ownerToken: args.ownerToken,
                    redisReleaseConfirmed: true
                };

                state.lock = null;
                events.push("redis:release:confirmed");

                return {
                    released: true,
                    replayed: false,
                    receipt
                };
            },

            async quarantineConnectorAndSocket(
                connector,
                socketId,
                reason,
                owner
            ) {
                requireLock(
                    connector,
                    socketId,
                    owner
                );

                state.quarantines.push(reason);

                events.push(
                    "redis:quarantine:" + reason
                );

                return true;
            },

            async getConnectorWatchdogStatus() {
                return fail(
                    "Unexpected acquisition retry/watchdog lookup"
                );
            }
        };

        const supabase = {
            from() {
                return fail(
                    "Unexpected Supabase query builder access"
                );
            },

            async rpc(name, args) {
                if (name === "record_manual_reconcile_coordinator_exit_receipt_v1") {
                    checkExecution(args);
                    assert.equal(args.p_server_instance_id, state.binding.p_server_instance_id);
                    assert.equal(args.p_connector_id, state.binding.p_connector_id);
                    assert.equal(args.p_socket_id, state.binding.p_socket_id);
                    assert.equal(args.p_owner_token, state.binding.p_owner_token);
                    assert.equal(args.p_coordinator_exited, true);
                    assert.equal(args.p_admission_sealed, true);
                    assert.equal(args.p_pending_work, 0);
                    assert.equal(args.p_callback_started, true);
                    assert.equal(args.p_callback_settled, true);
                    assert.equal(args.p_late_work_detected, false);
                    assert.equal(args.p_outcome, "LOCALLY_SETTLED_UNVERIFIED");
                    assert.equal(state.closed, false);
                    events.push(name);
                    state.exitReceipt = args;
                    return row({ recorded: true,
                        reason: "RECEIPT_RECORDED_OR_ALREADY_PRESENT" });
                }
                if (!Object.values(RPC).includes(name)) {
                    return fail(
                        "Unexpected RPC: " + name
                    );
                }

                events.push(name);

                if (name === RPC.admission) {
                    assert.equal(state.binding, null);
                    assert.equal(args.p_batch_id, BATCH);
                    assert.equal(
                        args.p_execution_claim_token,
                        CLAIM
                    );
                    assert.equal(
                        args.p_company_code,
                        "LOCAL_TEST_COMPANY"
                    );
                    assert.match(
                        args.p_attempt_id,
                        UUID
                    );

                    state.admission = args;

                    return row({
                        acquired: true,
                        reason: "ACQUIRED",
                        reservation_token: TOKEN
                    });
                }
if (name === RPC.prepareAcquisition) {
    assert.ok(state.admission, "admission must precede preparation");
    assert.equal(state.lock, null, "prepare must precede Redis lock");

    assert.equal(
        args.p_attempt_id,
        state.admission.p_attempt_id
    );
    assert.equal(args.p_operation_token, TOKEN);
    assert.match(args.p_execution_id, UUID);
    assert.match(args.p_server_instance_id, UUID);
    assert.equal(
        args.p_connector_id,
        "local-synthetic-connector"
    );
    assert.equal(args.p_socket_id, socket.id);
    assert.match(args.p_owner_token, UUID);

    state.acquisitionPreparation = args;

    return row({
        prepared: true,
        reason: "ACQUISITION_PREPARED"
    });
}

if (name === RPC.confirmAcquisition) {
    assert.ok(
        state.acquisitionPreparation,
        "preparation must precede confirmation"
    );

    const prepared = state.acquisitionPreparation;

    for (const key of [
        "p_attempt_id",
        "p_operation_token",
        "p_execution_id",
        "p_server_instance_id",
        "p_connector_id",
        "p_socket_id",
        "p_owner_token"
    ]) {
        assert.equal(
            args[key],
            prepared[key],
            `acquisition identity mismatch: ${key}`
        );
    }

    requireLock(
        args.p_connector_id,
        args.p_socket_id,
        args.p_owner_token
    );

    assert.equal(
        state.binding,
        null,
        "confirmation must precede execution binding"
    );

    state.acquisitionConfirmation = args;

    return row({
        confirmed: true,
        reason: "ACQUISITION_CONFIRMED"
    });
}
                if (name === RPC.bind) {
                    assert.ok(
                        state.acquisitionConfirmation,
                        "acquisition confirmation must precede execution binding"
                    );
                    assert.equal(
                        args.p_attempt_id,
                        state.admission.p_attempt_id
                    );

                    assert.equal(
                        args.p_operation_token,
                        TOKEN
                    );

                    assert.match(
                        args.p_execution_id,
                        UUID
                    );

                    assert.match(
                        args.p_server_instance_id,
                        UUID
                    );

                    requireLock(
                        args.p_connector_id,
                        args.p_socket_id,
                        args.p_owner_token
                    );

                    state.binding = args;

                    return row({
                        bound: true,
                        reason: "BOUND",
                        execution_id: args.p_execution_id
                    });
                }

                if (name === RPC.prepare) {
                    checkExecution(args);

                    assert.equal(
                        state.fetch,
                        null,
                        "no fetch retry"
                    );

                    assert.equal(
                        args.p_operation_ordinal,
                        1
                    );

                    assert.equal(
                        args.p_ledger_guid,
                        LEDGER
                    );

                    assert.match(
                        args.p_operation_id,
                        UUID
                    );

                    assert.match(
                        args.p_request_id,
                        UUID
                    );

                    state.fetch = args;
                    state.fetchState = "PREPARED";

                    return row({
                        prepared: true,
                        reason: "PREPARED",
                        operation_state: "PREPARED"
                    });
                }

                if (name === RPC.dispatch) {
                    checkExecution(args);

                    assert.equal(
                        args.p_operation_id,
                        state.fetch.p_operation_id
                    );

                    assert.equal(
                        args.p_request_id,
                        state.fetch.p_request_id
                    );

                    if (
                        mode === "duplicate" &&
                        !state.raceInjected
                    ) {
                        state.raceInjected = true;
                        events.push("competitor:dispatch");

                        await sendRequest(
                            socket,
                            "reconcileLedgerOpening",
                            state.fetch.p_input_payload,
                            {
                                requestId: args.p_request_id,

                                dispatch: () =>
                                    markDispatch({
                                        supabase,
                                        operationId:
                                            args.p_operation_id,
                                        attemptId:
                                            args.p_attempt_id,
                                        operationToken:
                                            args.p_operation_token,
                                        executionId:
                                            args.p_execution_id,
                                        requestId:
                                            args.p_request_id
                                    })
                            }
                        );
                    }

                    if (
                        state.fetchState !== "PREPARED"
                    ) {
                        return row({
                            marked: false,
                            reason:
                                "DISPATCH_ALREADY_MARKED_OR_SETTLED",
                            operation_state:
                                state.fetchState
                        });
                    }

                    state.fetchState =
                        "DISPATCH_MAY_HAVE_OCCURRED";

                    return row({
                        marked: true,
                        reason: "DISPATCH_MARKED",
                        operation_state:
                            state.fetchState
                    });
                }

                if (name === RPC.verify) {
                    checkExecution(args);

                    assert.equal(
                        state.fetchState,
                        "DISPATCH_MAY_HAVE_OCCURRED"
                    );

                    assert.equal(
                        args.p_operation_id,
                        state.fetch.p_operation_id
                    );

                    assert.equal(
                        args.p_request_id,
                        state.fetch.p_request_id
                    );

                    assert.deepEqual(
                        args.p_result_payload,
                        {
                            guid: LEDGER,
                            openingBalance: "100.00",
                            openingBalanceType: "CR"
                        }
                    );

                    state.fetchState =
                        "RESULT_VERIFIED";

                    return row({
                        verified: true,
                        reason: "RESULT_VERIFIED",
                        operation_state:
                            state.fetchState
                    });
                }

                if (name === RPC.prepareApply) {
                    checkExecution(args);

                    assert.equal(
                        state.fetchState,
                        "RESULT_VERIFIED"
                    );

                    assert.equal(
                        args.p_source_fetch_operation_id,
                        state.fetch.p_operation_id
                    );

                    assert.equal(
                        args.p_operation_ordinal,
                        2
                    );

                    state.apply = args;

                    return row({
                        prepared: true,
                        reason: "PREPARED",
                        operation_state: "PREPARED"
                    });
                }

                if (name === RPC.apply) {
                    checkExecution(args);

                    assert.equal(
                        args.p_operation_id,
                        state.apply.p_operation_id
                    );

                    assert.equal(
                        state.applied,
                        false,
                        "no duplicate mutation"
                    );

                    state.applied = true;

                    return row({
                        result: "UPDATED",
                        previous_balance: "10.00",
                        previous_type: "DR",
                        current_balance: "100.00",
                        current_type: "CR"
                    });
                }

                if (name === RPC.checkpoint) {
                    checkExecution(args);

                    assert.equal(
                        state.applied,
                        true
                    );

                    assert.equal(
                        state.mismatchReads,
                        2
                    );

                    assert.deepEqual(
                        args.p_remaining_mismatch_ledger_guids,
                        []
                    );

                    assert.deepEqual(
                        args.p_voucher_investigation_guids,
                        [],
                        "ledger-only flow must have no voucher investigations"
                    );

                    requireLock(
                        args.p_connector_id,
                        socket.id,
                        args.p_owner_token
                    );

                    state.checkpoint = true;

                    return {
                        data: true,
                        error: null
                    };
                }

                if (name === RPC.record) {
                    assert.equal(
                        state.lock,
                        null,
                        "Redis release must be confirmed"
                    );

                    assert.equal(
                        state.checkpoint,
                        true
                    );

                    assert.equal(
                        args.p_attempt_id,
                        state.binding.p_attempt_id
                    );

                    assert.equal(
                        args.p_operation_token,
                        TOKEN
                    );

                    assert.equal(
                        args.p_connector_id,
                        state.binding.p_connector_id
                    );

                    assert.equal(
                        args.p_release_owner_token,
                        state.binding.p_owner_token
                    );

                    assert.equal(
                        args.p_final_recheck_completed,
                        true
                    );

                    assert.equal(
                        args.p_connector_release_verified,
                        true
                    );

                    assert.deepEqual(
                        args.p_remaining_mismatch_ledger_guids,
                        []
                    );

                    assert.deepEqual(
                        args.p_voucher_investigation_guids,
                        [],
                        "ledger-only flow must have no voucher investigations"
                    );

                    assert.deepEqual(
                        args.p_release_evidence,
                        {
                            attemptId:
                                state.binding.p_attempt_id,
                            executionId:
                                state.binding.p_execution_id,
                            serverInstanceId:
                                state.binding.p_server_instance_id,
                            connectorId:
                                state.binding.p_connector_id,
                            socketId:
                                state.binding.p_socket_id,
                            ownerToken:
                                state.binding.p_owner_token,
                            redisReleaseConfirmed: true
                        }
                    );

                    state.recorded = true;

                    return row({
                        recorded: true,
                        reason: "EVIDENCE_RECORDED"
                    });
                }

                if (name === RPC.close) {
                    assert.equal(
                        state.recorded,
                        true
                    );

                    assert.equal(
                        state.lock,
                        null
                    );

                    assert.equal(
                        args.p_attempt_id,
                        state.binding.p_attempt_id
                    );

                    assert.equal(
                        args.p_operation_token,
                        TOKEN
                    );

                    state.closed = true;

                    return row({
                        closed: true,
                        reason: "CLOSED"
                    });
                }

                return fail(
                    "Unhandled RPC: " + name
                );
            }
        };

        const mocks = new Map([
            [
                HEALTH,
                health
            ],
            [
                REGISTRY,
                {
                    get(id) {
                        assert.equal(
                            id,
                            "local-synthetic-connector"
                        );

                        return socket;
                    }
                }
            ],
            [
                local(
                    "services/manualReconciliation/getMismatchLedgerGuids.js"
                ),
                {
                    async getMismatchLedgerGuids(args) {
                        assert.equal(
                            args.supabase,
                            supabase
                        );

                        assert.ok(state.lock);

                        events.push("mismatch:read");
                        state.mismatchReads++;

                        return state.mismatchReads === 1
                            ? [LEDGER]
                            : [];
                    }
                }
            ],
            [
                local(
                    "services/manualReconciliation/getReconciliationVoucherGuids.js"
                ),
                {
                    async getReconciliationVoucherGuids(args) {
                        assert.equal(
                            args.supabase,
                            supabase
                        );

                        assert.deepEqual(
                            args.ledgerGuids,
                            [LEDGER]
                        );

                        return [];
                    }
                }
            ],
            [
                local(
                    "services/manualReconciliation/getReconciliationVoucherDateRange.js"
                ),
                {
                    getReconciliationVoucherDateRange() {
                        return fail(
                            "Voucher path must stay disabled"
                        );
                    }
                }
            ]
        ]);

        for (const name of Object.keys(require.cache)) {
            if (scoped(name)) {
                delete require.cache[name];
            }
        }

        Module._load = function(
            request,
            parent,
            isMain
        ) {
            const appCaller =
                parent && scoped(parent.filename);

            if (
                appCaller &&
                !request.startsWith(".") &&
                !path.isAbsolute(request) &&
                request !== "crypto" &&
                request !== "node:crypto"
            ) {
                return fail(
                    "Unexpected app dependency (network boundary blocked): " +
                    request
                );
            }

            const resolved =
                Module._resolveFilename(
                    request,
                    parent,
                    isMain
                );

            if (mocks.has(resolved)) {
                return mocks.get(resolved);
            }

            if (
                appCaller &&
                path.isAbsolute(resolved) &&
                !scoped(resolved)
            ) {
                return fail(
                    "Unexpected app module: " + resolved
                );
            }

            return originalLoad.apply(
                this,
                arguments
            );
        };

        const sessionSource =
            fs.readFileSync(SESSION, "utf8");

        assert.doesNotMatch(
            sessionSource,
            /\bapplyVoucherAmountsDurable\b/,
            "voucher monetary apply must not be wired into the session"
        );

        const {
            runManualReconciliationSession
        } = require(SESSION);

        sendRequest = require(
            local(
                "socketio/sendDurableReconcileRequest.js"
            )
        ).sendDurableReconcileRequest;

        markDispatch = require(
            local(
                "services/manualReconciliation/manualReconcileDurableDispatch.js"
            )
        ).markDurableReconcileFetchDispatch;

        let settled = false;

        pending = runManualReconciliationSession({
            supabase,
            companyCode: "LOCAL_TEST_COMPANY",
            tallyOwner: "USER",
            connectorId: "local-synthetic-connector",
            tallyCompanyName:
                "Synthetic Local Company",
            tallyCompanyGuid: COMPANY_GUID,
            booksBeginningFrom: "20260401",
            batchId: BATCH,
            executionClaimToken: CLAIM
        }).then(
            value => {
                settled = true;
                return { value };
            },
            error => {
                settled = true;
                return { error };
            }
        );

        await body({
            state,
            events,
            socket,
            clock,
            settled: () => settled,

            async outcome() {
                await clock.until(
                    () => settled,
                    "session settlement"
                );

                return pending;
            }
        });

        if (mode === "success") {
            assert.equal(state.exitReceipt, undefined,
                "success release must not produce a failure exit receipt");
        } else {
            assert.ok(state.exitReceipt,
                "quiescent failure must persist its original identity receipt");
        }
        assert.deepEqual(
            violations,
            [],
            "no unexpected external calls"
        );

        assert.equal(
            socket.listenerCount(
                "reconcileLedgerOpeningResult"
            ),
            0
        );

        assert.equal(
            socket.listenerCount("disconnect"),
            0
        );

        assert.equal(
            clock.pending(),
            0,
            "coordinator/sender must clear their timers"
        );
    } finally {
        try {
            if (socket && pending) {
                socket.disconnect(true);
                await clock.advance(15000);
            }
        } finally {
            if (socket) {
                socket.removeAllListeners();
            }

            if (clock) {
                clock.restore();
            }

            console.error = originalConsoleError;

            if (originalFetch) {
                Object.defineProperty(
                    global,
                    "fetch",
                    originalFetch
                );
            } else {
                delete global.fetch;
            }

            Module._load = originalLoad;

            for (const name of Object.keys(require.cache)) {
                if (scoped(name)) {
                    delete require.cache[name];
                }
            }

            for (const [name, entry] of savedCache) {
                require.cache[name] = entry;
            }
        }
    }
}

function before(events, first, second) {
    assert.ok(
        events.indexOf(first) >= 0,
        "missing " + first
    );

    assert.ok(
        events.indexOf(second) >
        events.indexOf(first),
        first + " must precede " + second
    );
}

function noDownstream(events) {
    for (
        const name of [
            RPC.verify,
            RPC.prepareApply,
            RPC.apply,
            RPC.checkpoint,
            RPC.record,
            RPC.close
        ]
    ) {
        assert.equal(
            events.includes(name),
            false,
            "must not call " + name
        );
    }
}

function causeMessages(error) {
    const messages = [];

    for (
        let current = error;
        current;
        current = current.cause
    ) {
        messages.push(current.message);
    }

    return messages;
}

test(
    "success: one emit, durable verification/apply, checkpoint, release, finalization",
    { concurrency: false },
    async () => {
        await withHarness(
            "success",
            async h => {
                const {
                    value,
                    error
                } = await h.outcome();

                assert.ifError(error);

                assert.equal(
                    value.success,
                    true
                );

                assert.equal(
                    value.ledgers.length,
                    1
                );

                assert.equal(
                    value.ledgers[0].reconciliation.currentBalance,
                    "100.00"
                );

                assert.equal(
                    h.state.emits.length,
                    1
                );

                assert.equal(
                    h.state.closed,
                    true
                );

                assert.deepEqual(
                    h.state.quarantines,
                    []
                );

                before(
                    h.events,
                    RPC.admission,
                    "redis:acquire"
                );

                before(
                    h.events,
                    "redis:acquire",
                    RPC.bind
                );

                before(
                    h.events,
                    RPC.dispatch,
                    "socket:emit"
                );

                before(
                    h.events,
                    "socket:emit",
                    RPC.verify
                );

                before(
                    h.events,
                    RPC.verify,
                    RPC.prepareApply
                );

                before(
                    h.events,
                    RPC.prepareApply,
                    RPC.apply
                );

                before(
                    h.events,
                    RPC.apply,
                    RPC.checkpoint
                );

                before(
                    h.events,
                    RPC.checkpoint,
                    "redis:release"
                );

                before(
                    h.events,
                    "redis:release:confirmed",
                    RPC.record
                );

                before(
                    h.events,
                    RPC.record,
                    RPC.close
                );
            }
        );
    }
);

test(
    "duplicate dispatch: competing winner emits once; session emits no second request or downstream work",
    { concurrency: false },
    async () => {
        await withHarness(
            "duplicate",
            async h => {
                const {
                    error
                } = await h.outcome();

                assert.equal(
                    error?.reconcileRecoveryRequired,
                    true
                );

                assert.ok(
                    causeMessages(error).includes(
                        "DURABLE_RECONCILE_DISPATCH_NOT_AUTHORIZED"
                    )
                );

                assert.equal(
                    h.state.raceInjected,
                    true
                );

                assert.equal(
                    h.events.filter(
                        x => x === RPC.dispatch
                    ).length,
                    2
                );

                assert.equal(
                    h.state.emits.length,
                    1,
                    "only the competing dispatch may emit"
                );

                noDownstream(h.events);

                assert.equal(
                    h.events.includes(
                        "redis:release"
                    ),
                    false
                );

                assert.ok(
                    h.state.quarantines.length > 0
                );
            }
        );
    }
);

test(
    "timeout: controlled 30-second expiry, recovery and listener cleanup without retry or finalization",
    { concurrency: false },
    async () => {
        await withHarness(
            "timeout",
            async h => {
                await h.clock.until(
                    () =>
                        h.state.emits.length === 1 ||
                        h.settled(),
                    "first emit"
                );

                assert.equal(
                    h.settled(),
                    false,
                    "must reach the real sender before expiring"
                );

                assert.equal(
                    h.socket.listenerCount(
                        "reconcileLedgerOpeningResult"
                    ),
                    1
                );

                await h.clock.advance(29999);

                assert.equal(
                    h.settled(),
                    false
                );

                await h.clock.advance(1);

                const {
                    error
                } = await h.outcome();

                assert.equal(
                    error?.reconcileRecoveryRequired,
                    true
                );

                assert.ok(
                    causeMessages(error).includes(
                        "DURABLE_RECONCILE_CONNECTOR_TIMEOUT"
                    )
                );

                assert.equal(
                    h.state.emits.length,
                    1
                );

                assert.equal(
                    h.events.filter(
                        x => x === RPC.prepare
                    ).length,
                    1
                );

                assert.equal(
                    h.events.filter(
                        x => x === RPC.dispatch
                    ).length,
                    1
                );

                noDownstream(h.events);

                assert.equal(
                    h.socket.listenerCount(
                        "reconcileLedgerOpeningResult"
                    ),
                    0
                );

                assert.equal(
                    h.socket.listenerCount(
                        "disconnect"
                    ),
                    0
                );

                assert.equal(
                    h.clock.pending(),
                    0
                );
            }
        );
    }
);

test(
    "Redis release failure: quarantine/disconnect, recovery, no finalization or close",
    { concurrency: false },
    async () => {
        await withHarness(
            "release-failure",
            async h => {
                const {
                    error
                } = await h.outcome();

                assert.equal(
                    error?.reconcileRecoveryRequired,
                    true
                );

                assert.match(
                    error.message,
                    /Global lock release ownership mismatch/
                );

                assert.equal(
                    h.state.emits.length,
                    1
                );

                assert.equal(
                    h.state.applied,
                    true
                );

                assert.equal(
                    h.state.checkpoint,
                    true
                );

                assert.equal(
                    h.socket.connected,
                    false
                );

                assert.ok(
                    h.state.quarantines.includes(
                        "GLOBAL_LOCK_RELEASE_FAILED"
                    )
                );

                assert.equal(
                    h.events.includes(
                        "redis:release:confirmed"
                    ),
                    false
                );

                assert.equal(
                    h.events.includes(
                        RPC.record
                    ),
                    false
                );

                assert.equal(
                    h.events.includes(
                        RPC.close
                    ),
                    false
                );

                before(
                    h.events,
                    RPC.checkpoint,
                    "redis:release"
                );

                before(
                    h.events,
                    "redis:quarantine:GLOBAL_LOCK_RELEASE_FAILED",
                    "socket:disconnect"
                );
            }
        );
    }
);
