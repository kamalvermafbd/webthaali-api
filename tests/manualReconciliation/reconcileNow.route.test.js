
"use strict";

// LOCAL ONLY: Isolates the actual Express route handler.
// Does not start secondServer, workers, Redis, Tally or Supabase.

const assert = require("node:assert/strict");
const { test } = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const ROOT = path.resolve(__dirname, "../..");
const SERVER = path.join(ROOT, "secondServer.js");

const BATCH_ID =
    "22222222-2222-4222-8222-222222222222";
const CLAIM =
    "33333333-3333-4333-8333-333333333333";

function loadRoute({ batch, sessionResult } = {}) {
    const source = fs.readFileSync(SERVER, "utf8");

    const start = source.indexOf(
        'app.post("/reconcileNow",'
    );

    assert.ok(
        start >= 0,
        "Missing /reconcileNow route in secondServer.js"
    );

    const nextRoute = source.indexOf(
        'app.post("/reconcileRecovery",',
        start
    );

    assert.ok(
        nextRoute > start,
        "Missing /reconcileRecovery route boundary"
    );

    const routeSource = source.slice(start, nextRoute);

    const registered = [];
    const calls = {
        lookups: [],
        rpc: [],
        session: []
    };

    const effectiveBatch = batch === undefined
        ? {
            batch_id: BATCH_ID,
            company_code: "COMPANY_FROM_BATCH",
            tally_owner: "CA",
            batch_status: "COMPLETED",
            worker_status: "COMPLETED",
            batch_closed: false,
            ledger_reconciliation_completed: false,
            execution_claim_token: CLAIM,
            error_message: null,
            retry_claim_token: null,
            retry_lease_until: null,
            cleanup_claim_token: null,
            cleanup_lease_until: null
        }
        : batch;

    const companyRow = {
        company_code: "COMPANY_FROM_BATCH",
        client_connector_id: "USER_CONNECTOR",
        ca_connector_id: "CA_CONNECTOR",
        client_tally_company: "USER TALLY",
        ca_tally_company: "CA TALLY",
        client_tally_company_guid: "USER_GUID",
        ca_tally_company_guid: "CA_GUID",
        client_tally_books_begining_from: "20240401",
        ca_tally_books_begining_from: "20240401"
    };

    const supabase = {
        from(table) {
            const lookup = {
                table,
                filters: [],
                columns: null
            };

            calls.lookups.push(lookup);

            const query = {
                select(columns) {
                    lookup.columns = columns;
                    return query;
                },

                eq(column, value) {
                    lookup.filters.push({ column, value });
                    return query;
                },

                async maybeSingle() {
                    assert.equal(table, "sync_batches");

                    return {
                        data: effectiveBatch,
                        error: null
                    };
                },

                async single() {
                    assert.equal(table, "company");

                    return {
                        data: companyRow,
                        error: null
                    };
                }
            };

            return query;
        },

        async rpc(name, args) {
            calls.rpc.push({ name, args });

            assert.equal(
                name,
                "assert_sync_batch_reconciliation_claim"
            );

            return {
                data: true,
                error: null
            };
        }
    };

    const app = {
        post(route, handler) {
            registered.push({ route, handler });
        }
    };

    const context = {
        app,
        supabase,
        process: {
            env: {
                MANUAL_RECONCILE_EXECUTION_ENABLED: "true"
            }
        },
        console: {
            error() {}
        },
        runManualReconciliationSession: async input => {
            calls.session.push(input);

            return sessionResult || {
                success: true,
                requiresTransactionInvestigation: false,
                remainingMismatchLedgerGuids: [],
                ledgers: [],
                vouchers: []
            };
        }
    };
console.log("EXTRACTED ROUTE START:", routeSource.slice(0, 700));
    vm.runInNewContext(routeSource, context, {
        filename: "secondServer.js:/reconcileNow",
        timeout: 1000
    });

    const route = registered.find(
        entry => entry.route === "/reconcileNow"
    );

    assert.ok(route, "Route was not registered");

    async function request(body) {
        const response = {
            statusCode: 200,
            body: null,

            status(code) {
                this.statusCode = code;
                return this;
            },

            json(payload) {
                this.body = JSON.parse(
                    JSON.stringify(payload)
                );
                return this;
            }
        };
        console.log("TEST INPUT:", {
    body,
    batchId: body?.sync_batch_id,
    type: typeof body?.sync_batch_id,
    length: body?.sync_batch_id?.length,
    uuidMatches:
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
            body?.sync_batch_id
        )
});

        await route.handler({ body }, response);

        console.log("ROUTE TEST RESPONSE:", response.statusCode, response.body);

        return response;
    }

    return { request, calls };
}

test(
    "Derives company and owner from sync_batches",
    async () => {
        const h = loadRoute();

        const res = await h.request({
            sync_batch_id: BATCH_ID
        });

        assert.equal(res.statusCode, 200);
        assert.equal(res.body.success, true);

        assert.equal(
            res.body.company_code,
            "COMPANY_FROM_BATCH"
        );

        assert.equal(res.body.tally_owner, "CA");

        assert.equal(h.calls.session.length, 1);

        const input = h.calls.session[0];

        assert.equal(
            input.companyCode,
            "COMPANY_FROM_BATCH"
        );

        assert.equal(input.tallyOwner, "CA");
        assert.equal(input.batchId, BATCH_ID);
        assert.equal(input.executionClaimToken, CLAIM);
        assert.equal(input.connectorId, "CA_CONNECTOR");
        assert.equal(input.tallyCompanyName, "CA TALLY");
        assert.equal(input.tallyCompanyGuid, "CA_GUID");

        assert.deepEqual(
            h.calls.lookups.map(x => x.table),
            ["sync_batches", "company"]
        );

        assert.deepEqual(
            h.calls.lookups[1].filters,
            [{
                column: "company_code",
                value: "COMPANY_FROM_BATCH"
            }]
        );

        assert.equal(h.calls.rpc.length, 1);
    }
);

test(
    "Rejects ineligible batch before company lookup",
    async () => {
        const h = loadRoute({
            batch: {
                batch_id: BATCH_ID,
                company_code: "COMPANY_FROM_BATCH",
                tally_owner: "USER",
                batch_status: "FAILED",
                worker_status: "FAILED",
                batch_closed: false,
                ledger_reconciliation_completed: false,
                execution_claim_token: CLAIM
            }
        });

        const res = await h.request({
            sync_batch_id: BATCH_ID
        });

        assert.equal(res.statusCode, 409);
        assert.equal(
            res.body.error_code,
            "RECONCILE_BATCH_NOT_ELIGIBLE"
        );

        assert.equal(h.calls.session.length, 0);
        assert.equal(h.calls.rpc.length, 0);

        assert.deepEqual(
            h.calls.lookups.map(x => x.table),
            ["sync_batches"]
        );
    }
);

test(
    "Ignores caller-supplied company and owner",
    async () => {
        const h = loadRoute();

        const res = await h.request({
            sync_batch_id: BATCH_ID,
            company_code: "ATTACKER_COMPANY",
            tally_owner: "USER",
            is_ca: false
        });

        assert.equal(res.statusCode, 200);

        assert.equal(
            res.body.company_code,
            "COMPANY_FROM_BATCH"
        );

        assert.equal(res.body.tally_owner, "CA");

        assert.equal(h.calls.session.length, 1);

        const input = h.calls.session[0];

        assert.equal(
            input.companyCode,
            "COMPANY_FROM_BATCH"
        );

        assert.equal(input.tallyOwner, "CA");
        assert.equal(input.connectorId, "CA_CONNECTOR");

        assert.ok(
            !h.calls.lookups.some(
                x => x.filters.some(
                    f => f.value === "ATTACKER_COMPANY"
                )
            )
        );
    }
);
