
"use strict";

const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
    getMismatchLedgerGuids
} = require("../../services/manualReconciliation/getMismatchLedgerGuids");

function mockSupabase(rows, options = {}) {
    const calls = [];

    return {
        calls,

        from(table) {
            assert.equal(table, "balance_difference_view");

            let afterId = null;

            const query = {
                select(columns) {
                    assert.equal(columns, "id, guid");
                    return this;
                },

                eq(column, value) {
                    calls.push({ column, value });
                    return this;
                },

                order(column, config) {
                    assert.equal(column, "id");
                    assert.equal(config.ascending, true);
                    return this;
                },

                gt(column, value) {
                    assert.equal(column, "id");

                    afterId = String(value);

                    calls.push({
                        gtColumn: column,
                        gtValue: afterId
                    });

                    return this;
                },

                async limit(count) {
                    assert.equal(count, 500);

                    calls.push({
                        limit: count,
                        afterId
                    });

                    if (options.error) {
                        return {
                            data: null,
                            error: options.error
                        };
                    }

                    const filteredRows = rows
                        .filter(row =>
                            afterId === null ||
                            BigInt(row.id) > BigInt(afterId)
                        )
                        .sort((a, b) => {
                            const left = BigInt(a.id);
                            const right = BigInt(b.id);

                            return left < right
                                ? -1
                                : left > right
                                    ? 1
                                    : 0;
                        });

                    return {
                        data: filteredRows.slice(0, count),
                        error: null
                    };
                }
            };

            return query;
        }
    };
}

// =====================================================
// TEST 1: COMPANY AND OWNER FILTERS
// =====================================================

test("fetches mismatch GUIDs for company and owner", async () => {
    const supabase = mockSupabase([
        { id: "1", guid: "ledger-1" },
        { id: "2", guid: "ledger-2" }
    ]);

    const result = await getMismatchLedgerGuids({
        supabase,
        companyCode: "C0053",
        tallyOwner: "USER"
    });

    assert.deepEqual(result, [
        "ledger-1",
        "ledger-2"
    ]);

    assert.ok(
        supabase.calls.some(
            call =>
                call.column === "company_code" &&
                call.value === "C0053"
        )
    );

    assert.ok(
        supabase.calls.some(
            call =>
                call.column === "tally_owner" &&
                call.value === "USER"
        )
    );
});

// =====================================================
// TEST 2: NO MISMATCHES
// =====================================================

test("returns empty array when no mismatch exists", async () => {
    const supabase = mockSupabase([]);

    const result = await getMismatchLedgerGuids({
        supabase,
        companyCode: "C0053",
        tallyOwner: "USER"
    });

    assert.deepEqual(result, []);
});

// =====================================================
// TEST 3: DUPLICATE LEDGER GUID
// =====================================================

test("rejects duplicate ledger GUIDs", async () => {
    const supabase = mockSupabase([
        { id: "1", guid: "ledger-1" },
        { id: "2", guid: "ledger-1" }
    ]);

    await assert.rejects(
        getMismatchLedgerGuids({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER"
        }),
        /RECONCILE_DUPLICATE_MISMATCH_LEDGER_GUID/
    );
});

// =====================================================
// TEST 4: MISSING LEDGER GUID
// =====================================================

test("rejects missing ledger GUID", async () => {
    const supabase = mockSupabase([
        { id: "1", guid: null }
    ]);

    await assert.rejects(
        getMismatchLedgerGuids({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER"
        }),
        /RECONCILE_MISMATCH_LEDGER_ROW_INVALID/
    );
});

// =====================================================
// TEST 5: DATABASE FAILURE
// =====================================================

test("propagates database query failure", async () => {
    const supabase = mockSupabase([], {
        error: new Error("DATABASE_UNAVAILABLE")
    });

    await assert.rejects(
        getMismatchLedgerGuids({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER"
        }),
        /DATABASE_UNAVAILABLE/
    );
});

// =====================================================
// TEST 6: KEYSET PAGINATION
// =====================================================

test("fetches all pages without missing GUIDs", async () => {
    const rows = Array.from(
        { length: 501 },
        (_, index) => ({
            id: String(index + 1),
            guid: `ledger-${index + 1}`
        })
    );

    const supabase = mockSupabase(rows);

    const result = await getMismatchLedgerGuids({
        supabase,
        companyCode: "C0053",
        tallyOwner: "USER"
    });

    assert.equal(result.length, 501);
    assert.equal(result[0], "ledger-1");
    assert.equal(result[500], "ledger-501");

    assert.ok(
        supabase.calls.some(
            call =>
                call.gtColumn === "id" &&
                call.gtValue === "500"
        )
    );

    assert.equal(
        supabase.calls.filter(
            call => call.limit === 500
        ).length,
        2
    );
});

// =====================================================
// TEST 7: UNSAFE NUMERIC BIGINT ID
// =====================================================

test("rejects unsafe numeric ledger ID", async () => {
    const supabase = mockSupabase([
        {
            id: 9007199254740992,
            guid: "ledger-unsafe"
        }
    ]);

    await assert.rejects(
        () => getMismatchLedgerGuids({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER"
        }),
        /RECONCILE_MISMATCH_LEDGER_ROW_INVALID/
    );
});

// =====================================================
// TEST 8: LARGE STRING BIGINT IDS
// =====================================================

test("supports large bigint IDs without precision loss", async () => {
    const supabase = mockSupabase([
        {
            id: "9007199254740992",
            guid: "ledger-large-1"
        },
        {
            id: "9007199254740993",
            guid: "ledger-large-2"
        }
    ]);

    const result = await getMismatchLedgerGuids({
        supabase,
        companyCode: "C0053",
        tallyOwner: "USER"
    });

    assert.deepEqual(result, [
        "ledger-large-1",
        "ledger-large-2"
    ]);
});

// =====================================================
// TEST 9: INVALID PAGINATION ORDER
// =====================================================

test("rejects non-increasing ledger IDs", async () => {
    let limitCalls = 0;

    const supabase = {
        from(table) {
            assert.equal(table, "balance_difference_view");

            return {
                select(columns) {
                    assert.equal(columns, "id, guid");
                    return this;
                },

                eq() {
                    return this;
                },

                order(column, config) {
                    assert.equal(column, "id");
                    assert.equal(config.ascending, true);
                    return this;
                },

                gt() {
                    return this;
                },

                async limit() {
                    limitCalls++;

                    return {
                        data: [
                            { id: "2", guid: "ledger-2" },
                            { id: "1", guid: "ledger-1" }
                        ],
                        error: null
                    };
                }
            };
        }
    };

    await assert.rejects(
        () => getMismatchLedgerGuids({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER"
        }),
        /RECONCILE_MISMATCH_PAGINATION_ORDER_INVALID/
    );

    assert.equal(limitCalls, 1);
});