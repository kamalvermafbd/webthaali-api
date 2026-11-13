"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
    applyLedgerOpening
} = require("../../services/manualReconciliation/applyLedgerOpening");

test("NOT_FOUND is rejected as structural mismatch", async () => {
    let rpcCalls = 0;

    const supabase = {
        rpc: async () => {
            rpcCalls++;

            return {
                data: [{
                    result: "NOT_FOUND",
                    previous_balance: null,
                    previous_type: null,
                    current_balance: null,
                    current_type: null
                }],
                error: null
            };
        }
    };

    await assert.rejects(
        () => applyLedgerOpening({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER",
            reservationToken:
                "11111111-1111-4111-8111-111111111111",
            ledger: {
                guid: "ledger-1",
                openingBalance: "100.00",
                openingBalanceType: "DR"
            }
        }),
        error => {
            assert.equal(
                error.message,
                "RECONCILE_LEDGER_NOT_FOUND"
            );

            assert.equal(
                error.reconcileLedgerGuid,
                "ledger-1"
            );

            assert.equal(
                error.reconcileStructuralMismatch,
                true
            );

            return true;
        }
    );

    assert.equal(rpcCalls, 1);
});

// =====================================================
// TEST 2 & 3: VALID LEDGER APPLY RESULTS
// =====================================================

for (const result of ["UPDATED", "UNCHANGED"]) {
    test(`${result} is accepted as valid ledger apply result`, async () => {
        let rpcCalls = 0;

        const supabase = {
            rpc: async () => {
                rpcCalls++;

                return {
                    data: [{
                        result,
                        previous_balance: "50.00",
                        previous_type: "DR",
                        current_balance: "100.00",
                        current_type: "DR"
                    }],
                    error: null
                };
            }
        };

        const applied = await applyLedgerOpening({
            supabase,
            companyCode: "C0053",
            tallyOwner: "USER",
            reservationToken:
                "11111111-1111-4111-8111-111111111111",
            ledger: {
                guid: "ledger-1",
                openingBalance: "100.00",
                openingBalanceType: "DR"
            }
        });

        assert.equal(applied.result, result);
        assert.equal(applied.previousBalance, "50.00");
        assert.equal(applied.previousType, "DR");
        assert.equal(applied.currentBalance, "100.00");
        assert.equal(applied.currentType, "DR");
        assert.equal(rpcCalls, 1);
    });
}