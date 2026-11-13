"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { test } = require("node:test");

const {
    fetchLedgerOpening
} = require("../../services/manualReconciliation/fetchLedgerOpening");

function createMockSocket(responseFactory) {
    const socket = new EventEmitter();
    socket.connected = true;

    const originalEmit = socket.emit.bind(socket);

    socket.emit = function (event, payload) {
        if (event === "reconcileLedgerOpening") {
            queueMicrotask(() => {
                const response = responseFactory(payload);
                originalEmit("reconcileLedgerOpeningResult", response);
            });
            return true;
        }

        return originalEmit(event, payload);
    };

    return socket;
}

function fetch(socket, signal) {
    return fetchLedgerOpening({
        socket,
        signal,
        company: "Test Company",
        ledgerGuid: "ledger-123",
        booksBeginningFrom: "20260401"
    });
}

test("preserves exact large decimal string", async () => {
    const socket = createMockSocket(request => ({
        requestId: request.requestId,
        success: true,
        data: {
            guid: "ledger-123",
            openingBalance: "999999999999999999.99",
            openingBalanceType: "CR"
        }
    }));

    const result = await fetch(socket);

    assert.equal(
        result.openingBalance,
        "999999999999999999.99"
    );
    assert.equal(result.openingBalanceType, "CR");
});

test("rejects mismatched ledger GUID", async () => {
    const socket = createMockSocket(request => ({
        requestId: request.requestId,
        success: true,
        data: {
            guid: "wrong-guid",
            openingBalance: "100.00",
            openingBalanceType: "CR"
        }
    }));

    await assert.rejects(
        fetch(socket),
        /RECONCILE_LEDGER_GUID_MISMATCH/
    );
});

test("rejects numeric balance", async () => {
    const socket = createMockSocket(request => ({
        requestId: request.requestId,
        success: true,
        data: {
            guid: "ledger-123",
            openingBalance: 100.25,
            openingBalanceType: "CR"
        }
    }));

    await assert.rejects(
        fetch(socket),
        /RECONCILE_DECIMAL_INVALID/
    );
});

test("rejects nonzero amount with empty type", async () => {
    const socket = createMockSocket(request => ({
        requestId: request.requestId,
        success: true,
        data: {
            guid: "ledger-123",
            openingBalance: "100.00",
            openingBalanceType: ""
        }
    }));

    await assert.rejects(
        fetch(socket),
        /RECONCILE_LEDGER_TYPE_MISMATCH/
    );
});

test("rejects pre-aborted session without sending", async () => {
    const controller = new AbortController();
    controller.abort();

    let sent = false;

    const socket = createMockSocket(() => {
        sent = true;
        throw new Error("SHOULD_NOT_SEND");
    });

    await assert.rejects(
        fetch(socket, controller.signal),
        /RECONCILE_SESSION_ABORTED/
    );

    assert.equal(sent, false);
});