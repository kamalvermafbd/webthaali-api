"use strict";

const { performance } = require("node:perf_hooks");
const registry = require("../socketio/connectorRegistry");

const FINAL_EVENT = "getMastersTransferComplete";
const ACK_EVENT = "getMastersTransferCompleteAck";

const RECEIPT_TTL_MS = 180000;
const MAX_RECEIPTS = 64;

// Retain disposed state for an existing object to prevent reinstalling
// old state across a disconnect/reconnect of that same object.
const socketStates = new WeakMap();

// Candidates are opaque capabilities created only by begin().
const candidates = new WeakMap();

function validString(value) {
    return typeof value === "string" && value.length > 0;
}

function dispose(state) {
    if (state.disposed) return;

    state.disposed = true;
  state.receipts.clear();

for (const candidate of state.pendingCandidates.values()) {
    candidates.delete(candidate);
}
state.pendingCandidates.clear();

    state.socket.off(FINAL_EVENT, state.onFinal);
    state.socket.off("disconnect", state.onDisconnect);
}

function connectionValid(state) {
    if (
        state.disposed ||
        state.socket.connected !== true ||
        state.socket.id !== state.socketId
    ) {
        dispose(state);
        return false;
    }

    return true;
}

function eligible(state, record) {
    if (!connectionValid(state)) return false;

    const socket = state.socket;

    return (
        record.socket === socket &&
        record.socketId === state.socketId &&
        record.signal.aborted === false &&
        socket.protocolReceiver?.collectionCancelled === false &&
        socket.connectorIds instanceof Set &&
        socket.connectorIds.has(record.connectorId) &&
        registry.get(record.connectorId) === socket
    );
}

function prune(state, now) {
    for (const [requestId, receipt] of state.receipts) {
        if (receipt.expiresAt <= now) {
            state.receipts.delete(requestId);
        }
    }
}

function install(socket) {
    if (
        !socket ||
        typeof socket.on !== "function" ||
        typeof socket.once !== "function" ||
        typeof socket.off !== "function" ||
        typeof socket.emit !== "function"
    ) {
        return false;
    }

    const existing = socketStates.get(socket);
    if (existing) {
        return connectionValid(existing);
    }

    if (socket.connected !== true || !validString(socket.id)) {
        return false;
    }

    const state = {
        socket,
        socketId: socket.id,
        disposed: false,
        receipts: new Map(),
        pendingCandidates: new Map(),
        onFinal: null,
        onDisconnect: null
    };

    state.onFinal = (data) => {
        if (
            !data ||
            data.success !== true ||
            !validString(data.requestId) ||
            !connectionValid(state)
        ) {
            return;
        }

        prune(state, performance.now());

        const receipt = state.receipts.get(data.requestId);
        if (!receipt) return;

        if (!eligible(state, receipt)) {
            state.receipts.delete(data.requestId);
            return;
        }

        // Historical receipt only. No protocol advancement or work.
        socket.emit(ACK_EVENT, {
            requestId: receipt.requestId,
            success: true
        });
    };

    state.onDisconnect = () => {
        dispose(state);
    };

    socketStates.set(socket, state);

    socket.on(FINAL_EVENT, state.onFinal);
    socket.once("disconnect", state.onDisconnect);

    return true;
}

function begin({ socket, connectorId, requestId, signal } = {}) {
    if (
        !validString(connectorId) ||
        !validString(requestId) ||
        !signal ||
        typeof signal.aborted !== "boolean" ||
        !install(socket)
    ) {
        return null;
    }

    const state = socketStates.get(socket);

    // Same-request reuse invalidates its receipt and pending candidate.
state.receipts.delete(requestId);

const previous = state.pendingCandidates.get(requestId);

if (previous) {
    candidates.delete(previous);
    state.pendingCandidates.delete(requestId);
}

    const record = {
        socket,
        socketId: state.socketId,
        connectorId,
        requestId,
        signal
    };

    if (!eligible(state, record)) return null;

const candidate = Object.freeze({});

candidates.set(candidate, { state, record });
state.pendingCandidates.set(requestId, candidate);

// Bound pending candidates independently from published receipts.
while (state.pendingCandidates.size > MAX_RECEIPTS) {
    const oldestRequestId =
        state.pendingCandidates.keys().next().value;

    const oldestCandidate =
        state.pendingCandidates.get(oldestRequestId);

    state.pendingCandidates.delete(oldestRequestId);
    candidates.delete(oldestCandidate);
}

    return candidate;
}

function publish(candidate) {
    if (
        !candidate ||
        typeof candidate !== "object"
    ) {
        return false;
    }

    const entry = candidates.get(candidate);
    if (!entry) return false;

    candidates.delete(candidate);

    const { state, record } = entry;

   if (
    state.pendingCandidates.get(record.requestId) !== candidate
) {
    return false;
}

state.pendingCandidates.delete(record.requestId);

    if (!eligible(state, record)) return false;

    const now = performance.now();
    prune(state, now);

    state.receipts.delete(record.requestId);
    state.receipts.set(record.requestId, {
        ...record,
        expiresAt: now + RECEIPT_TTL_MS
    });

    while (state.receipts.size > MAX_RECEIPTS) {
        const oldest = state.receipts.keys().next().value;
        state.receipts.delete(oldest);
    }

    return true;
}

module.exports = {
    install,
    begin,
    publish
};
