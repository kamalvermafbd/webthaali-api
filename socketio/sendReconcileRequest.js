"use strict";

const { randomUUID } = require("crypto");

const RECONCILE_TIMEOUT_MS = 30000;

function sendReconcileRequest(
    socket,
    event,
    payload,
    options = {}
) {
    const requestId = randomUUID();
    const responseEvent = `${event}Result`;
    const signal = options.signal;

return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;
    let requestDispatched = false;

        function cleanup() {
            clearTimeout(timeout);
            socket.off(responseEvent, onResponse);
            socket.off("disconnect", onDisconnect);

            if (signal) {
                signal.removeEventListener("abort", onAbort);
            }
        }

       function finish(error, result) {
    if (settled) return;

    settled = true;
    cleanup();

    if (error) {
        error.requestDispatched = requestDispatched;
        error.requestId = requestId;
        reject(error);
    } else {
        resolve(result);
    }
}

        function onResponse(data) {
            if (data?.requestId !== requestId) {
                return;
            }

            if (data.success !== true) {
                finish(
                    new Error(
                        data.error || "RECONCILE_CONNECTOR_FAILED"
                    )
                );
                return;
            }

            finish(null, data.data);
        }

        function onDisconnect() {
            finish(
                new Error("RECONCILE_CONNECTOR_DISCONNECTED")
            );
        }

        function onAbort() {
            finish(
                signal.reason instanceof Error
                    ? signal.reason
                    : new Error("RECONCILE_REQUEST_ABORTED")
            );
        }

        if (!socket?.connected) {
            finish(new Error("RECONCILE_CONNECTOR_NOT_CONNECTED"));
            return;
        }

        if (signal?.aborted) {
            onAbort();
            return;
        }

        socket.on(responseEvent, onResponse);
        socket.once("disconnect", onDisconnect);

        if (signal) {
            signal.addEventListener("abort", onAbort, {
                once: true
            });

            if (signal.aborted) {
                onAbort();
                return;
            }
        }

        timeout = setTimeout(() => {
            finish(new Error("RECONCILE_CONNECTOR_TIMEOUT"));
        }, RECONCILE_TIMEOUT_MS);

        if (settled) return;

        try {
    requestDispatched = true;

    socket.emit(event, {
        ...payload,
        requestId
    });
        } catch (error) {
            finish(error);
        }
    });
}

module.exports = {
    sendReconcileRequest
};