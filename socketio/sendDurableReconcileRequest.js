
"use strict";

const RECONCILE_TIMEOUT_MS = 30000;

const UUID =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function uncertainError(code, requestId, cause) {
    const error = new Error(code, cause ? { cause } : undefined);

    error.requestId = requestId;
    error.reconcileOutcomeUncertain = true;
    error.reconcileRecoveryRequired = true;

    return error;
}

/**
 * Durable reconciliation socket request.
 *
 * dispatch() must commit the durable dispatch marker
 * before this function is allowed to emit.
 *
 * This sender never retries or generates a requestId.
 *
 * A dispatch RPC response lost after commit is uncertain:
 * it must NOT be followed by a socket emit.
 */
async function sendDurableReconcileRequest(
    socket,
    event,
    payload,
    {
        requestId,
        signal,
        dispatch
    } = {}
) {
    if (
        typeof requestId !== "string" ||
        !UUID.test(requestId) ||
        typeof event !== "string" ||
        !event.trim() ||
        !payload ||
        typeof payload !== "object" ||
        Array.isArray(payload) ||
        typeof dispatch !== "function"
    ) {
        throw new Error("DURABLE_RECONCILE_REQUEST_INVALID");
    }

    if (
        Object.prototype.hasOwnProperty.call(
            payload,
            "requestId"
        )
    ) {
        throw new Error(
            "DURABLE_RECONCILE_REQUEST_ID_OVERRIDE"
        );
    }

    if (signal?.aborted) {
        throw new Error("DURABLE_RECONCILE_ABORTED_BEFORE_DISPATCH");
    }

    if (!socket?.connected) {
        throw new Error(
            "DURABLE_RECONCILE_SOCKET_UNAVAILABLE_BEFORE_DISPATCH"
        );
    }

    // This must be a fresh PREPARED -> DISPATCH_MAY_HAVE_OCCURRED
    // transition, not an idempotent replay.
    let dispatchResult;

    try {
        dispatchResult = await dispatch();
    } catch (cause) {
        throw uncertainError(
            "DURABLE_RECONCILE_DISPATCH_UNVERIFIED",
            requestId,
            cause
        );
    }

    if (
        !dispatchResult ||
        dispatchResult.dispatchAuthorized !== true ||
        dispatchResult.requestId !== requestId ||
        dispatchResult.freshTransition !== true
    ) {
        throw uncertainError(
            "DURABLE_RECONCILE_DISPATCH_NOT_AUTHORIZED",
            requestId
        );
    }

    // An abort/disconnect may occur while dispatch RPC is in flight.
    // Do not emit if ownership is no longer usable.
    if (signal?.aborted || !socket.connected) {
        throw uncertainError(
            "DURABLE_RECONCILE_POST_DISPATCH_UNAVAILABLE",
            requestId
        );
    }

    const responseEvent = `${event}Result`;

    return new Promise((resolve, reject) => {
        let settled = false;
        let timeout = null;
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
                error.requestId = requestId;
                error.requestDispatched = requestDispatched;

                if (requestDispatched) {
                    error.reconcileOutcomeUncertain = true;
                    error.reconcileRecoveryRequired = true;
                }

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
                    uncertainError(
                        "DURABLE_RECONCILE_CONNECTOR_FAILURE",
                        requestId,
                        new Error(
                            String(
                                data.error ||
                                "RECONCILE_CONNECTOR_FAILED"
                            )
                        )
                    )
                );
                return;
            }

            finish(null, data.data);
        }

        function onDisconnect() {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_CONNECTOR_DISCONNECTED",
                    requestId
                )
            );
        }

        function onAbort() {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_REQUEST_ABORTED",
                    requestId,
                    signal?.reason instanceof Error
                        ? signal.reason
                        : undefined
                )
            );
        }

        if (!socket.connected || signal?.aborted) {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_SOCKET_UNAVAILABLE_AFTER_DISPATCH",
                    requestId
                )
            );
            return;
        }

        socket.on(responseEvent, onResponse);
        socket.once("disconnect", onDisconnect);

        if (signal) {
            signal.addEventListener("abort", onAbort, {
                once: true
            });
        }

        timeout = setTimeout(() => {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_CONNECTOR_TIMEOUT",
                    requestId
                )
            );
        }, RECONCILE_TIMEOUT_MS);

        if (settled) return;

        if (!socket.connected || signal?.aborted) {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_SOCKET_UNAVAILABLE_BEFORE_EMIT",
                    requestId
                )
            );
            return;
        }

        try {
            // Mark before emit because a thrown emit does not
            // prove the connector never received the request.
            requestDispatched = true;

            socket.emit(event, {
                ...payload,
                requestId
            });
        } catch (cause) {
            finish(
                uncertainError(
                    "DURABLE_RECONCILE_EMIT_UNVERIFIED",
                    requestId,
                    cause
                )
            );
        }
    });
}

module.exports = {
    sendDurableReconcileRequest
};
