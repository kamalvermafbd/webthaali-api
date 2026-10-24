const CONNECTOR_TIMEOUT = 30000;



function sendToConnector(socket, event, payload, options = {}) {

  const signal = options.signal;

  if (signal?.aborted) {
    return Promise.reject(
        signal.reason instanceof Error
            ? signal.reason
            : new Error(
                String(signal.reason || "Connector request aborted")
            )
    );
}
  return new Promise((resolve, reject) => {

    const responseEvent = event + "Result";

let timeout;
let settled = false;

function cleanup() {
    clearTimeout(timeout);

    socket.off(responseEvent, onResponse);
    socket.off(`${event}Progress`, onProgress);
    socket.off("disconnect", onDisconnect);

    if (signal) {
        signal.removeEventListener("abort", onAbort);
    }
}

function onAbort() {
    if (settled) return;

    settled = true;
    cleanup();

   reject(
    signal.reason instanceof Error
        ? signal.reason
        : new Error(
            String(signal.reason || "Connector request aborted")
        )
);
}

    function resetTimeout() {

    if (settled) return;

    clearTimeout(timeout);

    timeout = setTimeout(() => {

    if (settled) return;

    const lastHeartbeat =
        Number(socket.lastHeartbeat || 0);

            const heartbeatFresh =
                lastHeartbeat > 0 &&
                Date.now() - lastHeartbeat < 90000;

            if (
                socket.connected &&
                heartbeatFresh &&
                socket.tallyRequestActive === true
            ) {
                console.log(
                    "⏳ CONNECTOR REQUEST STILL ACTIVE — EXTENDING TIMEOUT",
                    { event, heartbeatAgeMs: Date.now() - lastHeartbeat }
                );

                resetTimeout();
                return;
            }

                      settled = true;
            cleanup();

            reject(new Error("Connector timeout"));

        }, CONNECTOR_TIMEOUT);
    }

    function onResponse(data) {
    if (settled) return;

    settled = true;

    console.log("✅ RESPONSE RECEIVED :", responseEvent);

    cleanup();

    resolve(data);
}

    function onProgress(data) {

    if (settled || !data) {
        return;
    }

    resetTimeout();

    console.log(
        "💓 Progress:",
        data.batchId
    );

}

    function onDisconnect(reason) {
    if (settled) return;

    settled = true;

    cleanup();

    reject(
        new Error(
            `Connector disconnected: ${reason}`
        )
    );
}

    socket.once(responseEvent, onResponse);

socket.on(`${event}Progress`, onProgress);

socket.once("disconnect", onDisconnect);

if (signal) {
    signal.addEventListener("abort", onAbort, { once: true });

    if (signal.aborted) {
        onAbort();
        return;
    }
}

console.log("Waiting for :", responseEvent);

    resetTimeout();

    console.log("SOCKET EMIT TRACE:", {
    event,
    socket_id: socket?.id,
    socket_connector_id: socket?.connectorId,
    socket_company_code: socket?.companyCode,
    payload
});

    if (settled || signal?.aborted) {
        if (!settled) {
            onAbort();
        }
        return;
    }

    socket.emit(event, payload);

  });

}

module.exports = {
  sendToConnector
};