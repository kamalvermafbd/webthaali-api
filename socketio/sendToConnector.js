const CONNECTOR_TIMEOUT = 30000;

function sendToConnector(socket, event, payload) {

  return new Promise((resolve, reject) => {

    const responseEvent = event + "Result";

    let timeout;

    function resetTimeout() {

        clearTimeout(timeout);

        timeout = setTimeout(() => {

            socket.off(responseEvent, onResponse);

            socket.off(`${event}Progress`, onProgress);

            socket.off("disconnect", onDisconnect);

            reject(new Error("Connector timeout"));

        }, CONNECTOR_TIMEOUT);

    }

    function onResponse(data) {

      console.log("✅ RESPONSE RECEIVED :", responseEvent);

      clearTimeout(timeout);

      socket.off(responseEvent, onResponse);

      socket.off(`${event}Progress`, onProgress);

      socket.off("disconnect", onDisconnect);

      resolve(data);

    }

    function onProgress(data) {

          if (!data) {
            return;
        }


        resetTimeout();

        console.log(
            "💓 Progress:",
            data.batchId
        );

    }

    function onDisconnect(reason) {

    clearTimeout(timeout);

    socket.off(responseEvent, onResponse);
    socket.off(`${event}Progress`, onProgress);

    socket.off("disconnect", onDisconnect);

      reject(
          new Error(
              `Connector disconnected: ${reason}`
          )
      );

  }

    socket.once(responseEvent, onResponse);

    socket.on(`${event}Progress`, onProgress);

    socket.once("disconnect", onDisconnect);

    console.log("Waiting for :", responseEvent);

    resetTimeout();

    console.log("SOCKET EMIT TRACE:", {
    event,
    socket_id: socket?.id,
    socket_connector_id: socket?.connectorId,
    socket_company_code: socket?.companyCode,
    payload
});

    socket.emit(event, payload);

  });

}

module.exports = {
  sendToConnector
};