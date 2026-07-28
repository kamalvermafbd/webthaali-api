function sendToConnector(socket, event, payload) {

  return new Promise((resolve, reject) => {

    const responseEvent = event + "Result";

    const timeout = setTimeout(() => {

      socket.off(responseEvent, onResponse);

      reject(new Error("Connector timeout"));

    }, 30000);

    function onResponse(data) {

      console.log("✅ RESPONSE RECEIVED :", responseEvent);

      clearTimeout(timeout);

      socket.off(responseEvent, onResponse);

      resolve(data);

    }

    socket.once(responseEvent, onResponse);

    console.log("Waiting for :", responseEvent);

    socket.emit(event, payload);

  });

}

module.exports = {
  sendToConnector
};