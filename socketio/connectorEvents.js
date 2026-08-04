const registry = require("./connectorRegistry");

const ServerProtocolReceiver =
    require("../utils/protocol/ServerProtocolReceiver");

function registerEvents(io) {

    io.on("connection", (socket) => {

        console.log("================================");
        console.log("✅ Connector Connected");
        console.log("Socket ID :", socket.id);
        console.log("================================");

        socket.protocolReceiver =
            new ServerProtocolReceiver(
                socket
            );

        socket.protocolReceiver.start();

        // Connector Register
        socket.on("register", (data) => {

            console.log("Register Request :", data);
            socket.companyCode = data.company_code;

            registry.register(data.company_code, socket);

        });

        socket.on("testExport", () => {

            console.log("================================");
            console.log("📦 Test Export Request");
            console.log("Sending XML to Connector...");
            console.log("================================");

            socket.emit("export", {
                xml: "<TEST>HELLO TALLY</TEST>"
            });

        });

        socket.on("getMastersProgress", (data) => {

            socket.lastHeartbeat = Date.now();

            console.log(
                "💓 Heartbeat :",
                data.batchId
            );

        });

        socket.on("disconnect", (reason) => {

            console.log("================================");
            console.log("❌ Connector Disconnected");
            console.log("Socket ID :", socket.id);
            console.log("Reason :", reason);

            if (socket.companyCode) {
                registry.remove(socket.companyCode);
            }

            console.log("================================");

        });

            });

        }

module.exports = {
    registerEvents
};