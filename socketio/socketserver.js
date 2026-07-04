const { Server } = require("socket.io");

function initializeSocket(server) {

    const io = new Server(server, {
        cors: {
            origin: "*",
            methods: ["GET", "POST"]
        }
    });

    return io;
}

module.exports = {
    initializeSocket
};