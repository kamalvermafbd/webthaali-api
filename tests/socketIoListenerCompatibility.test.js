
"use strict";

const http = require("node:http");
const assert = require("node:assert/strict");
const { Server } = require("socket.io");
const { io: Client } = require(
    require.resolve("socket.io-client", {
        paths: [
            require("node:path").join(
                require("node:os").tmpdir(),
                "billey-socket-test"
            )
        ]
    })
);

async function main() {
    const httpServer = http.createServer();
    const io = new Server(httpServer);

    let client;

    try {
        await new Promise(resolve => {
            httpServer.listen(0, "127.0.0.1", resolve);
        });

        const port = httpServer.address().port;

        const connection = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error("CONNECTION_TIMEOUT"));
            }, 5000);

            io.once("connection", socket => {
                clearTimeout(timeout);
                resolve(socket);
            });
        });

        client = Client(
            `http://127.0.0.1:${port}`,
            {
                transports: ["websocket"],
                reconnection: false
            }
        );

        const socket = await connection;

        for (const method of [
            "on",
            "once",
            "rawListeners",
            "removeListener",
            "off"
        ]) {
            assert.equal(
                typeof socket[method],
                "function",
                `${method} missing`
            );
        }

        console.log("PASS: Socket.IO methods available");

        const event = "test:cleanup";
        const before = socket.rawListeners(event).length;

        const listener = () => {};

        socket.on(event, listener);

        assert.equal(
            socket.rawListeners(event).length,
            before + 1
        );

        socket.removeListener(event, listener);

        assert.equal(
            socket.rawListeners(event).length,
            before
        );

        console.log("PASS: Listener registration and removal");
        console.log("\nPASS: REAL SOCKET.IO LISTENER COMPATIBILITY");

    } finally {
        if (client) {
            client.disconnect();
        }

        await new Promise(resolve => io.close(resolve));
    }
}

main().catch(error => {
    console.error(error.stack || error);
    process.exitCode = 1;
});
