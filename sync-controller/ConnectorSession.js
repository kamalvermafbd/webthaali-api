class ConnectorSession {

    constructor() {

        this.socket = null;

    }

    // ----------------------------------
    // Attach Connector
    // ----------------------------------

    attach(socket) {

        this.socket = socket;

    }

    // ----------------------------------
    // Is Connected
    // ----------------------------------

    isConnected() {

        return !!this.socket;

    }

    // ----------------------------------
    // Send Request
    // ----------------------------------

    async request({

        event,

        payload = {}

    }) {

        if (!this.socket) {

            throw new Error(

                "Connector is not attached"

            );

        }

        return new Promise((resolve, reject) => {

            this.socket.emit(

                event,

                payload,

                (response) => {

                    resolve(response);

                }

            );

        });

    }

}

module.exports =
    new ConnectorSession();