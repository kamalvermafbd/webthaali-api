const COLLECTIONS =
    require("./CollectionRegistry");

class ServerProtocolController {

    constructor(receiver) {

        this.receiver =
            receiver;

    }

    async receiveAll() {

        const result = {};

        for (const collection of COLLECTIONS) {

            console.log(

                `READY -> ${collection.key}`

            );

            this.receiver.sendReady(

                collection.event

            );

         const rows =
             await this.receiver.waitForCollection();

            result[
                collection.key
            ] = rows;

            console.log(

                `DONE -> ${collection.key}`

            );

        }

        return result;

    }

}

module.exports =
    ServerProtocolController;