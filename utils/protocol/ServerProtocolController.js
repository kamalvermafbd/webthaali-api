const COLLECTIONS =
    require("./CollectionRegistry");

class ServerProtocolController {

    constructor(receiver) {

        this.receiver =
            receiver;

    }

    async receiveAll(requestId) {

    if (!requestId) {
        throw new Error("Protocol receiveAll requires requestId");
    }

        const result = {};

        for (const collection of COLLECTIONS) {

            console.log(

                `READY -> ${collection.key}`

            );



const rowsPromise =
    this.receiver.waitForCollection();

let rows;

try {

    this.receiver.sendReady(
    collection.event,
    requestId
);
    rows = await rowsPromise;

} catch (error) {

    this.receiver.cancelCollection(
        `Protocol collection failed: ${collection.event}`
    );

    await rowsPromise.catch(() => {});

    throw error;

}

            result[
                collection.key
            ] = rows;

       

            /*
fs.writeFileSync(

    `./logs/FINAL_${collection.key}.json`,

    JSON.stringify(

        {
            collection: collection.key,
            total: rows.length,
            rows
        },

        null,

        2

    )

);
*/

            console.log(

                `DONE -> ${collection.key}`

            );

        }

        return result;

    }

}

module.exports =
    ServerProtocolController;