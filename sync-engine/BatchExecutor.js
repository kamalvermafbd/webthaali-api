const { createClient } =
    require("@supabase/supabase-js");

const {

    OPERATION_TYPE,

    DB_CONFIG

} = require("./constants");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

class BatchExecutor {

    // ----------------------------------
// Process Rows In Chunks
// ----------------------------------

    async processChunks({

        rows,

        callback

    }) {

        if (!Array.isArray(rows)) {

            throw new Error(

                "rows must be an array"

            );

        }

        if (typeof callback !== "function") {

            throw new Error(

                "callback must be a function"

            );

        }

        for (

            let i = 0;

            i < rows.length;

            i += DB_CONFIG.CHUNK_SIZE

        ) {

            const chunk =

                rows.slice(

                    i,

                    i + DB_CONFIG.CHUNK_SIZE

                );

            await callback(

                chunk

            );

        }

    }

    async execute(operation) {

            if (!operation) {

        throw new Error(

            "Operation is required"

        );

    }

        switch (operation.operation) {

            case OPERATION_TYPE.INSERT:

                return this.executeInsert(
                    operation
                );

            case OPERATION_TYPE.UPSERT:

                return this.executeUpsert(
                    operation
                );

            case OPERATION_TYPE.UPDATE:

                return this.executeUpdate(
                    operation
                );

            case OPERATION_TYPE.DELETE:

                return this.executeDelete(
                    operation
                );

            default:

                throw new Error(

                    `Unsupported operation : ${operation.operation}`

                );

        }

    }

        // ----------------------------------
    // INSERT
    // ----------------------------------

    async executeInsert(operation) {

        const {

            table,

            rows = []

        } = operation;

        if (rows.length === 0) {

            return true;

        }

        await this.processChunks({

        rows,

        callback: async (chunk) => {

        const {

            error

        } = await supabase

            .from(table)

            .insert(chunk);

        if (error) {

            throw new Error(

                `INSERT failed (${table}) : ${error.message}`

            );

        }

    }

});

return true;

    }

    // ----------------------------------
    // UPSERT
    // ----------------------------------

    async executeUpsert(operation) {

       const {

            table,

            rows = [],

            options = {}

        } = operation;

        if (rows.length === 0) {

            return true;

        }

     await this.processChunks({

        rows,

        callback: async (chunk) => {

            console.dir(chunk[0], {
    depth: null
});

console.log("OPTIONS :", options);

            const {

                error

            } = await supabase

                .from(table)

                

                .upsert(

                    chunk,

                    options

                );

            if (error) {

                throw new Error(

                    `UPSERT failed (${table}) : ${error.message}`

                );

            }

        }

    });

    return true;

    }

    // ----------------------------------
    // UPDATE
    // ----------------------------------

    async executeUpdate(operation) {

        const {

            table,

            values,

            filters = []

        } = operation;

        if (!values) {

            throw new Error(

                "Update values are required"

            );

        }

        let query =
            supabase
                .from(table)
                .update(values);

        if (!filters.length) {

            throw new Error(

                "Operation requires filters"

            );

        }

      for (const filter of filters) {

    if (typeof query[filter.type] !== "function") {

        throw new Error(

            `Unsupported filter type : ${filter.type}`

        );

    }

    query = query[filter.type](

        filter.column,

        filter.value

    );

}

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                `UPDATE failed (${table}) : ${error.message}`

            );

        }

        return true;

    }

    // ----------------------------------
    // DELETE
    // ----------------------------------

   async executeDelete(operation) {

        const {

            table,

            filters = []

        } = operation;

        if (!filters.length) {

            throw new Error(

                "Operation requires filters"

            );

        }

        for (const filter of filters) {

            if (

                filter.type === "in"

                &&

                Array.isArray(filter.value)

                &&

                filter.value.length === 0

            ) {

                return true;

            }

        }

        let query =

            supabase

                .from(table)

                .delete();

        for (const filter of filters) {

            if (typeof query[filter.type] !== "function") {

                throw new Error(

                    `Unsupported filter type : ${filter.type}`

                );

            }

            query = query[filter.type](

                filter.column,

                filter.value

            );

        }

        const {

            error

        } = await query;

        if (error) {

            throw new Error(

                `DELETE failed (${table}) : ${error.message}`

            );

        }

        return true;

    }




}

module.exports =
    new BatchExecutor();