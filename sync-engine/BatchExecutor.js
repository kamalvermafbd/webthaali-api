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

        console.log("=== BATCH EXECUTOR RECEIVED OPERATION ===");

console.log({
    operation: operation?.operation,
    table: operation?.table,
    entity: operation?.entity,
    rows: operation?.rows?.length || 0
});

if (operation?.table === "tally_vouchers") {
    console.dir(
        operation.rows?.[0],
        { depth: null }
    );
}

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

        const rowsWithBatch =
            rows.map(row => ({
                ...row,
                sync_batch_id:
                    operation.sync_batch_id
            }));


        if (rows.length === 0) {

            return true;

        }

        await this.processChunks({

        rows: rowsWithBatch,

        callback: async (chunk) => {

            console.log("================================");
            console.log("BATCH EXECUTOR");
            console.log("table :", table);
            console.log("chunk size :", chunk.length);
            console.log("total rows :", rows.length);
            console.log("================================");

       const {
    data,
    error
} = await supabase
    .from(table)
    .insert(chunk)
    .select("voucher_guid");

if (error) {

    throw new Error(
        `INSERT failed (${table}) : ${error.message}`
    );

}

console.log(
    "INSERT RESULT:",
    table,
    "requested:",
    chunk.length,
    "inserted:",
    data?.length || 0
);

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

        const rowsWithBatch =
            rows.map(row => ({
                ...row,
                sync_batch_id:
                    operation.sync_batch_id
            }));

        if (rows.length === 0) {

            return true;

        }

     await this.processChunks({

      //  rows,

        rows: rowsWithBatch,

        callback: async (chunk) => {

        console.dir(chunk[0], {
        depth: null
});

console.log("OPTIONS :", options);

           const {
                data,
                error
            } = await supabase
                .from(table)
                .upsert(
                    chunk,
                    options
                )
                .select("*");

            console.log(
                "=== UPSERT RESULT ===",
                {
                    table,
                    requested: chunk.length,
                    returned: data?.length || 0,
                    error: error?.message || null
                }
            );

            if (data?.length) {

                console.dir(
                    data[0],
                    { depth: null }
                );

            }

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

    if (!filters.length) {

        throw new Error(
            "Operation requires filters"
        );

    }

    const inFilter =
        filters.find(
            filter =>
                filter.type === "in" &&
                Array.isArray(filter.value)
        );

    // ----------------------------------
    // No IN filter → normal UPDATE
    // ----------------------------------

    if (!inFilter) {

        let query =
            supabase
                .from(table)
                .update(values);

        for (const filter of filters) {

            if (
                typeof query[filter.type] !==
                "function"
            ) {

                throw new Error(
                    `Unsupported filter type : ${filter.type}`
                );

            }

            query =
                query[filter.type](
                    filter.column,
                    filter.value
                );

        }

        const {
            data,
            error
        } = await query.select("*");

        console.log(
            "UPDATE RESULT:",
            table,
            "updated:",
            data?.length || 0
        );

            
        if (error) {

            throw new Error(
                `UPDATE failed (${table}) : ${error.message}`
            );

        }

        return true;

    }

    // ----------------------------------
    // Empty IN → nothing to update
    // ----------------------------------

    if (inFilter.value.length === 0) {

        return true;

    }

    // ----------------------------------
    // UPDATE IN chunks
    // ----------------------------------

    await this.processChunks({

        rows: inFilter.value,

        callback: async (chunk) => {

            let query =
                supabase
                    .from(table)
                    .update(values);

            for (const filter of filters) {

                if (
                    typeof query[filter.type] !==
                    "function"
                ) {

                    throw new Error(
                        `Unsupported filter type : ${filter.type}`
                    );

                }

                const value =
                    filter === inFilter
                        ? chunk
                        : filter.value;

                query =
                    query[filter.type](
                        filter.column,
                        value
                    );

            }

            console.log("================================");
            console.log("BATCH UPDATE");
            console.log(
                "table :",
                table
            );
            console.log(
                "chunk size :",
                chunk.length
            );
            console.log(
                "total values :",
                inFilter.value.length
            );
            console.log("================================");

            const {
                error
            } = await query;

            if (error) {

                throw new Error(
                    `UPDATE failed (${table}) : ${error.message}`
                );

            }

        }

    });

    return true;

}
    // ----------------------------------
    // DELETE
    // ----------------------------------
/*
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
    */

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

        const inFilter =
            filters.find(
                filter =>
                    filter.type === "in" &&
                    Array.isArray(filter.value)
            );

        // ----------------------------------
        // No IN filter → normal DELETE
        // ----------------------------------

        if (!inFilter) {

            let query =
                supabase
                    .from(table)
                    .delete();

            for (const filter of filters) {

                if (
                    typeof query[filter.type] !==
                    "function"
                ) {

                    throw new Error(
                        `Unsupported filter type : ${filter.type}`
                    );

                }

                query =
                    query[filter.type](
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

        // ----------------------------------
        // Empty IN → nothing to delete
        // ----------------------------------

        if (inFilter.value.length === 0) {

            return true;

        }

        // ----------------------------------
        // DELETE IN chunks
        // ----------------------------------

        await this.processChunks({

            rows: inFilter.value,

            callback: async (chunk) => {

                let query =
                    supabase
                        .from(table)
                        .delete();

                for (const filter of filters) {

                    if (
                        typeof query[filter.type] !==
                        "function"
                    ) {

                        throw new Error(
                            `Unsupported filter type : ${filter.type}`
                        );

                    }

                    const value =
                        filter === inFilter
                            ? chunk
                            : filter.value;

                    query =
                        query[filter.type](
                            filter.column,
                            value
                        );

                }

                console.log("================================");
                console.log("BATCH DELETE");
                console.log("table :", table);
                console.log("chunk size :", chunk.length);
                console.log("total values :", inFilter.value.length);
                console.log("================================");

                const {
                    data,
                    error
                } = await query.select("voucher_guid");

                if (error) {

                    throw new Error(
                        `DELETE failed (${table}) : ${error.message}`
                    );

                }

                console.log(
                    "DELETE RESULT:",
                    table,
                    "requested:",
                    inFilter.value.length,
                    "chunk:",
                    chunk.length,
                    "deleted:",
                    data?.length || 0
                );

            }

        });

        return true;

    }


}

module.exports =
    new BatchExecutor();