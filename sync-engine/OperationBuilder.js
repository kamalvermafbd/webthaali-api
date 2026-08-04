const crypto = require("crypto");

function build({

    entity,

    table,

    operation,

    rows = [],

    company_code,

    tally_owner,

    sync_batch_id

}) {

    return {

        operation_id:
            crypto.randomUUID(),

        entity,

        table,

        operation,

        rows,

        total_rows:
            rows.length,

        company_code,

        tally_owner,

        sync_batch_id,

        created_at:
            new Date().toISOString()

    };

}

module.exports = {

    build

};