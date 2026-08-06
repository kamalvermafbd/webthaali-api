const crypto = require("crypto");

const MasterOperationBuilder =
    require("./MasterOperationBuilder");

const VoucherOperationBuilder =
    require("./VoucherOperationBuilder");

const {

    MODULE_TYPE

} = require("./constants");


function build({

    entity,

    table,

    operation,

    rows = [],

    values = {},

    filters = [],

    options = {},

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

        values,

        filters,

        options,

        total_rows:
            rows.length,

        company_code,

        tally_owner,

        sync_batch_id,

        created_at:
            new Date().toISOString()

    };

}

function dispatch({

    module,

    ...args

}) {

    if (module === MODULE_TYPE.MASTER) {

        return [

            MasterOperationBuilder.buildOperation(

                args

            )

        ];

    }

    if (module === MODULE_TYPE.VOUCHER) {

        return VoucherOperationBuilder.buildOperation(

            args

        );

    }

    throw new Error(

        `Unsupported module : ${module}`

    );

}

module.exports = {

    build,

    dispatch

};