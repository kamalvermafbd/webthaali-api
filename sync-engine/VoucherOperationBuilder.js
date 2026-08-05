const OperationBuilder = require("./OperationBuilder");

const {
    ENTITY_TYPE,
    TABLES,
    OPERATION_TYPE
} = require("./constants");
function buildDeleteOperations({

    company_code,

    tally_owner,

    sync_batch_id,

    voucherGuids

}) {

    return [

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.VOUCHER_LEDGERS,

            operation: OPERATION_TYPE.DELETE,

            filters: [

                {

                    type: "in",

                    column: "voucher_guid",

                    value: voucherGuids

                }

            ],

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.VOUCHER_INVENTORY,

            operation: OPERATION_TYPE.DELETE,

            filters: [

                {

                    type: "in",

                    column: "voucher_guid",

                    value: voucherGuids

                }

            ],

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.STOCK_VOUCHERS,

            operation: OPERATION_TYPE.DELETE,

            filters: [

                {

                    type: "in",

                    column: "voucher_guid",

                    value: voucherGuids

                }

            ],

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.BILL_ALLOCATIONS,

            operation: OPERATION_TYPE.DELETE,

            filters: [

                {

                    type: "in",

                    column: "voucher_guid",

                    value: voucherGuids

                }

            ],

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.COST_CENTRE_ALLOCATIONS,

            operation: OPERATION_TYPE.DELETE,

            filters: [

                {

                    type: "in",

                    column: "voucher_guid",

                    value: voucherGuids

                }

            ],

            company_code,

            tally_owner,

            sync_batch_id

        })

    ];

}

function buildSaveOperations({

    company_code,

    tally_owner,

    sync_batch_id,

    voucherRows,

    ledgerRows,

    inventoryRows,

    stockVoucherRows,

    billAllocationRows,

    costCentreRows

}) {

    return [

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.VOUCHERS,

            operation: OPERATION_TYPE.UPSERT,

            rows: voucherRows,

            options: {

                onConflict:
                    "company_code,tally_owner,guid"

                  },


            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.VOUCHER_LEDGERS,

            operation: OPERATION_TYPE.INSERT,

            rows: ledgerRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.VOUCHER_INVENTORY,

            operation: OPERATION_TYPE.INSERT,

            rows: inventoryRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.STOCK_VOUCHERS,

            operation: OPERATION_TYPE.INSERT,

            rows: stockVoucherRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.BILL_ALLOCATIONS,

            operation: OPERATION_TYPE.INSERT,

            rows: billAllocationRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity: ENTITY_TYPE.VOUCHER,

            table: TABLES.COST_CENTRE_ALLOCATIONS,

            operation: OPERATION_TYPE.INSERT,

            rows: costCentreRows,

            company_code,

            tally_owner,

            sync_batch_id

        })

    ];

}

function buildVoucherOperations(args) {

    return [

        ...buildDeleteOperations(args),

        ...buildSaveOperations(args)

    ];

}

module.exports = {

    buildDeleteOperations,

    buildSaveOperations,

    buildVoucherOperations

};