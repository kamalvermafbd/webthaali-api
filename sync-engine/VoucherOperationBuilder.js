const OperationBuilder = require("./OperationBuilder");

const {
    ENTITY_TYPE,
    TABLES,
    OPERATION_TYPE,
    CONFLICT_KEYS,
    ALTER_ID_COLUMN,
    VOUCHER_RECONCILIATION
} = require("./constants");

const fs = require("fs");
/*
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
*/

function buildDeleteOperations({
    company_code,
    tally_owner,
    sync_batch_id,
    voucherGuids
}) {

    const operations = [];

    const childTables =
        Object.entries(
            VOUCHER_RECONCILIATION.CHILDREN
        );

    for (const [table, config] of childTables) {

        

fs.appendFileSync(
    "./logs/DEBUG-NORMAL-VOUCHER-DELETE.jsonl",
    JSON.stringify({
        source: "VoucherOperationBuilder.buildDeleteOperations",
        table,
        requestedGuidCount:
    voucherGuids?.length || 0,
        timestamp: new Date().toISOString()
    }) + "\n"
);

        operations.push(

            OperationBuilder.build({

                entity:
                    ENTITY_TYPE.VOUCHER,

                table,

                operation:
                    OPERATION_TYPE.DELETE,

                filters: [

                            {
                                type: "eq",

                                column: "company_code",

                                value: company_code

                            },

                            {
                                type: "eq",

                                column: "tally_owner",

                                value: tally_owner

                            },

                            {
                                type: "in",

                                column:
                                    config.guidColumn,

                                value:
                                    voucherGuids
                            }

                        ],

                company_code,

                tally_owner,

                sync_batch_id

            })

        );

    }

    return operations;
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

    costCentreRows,

    includeParent = true

}) {

    const operations = [];

       console.log(
    "CHILD REPAIR LEDGER OP ROWS:",
    ledgerRows?.length || 0
);

    if (includeParent !== false) {

     console.log("=== VOUCHER UPSERT PAYLOAD ===");

console.dir(
    voucherRows?.find(
        row =>
            row.guid ===
            "b06ee43a-c023-4bfc-b8d9-3fd85283e679-00002b73"
    ),
    { depth: null }
);

        operations.push(

            OperationBuilder.build({

                entity:
                    ENTITY_TYPE.VOUCHER,

                table:
                    TABLES.VOUCHERS,

                operation:
                    OPERATION_TYPE.UPSERT,

                rows:
                    voucherRows,

                    

                options: {

                    onConflict:
                        CONFLICT_KEYS[TABLES.VOUCHERS]

                },

                company_code,

                tally_owner,

                sync_batch_id

            })

        );

    }

    operations.push(

        OperationBuilder.build({

            entity:
                ENTITY_TYPE.VOUCHER,

            table:
                TABLES.VOUCHER_LEDGERS,

            operation:
                OPERATION_TYPE.INSERT,

            rows:
                ledgerRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity:
                ENTITY_TYPE.VOUCHER,

            table:
                TABLES.VOUCHER_INVENTORY,

            operation:
                OPERATION_TYPE.INSERT,

            rows:
                inventoryRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity:
                ENTITY_TYPE.VOUCHER,

            table:
                TABLES.STOCK_VOUCHERS,

            operation:
                OPERATION_TYPE.INSERT,

            rows:
                stockVoucherRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity:
                ENTITY_TYPE.VOUCHER,

            table:
                TABLES.BILL_ALLOCATIONS,

            operation:
                OPERATION_TYPE.INSERT,

            rows:
                billAllocationRows,

            company_code,

            tally_owner,

            sync_batch_id

        }),

        OperationBuilder.build({

            entity:
                ENTITY_TYPE.VOUCHER,

            table:
                TABLES.COST_CENTRE_ALLOCATIONS,

            operation:
                OPERATION_TYPE.INSERT,

            rows:
                costCentreRows,

            company_code,

            tally_owner,

            sync_batch_id

        })

    );

    return operations;

}

function buildOperation(args) {

    return buildVoucherOperations(

        args

    );

}

function buildVoucherOperations(args) {

    const deleteOperations =
        args.changedVoucherGuids?.length > 0
            ? buildDeleteOperations({
                ...args,
                voucherGuids:
                    args.changedVoucherGuids
            })
            : [];

    const orphanDeleteOperations =
        args.orphanGuids
            ? buildOrphanDeleteOperations({

                company_code:
                    args.company_code,

                tally_owner:
                    args.tally_owner,

                sync_batch_id:
                    args.sync_batch_id,

                orphanGuids:
                    args.orphanGuids

            })
            : [];

    return [
        ...deleteOperations,
        ...orphanDeleteOperations,
        ...buildSaveOperations(args)
    ];

}


function buildAlterUpdateOperations({
    table,
    company_code,
    tally_owner,
    sync_batch_id,
    alterChanged = []
}) {

    return alterChanged.map(item => ({
        entity: ENTITY_TYPE.VOUCHER,
        table: TABLES.VOUCHERS,
        operation: OPERATION_TYPE.UPDATE,

      values: {
                [ALTER_ID_COLUMN[TABLES.VOUCHERS]]:
                    item.newAlterId,

                sync_batch_id,

                updated_at:
                    new Date().toISOString()
            },

        filters: [
            {
                type: "eq",
                column: "company_code",
                value: company_code
            },
            {
                type: "eq",
                column: "tally_owner",
                value: tally_owner
            },
            {
                type: "eq",
                column: "guid",
                value: item.guid
            }
        ]
    }));
}

/* 080826

function buildSoftDeleteOperations({
    table,
    company_code,
    tally_owner,
    sync_batch_id,
    extraGuids = []
}) {

    return extraGuids.map(item => ({
        entity: ENTITY_TYPE.VOUCHER,
        table: TABLES.VOUCHERS,
        operation: OPERATION_TYPE.UPDATE,

        values: {
            is_deleted: true,
            sync_batch_id,
            updated_at: new Date().toISOString()
        },

        filters: [
            {
                type: "eq",
                column: "company_code",
                value: company_code
            },
            {
                type: "eq",
                column: "tally_owner",
                value: tally_owner
            },
            {
                type: "eq",
                column: "guid",
                value: item.guid
            },
            {
                type: "eq",
                column: "is_deleted",
                value: false
            }
        ]
    }));
}
*/

function buildOrphanDeleteOperations({
    company_code,
    tally_owner,
    sync_batch_id,
    orphanGuids = {}
}) {

    const operations = [];

    for (
        const [childTable, voucherGuids]
        of Object.entries(orphanGuids)
    ) {

        if (
            !Array.isArray(voucherGuids) ||
            voucherGuids.length === 0
        ) {
            continue;
        }

    

fs.appendFileSync(
    "./logs/DEBUG-ORPHAN-DELETE.jsonl",
    JSON.stringify({
        source: "VoucherOperationBuilder.buildOrphanDeleteOperations",
        table: childTable,
        requestedGuidCount:
    voucherGuids?.length || 0,
        timestamp: new Date().toISOString()
    }) + "\n"
);

        const childConfig =
            VOUCHER_RECONCILIATION
                .CHILDREN[childTable];

        if (!childConfig) {
            continue;
        }

        operations.push(

            OperationBuilder.build({

                entity:
                    ENTITY_TYPE.VOUCHER,

                table:
                    childTable,

                operation:
                    OPERATION_TYPE.DELETE,

                filters: [

                    {
                        type: "eq",
                        column: "company_code",
                        value: company_code
                    },

                    {
                        type: "eq",
                        column: "tally_owner",
                        value: tally_owner
                    },

                    {
                        type: "in",
                        column:
                            childConfig.guidColumn,
                        value:
                            voucherGuids
                    }

                ],

                company_code,
                tally_owner,
                sync_batch_id

            })

        );

    }

    return operations;
}

function buildSoftDeleteOperations({
    table,
    company_code,
    tally_owner,
    sync_batch_id,
    extraGuids = []
}) {

    fs.appendFileSync(
    "./logs/DEBUG-SOFT-DELETE.jsonl",
    JSON.stringify({
        source: "buildSoftDeleteOperations",
        sync_batch_id,
        extraGuidCount: extraGuids?.length || 0,
        extraGuids: (extraGuids || [])
            .map(item => item?.guid)
            .filter(Boolean),
        timestamp: new Date().toISOString()
    }) + "\n"
);

    const operations = [];

    for (const item of extraGuids) {

        const voucherGuid = item.guid;

        // --------------------------------------------------
        // 1. Child voucher rows → HARD DELETE
        // --------------------------------------------------

        operations.push(
            ...buildDeleteOperations({

                company_code,

                tally_owner,

                sync_batch_id,

                voucherGuids: [
                    voucherGuid
                ]

            })
        );

        // --------------------------------------------------
        // 2. Main voucher row → SOFT DELETE
        // --------------------------------------------------

        const root =
            VOUCHER_RECONCILIATION.ROOT;

        operations.push(

            OperationBuilder.build({

                entity:
                    ENTITY_TYPE.VOUCHER,

                table:
                    root.table,

                operation:
                    OPERATION_TYPE.UPDATE,

                values: {

                    [root.deletedColumn]:
                        true,

                    sync_batch_id,

                    [root.updatedColumn]:
                        new Date().toISOString()

                },

                filters: [

                    {
                        type: "eq",

                        column:
                            "company_code",

                        value:
                            company_code
                    },

                    {
                        type: "eq",

                        column:
                            "tally_owner",

                        value:
                            tally_owner
                    },

                    {
                        type: "eq",

                        column:
                            root.guidColumn,

                        value:
                            voucherGuid
                    },

                    {
                        type: "eq",

                        column:
                            root.deletedColumn,

                        value:
                            false
                    }

                ],

                company_code,

                tally_owner,

                sync_batch_id

            })

        );

    }

    return operations;
}


module.exports = {

    buildOperation,

    buildDeleteOperations,

    buildSaveOperations,

    buildVoucherOperations,

    buildAlterUpdateOperations,

    buildSoftDeleteOperations,

    buildOrphanDeleteOperations,

};