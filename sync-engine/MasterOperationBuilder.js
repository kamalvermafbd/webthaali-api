const {

    ENTITY_TYPE,

    TABLES,

    OPERATION_TYPE,

    ALTER_ID_COLUMN,

    CONFLICT_KEYS

} = require("./constants");

function buildGroupOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
    options = {}
}) {

    return buildMasterOperation({

        entity: ENTITY_TYPE.GROUP,

        table: TABLES.GROUPS,

        company_code,

        tally_owner,

        sync_batch_id,

        rows,

        options

    });
}

function buildStockGroupOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
     options = {}
}) {

    return buildMasterOperation({

        entity: ENTITY_TYPE.STOCK_GROUP,

        table: TABLES.STOCK_GROUPS,

        company_code,

        tally_owner,

        sync_batch_id,

        rows,

        options

    });

}

function buildLedgerOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
     options = {}
}) {

        
    return buildMasterOperation({

        entity: ENTITY_TYPE.LEDGER,

        table: TABLES.LEDGERS,

        company_code,

        tally_owner,

        sync_batch_id,

        rows,

        options:
            Object.keys(options).length
                ? options
                : {
                    onConflict: CONFLICT_KEYS[TABLES.LEDGERS]
                }

    });

}

function buildStockOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
     options = {}
}) {
        return buildMasterOperation({

            entity: ENTITY_TYPE.STOCK,

            table: TABLES.STOCKS,

            company_code,

            tally_owner,

            sync_batch_id,

            rows,

            options

        });

}

function buildUnitOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
     options = {}
}) {

      return buildMasterOperation({

            entity: ENTITY_TYPE.UNIT,

            table: TABLES.UNITS,

            company_code,

            tally_owner,

            sync_batch_id,

            rows,

            options

        });
}

function buildGodownOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
    options = {}
}) {

    return buildMasterOperation({

            entity: ENTITY_TYPE.GODOWN,

            table: TABLES.GODOWNS,

            company_code,

            tally_owner,

            sync_batch_id,

            rows,

            options

        });

}

function buildCostCentreOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows,
    options = {}
}) {

        return buildMasterOperation({

            entity: ENTITY_TYPE.COST_CENTRE,

            table: TABLES.COST_CENTRES,

            company_code,

            tally_owner,

            sync_batch_id,

            rows,

            options

        });

}

function buildMasterOperation({

    entity,

    table,

    company_code,

    tally_owner,

    sync_batch_id,

    rows,

    options = {}

}) {

    return {

        entity,

        table,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        options,

        company_code,

        tally_owner,

        sync_batch_id

    };

}

function buildOperation({

    entity,

    table,

    company_code,

    tally_owner,

    sync_batch_id,

    rows,

    options = {}

}) {

    return [

        buildMasterOperation({

            entity,

            table,

            company_code,

            tally_owner,

            sync_batch_id,

            rows,

            options

        })

    ];

}


function buildAlterUpdateOperations({

    table,

    company_code,

    tally_owner,

    sync_batch_id,

    alterChanged = []

}) {

    const now =

        new Date().toISOString();

    const alterColumn =

        ALTER_ID_COLUMN[table] ||

        "alter_id";

    return alterChanged.map(item => ({

        entity: null,

        table,

        operation: OPERATION_TYPE.UPDATE,

        values: {

            [alterColumn]:

                item.newAlterId,

            sync_batch_id,

            last_synced_at:

                now,

            updated_at:

                now

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


function buildSoftDeleteOperations({

    table,

    company_code,

    tally_owner,

    sync_batch_id,

    extraGuids = []

}) {

    const now =

        new Date().toISOString();

    return extraGuids.map(item => ({

        entity: null,

        table,

        operation: OPERATION_TYPE.UPDATE,

        values: {

            is_deleted: true,

            sync_batch_id,

            updated_at:

                now

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

module.exports = {

    buildOperation,

    buildMasterOperation,

    buildGroupOperation,

    buildStockGroupOperation,

    buildLedgerOperation,

    buildStockOperation,

    buildUnitOperation,

    buildGodownOperation,

    buildCostCentreOperation,

    buildAlterUpdateOperations,

    buildSoftDeleteOperations

};