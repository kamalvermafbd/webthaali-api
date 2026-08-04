const OperationBuilder = require("./OperationBuilder");

const {
    ENTITY_TYPE,
    TABLES,
    OPERATION_TYPE
} = require("./constants");

function buildGroupOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.GROUP,

        table: TABLES.GROUPS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildStockGroupOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.STOCK_GROUP,

        table: TABLES.STOCK_GROUPS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildLedgerOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.LEDGER,

        table: TABLES.LEDGERS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildStockOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.STOCK,

        table: TABLES.STOCKS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildUnitOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.UNIT,

        table: TABLES.UNITS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildGodownOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.GODOWN,

        table: TABLES.GODOWNS,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

function buildCostCentreOperation({
    company_code,
    tally_owner,
    sync_batch_id,
    rows
}) {

    return OperationBuilder.build({

        entity: ENTITY_TYPE.COST_CENTRE,

        table: TABLES.COST_CENTRES,

        operation: OPERATION_TYPE.UPSERT,

        rows,

        company_code,

        tally_owner,

        sync_batch_id

    });

}

module.exports = {

    buildGroupOperation,

    buildStockGroupOperation,

    buildLedgerOperation,

    buildStockOperation,

    buildUnitOperation,

    buildGodownOperation,

    buildCostCentreOperation

};