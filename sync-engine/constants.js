// ======================================================
// SYNC ENGINE CONSTANTS
// ======================================================

// ------------------------------------------------------
// Database Operations
// ------------------------------------------------------

const GroupRowBuilder =
    require("../services/GroupRowBuilder");

const StockGroupRowBuilder =
    require("../services/StockGroupRowBuilder");

const LedgerRowBuilder =
    require("../services/LedgerRowBuilder");

const StockRowBuilder =
    require("../services/StockRowBuilder");

const UnitRowBuilder =
    require("../services/UnitRowBuilder");

const GodownRowBuilder =
    require("../services/GodownRowBuilder");

const CostCentreRowBuilder =
    require("../services/CostCentreRowBuilder");

const VoucherRowBuilder =
    require("../services/VoucherRowBuilder");



const OPERATION_TYPE = {

    INSERT: "INSERT",

    UPSERT: "UPSERT",

    UPDATE: "UPDATE",

    DELETE: "DELETE"

};


// ------------------------------------------------------
// Entity Types
// ------------------------------------------------------

const ENTITY_TYPE = {

    GROUP: "GROUP",

    STOCK_GROUP: "STOCK_GROUP",

    LEDGER: "LEDGER",

    STOCK: "STOCK",

    UNIT: "UNIT",

    GODOWN: "GODOWN",

    COST_CENTRE: "COST_CENTRE",

    VOUCHER: "VOUCHER"

};


// ------------------------------------------------------
// Database Tables
// ------------------------------------------------------

const TABLES = {

    GROUPS: "tally_sync_groups",

    STOCK_GROUPS: "tally_sync_stock_groups",

    LEDGERS: "tally_sync_ledgers",

    STOCKS: "tally_sync_stocks",

    UNITS: "tally_sync_units",

    GODOWNS: "tally_sync_godowns",

    COST_CENTRES: "tally_sync_cost_centres",

    VOUCHERS: "tally_vouchers",

    VOUCHER_LEDGERS: "tally_voucher_ledgers",

    VOUCHER_INVENTORY: "tally_voucher_inventory",

    STOCK_VOUCHERS: "tally_stock_vouchers",

    BILL_ALLOCATIONS: "tally_bill_allocations",

    COST_CENTRE_ALLOCATIONS: "tally_costcentre_allocations",

    SNAPSHOT: "tally_sync_snapshot",

    SYNC_BATCHES: "sync_batches"

};


const VALIDATION_SELECT_COLUMNS = {

    [TABLES.GROUPS]: "guid,alter_id",

    [TABLES.STOCK_GROUPS]: "guid,alter_id",

    [TABLES.LEDGERS]: "guid,alter_id",

    [TABLES.STOCKS]: "guid,alterid",

    [TABLES.UNITS]: "guid,alterid",

    [TABLES.GODOWNS]: "guid,alterid",

    [TABLES.COST_CENTRES]: "guid,alterid",

    [TABLES.VOUCHERS]: "guid,alterid"

};


const ALTER_ID_COLUMN = {

    [TABLES.GROUPS]: "alter_id",

    [TABLES.STOCK_GROUPS]: "alter_id",

    [TABLES.LEDGERS]: "alter_id",

    [TABLES.STOCKS]: "alterid",

    [TABLES.UNITS]: "alterid",

    [TABLES.GODOWNS]: "alterid",

    [TABLES.COST_CENTRES]: "alterid",

    [TABLES.VOUCHERS]: "alterid"

};


// ------------------------------------------------------
// Module Types
// ------------------------------------------------------

const MODULE_TYPE = {

    MASTER: "MASTER",

    VOUCHER: "VOUCHER"

};

// ------------------------------------------------------
// Entity Metadata Registry
// ------------------------------------------------------
const ENTITY_METADATA = {

    [ENTITY_TYPE.GROUP]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.GROUPS,

        builder: GroupRowBuilder,

        inputKey: "group"

    },

    [ENTITY_TYPE.STOCK_GROUP]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.STOCK_GROUPS,

        builder: StockGroupRowBuilder,

        inputKey: "group"

    },

    [ENTITY_TYPE.LEDGER]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.LEDGERS,

        builder: LedgerRowBuilder,

        inputKey: "ledger"

    },

    [ENTITY_TYPE.STOCK]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.STOCKS,

        builder: StockRowBuilder,

        inputKey: "stock"

    },

    [ENTITY_TYPE.UNIT]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.UNITS,

        builder: UnitRowBuilder,

        inputKey: "unit"

    },

    [ENTITY_TYPE.GODOWN]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.GODOWNS,

        builder: GodownRowBuilder,

        inputKey: "godown"

    },

    [ENTITY_TYPE.COST_CENTRE]: {

        module: MODULE_TYPE.MASTER,

        table: TABLES.COST_CENTRES,

        builder: CostCentreRowBuilder,

        inputKey: "costCentre"

    },

    [ENTITY_TYPE.VOUCHER]: {

        module: MODULE_TYPE.VOUCHER,

        table: TABLES.VOUCHERS,

        builder: VoucherRowBuilder,

        inputKey: "voucher"

    }

};


// ------------------------------------------------------
// Entity Sequence
// ------------------------------------------------------
const MASTER_ENTITY_SEQUENCE = [

    ENTITY_TYPE.GROUP,

    ENTITY_TYPE.STOCK_GROUP,

    ENTITY_TYPE.LEDGER,

    ENTITY_TYPE.STOCK,

    ENTITY_TYPE.UNIT,

    ENTITY_TYPE.GODOWN,

    ENTITY_TYPE.COST_CENTRE

];

const VOUCHER_ENTITY_SEQUENCE = [

    ENTITY_TYPE.VOUCHER

];

const ENTITY_SEQUENCE = [

    ...MASTER_ENTITY_SEQUENCE,

    ...VOUCHER_ENTITY_SEQUENCE

];

//Ye hardcoded if (GROUP)... else if (LEDGER)... ko future me replace karega.

// ------------------------------------------------------
// Conflict Keys
// ------------------------------------------------------

const CONFLICT_KEYS = {

    [TABLES.GROUPS]:
        "company_code,tally_owner,guid",

    [TABLES.STOCK_GROUPS]:
        "company_code,tally_owner,guid",

    [TABLES.LEDGERS]:
        "company_code,tally_owner,guid",

    [TABLES.STOCKS]:
        "company_code,tally_owner,guid",

    [TABLES.UNITS]:
        "company_code,tally_owner,guid",

    [TABLES.GODOWNS]:
        "company_code,tally_owner,guid",

    [TABLES.COST_CENTRES]:
        "company_code,tally_owner,guid",

    [TABLES.VOUCHERS]:
        "company_code,tally_owner,guid",

    [TABLES.SNAPSHOT]:
        "company_code,tally_owner,module,entity_type,guid"

};

// ------------------------------------------------------
// Voucher Table Columns
// ------------------------------------------------------

const VOUCHER_COLUMNS = {

    [TABLES.VOUCHERS]: {
        GUID: "guid",
        ALTER_ID: "alterid",
        IS_DELETED: "is_deleted",
        UPDATED_AT: "updated_at"
    },

    [TABLES.VOUCHER_LEDGERS]: {
        GUID: "voucher_guid"
    },

    [TABLES.VOUCHER_INVENTORY]: {
        GUID: "voucher_guid",
        IS_DELETED: "is_deleted",
        UPDATED_AT: "updated_at"
    },

    [TABLES.STOCK_VOUCHERS]: {
        GUID: "voucher_guid",
        IS_DELETED: "is_deleted",
        UPDATED_AT: "updated_at"
    },

    [TABLES.BILL_ALLOCATIONS]: {
        GUID: "voucher_guid",
        IS_DELETED: "is_deleted",
        UPDATED_AT: "updated_at"
    },

    [TABLES.COST_CENTRE_ALLOCATIONS]: {
        GUID: "voucher_guid",
        IS_DELETED: "is_deleted"
    }

};


// ------------------------------------------------------
// Voucher Reconciliation Columns
// ------------------------------------------------------
const VOUCHER_RECONCILIATION = {

    ROOT: {
        table: TABLES.VOUCHERS,
        guidColumn: "guid",
        deletedColumn: "is_deleted",
        updatedColumn: "updated_at",
        deleteMode: "SOFT_DELETE"
    },

    CHILDREN: {

        [TABLES.VOUCHER_LEDGERS]: {
            guidColumn: "voucher_guid",
            deleteMode: "HARD_DELETE"
        },

        [TABLES.VOUCHER_INVENTORY]: {
            guidColumn: "voucher_guid",
            deleteMode: "HARD_DELETE"
        },

        [TABLES.STOCK_VOUCHERS]: {
            guidColumn: "voucher_guid",
            deleteMode: "HARD_DELETE"
        },

        [TABLES.BILL_ALLOCATIONS]: {
            guidColumn: "voucher_guid",
            deleteMode: "HARD_DELETE"
        },

        [TABLES.COST_CENTRE_ALLOCATIONS]: {
            guidColumn: "voucher_guid",
            deleteMode: "HARD_DELETE"
        }

    }

};

// ------------------------------------------------------
// Batch Status
// ------------------------------------------------------

const BATCH_STATUS = {

    PENDING: "PENDING",

    RUNNING: "RUNNING",

    RECONCILING: "RECONCILING",

    COMPLETED: "COMPLETED",

    FAILED: "FAILED"

};


// ------------------------------------------------------
// Sync Stages
// ------------------------------------------------------

const SYNC_STAGE = {

    INIT: "INIT",

    WAITING_CONNECTOR: "WAITING_CONNECTOR",

    CONNECTOR_READY: "CONNECTOR_READY",

    RECEIVING: "RECEIVING",

    VALIDATING: "VALIDATING",

    EXECUTING: "EXECUTING",

    RETRY: "RETRY",

    SNAPSHOT: "SNAPSHOT",

    RECONCILIATION: "RECONCILIATION",

    ENTITY_COMPLETED: "ENTITY_COMPLETED",

    GUID_SCAN: "GUID_SCAN",

    POST_PROCESS: "POST_PROCESS",

    COMPLETED: "COMPLETED"

};


// ------------------------------------------------------
// Queue Status
// ------------------------------------------------------

const QUEUE_STATUS = {

    WAITING: "WAITING",

    PROCESSING: "PROCESSING",

    SUCCESS: "SUCCESS",

    FAILED: "FAILED"

};


// ------------------------------------------------------
// Snapshot Status
// ------------------------------------------------------

const SNAPSHOT_STATUS = {

    PENDING: "PENDING",

    COMPLETED: "COMPLETED",

    DELETED: "DELETED"

};


// ------------------------------------------------------
// Validation Result
// ------------------------------------------------------

const VALIDATION_ACTION = {

    INSERT: "INSERT",

    UPDATE: "UPDATE",

    DELETE: "DELETE",

    SKIP: "SKIP",

    FORCE_UPDATE: "FORCE_UPDATE"

};


// ------------------------------------------------------
// Voucher Sync Period
// ------------------------------------------------------

const SYNC_MODE = {
    FULL: "FULL",
    PERIODIC: "PERIODIC"
};

const VOUCHER_SYNC_PERIOD = {
    DEFAULT: "WEEKLY",
    WEEKLY: "WEEKLY",
    THREE_MONTHS: "3_MONTHS",
    SIX_MONTHS: "SIX_MONTHS",
    CUSTOM: "CUSTOM"
};


// ------------------------------------------------------
// Retry Configuration
// ------------------------------------------------------

const RETRY = {

    MAX_ATTEMPTS: 3,

    DELAY_MS: 3000

};

// ------------------------------------------------------
// Database Configuration
// ------------------------------------------------------

const DB_CONFIG = {

    CHUNK_SIZE: 500

};

const FILTERS = {

    ACTIVE: {

        column: "is_deleted",

        value: false

    }

};


// ------------------------------------------------------
// Exports
// ------------------------------------------------------
module.exports = {

    OPERATION_TYPE,

    ENTITY_TYPE,

    TABLES,

    VALIDATION_SELECT_COLUMNS,

    ALTER_ID_COLUMN,

    VOUCHER_COLUMNS,

    VOUCHER_RECONCILIATION,
   
    MODULE_TYPE,

    ENTITY_SEQUENCE,

    MASTER_ENTITY_SEQUENCE,

    VOUCHER_ENTITY_SEQUENCE,

    SYNC_STAGE,

    ENTITY_METADATA,

    CONFLICT_KEYS,

    BATCH_STATUS,

    QUEUE_STATUS,

    SNAPSHOT_STATUS,

    VALIDATION_ACTION,

    DB_CONFIG,

     SYNC_MODE,

    VOUCHER_SYNC_PERIOD,

    RETRY,

    FILTERS,

};