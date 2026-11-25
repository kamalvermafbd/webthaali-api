"use strict";

// Compatibility wrapper for Manual Reconciliation failure recovery.
// All Redis operations are centralized in connectorHealthStore.

const {
    stageManualReconcileFailureCleanup,
    readManualReconcileFailureCleanupReceipt,
    inspectManualReconcileFailureRedis,
    unblockManualReconcileAfterSqlClosure,
    readManualReconcileFailureUnblockReceipt
} = require("./connectorHealthStore");

module.exports = {
    stageManualReconcileFailureCleanup,
    readManualReconcileFailureCleanupReceipt,
    inspectManualReconcileFailureRedis,
    unblockManualReconcileAfterSqlClosure,
    readManualReconcileFailureUnblockReceipt
};


