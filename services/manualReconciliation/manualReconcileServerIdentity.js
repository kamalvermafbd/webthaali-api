"use strict";

const crypto = require("node:crypto");

// One identity per server process lifetime.
const serverInstanceId = crypto.randomUUID();

function getManualReconcileServerInstanceId() {
    return serverInstanceId;
}

module.exports = {
    getManualReconcileServerInstanceId
};