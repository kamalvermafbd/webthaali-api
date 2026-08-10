const fs = require("fs");
const path = require("path");

//const dir = path.join(__dirname, "services");
const dir = path.join(__dirname, "logs");

const patterns = [
    /^00-(COST_CENTRE|GODOWN|GROUP|LEDGER|STOCK_GROUP|STOCK|UNIT|VOUCHER)-snapshot-loaded\.json$/,
/^01-(COST_CENTRE|GODOWN|GROUP|LEDGER|STOCK_GROUP|STOCK|UNIT|VOUCHER)-(incoming-snapshot|reconciliation-input)\.json$/,
/^02-before-upsert-(COST_CENTRE|GODOWN|GROUP|LEDGER|STOCK_GROUP|STOCK|UNIT|VOUCHER)\.json$/,
  /^00-.*\.json$/,
  /^01-.*\.json$/,
  /^02-.*\.json$/,
  /^03-.*\.json$/,
  /^BEFORE_SAVE_.*\.json$/,
  /^AFTER_SAVE_.*\.json$/,
/^FINAL_.*\.json$/,
/^PRE_SAVE_.*\.json$/,
/^PROTO_.*\.json$/,
/^REMOVE_.*\.json$/,
    /^incoming-vouchers-.*\.json$/,
    /^integrity-entry-.*\.txt$/,
    /^voucher-integrity-debug-.*\.json$/,
    /^validator-start-.*\.txt$/,
    /^ledger-after-delete-.*\.json$/,
/^ledger-delete-debug-.*\.json$/,
/^voucher-sync-debug-.*\.json$/,
    /^validator-end-.*\.txt$/,
    /^integrity-exit-.*\.txt$/,
    /^integrity-error-.*\.txt$/,
    /^voucher16-debug-.*\.json$/,
    /^voucher16-ledgers-before-save-.*\.json$/,
    /^voucher-ledgers-before-save-.*\.json$/,
    /^voucher-sync-debug-before-.*\.json$/,
    /^voucher-sync-debug-.*\.json$/,
    /^ledger-delete-debug-.*\.json$/,
    /^ledger-after-delete-.*\.json$/,
    /^ledgerRows-before-insert-.*\.json$/
];

for (const file of fs.readdirSync(dir)) {

    if (patterns.some(r => r.test(file))) {

        fs.unlinkSync(path.join(dir, file));
        console.log("Deleted:", file);

    }

}