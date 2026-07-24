const fs = require("fs");
const path = require("path");

const dir = path.join(__dirname, "services");

const patterns = [
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