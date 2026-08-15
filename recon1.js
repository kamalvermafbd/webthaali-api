require("dotenv").config();

const ReconciliationManager =
    require("./sync-engine/ReconciliationManager");

const fs = require("fs");
const path = require("path");

// ======================================================
// RECONCILIATION MANAGER TEST
// ======================================================
//
// Purpose:
//
// tally_sync_snapshot
//        VS
// tally_vouchers
//
// READ ONLY TEST
//
// No INSERT
// No DELETE
// No UPDATE
//
// Output:
// - Snapshot count
// - DB count
// - Exact missing GUID count
// - COMPLETE missing GUID list
// ======================================================

const company_code = "C0035";
const tally_owner = "USER";

// ======================================================
// VOUCHER TEST
// ======================================================

const test = {

    name:
        "VOUCHER",

    table:
        "tally_vouchers",

    module:
        "VOUCHER",

    entity_type:
        "VOUCHER"

};


// ======================================================
// RUN TEST
// ======================================================

async function run() {

    console.log(
        "=============================================="
    );

    console.log(
        "RECONCILIATION MANAGER TEST"
    );

    console.log(
        "=============================================="
    );

    console.log(
        "Company:",
        company_code
    );

    console.log(
        "Tally Owner:",
        tally_owner
    );

    console.log(
        "Table:",
        test.table
    );

    console.log(
        "=============================================="
    );


    try {

        const result =
            await ReconciliationManager.reconcile({

                table:
                    test.table,

                company_code,

                tally_owner,

                module:
                    test.module,

                entity_type:
                    test.entity_type,

                sync_batch_id:
                    `RECON-TEST-${Date.now()}`
            });


        // ==================================================
        // EXACT MISSING GUIDS
        // ==================================================

        const missingGuids =
            (result.missingGuids || [])
                .map(
                    row => row.guid
                )
                .filter(Boolean);


        // ==================================================
        // RESULT
        // ==================================================

        const output = {

            test:
                "RECONCILIATION MANAGER",

            company_code,

            tally_owner,

            table:
                test.table,

            module:
                test.module,

            entity_type:
                test.entity_type,

            success:
                result.success,

            snapshotCount:
                result.snapshotCount,

            dbCount:
                result.dbCount,

            missingCount:
                missingGuids.length,

            // COMPLETE LIST
            // No slice / no limit

            missingGuids

        };


        // ==================================================
        // CONSOLE SUMMARY
        // ==================================================

        console.log("");

        console.log(
            "=============================================="
        );

        console.log(
            "RECONCILIATION RESULT"
        );

        console.log(
            "=============================================="
        );

        console.log(
            "Snapshot Count :",
            output.snapshotCount
        );

        console.log(
            "DB Count       :",
            output.dbCount
        );

        console.log(
            "Missing Count  :",
            output.missingCount
        );

        console.log(
            "=============================================="
        );


        // ==================================================
        // PRINT EVERY MISSING GUID
        // ==================================================

        console.log("");

        console.log(
            "========== EXACT MISSING GUIDS =========="
        );

        missingGuids.forEach(
            (guid, index) => {

                console.log(
                    `${index + 1}. ${guid}`
                );

            }
        );

        console.log(
            "========== END MISSING GUIDS =========="
        );


        // ==================================================
        // SAVE RESULT
        // ==================================================

        const logDir =
            path.join(
                __dirname,
                "logs"
            );


        fs.mkdirSync(
            logDir,
            {
                recursive: true
            }
        );


        const logFile =
            path.join(
                logDir,
                "reconciliationMissingVoucherGuids.json"
            );


        fs.writeFileSync(

            logFile,

            JSON.stringify(
                output,
                null,
                2
            ),

            "utf8"

        );


        console.log("");

        console.log(
            "=============================================="
        );

        console.log(
            "RESULT FILE:"
        );

        console.log(
            logFile
        );

        console.log(
            "=============================================="
        );


    }
    catch (error) {

        console.error("");

        console.error(
            "RECONCILIATION TEST FAILED"
        );

        console.error(
            error
        );

        process.exit(1);

    }

}


// ======================================================
// START
// ======================================================

run();