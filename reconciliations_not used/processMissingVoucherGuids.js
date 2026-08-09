const fs = require("fs");

async function processMissingVoucherGuids({

    company_code,

    tally_owner,

    

    voucherGuids = []

}) {

    //--------------------------------------------------
    // Nothing to process
    //--------------------------------------------------

    if (voucherGuids.length === 0) {

        return;

    }

    //--------------------------------------------------
    // Debug
    //--------------------------------------------------

    fs.writeFileSync(

        "./logs/missing-voucher-guids.json",

        JSON.stringify(

            {

                company_code,

                tally_owner,

                totalMissing: voucherGuids.length,

                voucherGuids

            },

            null,

            2

        )

    );

    fs.appendFileSync(

        "./logs/voucher-guid-debug.jsonl",

        JSON.stringify({

            stage: "PROCESS_MISSING_VOUCHERS",

            company_code,

            tally_owner,

           

            totalMissing: voucherGuids.length,

            voucherGuids

        }) + "\n"

    );

    //--------------------------------------------------
    // TODO
    // Existing voucherByGuid() ko voucherGuids pass karne hain.
    // Fetched vouchers existing saveVouchers() pipeline me jayenge.
    //--------------------------------------------------

}

module.exports = {

    processMissingVoucherGuids

};