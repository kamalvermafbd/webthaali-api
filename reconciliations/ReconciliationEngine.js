/***********************************************************************
 * ReconciliationEngine
 *
 * Responsibility
 * ---------------------------------------------------------------------
 * Generic GUID reconciliation engine.
 *
 * This engine ONLY compares incoming GUIDs from Tally with existing
 * GUIDs stored in Supabase.
 *
 * It DOES NOT:
 *   - delete records
 *   - insert records
 *   - update records
 *   - write logs
 *   - update sync_batches
 *
 * Those responsibilities belong to the caller
 * (saveVouchers / saveMasters).
 ***********************************************************************/

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

/***********************************************************************
 * PUBLIC API
 ***********************************************************************/

/**
 * Generic reconciliation.
 *
 * Input:
 *  - company_code
 *  - tally_owner
 *  - table
 *  - guidField
 *  - incomingGuids
 *
 * Output:
 *  {
 *      matched,
 *      missingInDB,
 *      missingInTally,
 *      summary
 *  }
 */


/***********************************************************************
 * PRIVATE HELPERS
 ***********************************************************************/
async function reconcile({

    company_code,

    tally_owner,

    table,

    guidField,

    incomingGuids

}) {

    try {

        //--------------------------------------------------
        // STEP 1
        // Load all GUIDs from database
        //--------------------------------------------------

        const dbGuids = await loadDbGuids({

            company_code,

            tally_owner,

            table,

            guidField

        });

 //--------------------------------------------------
// Validate Incoming GUIDs
//--------------------------------------------------

if (!(incomingGuids instanceof Set)) {

    throw new Error(

        "incomingGuids must be a Set"

    );

}

        //--------------------------------------------------
        // STEP 2
        // Compare DB GUIDs vs Incoming GUIDs
        //--------------------------------------------------

        const {

            matched,

            missingInDB,

            missingInTally

        } = compareGuids({

            dbGuids,

            incomingGuids

        });

        //--------------------------------------------------
        // STEP 3
        // Build reconciliation summary
        //--------------------------------------------------

        const summary = buildSummary({

            dbGuids,

            incomingGuids,

            matched,

            missingInDB,

            missingInTally

        });

        //--------------------------------------------------
        // STEP 4
        // Return result
        //--------------------------------------------------

        return {

            matched,

            missingInDB,

            missingInTally,

            summary

        };

    } catch (error) {

        throw new Error(

            `Reconciliation failed for ${table}: ${error.message}`

        );

    }

}
/**
 * Load all GUIDs for a table.
 *
 * Returns:
 *   Set()
 */

async function loadDbGuids({

    company_code,

    tally_owner,

    table,

    guidField

}) {

    //--------------------------------------------------
    // Read GUID column
    //--------------------------------------------------

    const {

        data,

        error

    } = await supabase

        .from(table)

        .select(guidField)

        .eq("company_code", company_code)

        .eq("tally_owner", tally_owner);

    //--------------------------------------------------
    // Validation
    //--------------------------------------------------

    if (error) {

        throw new Error(

            `Failed to load GUIDs from ${table}: ${error.message}`

        );

    }

    //--------------------------------------------------
    // Validate GUID field
    //--------------------------------------------------

    if (!data) {

        throw new Error(

            `No data returned from ${table}`

        );

    }

    if (data.length > 0 && !(guidField in data[0])) {

        throw new Error(

            `GUID field '${guidField}' not found in ${table}`

        );

    }

    //--------------------------------------------------
    // Convert to Set
    //--------------------------------------------------

    const dbGuids = new Set(

        data

            .map(row => row[guidField])

            .filter(Boolean)

    );

    //--------------------------------------------------
    // Return
    //--------------------------------------------------

    return dbGuids;

}

/**
 * Compare incoming GUIDs with DB GUIDs.
 *
 * Returns:
 * {
 *      matched,
 *      missingInDB,
 *      missingInTally
 * }
 */



function compareGuids({

    dbGuids,

    incomingGuids

}) {

    //--------------------------------------------------
    // Initialize result collections
    //--------------------------------------------------

    const matched = [];

    const missingInDB = [];

    const missingInTally = [];

 
    //--------------------------------------------------
    // Compare Incoming GUIDs against DB GUIDs
    //
    // matched
    // missingInDB
    //--------------------------------------------------

    for (const guid of incomingGuids) {

        if (dbGuids.has(guid)) {

            matched.push(guid);

        } else {

            missingInDB.push(guid);

        }

    }

   
    //--------------------------------------------------
    // Find GUIDs present in DB but missing in Tally
    //--------------------------------------------------
 

    for (const guid of dbGuids) {

        if (!incomingGuids.has(guid)) {

            missingInTally.push(guid);

        }

    }

    //--------------------------------------------------
    // Return comparison
    //--------------------------------------------------

    return {

        matched,

        missingInDB,

        missingInTally

    };

}

/**
 * Build reconciliation summary.
 *
 * Returns:
 * {
 *      totalIncoming,
 *      totalDb,
 *      matched,
 *      missingInDB,
 *      missingInTally
 * }
 */

function buildSummary({

    dbGuids,

    incomingGuids,

    matched,

    missingInDB,

    missingInTally

}) {

    //--------------------------------------------------
    // Calculate totals
    //--------------------------------------------------

    const summary = {

        totalIncoming: incomingGuids.size,

        totalDb: dbGuids.size,

        matched: matched.length,

        missingInDB: missingInDB.length,

        missingInTally: missingInTally.length,

        status:

            missingInDB.length === 0 &&

            missingInTally.length === 0

                ? "MATCHED"

                : "DIFFERENCE"

    };

    //--------------------------------------------------
    // Return summary object
    //--------------------------------------------------

    return summary;

}

/***********************************************************************
 * EXPORTS
 ***********************************************************************/

module.exports = {

    reconcile

};