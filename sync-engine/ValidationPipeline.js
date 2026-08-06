const { createClient } =
    require("@supabase/supabase-js");

const supabase = createClient(

    process.env.SUPABASE_URL,

    process.env.SUPABASE_SERVICE_KEY

);

const {

    VALIDATION_SELECT_COLUMNS,
    ALTER_ID_COLUMN

} = require("./constants");



class ValidationPipeline {

    // ======================================
// TODO
// ======================================
//
// 1. Add field-level validation.
// 2. Detect duplicate GUIDs.
// 3. Queue invalid rows separately.
// Fetch only required fields (guid, alter_id)
//

    // ----------------------------------
// Validate
// ----------------------------------

async validate({

    table,

    company_code,

    tally_owner,

    rows = []

}) {

    if (!table) {

    throw new Error(

        "table is required"

    );

}

if (!company_code) {

    throw new Error(

        "company_code is required"

    );

}

if (!tally_owner) {

    throw new Error(

        "tally_owner is required"

    );

}

if (!Array.isArray(rows)) {

    throw new Error(

        "rows must be an array"

    );

}

if (rows.length === 0) {

        return {

            total: 0,

            newRows: [],

            changedRows: [],

            unchangedRows: [],

            skippedRows: [],

            summary: {

                new: 0,

                changed: 0,

                unchanged: 0,

                skipped: 0

            }

        };

}

        const {

            data: existingRows,

            error

        } = await supabase

            .from(table)

            .select(

                VALIDATION_SELECT_COLUMNS[table] || "*"

            )

            .eq(

                "company_code",

                company_code

            )

            .eq(

                "tally_owner",

                tally_owner

            )

            .eq(

                "is_deleted",

                false

            );

        if (error) {

            throw new Error(

                "Failed to load existing rows : " +

                error.message

            );

        }


        const dbMap =

            new Map();

        for (const row of existingRows || []) {

            if (!row.guid) {

                continue;

            }

            dbMap.set(

                String(

                    row.guid

                ).trim(),

                row

            );

        }


        const skippedRows = [];

        const newRows = [];

        const changedRows = [];

        const unchangedRows = [];

        const alterColumn =

            ALTER_ID_COLUMN[table] ||

            "alter_id";

        for (const row of rows) {

            const guid =

            String(

                row.guid || ""

            ).trim();

          if (!guid) {

                skippedRows.push({

                    ...row,

                    reason: "MISSING_GUID"

                });

                continue;

            }

            const existing =

                dbMap.get(guid);

            if (!existing) {

                newRows.push(row);

                continue;

            }

            const dbAlterId =

                existing[alterColumn] ??

                null;

            const rowAlterId =

                row.alterId ??

                row.alter_id ??

                row.alterid ??

                null;

           const dbAlter =

                Number(dbAlterId);

            const incomingAlter =

                Number(rowAlterId);

            const dbValue =

                Number.isFinite(dbAlter)

                    ? dbAlter

                    : 0;

            const incomingValue =

                Number.isFinite(incomingAlter)

                    ? incomingAlter

                    : 0;

            if (incomingValue < dbValue) {

                skippedRows.push({

                    ...row,

                    oldAlterId:

                        dbValue,

                    newAlterId:

                        incomingValue,

                    reason:

                        "STALE_ALTER_ID"

                });

                continue;

            }

            if (incomingValue > dbValue) {

                changedRows.push({

                    ...row,

                    oldAlterId:

                        dbValue,

                    newAlterId:

                        incomingValue

                });

                continue;

            }

            unchangedRows.push(row);

        }        


        return {

            total:

                rows.length,

            newRows,

            changedRows,

            unchangedRows,

            skippedRows,

            summary: {

                new:

                    newRows.length,

                changed:

                    changedRows.length,

                unchanged:

                    unchangedRows.length,

                skipped:

                    skippedRows.length

            }

        };

}


}

module.exports =
    new ValidationPipeline();