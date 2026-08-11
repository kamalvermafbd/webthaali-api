// ======================================================
// UNIT ROW BUILDER
// ======================================================
//
// Converts raw Tally Unit
// into database row.
//
// Shared by:
// saveUnits.js
// BatchManager
//
// ======================================================
function safeTrim(value) {
    if (value == null) return "";

    if (typeof value === "string") {
        return value.trim();
    }

    if (
        typeof value === "number" ||
        typeof value === "boolean"
    ) {
        return String(value).trim();
    }

    return "";
}


class UnitRowBuilder {

    build({

        unit,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    }) {

        const timestamp =

            now ||

            new Date().toISOString();

        return {

            company_code,

            tally_owner,

           guid:
    safeTrim(unit.guid) || null,

            masterid:
                (
                    unit.masterId
                    ??
                    unit.master_id
                    ??
                    unit.masterid
                )?.toString()
                ||
                null,

            alterid:
                unit.alterId
                ??
                unit.alter_id
                ??
                unit.alterid
                ??
                null,

      name:
    safeTrim(unit.name) || null,

          formal_name:
    safeTrim(
        unit.formalName
        ??
        unit.formal_name
    ) || null,

            decimal_places:
                (
                    unit.decimalPlaces
                    ??
                    unit.decimal_places
                ) == null
                    ? null
                    : Number(

                        unit.decimalPlaces
                        ??
                        unit.decimal_places

                    ),

            is_deleted: false,

            created_at:
                timestamp,

            updated_at:
                timestamp,

            last_synced_at:
                timestamp,

            sync_batch_id

        };

    }

}

module.exports =
    new UnitRowBuilder();