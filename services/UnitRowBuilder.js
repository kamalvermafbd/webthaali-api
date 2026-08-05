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
                unit.guid?.trim() || null,

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
                unit.name?.trim() || null,

            formal_name:
                (
                    unit.formalName
                    ??
                    unit.formal_name
                )?.trim()
                ||
                null,

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