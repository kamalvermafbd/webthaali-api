// ======================================================
// COST CENTRE ROW BUILDER
// ======================================================
//
// Converts raw Tally Cost Centre
// into database row.
//
// Shared by:
// saveCostCentres.js
// BatchManager
//
// ======================================================

class CostCentreRowBuilder {

    build({

        costCentre,

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
                costCentre.guid?.trim() || null,

            masterid:
                (
                    costCentre.masterId
                    ??
                    costCentre.master_id
                    ??
                    costCentre.masterid
                )?.toString()
                ||
                null,

            alterid:
                costCentre.alterId
                ??
                costCentre.alter_id
                ??
                costCentre.alterid
                ??
                null,

            name:
                costCentre.name?.trim() || null,

            parent:
                costCentre.parent?.trim() || null,

            category:
                costCentre.category?.trim() || null,

            reserved_name:
                (
                    costCentre.reservedName
                    ??
                    costCentre.reserved_name
                )?.trim()
                ||
                null,

            is_deleted:
                false,

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
    new CostCentreRowBuilder();