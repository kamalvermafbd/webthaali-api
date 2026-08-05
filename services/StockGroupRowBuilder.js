// ======================================================
// STOCK GROUP ROW BUILDER
// ======================================================
//
// Converts raw Tally Stock Group
// into database row.
//
// Shared by:
// saveStockGroups.js
// BatchManager
//
// ======================================================

class StockGroupRowBuilder {

    build({

        group,

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
                group.guid?.trim() || null,

            alter_id:
                group.alterId
                ??
                group.alter_id
                ??
                null,

            master_id:
                (
                    group.masterId
                    ??
                    group.master_id
                )?.toString()
                ||
                null,

            name:
                group.name?.trim() || null,

            parent:
                group.parent?.trim()
                ||
                null,

            reserved_name:
                (
                    group.reservedName
                    ??
                    group.reserved_name
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
    new StockGroupRowBuilder();