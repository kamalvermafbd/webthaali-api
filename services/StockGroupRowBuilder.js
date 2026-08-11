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
                safeTrim(group.guid) || null,

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
    safeTrim(group.name) || null,

            parent:
    safeTrim(group.parent) || null,

           reserved_name:
    safeTrim(
        group.reservedName
        ??
        group.reserved_name
    ) || null,

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