// ======================================================
// GROUP ROW BUILDER
// ======================================================
//
// Converts raw Tally Group
// into database row.
//
// Shared by:
// saveGroups.js
// BatchManager
//
// ======================================================

class GroupRowBuilder {

    build({

        company_code,

        tally_owner,

        sync_batch_id,

        group

    }) {

        const now =

            new Date().toISOString();

        return {

            company_code,

            tally_owner,

            guid:
                group.guid?.trim() || null,

            alter_id:
                group.alterId ?? null,

            master_id:
                group.masterId?.toString() || null,

            name:
                group.name?.trim(),

            parent:
                group.parent?.trim() || null,

            reserved_name:
                group.reservedName?.trim() || null,

            parent_guid:
                group.parentGuid?.trim() || null,

            parent_master_id:
                group.parentMasterId?.toString() || null,

            parent_alter_id:
                group.parentAlterId ?? null,

            is_subledger:
                group.isSubledger ?? false,

            is_billwise_on:
                group.isBillwiseOn ?? false,

            track_negative_balances:
                group.trackNegativeBalances ?? false,

            is_condensed:
                group.isCondensed ?? false,

            is_revenue:
                group.isRevenue ?? null,

            is_deemed_positive:
                group.isDeemedPositive ?? null,

            is_deleted: false,

            created_at:
                now,

            updated_at:
                now,

            last_synced_at:
                now,

            sync_batch_id

        };

    }

}

module.exports =
    new GroupRowBuilder();