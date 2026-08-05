// ======================================================
// GODOWN ROW BUILDER
// ======================================================
//
// Converts raw Tally Godown
// into database row.
//
// Shared by:
// saveGodowns.js
// BatchManager
//
// ======================================================

class GodownRowBuilder {

    build({

        godown,

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
                godown.guid?.trim() || null,

            masterid:
                (
                    godown.masterId
                    ??
                    godown.master_id
                    ??
                    godown.masterid
                )?.toString()
                ||
                null,

            alterid:
                godown.alterId
                ??
                godown.alter_id
                ??
                godown.alterid
                ??
                null,

            name:
                godown.name?.trim() || null,

            parent:
                godown.parent?.trim() || null,

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
    new GodownRowBuilder();