// ======================================================
// LEDGER ROW BUILDER
// ======================================================
//
// Converts raw Tally Ledger
// into database row.
//
// Shared by:
// saveLedgers.js
// BatchManager
//
// ======================================================


class LedgerRowBuilder {

    build({

        ledger,

        company_code,

        tally_owner,

        sync_batch_id,

        now

    }) {

        const timestamp =
    now || new Date().toISOString();
    

        return {

            company_code,

            tally_owner,

            guid:
                ledger.guid?.trim() || null,

            alter_id:
                ledger.alterId ?? null,

            master_id:
                ledger.masterId?.toString() || null,

            name:
                ledger.name?.trim() || null,

            parent:
                ledger.parent?.trim() || null,

            parent_group_guid:
                ledger.parentGroupGuid || null,

            parent_group_master_id:
                ledger.parentGroupMasterId || null,

            parent_group_alter_id:
                ledger.parentGroupAlterId || null,

            root_group:
                null,

            reserved_name:
                ledger.reservedName?.trim() || null,

            gst_applicable:
                ledger.gstApplicable?.trim() || null,

            gst_registration_type:
                ledger.gstRegistrationType?.trim() || null,

            gstin:
                ledger.gstin?.trim() || null,

            mailing_name:
                ledger.mailingName?.trim() || null,

            address:
                ledger.address?.trim() || null,

            state_name:
                ledger.stateName?.trim() || null,

            country:
                ledger.country?.trim() || null,

            pin_code:
                ledger.pinCode?.trim() || null,

            phone:
                ledger.phone?.trim() || null,

            email:
                ledger.email?.trim() || null,

            contact_person:
                ledger.contactPerson?.trim() || null,

            opening_balance:
                ledger.openingBalanceAmount ?? 0,

            opening_balance_type:
                ledger.openingBalanceType ?? null,

            is_bill_wise:
                ledger.isBillWise ?? false,

            is_revenue:
                ledger.isRevenue ?? false,

            is_deemed_positive:
                ledger.isDeemedPositive ?? false,

            gst_duty_type:
                ledger.gstDutyType?.trim() || null,

            gst_tax_type:
                ledger.gstTaxType?.trim() || null,

            gst_rate:
                ledger.gstRate ?? null,

            tax_percentage_of_calculation:
                ledger.gstRate ?? null,

            is_deleted: false,

            created_at: timestamp,
            updated_at: timestamp,
            last_synced_at: timestamp,

            sync_batch_id

        };

    }

}

module.exports =
    new LedgerRowBuilder();