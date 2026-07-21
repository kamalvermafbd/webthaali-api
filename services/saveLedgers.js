const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveLedgers({
    company_code,
    tally_owner,
    sync_batch_id,
    ledgers = []
}) {

    if (!Array.isArray(ledgers) || ledgers.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const withGuid = [];
   // const withoutGuid = [];

    // ===========================
// LOAD GROUPS FOR LOOKUP
// ===========================

const { data: groups, error: groupsError } = await supabase
    .from("tally_sync_groups")
    .select("name,guid,master_id,alter_id")
    .eq("company_code", company_code)
    .eq("tally_owner", tally_owner);

if (groupsError) {
    throw new Error("Failed to load groups : " + groupsError.message);
}

const groupMap = {};

for (const group of groups || []) {

    groupMap[(group.name || "").trim().toUpperCase()] = group;

}


    for (const ledger of ledgers) {

        const parentGroup =
    groupMap[(ledger.parent || "").trim().toUpperCase()];

   if (!parentGroup && ledger.parent) {
    console.warn(
        `[${company_code}] Parent group not found for Ledger "${ledger.name}". Parent="${ledger.parent}"`
    );
}

        const row = {

            company_code,
            tally_owner,

            guid: ledger.guid?.trim() || null,
            alter_id: ledger.alterId ?? null,
            master_id: ledger.masterId?.toString() || null,

            name: ledger.name?.trim(),
            parent: ledger.parent?.trim() || null,

            parent_group_guid:
                parentGroup?.guid || null,

            parent_group_master_id:
                parentGroup?.master_id || null,

            parent_group_alter_id:
                parentGroup?.alter_id || null,

            root_group:
                parentGroup?.root_group || null,
                
            reserved_name: ledger.reservedName?.trim() || null,

            gst_applicable: ledger.gstApplicable?.trim() || null,
            gst_registration_type: ledger.gstRegistrationType?.trim() || null,
            gstin: ledger.gstin?.trim() || null,

            mailing_name: ledger.mailingName?.trim() || null,
            address: ledger.address?.trim() || null,
            state_name: ledger.stateName?.trim() || null,
            country: ledger.country?.trim() || null,
            pin_code: ledger.pinCode?.trim() || null,

            phone: ledger.phone?.trim() || null,
            email: ledger.email?.trim() || null,
            contact_person: ledger.contactPerson?.trim() || null,

            opening_balance: ledger.openingBalanceAmount ?? 0,
            opening_balance_type: ledger.openingBalanceType ?? null,

            is_bill_wise: ledger.isBillWise ?? null,
            is_revenue: ledger.isRevenue ?? null,
            is_deemed_positive: ledger.isDeemedPositive ?? null,

          //  root_group: ledger.rootGroup?.trim() || null,

            is_deleted: false,

            last_synced_at: now,
            sync_batch_id,

            updated_at: now

        };

        if (row.guid) {

            withGuid.push(row);

      } else {

 console.warn(
    `[${company_code}] [${tally_owner}] Skipping Ledger "${row.name}" because GUID is missing.`
);

}

    }


   

    let success = 0;

    // ===========================
    // GUID BASED UPSERT
    // ===========================

    if (withGuid.length > 0) {

        const { error } = await supabase

            .from("tally_sync_ledgers")

            .upsert(
                withGuid,
                {
                    onConflict: "company_code,tally_owner,guid"
                }
            );

        if (error) {

            throw new Error(
                "Failed to save ledgers (GUID Upsert): " +
                error.message
            );

        }

        success += withGuid.length;

    }

    

    return {

        total: ledgers.length,

        success,

        failed: ledgers.length - success

    };

}

module.exports = {

    saveLedgers

};