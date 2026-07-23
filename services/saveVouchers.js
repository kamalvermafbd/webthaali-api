const { createClient } = require("@supabase/supabase-js");
const fs = require("fs");
const path = require("path");

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

async function saveVouchers({
    company_code,
    tally_owner,
    sync_batch_id,
    vouchers = []
}) {

    if (!Array.isArray(vouchers) || vouchers.length === 0) {

        return {
            total: 0,
            success: 0,
            failed: 0
        };

    }

    const now = new Date().toISOString();

    const voucherRows = [];

    const ledgerRows = [];

    const inventoryRows = [];

    const { data: existingVouchers, error: existingError } =
    await supabase
        .from("tally_vouchers")
        .select("guid, alterid")
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner);

if (existingError) {

    throw new Error(
        "Failed to load existing vouchers: " +
        existingError.message
    );

}

const existingVoucherMap = new Map(
    (existingVouchers || []).map(v => [
        v.guid,
        Number(v.alterid)
    ])
);

const newVoucherRows = [];

const changedVoucherRows = [];

const unchangedVoucherGuids = [];

const deletedVoucherGuids = [];


    for (const voucher of vouchers) {

        const header = voucher.header || {};

        

       if (!header.guid?.trim()) {

            console.warn(
                `[${company_code}] [${tally_owner}] Skipping Voucher "${header.voucherNumber}" because GUID is missing.`
            );

            continue;

        }



const existingAlterId =
    existingVoucherMap.get(header.guid.trim());

if (existingAlterId === undefined) {

    newVoucherRows.push(header.guid.trim());

} else if (
    existingAlterId === Number(header.alterid)
) {

    unchangedVoucherGuids.push(header.guid.trim());

} else {

    changedVoucherRows.push(header.guid.trim());

}

if (
    existingAlterId !== undefined &&
    existingAlterId === Number(header.alterid)
) {

    continue;

}

        voucherRows.push({

            company_code,
            tally_owner,

            guid: header.guid.trim(),

           masterid: header.masterid ?? null,

            alterid: header.alterid ?? null,

            voucher_type: header.voucherType ?? null,

            voucher_number: header.voucherNumber ?? null,

            voucher_date: header.voucherDate || null,

            effective_date: header.effectiveDate || null,

            reference: header.reference || null,

            reference_date: header.referenceDate || null,

            party_ledger: header.partyLedger || null,

            narration: header.narration || null,

            gstin: header.gstin || null,

            place_of_supply: header.placeOfSupply || null,

            buyer_name: header.buyerName || null,

            buyer_address: header.buyerAddress || null,

            gst_registration_type:
                header.gstRegistrationType || null,

            persisted_view:
                header.persistedView || null,

            is_invoice:
                header.isInvoice === "Yes",

            is_optional:
                header.isOptional === "Yes",

            is_cancelled:
                header.isCancelled === "Yes",

            is_deleted: false,

            last_synced_at: now,

            sync_batch_id,

            updated_at: now

        });

    for (const ledger of (voucher.ledgers || [])) {

            ledgerRows.push({

                voucher_guid: header.guid.trim(),

                company_code,

                tally_owner,

                ledger_name:
                    ledger.ledgerName?.trim() || null,

                ledger_masterid:
                    ledger.ledgerMasterId ?? null,

                amount:
                    ledger.amount ?? null,

                debit:
                    ledger.debit ?? null,

                credit:
                    ledger.credit ?? null,

               is_deemed_positive:
                    ledger.isDeemedPositive === "Yes",

                is_party_ledger:
                    ledger.isPartyLedger === "Yes",

                is_last_deemed_positive:
                    ledger.isLastDeemedPositive === "Yes",

                remove_zero_entries:
                    ledger.removeZeroEntries === "Yes",

                bill_allocations:
                    ledger.billAllocations ?? [],

                costcentre_allocations:
                    ledger.costCentreAllocations ?? []

            });

        }


    for (const item of (voucher.inventory || [])) {

     const gstRates = item.gstRates ?? [];

    const cgstRate =
        gstRates.find(r => r.dutyHead === "CGST")?.rate ?? null;

    const sgstRate =
        gstRates.find(r => r.dutyHead === "SGST/UTGST")?.rate ?? null;

    const igstRate =
        gstRates.find(r => r.dutyHead === "IGST")?.rate ?? null;

        const hsnCode =
            item.hsnCode || null;

          console.log({
    stock: item.stockItem,
    taxable: item.taxableAmount,
    cgst: item.cgstAmount,
    sgst: item.sgstAmount,
    igst: item.igstAmount
});

            inventoryRows.push({

                voucher_guid: header.guid.trim(),

                company_code,

                tally_owner,

                stock_item:
                    item.stockItem?.trim() || null,
          

                actual_qty:
                    item.actualQty || null,

                actual_qty_value:
                    item.actualQtyValue || null,

                billed_qty:
                    item.billedQty || null,

                billed_qty_value:
                    item.billedQtyValue || null,

                unit:
                    item.unit?.trim() || null,

                rate:
                    item.rate || null,

                rate_value:
                    item.rateValue || null,

                amount:
                    item.amount ?? null,

             hsn_code: hsnCode,

                discount:
                    item.discount ?? null,

                godown:
                    item.godown?.trim() || null,

                batches:
                    item.batches ?? [],

                accounting:
                    item.accounting ?? [],

                stock_guid:
    item.stockGuid ?? null,

stock_masterid:
    item.stockMasterIdResolved ?? null,

stock_alterid:
    item.stockAlterId ?? null,

voucher_master_id:
    item.voucherMasterId ?? null,

voucher_alter_id:
    item.voucherAlterId ?? null,

voucher_date:
    item.voucherDate ?? null,

voucher_type:
    item.voucherType ?? null,

transaction_type:
    item.transactionType ?? null,

ledger_name:
    item.ledgerName ?? null,

ledger_guid:
    item.ledgerGuid ?? null,

ledger_master_id:
    item.ledgerMasterId ?? null,

ledger_alter_id:
    item.ledgerAlterId ?? null,

ledger_parent_name:
    item.ledgerParentName ?? null,

ledger_parent_guid:
    item.ledgerParentGuid ?? null,

ledger_parent_master_id:
    item.ledgerParentMasterId ?? null,

ledger_parent_alter_id:
    item.ledgerParentAlterId ?? null,    

party_name:
    item.partyName ?? null,

party_guid:
    item.partyGuid ?? null,

party_master_id:
    item.partyMasterId ?? null,

party_alter_id:
    item.partyAlterId ?? null,

party_parent_name:
    item.partyParentName ?? null,

party_parent_guid:
    item.partyParentGuid ?? null,

party_parent_master_id:
    item.partyParentMasterId ?? null,

party_parent_alter_id:
    item.partyParentAlterId ?? null,

                cgst_rate: cgstRate,

                sgst_rate: sgstRate,

                igst_rate: igstRate,


                gst_rate:
                    igstRate ??
                    ((cgstRate || 0) + (sgstRate || 0)),


               cgst_amount:
                    item.cgstAmount ?? 0,

                sgst_amount:
                    item.sgstAmount ?? 0,

                igst_amount:
                    item.igstAmount ?? 0,

                taxable_amount:
                    item.taxableAmount ?? 0,

              gst_rates: item.gstRates ?? [],

                costcentre_allocations:
                    item.costCentreAllocations ?? []

            });

        }

    }

    const incomingVoucherGuids = new Set(
    vouchers
        .map(v => v.header?.guid?.trim())
        .filter(Boolean)
);

for (const guid of existingVoucherMap.keys()) {

    if (!incomingVoucherGuids.has(guid)) {

        deletedVoucherGuids.push(guid);

    }

}


        let success = 0;

        let rowsToSave = [];

let voucherGuids = [];

let ledgerVoucherGuids = [];

let inventoryVoucherGuids = [];


fs.writeFileSync(
    path.join(__dirname, "voucher-sync-debug.json"),
    JSON.stringify(
        {
            generatedAt: new Date().toISOString(),

                processId: process.pid,

            incoming: vouchers.length,

            incomingGuids: vouchers.map(v => ({
                guid: v.header?.guid?.trim(),
                voucherNumber: v.header?.voucherNumber
              })),

            new: newVoucherRows,

            changed: changedVoucherRows,

            deleted: deletedVoucherGuids,

            unchanged: unchangedVoucherGuids,

            rowsToSave: rowsToSave.map(r => ({
                guid: r.guid,
                alterid: r.alterid,
                voucher_number: r.voucher_number
            })),

            deleteOperation: {
                voucherGuids: voucherGuids,
                totalVoucherGuids: voucherGuids.length
            },

            ledgerInsert: {
                totalRows: ledgerRows.length,
                voucherGuids: ledgerVoucherGuids
            },

            inventoryInsert: {
                totalRows: inventoryRows.length,
                voucherGuids: inventoryVoucherGuids
            }
        },
        null,
        2
    )
);

    if (voucherRows.length > 0) {

      rowsToSave = voucherRows.filter(
    row =>
        newVoucherRows.includes(row.guid) ||
        changedVoucherRows.includes(row.guid)
);

     voucherGuids = rowsToSave.map(
            row => row.guid
        );

     ledgerVoucherGuids = [
            ...new Set(ledgerRows.map(r => r.voucher_guid))
        ];

        inventoryVoucherGuids = [
            ...new Set(inventoryRows.map(r => r.voucher_guid))
        ];


const { error } = await supabase

    .from("tally_vouchers")

    .upsert(
        rowsToSave,
        {
            onConflict:
                "company_code,tally_owner,guid"
        }
    );

        if (error) {

            throw new Error(
                "Failed to save Vouchers: " +
                error.message
            );

        }

        success = rowsToSave.length;

   

        const { error: deleteLedgerError } =
            await supabase

                .from("tally_voucher_ledgers")

                .delete()

                .eq("company_code", company_code)

                .eq("tally_owner", tally_owner)

                .in("voucher_guid", voucherGuids);

        if (deleteLedgerError) {

            throw new Error(
                "Failed to delete Voucher Ledgers: " +
                deleteLedgerError.message
            );

        }

                if (ledgerRows.length > 0) {

                    fs.writeFileSync(
    "./ledgerRows-before-insert.json",
    JSON.stringify(
        ledgerRows,
        null,
        2
    )
);

            const { error: insertLedgerError } =
                await supabase

                    .from("tally_voucher_ledgers")

                    .insert(ledgerRows);

            if (insertLedgerError) {

                throw new Error(
                    "Failed to save Voucher Ledgers: " +
                    insertLedgerError.message
                );

            }

        }

                const { error: deleteInventoryError } =
            await supabase

                .from("tally_voucher_inventory")

                .delete()

                .eq("company_code", company_code)

                .eq("tally_owner", tally_owner)

                .in("voucher_guid", voucherGuids);

        if (deleteInventoryError) {

            throw new Error(
                "Failed to delete Voucher Inventory: " +
                deleteInventoryError.message
            );

        }

                if (inventoryRows.length > 0) {

            const { error: insertInventoryError } =
                await supabase

                    .from("tally_voucher_inventory")

                    .insert(inventoryRows);

            if (insertInventoryError) {

                throw new Error(
                    "Failed to save Voucher Inventory: " +
                    insertInventoryError.message
                );

            }

        }

    }

    if (deletedVoucherGuids.length > 0) {

    await supabase
        .from("tally_voucher_inventory")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("voucher_guid", deletedVoucherGuids);

    await supabase
        .from("tally_voucher_ledgers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("voucher_guid", deletedVoucherGuids);

    await supabase
        .from("tally_vouchers")
        .delete()
        .eq("company_code", company_code)
        .eq("tally_owner", tally_owner)
        .in("guid", deletedVoucherGuids);

}

    return {

    total: vouchers.length,

    success,

    failed: vouchers.length - success

};

}

module.exports = {

    saveVouchers

};