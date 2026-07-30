function buildVoucherRow({

    header,

    company_code,

    tally_owner,

    sync_batch_id,

    now

}) {

    return {

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

    };

}


function buildLedgerRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    for (const ledger of (voucher.ledgers || [])) {

        rows.push({

            voucher_guid: header.guid.trim(),

            company_code,

            tally_owner,

            ledger_name:
                ledger.ledgerName?.trim() || null,

            ledger_masterid:
                ledger.ledgerMasterId ?? null,

            ledger_guid:
                ledger.ledgerGuid ?? null,

            ledger_alterid:
                ledger.ledgerAlterId ?? null,

            amount:
                ledger.amount ?? null,

            debit:
                ledger.debit ?? null,

            credit:
                ledger.credit ?? null,

            bill_allocations:
                ledger.billAllocations ?? [],

            costcentre_allocations:
                ledger.costCentreAllocations ?? []

        });

    }

    return rows;

}

function buildInventoryRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    const skipStockConsumption =
[
    "Multi Consumption Voucher View",
    "Consumption Voucher View"
].includes(header.persistedView);
    
if (skipStockConsumption) {
    return rows;
}
    for (const item of (voucher.inventory || [])) {

  

        if (
            item.inventoryNode !==
            "ALLINVENTORYENTRIES.LIST"
        ) {

            continue;

        }

        const gstRates = item.gstRates ?? [];

        const cgstRate =
            gstRates.find(r => r.dutyHead === "CGST")?.rate ?? null;

        const sgstRate =
            gstRates.find(r => r.dutyHead === "SGST/UTGST")?.rate ?? null;

        const igstRate =
            gstRates.find(r => r.dutyHead === "IGST")?.rate ?? null;

        const hsnCode =
            item.hsnCode || null;

        rows.push({

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

            hsn_code:
                hsnCode,

            discount:
                item.discount ?? null,

            godown:
                item.godown?.trim() || null,

            batch_id:
                item.batchId ?? null,

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

            party_name:
                item.partyName ?? null,

            party_guid:
                item.partyGuid ?? null,

            party_master_id:
                item.partyMasterId ?? null,

            party_alter_id:
                item.partyAlterId ?? null,

            cgst_rate:
                cgstRate,

            sgst_rate:
                sgstRate,

            igst_rate:
                igstRate,

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

            gst_rates:
                item.gstRates ?? [],

            costcentre_allocations:
                item.costCentreAllocations ?? []

        });

    }

    return rows;

}

function buildStockVoucherRows({

    voucher,

    company_code,

    tally_owner

}) {

    const rows = [];

    const header = voucher.header || {};

    for (const item of (voucher.inventory || [])) {

        if (
            item.inventoryNode ===
            "ALLINVENTORYENTRIES.LIST"
        ) {

            continue;

        }

        rows.push({

           
                   voucher_guid: header.guid.trim(),
           
                   company_code,
           
                   tally_owner,
           
                   stock_guid:
                       item.stockGuid ?? null,
           
                   stock_masterid:
                       item.stockMasterIdResolved ?? null,
           
                   stock_alterid:
                       item.stockAlterId ?? null,
           
                   stock_item:
                       item.stockItem?.trim() || null,
           
                   movement_type:
                       item.movementType ?? null,
           
                   actual_qty:
                       item.actualQty ?? null,
           
                   actual_qty_value:
                       item.actualQtyValue ?? null,
           
                   billed_qty:
                       item.billedQty ?? null,
           
                   billed_qty_value:
                       item.billedQtyValue ?? null,
           
                   unit:
                       item.unit?.trim() || null,
           
                   rate:
                       item.rate ?? null,
           
                   rate_value:
                       item.rateValue ?? null,
           
                   amount:
                       item.amount ?? null,
           
                   godown:
                       item.godown?.trim() || null,
           
                   batch_name:
                       item.batchName ?? null,
           
                   batch_id:
                       item.batchId ?? null,
           
                   inventory_node:
                       item.inventoryNode ?? null,
           
                   xml_payload:
                       item.raw ?? null,
           
                       voucher_type_name:
               header.voucherTypeName ?? null,
           
           voucher_type:
               header.voucherType ?? null,
           
           voucher_number:
               header.voucherNumber ?? null,
           
           voucher_date:
               header.voucherDate || null,
           
           effective_date:
               header.effectiveDate || null,
           
           reference:
               header.reference || null,
           
           narration:
               header.narration || null,
           
           party_ledger_name:
               header.partyLedger || null,
           
           party_gstin:
               header.gstin || null,
           
           place_of_supply:
               header.placeOfSupply || null,
           
           gst_registration_type:
               header.gstRegistrationType || null,
           
           persisted_view:
               header.persistedView || null,
           
           is_invoice:
               header.isInvoice === "Yes",
           
           is_cancelled:
               header.isCancelled === "Yes",
           
           is_optional:
               header.isOptional === "Yes",
           
           is_deleted:
               header.isDeleted === "Yes",
           
           ledger_name:
               item.ledgerName ?? null,
           
           discount:
               item.discount ?? null,
           
           additional_amount:
               item.additionalAmount ?? null,
           
           batch_rate:
               item.batchRate ?? null,
           
           batch_rate_value:
               item.batchRateValue ?? null,
           
           batch_amount:
               item.batchAmount ?? null,
           
               

        });

    }

    return rows;

}

module.exports = {

    buildVoucherRow,

    buildLedgerRows,

    buildInventoryRows,

    buildStockVoucherRows

};