
const fs = require("fs");
const path = require("path");

const debugFile =
    "./logs/due-date-debug.jsonl";


function writeDueDebug(data) {

    fs.appendFileSync(
        debugFile,
        JSON.stringify(data, null, 2) + "\n\n"
    );

}



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

function getTransactionType({
    header,
    ledgers
}) {

    const voucherType =
        (header.voucherTypeName || "")
        .toUpperCase();


    if (
        voucherType.includes("CREDIT NOTE")
    ) {
        return "CREDIT_NOTE";
    }


    if (
        voucherType.includes("DEBIT NOTE")
    ) {
        return "DEBIT_NOTE";
    }


    const parents =
        (ledgers || []).map(l =>
            (l.ledgerParentName || "")
            .toUpperCase()
        );


    // Sales Account
    if (
        parents.some(p =>
            p.includes("SALES")
        )
    ) {
        return "SALE";
    }


    // Purchase Account
    if (
        parents.some(p =>
            p.includes("PURCHASE")
        )
    ) {
        return "PURCHASE";
    }


    // Expense Account
    if (
        parents.some(p =>
            p.includes("EXPENSE")
        )
    ) {
        return "EXPENSE";
    }


    return null;

}

function formatLocalDate(date){

    return `${date.getFullYear()}-${
        String(date.getMonth()+1).padStart(2,"0")
    }-${
        String(date.getDate()).padStart(2,"0")
    }`;

}

function parseTallyDate(value) {

    if (!value)
        return null;


    const str =
        String(value).trim();


    // YYYYMMDD format from Tally
    if (
        /^\d{8}$/.test(str)
    ) {

        return new Date(
            Number(str.substring(0, 4)),
            Number(str.substring(4, 6)) - 1,
            Number(str.substring(6, 8))
        );

    }


    // YYYY-MM-DD
    if (
        /^\d{4}-\d{2}-\d{2}$/.test(str)
    ) {

        return new Date(
            str + "T00:00:00"
        );

    }


    // 1-Sep-26 format
    const parts =
        str.split("-");


    if (parts.length === 3) {

        const day =
            parseInt(parts[0]);

        const month =
            new Date(
                `${parts[1]} 1, 2000`
            ).getMonth();

        let year =
            parseInt(parts[2]);


        if (year < 100)
            year += 2000;


        return new Date(
            year,
            month,
            day
        );

    }


    return null;

}


function calculateDueDate({ 
    baseDate,
    creditPeriod
}) {

    if (!baseDate || !creditPeriod)
        return {
            dueDate: null,
            creditDays: 0
        };


    const date =
        parseTallyDate(baseDate);


    writeDueDebug({
        step: "INPUT",
        baseDate,
        creditPeriod,
        parsedDate: date
    });


    if (!date)
        return {
            dueDate: null,
            creditDays: 0
        };


    const period =
        String(creditPeriod)
        .toUpperCase()
        .trim();


    if (period.includes("DAY")) {

        date.setDate(
            date.getDate() +
            (parseInt(period) || 0)
        );

    }
    else if (period.includes("MONTH")) {

        date.setMonth(
            date.getMonth() +
            (parseInt(period) || 0)
        );

    }
    else if (period.includes("YEAR")) {

        date.setFullYear(
            date.getFullYear() +
            (parseInt(period) || 0)
        );

    }
    else {

        const parts =
            creditPeriod.split("-");


        if (parts.length === 3) {

            const monthMap = {
                JAN:0,
                FEB:1,
                MAR:2,
                APR:3,
                MAY:4,
                JUN:5,
                JUL:6,
                AUG:7,
                SEP:8,
                OCT:9,
                NOV:10,
                DEC:11
            };


            const directDate =
                new Date(
                    2000 + Number(parts[2]),
                    monthMap[parts[1].toUpperCase()],
                    Number(parts[0])
                );

                directDate.setHours(12,0,0,0);

            const result = {

                dueDate:
                 formatLocalDate(directDate),

                creditDays:
                    Math.ceil(
                        (
                            directDate -
                            parseTallyDate(baseDate)
                        )
                        /
                        (1000 * 60 * 60 * 24)
                    )

            };


            writeDueDebug({
                step: "DIRECT_DATE_OUTPUT",
                result
            });


            return result;

        }


        return {
            dueDate: null,
            creditDays: 0
        };

    }


    const result = {

        dueDate:
          formatLocalDate(date),

        creditDays:
            Math.ceil(
                (
                    date -
                    parseTallyDate(baseDate)
                )
                /
                (1000 * 60 * 60 * 24)
            )

    };


    writeDueDebug({
        step: "FINAL_OUTPUT",
        result
    });


    return result;

}

function buildBillAllocationRows({

    voucher,

    company_code,

    tally_owner,
    
    country,
    
    ledgerMap

}) {

    const rows = [];

    const header = voucher.header || {};

      const enrichedLedgers =
            (voucher.ledgers || []).map(l => ({
                ...l,
                ledgerParentName:
                    ledgerMap?.get(l.ledgerGuid)?.parent || null
            }));

/*

    const baseDate =
    (
        transaction_type === "SALE"
    )
    ?
    header.referenceDate ||
    header.voucherDate
    :
    header.referenceDate ||
    header.voucherDate;


const {
    dueDate,
    creditDays
} =
calculateDueDate({
    baseDate,
    creditPeriod: bill.creditPeriod
});

*/

 const transactionType =
    getTransactionType({
        header,
        ledgers: enrichedLedgers
    });

    
    for (const ledger of enrichedLedgers) {

      

        

    for (const bill of (ledger.billAllocations || [])) {

     const baseDate =
     transactionType === "SALE"
        ?
        header.voucherDate
        :
        header.referenceDate ||
        header.voucherDate;


        const {
            dueDate,
            creditDays
        }
        =
        calculateDueDate({
            baseDate,
            creditPeriod: bill.creditPeriod
        });

        rows.push({

            voucher_guid:
                header.guid.trim(),

            ledger_guid:
                ledger.ledgerGuid ?? null,

            ledger_name:
                ledger.ledgerName?.trim() || null,

            company_code,

            tally_owner,

            voucher_number:
                header.voucherNumber ?? null,

            voucher_date:
                header.voucherDate || null,

            reference_date:
                header.referenceDate || null,

            document_date:
                header.referenceDate ||
                header.voucherDate ||
                null,

            bill_name:
                bill.billName ?? null,

            bill_type:
                bill.billType ?? null,

            bill_date:
                bill.billDate || null,

            amount:
                bill.amount ?? 0,

           due_date:
                dueDate,

            credit_days:
                creditDays,

            credit_period:
                bill.creditPeriod ?? null,

         /*   transaction_type:
                getTransactionType({
                    header,
                    ledgers: enrichedLedgers
                }),
                */

            transaction_type:
                transactionType,
                
            mode:
                bill.mode ?? null

        });

    }

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

    buildBillAllocationRows,

    buildInventoryRows,

    buildStockVoucherRows

};