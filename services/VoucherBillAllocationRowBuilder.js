const fs = require("fs");

const debugFile =
    "./logs/due-date-debug.jsonl";



function writeDueDebug(data) {

    fs.appendFileSync(
        debugFile,
        JSON.stringify(data, null, 2) + "\n\n"
    );

}

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

    if (
        parents.some(p =>
            p.includes("SALES")
        )
    ) {
        return "SALE";
    }

    if (
        parents.some(p =>
            p.includes("PURCHASE")
        )
    ) {
        return "PURCHASE";
    }

    if (
        parents.some(p =>
            p.includes("EXPENSE")
        )
    ) {
        return "EXPENSE";
    }

    return null;

}

function formatLocalDate(date) {

    return `${date.getFullYear()}-${
        String(date.getMonth() + 1)
            .padStart(2, "0")
    }-${
        String(date.getDate())
            .padStart(2, "0")
    }`;

}

function parseTallyDate(value) {

    if (!value)
        return null;

    const str =
        String(value).trim();

    if (
        /^\d{8}$/.test(str)
    ) {

        return new Date(
            Number(str.substring(0, 4)),
            Number(str.substring(4, 6)) - 1,
            Number(str.substring(6, 8))
        );

    }

    if (
        /^\d{4}-\d{2}-\d{2}$/.test(str)
    ) {

        return new Date(
            str + "T00:00:00"
        );

    }

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

                JAN: 0,
                FEB: 1,
                MAR: 2,
                APR: 3,
                MAY: 4,
                JUN: 5,
                JUL: 6,
                AUG: 7,
                SEP: 8,
                OCT: 9,
                NOV: 10,
                DEC: 11

            };

            const directDate =
                new Date(

                    2000 + Number(parts[2]),

                    monthMap[
                        parts[1].toUpperCase()
                    ],

                    Number(parts[0])

                );

            directDate.setHours(
                12, 0, 0, 0
            );

            const result = {

                dueDate:
                    formatLocalDate(
                        directDate
                    ),

                creditDays:
                    Math.ceil(

                        (

                            directDate -

                            parseTallyDate(
                                baseDate
                            )

                        ) /

                        (1000 * 60 * 60 * 24)

                    )

            };

            writeDueDebug({

                step:
                    "DIRECT_DATE_OUTPUT",

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

                    parseTallyDate(
                        baseDate
                    )

                ) /

                (1000 * 60 * 60 * 24)

            )

    };

    writeDueDebug({

        step:
            "FINAL_OUTPUT",

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

    const header =
        voucher.header || {};

    const enrichedLedgers =
        (voucher.ledgers || []).map(l => ({

            ...l,

            ledgerParentName:
                ledgerMap?.get(
                    l.ledgerGuid
                )?.parent || null

        }));

    const transactionType =
        getTransactionType({

            header,

            ledgers:
                enrichedLedgers

        });

    for (const ledger of enrichedLedgers) {

        for (const bill of (

            ledger.billAllocations || []

        )) {

            const baseDate =
                transactionType === "SALE"

                    ? header.voucherDate

                    : header.referenceDate ||

                    header.voucherDate;

            const {

                dueDate,

                creditDays

            } = calculateDueDate({

                baseDate,

                creditPeriod:
                    bill.creditPeriod

            });

            rows.push({

               voucher_guid:
                     safeTrim(header.guid),

                ledger_guid:
                    ledger.ledgerGuid ?? null,

                ledger_name:
                 safeTrim(ledger.ledgerName) || null,

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

                transaction_type:
                    transactionType,

                mode:
                    bill.mode ?? null

            });

        }

    }

    return rows;

}

module.exports = {

    buildBillAllocationRows

};