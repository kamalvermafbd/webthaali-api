
"use strict";

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const PAGE_SIZE = 500;

function toTallyDate(date) {
    if (typeof date !== "string" || !DATE.test(date)) {
        throw new Error("RECONCILE_VOUCHER_DATE_INVALID");
    }

    return date.replace(/-/g, "");
}

async function getReconciliationVoucherDateRange({
    supabase,
    companyCode,
    tallyOwner,
    voucherGuids
}) {
    if (
        !supabase ||
        typeof supabase.from !== "function" ||
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        !Array.isArray(voucherGuids) ||
        !voucherGuids.length ||
        voucherGuids.some(
            guid => typeof guid !== "string" || !guid.trim()
        )
    ) {
        throw new Error("RECONCILE_VOUCHER_DATE_INPUT_INVALID");
    }

    const uniqueGuids = [...new Set(voucherGuids)];
    let earliest = null;
    let latest = null;
    let matched = 0;

    for (let offset = 0; offset < uniqueGuids.length; offset += PAGE_SIZE) {
        const chunk = uniqueGuids.slice(offset, offset + PAGE_SIZE);

        const { data, error } = await supabase
            .from("tally_vouchers")
            .select("guid, voucher_date")
            .eq("company_code", companyCode.trim())
            .eq("tally_owner", tallyOwner)
            .in("guid", chunk);

        if (error) {
            throw new Error(
                "RECONCILE_VOUCHER_DATE_QUERY_FAILED",
                { cause: error }
            );
        }

        if (!Array.isArray(data)) {
            throw new Error("RECONCILE_VOUCHER_DATE_RESULT_INVALID");
        }

        const requested = new Set(chunk);
        const seen = new Set();

        for (const row of data) {
            if (
                !row ||
                !requested.has(row.guid) ||
                seen.has(row.guid) ||
                typeof row.voucher_date !== "string" ||
                !DATE.test(row.voucher_date)
            ) {
                throw new Error("RECONCILE_VOUCHER_DATE_DATA_INVALID");
            }

            seen.add(row.guid);
            matched++;

            if (!earliest || row.voucher_date < earliest) {
                earliest = row.voucher_date;
            }

            if (!latest || row.voucher_date > latest) {
                latest = row.voucher_date;
            }
        }

        if (seen.size !== requested.size) {
            throw new Error("RECONCILE_VOUCHER_DATE_COVERAGE_INCOMPLETE");
        }
    }

    if (matched !== uniqueGuids.length || !earliest || !latest) {
        throw new Error("RECONCILE_VOUCHER_DATE_COVERAGE_INCOMPLETE");
    }

    return {
        fromDate: toTallyDate(earliest),
        toDate: toTallyDate(latest),
        voucherCount: matched
    };
}

module.exports = { getReconciliationVoucherDateRange };
