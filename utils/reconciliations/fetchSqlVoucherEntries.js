
"use strict";

const MAX_ID = 9223372036854775807n;
const MAX_PAISE = BigInt(Number.MAX_SAFE_INTEGER);

const GUID = /^[A-Za-z0-9][A-Za-z0-9-]{0,127}$/;

function parseId(value) {
    if (
        typeof value !== "string" ||
        !/^[1-9]\d*$/.test(value)
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_ID_INVALID"
        );
    }

    const id = BigInt(value);

    if (id > MAX_ID) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_ID_INVALID"
        );
    }

    return id;
}

function normalizeMoney(value, signed) {
    if (typeof value !== "string") {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_AMOUNT_INVALID"
        );
    }

    // PostgreSQL numeric::text may omit trailing zeroes.
    // Normalize without JavaScript floating-point conversion.
    const match =
        /^(-?)(0|[1-9]\d*)(?:\.(\d{1,2}))?$/.exec(value);

    if (
        !match ||
        (!signed && match[1] === "-")
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_AMOUNT_INVALID"
        );
    }

    const fraction =
        (match[3] || "").padEnd(2, "0");

    const paise =
        BigInt(match[2]) * 100n +
        BigInt(fraction);

    if (paise > MAX_PAISE) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_AMOUNT_INVALID"
        );
    }

    const sign = match[1] === "-" ? "-" : "";

    return `${sign}${paise / 100n}.${String(
        paise % 100n
    ).padStart(2, "0")}`;
}

function validateAllocations(value) {
    if (!Array.isArray(value)) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_ALLOCATIONS_INVALID"
        );
    }

    return value;
}

async function fetchSqlVoucherEntries({
    supabase,
    companyCode,
    tallyOwner,
    voucherGuids
}) {
    if (
        !supabase ||
        typeof supabase.rpc !== "function"
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_DEPENDENCY_INVALID"
        );
    }

    if (
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        companyCode !== companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        !Array.isArray(voucherGuids) ||
        voucherGuids.length === 0 ||
        voucherGuids.length > 1000
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_SQL_REQUEST_INVALID"
        );
    }

    const requested = new Set();
    const folded = new Set();

    for (const guid of voucherGuids) {
        if (
            typeof guid !== "string" ||
            !GUID.test(guid) ||
            folded.has(guid.toLowerCase())
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_SQL_GUID_INVALID"
            );
        }

        requested.add(guid);
        folded.add(guid.toLowerCase());
    }

    const pageSize = 500;
    const allRows = [];
    const seenIds = new Set();

    let afterId = 0n;

    while (true) {
        const { data, error } = await supabase.rpc(
            "get_manual_reconcile_voucher_entries_page_v1",
            {
                p_company_code: companyCode,
                p_tally_owner: tallyOwner,
                p_voucher_guids: voucherGuids,
                p_after_entry_id: afterId.toString(),
                p_page_size: pageSize
            }
        );

        if (error) {
            throw error;
        }

        if (
            !Array.isArray(data) ||
            data.length > pageSize
        ) {
            throw new Error(
                "RECONCILE_VOUCHER_SQL_PAGE_INVALID"
            );
        }

        for (const row of data) {
            if (
                !row ||
                typeof row !== "object" ||
                Array.isArray(row)
            ) {
                throw new Error(
                    "RECONCILE_VOUCHER_SQL_ROW_INVALID"
                );
            }

            const id = parseId(row.id);

            if (
                id <= afterId ||
                seenIds.has(row.id)
            ) {
                throw new Error(
                    "RECONCILE_VOUCHER_SQL_PAGINATION_INVALID"
                );
            }

            if (
                row.company_code !== companyCode ||
                row.tally_owner !== tallyOwner ||
                !requested.has(row.voucher_guid) ||
                typeof row.ledger_guid !== "string" ||
                !GUID.test(row.ledger_guid)
            ) {
                throw new Error(
                    "RECONCILE_VOUCHER_SQL_SCOPE_INVALID"
                );
            }

            // Deleted or unknown-state entries are unsafe.
            if (row.is_deleted !== false) {
                throw new Error(
                    "RECONCILE_VOUCHER_SQL_DELETED_STATE_UNSAFE"
                );
            }

            // Preserve allocation evidence for later classification.
            const billAllocations =
                validateAllocations(
                    row.bill_allocations
                );

            const costcentreAllocations =
                validateAllocations(
                    row.costcentre_allocations
                );

            const amount = normalizeMoney(
                row.amount,
                true
            );

            const debit = normalizeMoney(
                row.debit,
                false
            );

            const credit = normalizeMoney(
                row.credit,
                false
            );

            const amountPaise = BigInt(
                amount
                    .replace(/^-/, "")
                    .replace(".", "")
            );

            const debitPaise = BigInt(
                debit.replace(".", "")
            );

            const creditPaise = BigInt(
                credit.replace(".", "")
            );

            if (
                (
                    debitPaise > 0n &&
                    creditPaise > 0n
                ) ||
                (
    amountPaise !== (debitPaise + creditPaise)
)
            ) {
                throw new Error(
                    "RECONCILE_VOUCHER_SQL_AMOUNT_INCONSISTENT"
                );
            }

            seenIds.add(row.id);
            afterId = id;

            allRows.push({
                id: row.id,
                company_code: row.company_code,
                tally_owner: row.tally_owner,
                voucher_guid: row.voucher_guid,
                ledger_guid: row.ledger_guid,
                amount,
                debit,
                credit,
                is_deleted: row.is_deleted,
                bill_allocations: billAllocations,
                costcentre_allocations:
                    costcentreAllocations,
                requiresStructuralReview:
                    billAllocations.length > 0 ||
                    costcentreAllocations.length > 0
            });
        }

        if (data.length < pageSize) {
            break;
        }
    }

    return allRows;
}

module.exports = {
    fetchSqlVoucherEntries
};
