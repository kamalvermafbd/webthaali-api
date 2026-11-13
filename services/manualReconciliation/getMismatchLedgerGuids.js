"use strict";

const PAGE_SIZE = 500;

async function getMismatchLedgerGuids({
    supabase,
    companyCode,
    tallyOwner
}) {
    if (!supabase || typeof supabase.from !== "function") {
        throw new Error("RECONCILE_SUPABASE_INVALID");
    }

    if (
        typeof companyCode !== "string" ||
        !companyCode.trim()
    ) {
        throw new Error("RECONCILE_COMPANY_CODE_INVALID");
    }

    if (!["USER", "CA"].includes(tallyOwner)) {
        throw new Error("RECONCILE_TALLY_OWNER_INVALID");
    }

    const ledgerGuids = [];
    const seen = new Set();

    let lastId = null;

    while (true) {
               let query = supabase
            .from("balance_difference_view")
            .select("id, guid")
            .eq("company_code", companyCode.trim())
            .eq("tally_owner", tallyOwner)
            .order("id", { ascending: true });

        if (lastId !== null) {
            query = query.gt("id", lastId);
        }

        const { data, error } = await query.limit(PAGE_SIZE);

        if (error) {
            throw error;
        }

        if (!Array.isArray(data)) {
            throw new Error(
                "RECONCILE_MISMATCH_QUERY_RESULT_INVALID"
            );
        }

                for (const row of data) {
            if (
                !row ||
                typeof row.guid !== "string" ||
                !row.guid.trim() ||
              (
    typeof row.id !== "number" &&
    typeof row.id !== "string"
) ||
(
    typeof row.id === "number" &&
    !Number.isSafeInteger(row.id)
) ||
!/^[0-9]+$/.test(String(row.id))
            ) {
                throw new Error(
                    "RECONCILE_MISMATCH_LEDGER_ROW_INVALID"
                );
            }

            const id = String(row.id);

            if (
                lastId !== null &&
                BigInt(id) <= BigInt(lastId)
            ) {
                throw new Error(
                    "RECONCILE_MISMATCH_PAGINATION_ORDER_INVALID"
                );
            }

            lastId = id;

            const guid = row.guid.trim();

            if (seen.has(guid)) {
                throw new Error(
                    "RECONCILE_DUPLICATE_MISMATCH_LEDGER_GUID"
                );
            }

            seen.add(guid);
            ledgerGuids.push(guid);
        }

        if (data.length < PAGE_SIZE) {
            break;
        }
    }

    return ledgerGuids;
}

module.exports = {
    getMismatchLedgerGuids
};