
"use strict";

/**
 * Get voucher GUIDs associated with selected ledger GUIDs.
 *
 * READ ONLY.
 * No INSERT / UPDATE / DELETE.
 *
 * Scope:
 * company_code + tally_owner + ledger_guid
 */

const PAGE_SIZE = 1000;
const GUID_CHUNK_SIZE = 100;

async function getReconciliationVoucherGuids({
    supabase,
    companyCode,
    tallyOwner,
    ledgerGuids
}) {
    if (
        !supabase ||
        typeof supabase.from !== "function"
    ) {
        throw new Error(
            "RECONCILE_SUPABASE_CLIENT_INVALID"
        );
    }

    if (
        typeof companyCode !== "string" ||
        !companyCode.trim() ||
        !["USER", "CA"].includes(tallyOwner) ||
        !Array.isArray(ledgerGuids)
    ) {
        throw new Error(
            "RECONCILE_VOUCHER_SELECTION_INVALID"
        );
    }

    const uniqueLedgerGuids = [
        ...new Set(
            ledgerGuids
                .filter(
                    guid =>
                        typeof guid === "string" &&
                        guid.trim()
                )
                .map(guid => guid.trim())
        )
    ];

    if (uniqueLedgerGuids.length === 0) {
        return [];
    }

    const voucherGuids = new Set();

    for (
        let offset = 0;
        offset < uniqueLedgerGuids.length;
        offset += GUID_CHUNK_SIZE
    ) {
        const chunk = uniqueLedgerGuids.slice(
            offset,
            offset + GUID_CHUNK_SIZE
        );

        let page = 0;

        while (true) {
            const from = page * PAGE_SIZE;
            const to = from + PAGE_SIZE - 1;

            const { data, error } = await supabase
                .from("tally_voucher_ledgers")
                .select("voucher_guid")
                .eq("company_code", companyCode.trim())
                .eq("tally_owner", tallyOwner)
                .in("ledger_guid", chunk)
                .order("voucher_guid", {
                    ascending: true
                })
                .range(from, to);

            if (error) {
                throw new Error(
                    "RECONCILE_VOUCHER_GUID_QUERY_FAILED: " +
                    error.message
                );
            }

            if (!Array.isArray(data)) {
                throw new Error(
                    "RECONCILE_VOUCHER_GUID_RESPONSE_INVALID"
                );
            }

            for (const row of data) {
                const guid =
                    typeof row.voucher_guid === "string"
                        ? row.voucher_guid.trim()
                        : "";

                if (guid) {
                    voucherGuids.add(guid);
                }
            }

            if (data.length < PAGE_SIZE) {
                break;
            }

            page++;
        }
    }

    return [...voucherGuids].sort();
}

module.exports = {
    getReconciliationVoucherGuids
};
