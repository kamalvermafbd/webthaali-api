const TABLES = {
    groups: "tally_sync_groups",
    stockGroups: "tally_sync_stock_groups",
    ledgers: "tally_sync_ledgers",
    stocks: "tally_sync_stocks",
    units: "tally_sync_units",
    godowns: "tally_sync_godowns",
    costCentres: "tally_sync_cost_centres",

    vouchers: "tally_vouchers",
    voucherLedgers: "tally_voucher_ledgers",
    voucherInventory: "tally_voucher_inventory",
    voucherInventoryGodowns: "tally_voucher_inventory_godowns",
    stockVouchers: "tally_stock_vouchers",
    billAllocations: "tally_bill_allocations",
    costCentreAllocations: "tally_costcentre_allocations",

    openingBalances: "opening_balance_allocations",
    stockOpeningBalances: "tally_stock_opening_balances"
};

const TABLE_LIST = Object.keys(TABLES);

// Tables whose data is linked to vouchers
const VOUCHER_CHILD_TABLES = new Set([
    "voucherLedgers",
    "voucherInventory",
    "voucherInventoryGodowns",
    "stockVouchers",
    "billAllocations",
    "costCentreAllocations"
]);

class LocalCacheSyncService {

    constructor(supabase) {
        this.supabase = supabase;
    }

    // =====================================================
    // VALIDATE SCOPE
    // =====================================================

    validateScope(company_code, tally_owner) {

        if (!company_code) {
            throw new Error("company_code is required");
        }

        const owner = String(tally_owner || "")
            .trim()
            .toUpperCase();

        if (owner !== "CA" && owner !== "USER") {
            throw new Error(
                "tally_owner must be CA or USER"
            );
        }

        return owner;
    }

    // =====================================================
    // GET LATEST CLOSED BATCH
    // =====================================================

    async getLatestClosedBatch({
        company_code,
        tally_owner
    }) {

        const owner = this.validateScope(
            company_code,
            tally_owner
        );

        const {
            data,
            error
        } = await this.supabase
            .from("sync_batches")
            .select(`
                id,
                batch_id,
                company_code,
                tally_owner,
                batch_status,
                batch_closed,
                batch_type,
                sync_mode,
                sync_period,
                from_alterid,
                to_alterid,
                completed_at,
                created_at,
                ledger_reconciliation_completed,
                stock_reconciliation_completed
            `)
            .eq(
                "company_code",
                company_code
            )
            .eq(
                "tally_owner",
                owner
            )
            .eq(
                "batch_status",
                "CLOSED"
            )
            .eq(
                "batch_closed",
                true
            )
            .order(
                "completed_at",
                {
                    ascending: false
                }
            )
            .limit(1)
            .maybeSingle();

        if (error) {
            throw error;
        }

        return data || null;
    }

    // =====================================================
    // CACHE STATUS
    // =====================================================

    async getCacheStatus({
        company_code,
        tally_owner
    }) {

        const owner = this.validateScope(
            company_code,
            tally_owner
        );

        const latestBatch =
            await this.getLatestClosedBatch({
                company_code,
                tally_owner: owner
            });

        return {
            company_code,
            tally_owner: owner,

            latest_closed_batch:
                latestBatch?.batch_id || null,

            latest_closed_at:
                latestBatch?.completed_at || null,

            cache_sync_available:
                Boolean(latestBatch),

            tables: TABLE_LIST
        };
    }

    // =====================================================
    // GET TABLE COUNT
    // =====================================================
async getTableCount({
    tableKey,
    company_code,
    tally_owner
}) {

    const owner = this.validateScope(
        company_code,
        tally_owner
    );

    const tableName =
        TABLES[tableKey];

    if (!tableName) {
        throw new Error(
            `Invalid cache table: ${tableKey}`
        );
    }

    let query = this.supabase
        .from(tableName)
        .select("*", {
            count: "exact",
            head: true
        })
        .eq(
            "company_code",
            company_code
        )
        .eq(
            "tally_owner",
            owner
        );

    // Normal Tally tables have is_deleted.
    // Opening balance tables do not.
    if (
        tableKey !== "openingBalances" &&
        tableKey !== "stockOpeningBalances"
    ) {
        query = query.eq(
            "is_deleted",
            false
        );
    }

    const {
        count,
        error
    } = await query;

    if (error) {
        throw error;
    }

    return count || 0;
}
    // =====================================================
    // GET MASTER / OPENING BALANCE CHUNK
    // =====================================================

    async getTableChunk({
        tableKey,
        company_code,
        tally_owner,
        page = 0,
        chunkSize = 500
    }) {

        const owner = this.validateScope(
            company_code,
            tally_owner
        );

        const tableName =
            TABLES[tableKey];

        if (!tableName) {
            throw new Error(
                `Invalid cache table: ${tableKey}`
            );
        }

        if (VOUCHER_CHILD_TABLES.has(tableKey)) {

            throw new Error(
                `${tableKey} must be fetched using voucher GUIDs`
            );
        }

        const safePage =
            Math.max(
                0,
                Number(page) || 0
            );

        const safeChunkSize =
            Math.min(
                500,
                Math.max(
                    1,
                    Number(chunkSize) || 500
                )
            );

        const from =
            safePage * safeChunkSize;

        const to =
            from + safeChunkSize - 1;

        const {
            data,
            error
        } = await this.supabase
            .from(tableName)
            .select("*")
            .eq(
                "company_code",
                company_code
            )
            .eq(
                "tally_owner",
                owner
            )
            .eq("is_deleted", false)
            .order(
                "id",
                {
                    ascending: true
                }
            )
            .range(
                from,
                to
            );

        if (error) {
            throw error;
        }

        return {
            table: tableKey,
            page: safePage,
            chunk_size: safeChunkSize,
            rows: data || [],
            has_more:
                (data || []).length === safeChunkSize
        };
    }

    // =====================================================
    // GET VOUCHER CHUNK
    // =====================================================

    async getVoucherChunk({
        company_code,
        tally_owner,
        page = 0,
        chunkSize = 500,
        from_date = null,
        to_date = null
    }) {

        const owner = this.validateScope(
            company_code,
            tally_owner
        );

        const safePage =
            Math.max(
                0,
                Number(page) || 0
            );

        const safeChunkSize =
            Math.min(
                500,
                Math.max(
                    1,
                    Number(chunkSize) || 500
                )
            );

        const from =
            safePage * safeChunkSize;

        const to =
            from + safeChunkSize - 1;

        let query = this.supabase
            .from("tally_vouchers")
            .select("*")
            .eq(
                "company_code",
                company_code
            )
            .eq(
                "tally_owner",
                owner
            )
            .eq(
                "is_deleted",
                false
            );

        // Initial cache can optionally be limited
        // to a date range.
        if (from_date) {
            query = query.gte(
                "voucher_date",
                from_date
            );
        }

        if (to_date) {
            query = query.lte(
                "voucher_date",
                to_date
            );
        }

        const {
            data,
            error
        } = await query
            .order(
                "voucher_date",
                {
                    ascending: true
                }
            )
            .order(
                "id",
                {
                    ascending: true
                }
            )
            .range(
                from,
                to
            );

        if (error) {
            throw error;
        }

        return {
            table: "vouchers",
            page: safePage,
            chunk_size: safeChunkSize,
            rows: data || [],
            has_more:
                (data || []).length === safeChunkSize
        };
    }

    // =====================================================
    // GET CHILD DATA FOR VOUCHERS
    // =====================================================

    async getVoucherChildren({
        tableKey,
        company_code,
        tally_owner,
        voucher_guids
    }) {

        const owner = this.validateScope(
            company_code,
            tally_owner
        );

        const tableName =
            TABLES[tableKey];

        if (!VOUCHER_CHILD_TABLES.has(tableKey)) {

            throw new Error(
                `${tableKey} is not a voucher child table`
            );
        }

        if (
            !Array.isArray(voucher_guids) ||
            voucher_guids.length === 0
        ) {
            return [];
        }

        const {
            data,
            error
        } = await this.supabase
            .from(tableName)
            .select("*")
            .eq(
                "company_code",
                company_code
            )
            .eq(
                "tally_owner",
                owner
            )
            .in(
                "voucher_guid",
                voucher_guids
            );

        if (error) {
            throw error;
        }

        return data || [];
    }

    
// =====================================================
// GET INITIAL CACHE DATA
// =====================================================

async getInitialCacheData({
    company_code,
    tally_owner,
    tableKey,
    page = 0,
    chunkSize = 500
}) {

    const owner = this.validateScope(
        company_code,
        tally_owner
    );

    const safeChunkSize = Math.min(
        500,
        Math.max(
            1,
            Number(chunkSize) || 500
        )
    );

    const safePage = Math.max(
        0,
        Number(page) || 0
    );

    // Voucher table has its own date-aware method
    if (tableKey === "vouchers") {
        return this.getVoucherChunk({
            company_code,
            tally_owner: owner,
            page: safePage,
            chunkSize: safeChunkSize
        });
    }

    // Voucher children must NOT be loaded independently
    // They will be loaded using voucher GUIDs.
    if (VOUCHER_CHILD_TABLES.has(tableKey)) {
        throw new Error(
            `${tableKey} must be fetched using voucher GUIDs`
        );
    }

    return this.getTableChunk({
        tableKey,
        company_code,
        tally_owner: owner,
        page: safePage,
        chunkSize: safeChunkSize
    });
}

}

module.exports = {
    LocalCacheSyncService,
    TABLES,
    TABLE_LIST
};