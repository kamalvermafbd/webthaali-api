
-- Prevent duplicate active Tally ledgers within the same
-- company, Tally owner and ledger GUID.
--
-- Active means is_deleted IS DISTINCT FROM TRUE.
--
-- Local migration only.
-- Do not deploy until concurrency and compatibility
-- checks have been completed.

BEGIN;

DO $fn$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.tally_sync_ledgers
        WHERE is_deleted IS DISTINCT FROM TRUE
          AND guid IS NOT NULL
        GROUP BY company_code, tally_owner, guid
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'ACTIVE_TALLY_LEDGER_DUPLICATES_EXIST';
    END IF;
END;
$fn$;

CREATE UNIQUE INDEX
    tally_sync_ledgers_active_scope_guid_uidx
ON public.tally_sync_ledgers (
    company_code,
    tally_owner,
    guid
)
WHERE is_deleted IS DISTINCT FROM TRUE
  AND guid IS NOT NULL;

COMMIT;
