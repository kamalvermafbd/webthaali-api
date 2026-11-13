
BEGIN;

REVOKE EXECUTE ON FUNCTION
public.apply_manual_reconcile_ledger_opening(
    text,
    text,
    uuid,
    text,
    text,
    text
)
FROM PUBLIC, anon, authenticated, service_role;

COMMIT;
