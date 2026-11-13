
-- ============================================================
-- Manual Reconciliation V2 - Journal Sealing Guard
--
-- Local migration only. DO NOT DEPLOY YET.
--
-- Depends on:
--   manual_reconcile_attempts_v2
--   manual_reconcile_operations_v1
--   manual_reconcile_finalization_v1
--
-- Prevents journal mutation once finalization evidence
-- has been recorded for an attempt.
--
-- Existing normal Tally sync tables are not modified.
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Guard operation journal mutations
-- ============================================================

CREATE FUNCTION public.guard_manual_reconcile_journal_seal_v1()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt_id uuid;
    v_finalization_state text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_attempt_id := OLD.attempt_id;
    ELSE
        v_attempt_id := NEW.attempt_id;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.operation_id IS DISTINCT FROM OLD.operation_id
           OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
           OR NEW.reservation_id IS DISTINCT FROM OLD.reservation_id
           OR NEW.operation_token IS DISTINCT FROM OLD.operation_token
        THEN
            RAISE EXCEPTION
                'RECONCILE_JOURNAL_IDENTITY_IMMUTABLE';
        END IF;
    END IF;

    -- Serialize journal mutation against finalization.
    PERFORM 1
    FROM public.manual_reconcile_status s
    JOIN public.manual_reconcile_attempts a
      ON a.reservation_id = s.id
    WHERE a.attempt_id = v_attempt_id
    FOR UPDATE OF s;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'RECONCILE_JOURNAL_ATTEMPT_NOT_FOUND';
    END IF;

    SELECT f.state
    INTO v_finalization_state
    FROM public.manual_reconcile_finalizations f
    WHERE f.attempt_id = v_attempt_id;

    IF FOUND THEN
        RAISE EXCEPTION
            'RECONCILE_JOURNAL_SEALED: %',
            v_finalization_state;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$fn$;

-- ============================================================
-- 2. Attach guard to journal table
-- ============================================================

CREATE TRIGGER guard_manual_reconcile_journal_seal
BEFORE INSERT OR UPDATE OR DELETE
ON public.manual_reconcile_operations
FOR EACH ROW
EXECUTE FUNCTION public.guard_manual_reconcile_journal_seal_v1();

-- ============================================================
-- 3. Permissions
-- ============================================================

ALTER FUNCTION public.guard_manual_reconcile_journal_seal_v1()
OWNER TO postgres;

REVOKE ALL ON FUNCTION
public.guard_manual_reconcile_journal_seal_v1()
FROM PUBLIC, anon, authenticated, service_role;

COMMIT;
