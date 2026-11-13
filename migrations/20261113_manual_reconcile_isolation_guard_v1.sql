-- DRAFT ONLY. Staging regression/concurrency tests required before deployment.
-- Forward-only isolation guard, to run AFTER the checkpoint and historical
-- finalization drafts. Do not alter the historical migrations.
-- Reject REPEATABLE READ and SERIALIZABLE transactions because the
-- journal-seal protocol relies on READ COMMITTED statement snapshots.
BEGIN;

CREATE OR REPLACE FUNCTION public.record_manual_reconcile_pre_release_checkpoint_v1(
    p_attempt_id uuid,
    p_operation_token uuid,
    p_execution_id uuid,
    p_connector_id text,
    p_owner_token uuid,
    p_remaining_mismatch_ledger_guids jsonb
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO '' AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_existing public.manual_reconcile_pre_release_checkpoints%ROWTYPE;
    v_item jsonb;
BEGIN
    -- Row-lock serialization is only proven under READ COMMITTED.
    -- Reject stale-snapshot isolation before any checkpoint/journal reads.
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'RECONCILE_REQUIRES_READ_COMMITTED'
            USING ERRCODE = '25000';
    END IF;

    IF p_attempt_id IS NULL OR p_operation_token IS NULL
       OR p_execution_id IS NULL OR p_owner_token IS NULL
       OR NULLIF(pg_catalog.btrim(p_connector_id), '') IS NULL
       OR pg_catalog.jsonb_typeof(p_remaining_mismatch_ledger_guids)
          IS DISTINCT FROM 'array'
    THEN
        RAISE EXCEPTION 'INVALID_PRE_RELEASE_CHECKPOINT';
    END IF;

    -- Existing exact checkpoint replay is evidence inspection only.
    -- It must not authorize a new operation or a fresh checkpoint.
    SELECT c.* INTO v_existing
    FROM public.manual_reconcile_pre_release_checkpoints AS c
    WHERE c.attempt_id = p_attempt_id;
    IF FOUND THEN
        IF v_existing.operation_token IS NOT DISTINCT FROM p_operation_token
           AND v_existing.execution_id IS NOT DISTINCT FROM p_execution_id
           AND v_existing.connector_id IS NOT DISTINCT FROM p_connector_id
           AND v_existing.owner_token IS NOT DISTINCT FROM p_owner_token
           AND v_existing.remaining_mismatch_ledger_guids
               IS NOT DISTINCT FROM p_remaining_mismatch_ledger_guids
           AND v_existing.final_recheck_completed IS TRUE
        THEN
            RETURN true;
        END IF;
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_CONFLICT';
    END IF;

    -- Same advisory + reservation lock ordering as execution-fenced RPCs.
    -- This also verifies the CURRENT RUNNING reservation and token.
    SELECT e.* INTO v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id, p_operation_token, p_execution_id
    ) AS e;

    IF v_execution.connector_id IS DISTINCT FROM p_connector_id
       OR v_execution.owner_token IS DISTINCT FROM p_owner_token
    THEN
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_EXECUTION_MISMATCH';
    END IF;

    -- Recheck after acquiring the reservation lock, so a concurrent
    -- checkpoint cannot be inserted between the first read and seal.
    SELECT c.* INTO v_existing
    FROM public.manual_reconcile_pre_release_checkpoints AS c
    WHERE c.attempt_id = p_attempt_id;
    IF FOUND THEN
        IF v_existing.operation_token IS NOT DISTINCT FROM p_operation_token
           AND v_existing.execution_id IS NOT DISTINCT FROM p_execution_id
           AND v_existing.connector_id IS NOT DISTINCT FROM p_connector_id
           AND v_existing.owner_token IS NOT DISTINCT FROM p_owner_token
           AND v_existing.remaining_mismatch_ledger_guids
               IS NOT DISTINCT FROM p_remaining_mismatch_ledger_guids
           AND v_existing.final_recheck_completed IS TRUE
        THEN
            RETURN true;
        END IF;
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_CONFLICT';
    END IF;

    -- The caller reports the final mismatch recheck. Validate its shape;
    -- SQL cannot independently prove that the JS recheck occurred.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            p_remaining_mismatch_ledger_guids
        ) AS x(value)
        WHERE pg_catalog.jsonb_typeof(x.value) IS DISTINCT FROM 'string'
           OR NULLIF(pg_catalog.btrim(x.value #>> '{}'), '') IS NULL
    ) OR (
        SELECT pg_catalog.count(*) <> pg_catalog.count(DISTINCT x.value)
        FROM pg_catalog.jsonb_array_elements(
            p_remaining_mismatch_ledger_guids
        ) AS x(value)
    ) THEN
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_MISMATCH_LIST_INVALID';
    END IF;

    -- Every journal entry belongs to this execution and is verified.
    IF EXISTS (
        SELECT 1 FROM public.manual_reconcile_operations AS o
        WHERE o.attempt_id = p_attempt_id
          AND (
              o.reservation_id IS DISTINCT FROM v_execution.reservation_id
              OR o.operation_token IS DISTINCT FROM p_operation_token
              OR o.execution_id IS DISTINCT FROM p_execution_id
              OR o.state IS DISTINCT FROM 'RESULT_VERIFIED'
              OR o.operation_type NOT IN ('TALLY_FETCH', 'LEDGER_APPLY')
          )
    ) THEN
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_JOURNAL_NOT_VERIFIED';
    END IF;

    -- Exactly one successful, verified apply per verified fetch.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations AS f
        WHERE f.attempt_id = p_attempt_id
          AND f.operation_type = 'TALLY_FETCH'
          AND (
              SELECT pg_catalog.count(*)
              FROM public.manual_reconcile_operations AS a
              WHERE a.attempt_id = f.attempt_id
                AND a.reservation_id = f.reservation_id
                AND a.operation_token = f.operation_token
                AND a.execution_id = f.execution_id
                AND a.source_fetch_operation_id = f.operation_id
                AND a.operation_type = 'LEDGER_APPLY'
                AND a.ledger_guid IS NOT DISTINCT FROM f.ledger_guid
                AND a.input_payload IS NOT DISTINCT FROM f.result_payload
                AND a.state = 'RESULT_VERIFIED'
                AND a.result_payload->>'result' IN ('UPDATED', 'UNCHANGED')
          ) <> 1
    ) THEN
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_FETCH_APPLY_MISMATCH';
    END IF;

    -- No orphan apply, failed apply, or apply linked to a different fetch.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations AS a
        WHERE a.attempt_id = p_attempt_id
          AND a.operation_type = 'LEDGER_APPLY'
          AND (
              a.result_payload->>'result' NOT IN ('UPDATED', 'UNCHANGED')
              OR a.result_payload->>'result' IS NULL
              OR NOT EXISTS (
                  SELECT 1 FROM public.manual_reconcile_operations AS f
                  WHERE f.operation_id = a.source_fetch_operation_id
                    AND f.attempt_id = a.attempt_id
                    AND f.reservation_id = a.reservation_id
                    AND f.operation_token = a.operation_token
                    AND f.execution_id = a.execution_id
                    AND f.operation_type = 'TALLY_FETCH'
                    AND f.state = 'RESULT_VERIFIED'
                    AND f.ledger_guid IS NOT DISTINCT FROM a.ledger_guid
                    AND f.result_payload IS NOT DISTINCT FROM a.input_payload
              )
          )
    ) THEN
        RAISE EXCEPTION 'RECONCILE_CHECKPOINT_APPLY_EVIDENCE_INVALID';
    END IF;

    INSERT INTO public.manual_reconcile_pre_release_checkpoints (
        attempt_id, reservation_id, operation_token, execution_id,
        connector_id, owner_token, remaining_mismatch_ledger_guids,
        final_recheck_completed
    ) VALUES (
        p_attempt_id, v_execution.reservation_id, p_operation_token,
        p_execution_id, p_connector_id, p_owner_token,
        p_remaining_mismatch_ledger_guids, true
    );
    RETURN true;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.guard_manual_reconcile_journal_seal_v1()
RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO '' AS $fn$
DECLARE
    v_attempt_id uuid;
BEGIN
    -- Row-lock serialization is only proven under READ COMMITTED.
    -- Reject stale-snapshot isolation before any checkpoint/journal reads.
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'RECONCILE_REQUIRES_READ_COMMITTED'
            USING ERRCODE = '25000';
    END IF;

    v_attempt_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.attempt_id ELSE NEW.attempt_id END;

    IF TG_OP = 'UPDATE' AND (
        NEW.operation_id IS DISTINCT FROM OLD.operation_id OR
        NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR
        NEW.reservation_id IS DISTINCT FROM OLD.reservation_id OR
        NEW.operation_token IS DISTINCT FROM OLD.operation_token
    ) THEN
        RAISE EXCEPTION 'RECONCILE_JOURNAL_IDENTITY_IMMUTABLE';
    END IF;

    PERFORM 1
    FROM public.manual_reconcile_status s
    JOIN public.manual_reconcile_attempts a ON a.reservation_id = s.id
    WHERE a.attempt_id = v_attempt_id
    FOR UPDATE OF s;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'RECONCILE_JOURNAL_ATTEMPT_NOT_FOUND';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.manual_reconcile_pre_release_checkpoints c
        WHERE c.attempt_id = v_attempt_id
    ) OR EXISTS (
        SELECT 1 FROM public.manual_reconcile_finalizations f
        WHERE f.attempt_id = v_attempt_id
    ) THEN
        RAISE EXCEPTION 'RECONCILE_JOURNAL_SEALED';
    END IF;

    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$fn$;

-- CREATE OR REPLACE preserves existing function grants.
-- Explicitly retain expected function ownership; no new EXECUTE grants.
ALTER FUNCTION public.record_manual_reconcile_pre_release_checkpoint_v1(uuid,uuid,uuid,text,uuid,jsonb) OWNER TO postgres;
ALTER FUNCTION public.guard_manual_reconcile_journal_seal_v1() OWNER TO postgres;
COMMIT;
