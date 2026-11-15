
-- ============================================================
-- Manual Reconciliation V2 - Finalization
--
-- Local draft migration. DO NOT DEPLOY YET.
--
-- Dependencies:
--   manual_reconcile_attempts_v2
--   manual_reconcile_operations_v1
--   manual_reconcile_verified_result_v1
--   manual_reconcile_atomic_apply_v2
--
-- This migration:
--   1. Stores final recheck and release evidence.
--   2. Verifies all fetch/apply journal operations.
--   3. Rejects uncertain or incomplete operations.
--   4. Atomically seals evidence and closes reservation.
--
-- Important:
--   Release evidence is an assertion from trusted server code.
--   PostgreSQL cannot independently verify a Redis release.
--   The JavaScript coordinator must be wired and tested.
--
-- Existing close_manual_reconcile remains unchanged.
-- ============================================================

BEGIN;
-- ============================================================
-- Shared validator for final reconciliation outcome
-- ============================================================

CREATE FUNCTION public.manual_reconcile_final_outcome_v1(
    p_guids jsonb
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
SECURITY INVOKER
SET search_path TO ''
AS $fn$
BEGIN
    IF p_guids IS NULL
       OR pg_catalog.jsonb_typeof(p_guids)
          IS DISTINCT FROM 'array'
    THEN
        RETURN NULL;
    END IF;

    -- Reject non-string, empty and whitespace-only GUIDs.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(p_guids)
            AS e(value)
            WHERE pg_catalog.jsonb_typeof(e.value)
              IS DISTINCT FROM 'string'
           OR (e.value #>> '{}') ~ '^[[:space:]]*$'
    ) THEN
        RETURN NULL;
    END IF;

    -- Reject exact duplicate GUID strings.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(p_guids)
            AS e(value)
        GROUP BY (e.value #>> '{}') COLLATE "C"
        HAVING count(*) > 1
    ) THEN
        RETURN NULL;
    END IF;

    RETURN CASE
        WHEN pg_catalog.jsonb_array_length(p_guids) = 0
            THEN 'RECONCILED'
        ELSE 'TRANSACTION_INVESTIGATION_REQUIRED'
    END;
END;
$fn$;

ALTER FUNCTION
    public.manual_reconcile_final_outcome_v1(jsonb)
OWNER TO postgres;
-- ============================================================
-- 1. Finalization evidence table
-- ============================================================

CREATE TABLE public.manual_reconcile_finalizations (
    attempt_id uuid PRIMARY KEY
        REFERENCES public.manual_reconcile_attempts(attempt_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    reservation_id bigint NOT NULL
        REFERENCES public.manual_reconcile_status(id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    operation_token uuid NOT NULL,

    remaining_mismatch_ledger_guids jsonb NOT NULL,

    outcome text GENERATED ALWAYS AS (
        public.manual_reconcile_final_outcome_v1(
            remaining_mismatch_ledger_guids
        )
    ) STORED NOT NULL,

    final_recheck_completed boolean NOT NULL,

    connector_release_verified boolean NOT NULL,

    connector_id text NOT NULL,

    release_owner_token text NOT NULL,

    release_evidence jsonb NOT NULL,

    evidence_recorded_at timestamptz NOT NULL
        DEFAULT transaction_timestamp(),

    sealed_at timestamptz,

    closed_at timestamptz,

    state text NOT NULL DEFAULT 'EVIDENCE_RECORDED'
        CHECK (
            state IN (
                'EVIDENCE_RECORDED',
                'CLOSED'
            )
        ),

    CHECK (
        jsonb_typeof(remaining_mismatch_ledger_guids) = 'array'
    ),

    CHECK (
        jsonb_typeof(release_evidence) = 'object'
    ),

    CHECK (
        btrim(connector_id) <> ''
    ),

    CHECK (
        btrim(release_owner_token) <> ''
    ),

    CHECK (
        (
            state = 'EVIDENCE_RECORDED'
            AND sealed_at IS NULL
            AND closed_at IS NULL
        )
        OR
        (
            state = 'CLOSED'
            AND sealed_at IS NOT NULL
            AND closed_at IS NOT NULL
        )
    ),

    FOREIGN KEY (
        attempt_id,
        reservation_id,
        operation_token
    )
    REFERENCES public.manual_reconcile_attempts (
        attempt_id,
        reservation_id,
        operation_token
    )
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
);

CREATE INDEX manual_reconcile_finalizations_reservation_idx
ON public.manual_reconcile_finalizations (
    reservation_id,
    operation_token
);

ALTER TABLE public.manual_reconcile_finalizations
ENABLE ROW LEVEL SECURITY;

REVOKE ALL
ON TABLE public.manual_reconcile_finalizations
FROM PUBLIC, anon, authenticated, service_role;

-- ============================================================
-- 2. Record finalization evidence
--
-- Must be called only after:
--   - final mismatch recheck
--   - coordinator release completed successfully
--
-- The coordinator release must be checked by server code.
-- ============================================================

CREATE FUNCTION public.record_manual_reconcile_finalization_v1(
    p_attempt_id uuid,
    p_operation_token uuid,
    p_remaining_mismatch_ledger_guids jsonb,
    p_final_recheck_completed boolean,
    p_connector_release_verified boolean,
    p_connector_id text,
    p_release_owner_token text,
    p_release_evidence jsonb
)
RETURNS TABLE (
    recorded boolean,
    reason text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_existing public.manual_reconcile_finalizations%ROWTYPE;
        v_outcome text;
BEGIN
    v_outcome :=
        public.manual_reconcile_final_outcome_v1(
            p_remaining_mismatch_ledger_guids
        );
    IF p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_final_recheck_completed IS DISTINCT FROM TRUE
       OR p_connector_release_verified IS DISTINCT FROM TRUE
           OR v_outcome IS NULL
       OR NULLIF(btrim(p_connector_id), '') IS NULL
       OR NULLIF(btrim(p_release_owner_token), '') IS NULL
       OR p_release_evidence IS NULL
       OR jsonb_typeof(p_release_evidence)
          IS DISTINCT FROM 'object'
       OR p_release_evidence = '{}'::jsonb
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_FINALIZATION_EVIDENCE'::text;
        RETURN;
    END IF;

    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token IS DISTINCT FROM p_operation_token
    THEN
        RETURN QUERY
        SELECT false, 'ATTEMPT_NOT_FOUND'::text;
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_attempt.company_code || ':' || v_attempt.tally_owner,
            0
        )
    );

    SELECT s.*
    INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY
        SELECT false, 'RESERVATION_NOT_OWNED'::text;
        RETURN;
    END IF;

    -- No pending, unknown, or unsupported journal states.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations o
        WHERE o.attempt_id = p_attempt_id
          AND (
              o.state IS DISTINCT FROM 'RESULT_VERIFIED'
              OR o.operation_type NOT IN (
                  'TALLY_FETCH',
                  'LEDGER_APPLY'
              )
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'JOURNAL_NOT_SETTLED'::text;
        RETURN;
    END IF;

    -- Every verified Tally fetch must have exactly one
    -- verified apply operation linked to that fetch.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations f
        WHERE f.attempt_id = p_attempt_id
          AND f.operation_type = 'TALLY_FETCH'
          AND NOT EXISTS (
              SELECT 1
              FROM public.manual_reconcile_operations a
              WHERE a.source_fetch_operation_id = f.operation_id
                AND a.attempt_id = f.attempt_id
                AND a.operation_token = f.operation_token
                AND a.reservation_id = f.reservation_id
                AND a.operation_type = 'LEDGER_APPLY'
                AND a.state = 'RESULT_VERIFIED'
                AND a.result_payload->>'result'
    IN ('UPDATED', 'UNCHANGED')
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'FETCH_WITHOUT_SUCCESSFUL_APPLY'::text;
        RETURN;
    END IF;

    -- Reject orphan apply operations or unsuccessful outcomes.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations a
        WHERE a.attempt_id = p_attempt_id
          AND a.operation_type = 'LEDGER_APPLY'
          AND (
              (
    a.result_payload->>'result'
        IS DISTINCT FROM 'UPDATED'
    AND
    a.result_payload->>'result'
        IS DISTINCT FROM 'UNCHANGED'
)
              OR NOT EXISTS (
                  SELECT 1
                  FROM public.manual_reconcile_operations f
                  WHERE f.operation_id = a.source_fetch_operation_id
                    AND f.attempt_id = a.attempt_id
                    AND f.operation_type = 'TALLY_FETCH'
                    AND f.state = 'RESULT_VERIFIED'
              )
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'APPLY_EVIDENCE_INVALID'::text;
        RETURN;
    END IF;

    SELECT x.*
    INTO v_existing
    FROM public.manual_reconcile_finalizations x
    WHERE x.attempt_id = p_attempt_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.operation_token
              IS DISTINCT FROM p_operation_token
           OR v_existing.reservation_id
              IS DISTINCT FROM v_attempt.reservation_id
                   OR v_existing.remaining_mismatch_ledger_guids
              IS DISTINCT FROM p_remaining_mismatch_ledger_guids
           OR v_existing.outcome
              IS DISTINCT FROM v_outcome
           OR v_existing.final_recheck_completed
              IS DISTINCT FROM p_final_recheck_completed
           OR v_existing.connector_release_verified
              IS DISTINCT FROM p_connector_release_verified
           OR v_existing.connector_id
              IS DISTINCT FROM p_connector_id
           OR v_existing.release_owner_token
              IS DISTINCT FROM p_release_owner_token
           OR v_existing.release_evidence
              IS DISTINCT FROM p_release_evidence
        THEN
            RETURN QUERY
            SELECT false, 'FINALIZATION_EVIDENCE_CONFLICT'::text;
            RETURN;
        END IF;

        RETURN QUERY
        SELECT true, 'ALREADY_RECORDED'::text;
        RETURN;
    END IF;

    INSERT INTO public.manual_reconcile_finalizations (
        attempt_id,
        reservation_id,
        operation_token,
        remaining_mismatch_ledger_guids,
        final_recheck_completed,
        connector_release_verified,
        connector_id,
        release_owner_token,
        release_evidence
    )
    VALUES (
        p_attempt_id,
        v_attempt.reservation_id,
        p_operation_token,
        p_remaining_mismatch_ledger_guids,
        p_final_recheck_completed,
        p_connector_release_verified,
        p_connector_id,
        p_release_owner_token,
        p_release_evidence
    );

    RETURN QUERY
    SELECT true, 'EVIDENCE_RECORDED'::text;
END;
$fn$;

-- ============================================================
-- 3. Atomic final close
--
-- Rechecks the journal while holding the reservation lock.
-- Seals evidence and closes reservation in one transaction.
-- ============================================================

CREATE FUNCTION public.close_manual_reconcile_v2(
    p_attempt_id uuid,
    p_operation_token uuid
)
RETURNS TABLE (
    closed boolean,
    reason text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_final public.manual_reconcile_finalizations%ROWTYPE;
BEGIN
    IF p_attempt_id IS NULL
       OR p_operation_token IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text;
        RETURN;
    END IF;

    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token IS DISTINCT FROM p_operation_token
    THEN
        RETURN QUERY
        SELECT false, 'ATTEMPT_NOT_FOUND'::text;
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_attempt.company_code || ':' || v_attempt.tally_owner,
            0
        )
    );

    SELECT s.*
    INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    SELECT f.*
    INTO v_final
    FROM public.manual_reconcile_finalizations f
    WHERE f.attempt_id = p_attempt_id
    FOR UPDATE;

    -- Idempotent replay after a committed close.
    IF FOUND
       AND v_final.state = 'CLOSED'
       AND v_final.operation_token = p_operation_token
       AND v_status.status = 'CLOSED'
       AND v_status.operation_token IS NULL
    THEN
        RETURN QUERY
        SELECT true, 'ALREADY_CLOSED'::text;
        RETURN;
    END IF;

    IF v_status.id IS NULL
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY
        SELECT false, 'RESERVATION_NOT_OWNED'::text;
        RETURN;
    END IF;

    IF v_final.attempt_id IS NULL
       OR v_final.state IS DISTINCT FROM 'EVIDENCE_RECORDED'
       OR v_final.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_final.operation_token IS DISTINCT FROM p_operation_token
       OR v_final.final_recheck_completed IS DISTINCT FROM TRUE
       OR v_final.connector_release_verified IS DISTINCT FROM TRUE
       OR public.manual_reconcile_final_outcome_v1(
              v_final.remaining_mismatch_ledger_guids
          ) IS NULL
       OR v_final.outcome IS DISTINCT FROM
          public.manual_reconcile_final_outcome_v1(
              v_final.remaining_mismatch_ledger_guids
          )
    THEN
        RETURN QUERY
        SELECT false, 'FINALIZATION_NOT_VERIFIED'::text;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations o
        WHERE o.attempt_id = p_attempt_id
          AND (
              o.state IS DISTINCT FROM 'RESULT_VERIFIED'
              OR o.operation_type NOT IN (
                  'TALLY_FETCH',
                  'LEDGER_APPLY'
              )
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'JOURNAL_NOT_SETTLED'::text;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations f
        WHERE f.attempt_id = p_attempt_id
          AND f.operation_type = 'TALLY_FETCH'
          AND NOT EXISTS (
              SELECT 1
              FROM public.manual_reconcile_operations a
              WHERE a.source_fetch_operation_id = f.operation_id
                AND a.attempt_id = f.attempt_id
                AND a.reservation_id = f.reservation_id
                AND a.operation_token = f.operation_token
                AND a.operation_type = 'LEDGER_APPLY'
                AND a.state = 'RESULT_VERIFIED'
                AND a.result_payload->>'result'
                    IN ('UPDATED', 'UNCHANGED')
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'FETCH_WITHOUT_SUCCESSFUL_APPLY'::text;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations a
        WHERE a.attempt_id = p_attempt_id
          AND a.operation_type = 'LEDGER_APPLY'
          AND (
              (
    a.result_payload->>'result'
        IS DISTINCT FROM 'UPDATED'
    AND
    a.result_payload->>'result'
        IS DISTINCT FROM 'UNCHANGED'
)
              OR NOT EXISTS (
                  SELECT 1
                  FROM public.manual_reconcile_operations f
                  WHERE f.operation_id = a.source_fetch_operation_id
                    AND f.attempt_id = a.attempt_id
                    AND f.operation_type = 'TALLY_FETCH'
                    AND f.state = 'RESULT_VERIFIED'
              )
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'APPLY_EVIDENCE_INVALID'::text;
        RETURN;
    END IF;

    -- Seal journal evidence.
    UPDATE public.manual_reconcile_finalizations f
    SET
        state = 'CLOSED',
        sealed_at = transaction_timestamp(),
        closed_at = transaction_timestamp()
    WHERE f.attempt_id = p_attempt_id
      AND f.state = 'EVIDENCE_RECORDED';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RECONCILE_FINALIZATION_SEAL_FAILED';
    END IF;

    -- Close reservation in the same database transaction.
    UPDATE public.manual_reconcile_status s
    SET
        status = 'CLOSED',
        operation_token = NULL,
        closed_at = transaction_timestamp(),
        error_message = NULL,
        updated_at = transaction_timestamp()
    WHERE s.id = v_attempt.reservation_id
      AND s.status = 'RUNNING'
      AND s.operation_token = p_operation_token;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RECONCILE_ATOMIC_CLOSE_FAILED';
    END IF;

    RETURN QUERY
    SELECT true, 'CLOSED'::text;
END;
$fn$;

-- ============================================================
-- 4. Permissions
-- ============================================================

ALTER FUNCTION public.record_manual_reconcile_finalization_v1(
    uuid, uuid, jsonb, boolean, boolean, text, text, jsonb
) OWNER TO postgres;

ALTER FUNCTION public.close_manual_reconcile_v2(
    uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
public.record_manual_reconcile_finalization_v1(
    uuid, uuid, jsonb, boolean, boolean, text, text, jsonb
)
FROM PUBLIC, anon, authenticated;

REVOKE ALL ON FUNCTION
public.close_manual_reconcile_v2(uuid, uuid)
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION
public.record_manual_reconcile_finalization_v1(
    uuid, uuid, jsonb, boolean, boolean, text, text, jsonb
)
TO service_role;

GRANT EXECUTE ON FUNCTION
public.close_manual_reconcile_v2(uuid, uuid)
TO service_role;

COMMIT;
