
-- Patch 2A-2, stage 1: persist immutable journal execution identity.
-- DRAFT ONLY: Do not deploy until the five fenced RPCs, caller cutover,
-- legacy RPC revocations, and concurrency tests are ready.
-- Requires 20261112_manual_reconcile_z_execution_binding_v1.sql.
BEGIN;

ALTER TABLE public.manual_reconcile_operations
    ADD COLUMN execution_id uuid;

ALTER TABLE public.manual_reconcile_operations
    ADD CONSTRAINT manual_reconcile_operations_execution_fk
    FOREIGN KEY (
        attempt_id,
        reservation_id,
        operation_token,
        execution_id
    )
    REFERENCES public.manual_reconcile_executions (
        attempt_id,
        reservation_id,
        operation_token,
        execution_id
    )
    ON UPDATE RESTRICT
    ON DELETE RESTRICT;

-- Enforced for new inserts and updates; historical rows are not scanned.
ALTER TABLE public.manual_reconcile_operations
    ADD CONSTRAINT manual_reconcile_operations_execution_required
    CHECK (execution_id IS NOT NULL)
    NOT VALID;

CREATE FUNCTION public.guard_manual_reconcile_operation_execution_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $fn$
BEGIN
    IF NEW.execution_id IS DISTINCT FROM OLD.execution_id THEN
        RAISE EXCEPTION 'RECONCILE_OPERATION_EXECUTION_IMMUTABLE'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$fn$;

ALTER FUNCTION public.guard_manual_reconcile_operation_execution_v1()
    OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.guard_manual_reconcile_operation_execution_v1()
    FROM PUBLIC, anon, authenticated, service_role;

CREATE TRIGGER manual_reconcile_operation_execution_immutable
BEFORE UPDATE
ON public.manual_reconcile_operations
FOR EACH ROW
EXECUTE FUNCTION
    public.guard_manual_reconcile_operation_execution_v1();

-- Shared authorization for execution-fenced journal RPCs.
-- Lock order: company advisory -> reservation row -> journal rows.
-- Does not verify Redis ownership or authorize replay.

CREATE FUNCTION public.require_manual_reconcile_execution_v1(
    p_attempt_id uuid,
    p_operation_token uuid,
    p_execution_id uuid
)
RETURNS public.manual_reconcile_executions
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_execution public.manual_reconcile_executions%ROWTYPE;
BEGIN
    IF p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_execution_id IS NULL
    THEN
        RAISE EXCEPTION 'RECONCILE_EXECUTION_INPUT_INVALID';
    END IF;

    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token
          IS DISTINCT FROM p_operation_token
    THEN
        RAISE EXCEPTION 'RECONCILE_ATTEMPT_NOT_CURRENT';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_attempt.company_code || ':' || v_attempt.tally_owner,
            0
        )
    );

    SELECT s.*
    INTO v_status
    FROM public.manual_reconcile_status AS s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_status.company_code
          IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner
          IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RAISE EXCEPTION 'RECONCILE_RESERVATION_NOT_OWNED';
    END IF;

    SELECT e.*
    INTO v_execution
    FROM public.manual_reconcile_executions AS e
    WHERE e.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_execution.attempt_id
          IS DISTINCT FROM p_attempt_id
       OR v_execution.reservation_id
          IS DISTINCT FROM v_attempt.reservation_id
       OR v_execution.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_execution.execution_id
          IS DISTINCT FROM p_execution_id
    THEN
        RAISE EXCEPTION 'RECONCILE_EXECUTION_NOT_BOUND';
    END IF;

    RETURN v_execution;
END;
$fn$;

ALTER FUNCTION public.require_manual_reconcile_execution_v1(
    uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.require_manual_reconcile_execution_v1(uuid, uuid, uuid)
    FROM PUBLIC, anon, authenticated, service_role;

-- ============================================================
-- PATCH 2A-2: RPC 1
-- Execution-fenced TALLY_FETCH preparation.
-- ALREADY_PREPARED is acknowledgement only, not dispatch authority.
-- ============================================================

CREATE FUNCTION public.prepare_manual_reconcile_fetch_v2(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_operation_ordinal integer,
    p_request_id uuid,
    p_ledger_guid text,
    p_connector_id text,
    p_tally_company_guid text,
    p_input_payload jsonb,
    p_input_fingerprint text,
    p_execution_id uuid
)
RETURNS TABLE(
    prepared boolean,
    reason text,
    operation_state text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
BEGIN
    -- Preserve existing V1 input validation.
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_request_id IS NULL
       OR p_execution_id IS NULL
       OR p_operation_ordinal IS NULL
       OR p_operation_ordinal <= 0
       OR NULLIF(pg_catalog.btrim(p_ledger_guid), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_connector_id), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_tally_company_guid), '') IS NULL
       OR p_input_payload IS NULL
       OR p_input_fingerprint IS NULL
       OR NULLIF(pg_catalog.btrim(p_input_fingerprint), '') IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Authorize the exact immutable execution.
    -- Helper acquires company advisory lock, then reservation lock.
    -- Both remain held through this transaction.
    SELECT e.*
    INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id,
        p_operation_token,
        p_execution_id
    ) AS e;

    -- Connector must match the bound execution.
    IF p_connector_id IS DISTINCT FROM v_execution.connector_id THEN
        RETURN QUERY
        SELECT false, 'EXECUTION_CONNECTOR_CONFLICT'::text,
               NULL::text;
        RETURN;
    END IF;

    -- Lock the existing journal operation after authorization.
    SELECT o.*
    INTO v_existing
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.attempt_id IS DISTINCT FROM p_attempt_id
           OR v_existing.reservation_id
              IS DISTINCT FROM v_execution.reservation_id
           OR v_existing.operation_token
              IS DISTINCT FROM p_operation_token
           OR v_existing.execution_id
              IS DISTINCT FROM p_execution_id
           OR v_existing.operation_ordinal
              IS DISTINCT FROM p_operation_ordinal
           OR v_existing.operation_type
              IS DISTINCT FROM 'TALLY_FETCH'
           OR v_existing.request_id
              IS DISTINCT FROM p_request_id
           OR v_existing.ledger_guid
              IS DISTINCT FROM p_ledger_guid
           OR v_existing.connector_id
              IS DISTINCT FROM p_connector_id
           OR v_existing.tally_company_guid
              IS DISTINCT FROM p_tally_company_guid
           OR v_existing.input_payload
              IS DISTINCT FROM p_input_payload
           OR v_existing.input_fingerprint
              IS DISTINCT FROM p_input_fingerprint
        THEN
            RETURN QUERY
            SELECT false, 'OPERATION_ID_CONFLICT'::text,
                   v_existing.state;
        ELSE
            RETURN QUERY
            SELECT true, 'ALREADY_PREPARED'::text,
                   v_existing.state;
        END IF;

        RETURN;
    END IF;

    -- Preserve V1 ordinal and request identity protection.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations AS o
        WHERE (
            o.attempt_id = p_attempt_id
            AND o.operation_ordinal = p_operation_ordinal
        )
        OR o.request_id = p_request_id
    ) THEN
        RETURN QUERY
        SELECT false, 'OPERATION_IDENTITY_CONFLICT'::text,
               NULL::text;
        RETURN;
    END IF;

    -- Persist the immutable execution identity.
    INSERT INTO public.manual_reconcile_operations (
        operation_id,
        attempt_id,
        reservation_id,
        operation_token,
        execution_id,
        operation_ordinal,
        operation_type,
        ledger_guid,
        request_id,
        connector_id,
        tally_company_guid,
        input_payload,
        input_fingerprint
    )
    VALUES (
        p_operation_id,
        p_attempt_id,
        v_execution.reservation_id,
        p_operation_token,
        p_execution_id,
        p_operation_ordinal,
        'TALLY_FETCH',
        p_ledger_guid,
        p_request_id,
        p_connector_id,
        p_tally_company_guid,
        p_input_payload,
        p_input_fingerprint
    );

    RETURN QUERY
    SELECT true, 'PREPARED'::text, 'PREPARED'::text;
END;
$fn$;

ALTER FUNCTION public.prepare_manual_reconcile_fetch_v2(
    uuid, uuid, uuid, integer, uuid,
    text, text, text, jsonb, text, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.prepare_manual_reconcile_fetch_v2(
    uuid, uuid, uuid, integer, uuid,
    text, text, text, jsonb, text, uuid
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION public.prepare_manual_reconcile_fetch_v2(
    uuid, uuid, uuid, integer, uuid,
    text, text, text, jsonb, text, uuid
) TO service_role;

-- ============================================================
-- PATCH 2A-2: RPC 2
-- Execution-fenced TALLY_FETCH dispatch marking.
-- Only a fresh PREPARED -> DISPATCH_MAY_HAVE_OCCURRED
-- transition may authorize the caller to emit a Tally request.
-- ============================================================

CREATE FUNCTION public.mark_manual_reconcile_fetch_dispatch_v2(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid,
    p_execution_id uuid
)
RETURNS TABLE(
    marked boolean,
    reason text,
    operation_state text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
BEGIN
    -- Preserve V1 input validation and require execution identity.
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_request_id IS NULL
       OR p_execution_id IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Lock order:
    -- company advisory -> reservation row -> operation row.
    SELECT e.*
    INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id,
        p_operation_token,
        p_execution_id
    ) AS e;

    SELECT o.*
    INTO v_operation
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    -- Reject stale, foreign or cross-execution operations.
    IF NOT FOUND
       OR v_operation.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_operation.reservation_id
          IS DISTINCT FROM v_execution.reservation_id
       OR v_operation.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_operation.execution_id
          IS DISTINCT FROM p_execution_id
       OR v_operation.connector_id
          IS DISTINCT FROM v_execution.connector_id
       OR v_operation.request_id
          IS DISTINCT FROM p_request_id
       OR v_operation.operation_type
          IS DISTINCT FROM 'TALLY_FETCH'
    THEN
        RETURN QUERY
        SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text,
               NULL::text;
        RETURN;
    END IF;

    -- One-way dispatch transition.
    -- A repeated request must never authorize re-emission.
    IF v_operation.state IS DISTINCT FROM 'PREPARED' THEN
        RETURN QUERY
        SELECT false, 'DISPATCH_ALREADY_MARKED_OR_SETTLED'::text,
               v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations AS o
    SET state = 'DISPATCH_MAY_HAVE_OCCURRED',
        dispatch_marked_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY
    SELECT true, 'DISPATCH_MARKED'::text,
           'DISPATCH_MAY_HAVE_OCCURRED'::text;
END;
$fn$;

ALTER FUNCTION public.mark_manual_reconcile_fetch_dispatch_v2(
    uuid, uuid, uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.mark_manual_reconcile_fetch_dispatch_v2(
        uuid, uuid, uuid, uuid, uuid
    )
    FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
    public.mark_manual_reconcile_fetch_dispatch_v2(
        uuid, uuid, uuid, uuid, uuid
    )
    TO service_role;

-- ============================================================
-- PATCH 2A-2: RPC 3
-- Execution-fenced verified Tally fetch result.
-- ============================================================

CREATE FUNCTION public.verify_manual_reconcile_fetch_result_v2(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid,
    p_result_payload jsonb,
    p_execution_id uuid
)
RETURNS TABLE (
    verified boolean,
    reason text,
    operation_state text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
    v_guid text;
    v_balance text;
    v_type text;
    v_normalized_balance text;
    v_is_zero boolean;
    v_normalized_payload jsonb;
BEGIN
    -- Existing V1 input validation plus mandatory execution ID.
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_request_id IS NULL
       OR p_result_payload IS NULL
       OR pg_catalog.jsonb_typeof(p_result_payload)
          IS DISTINCT FROM 'object'
       OR p_execution_id IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Execution authorization must precede journal row locking.
    -- Lock order: company advisory -> reservation -> journal.
    SELECT e.*
    INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id,
        p_operation_token,
        p_execution_id
    ) AS e;

    SELECT o.*
    INTO v_operation
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_operation.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_operation.reservation_id
          IS DISTINCT FROM v_execution.reservation_id
       OR v_operation.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_operation.execution_id
          IS DISTINCT FROM p_execution_id
       OR v_operation.connector_id
          IS DISTINCT FROM v_execution.connector_id
       OR v_operation.request_id
          IS DISTINCT FROM p_request_id
       OR v_operation.operation_type
          IS DISTINCT FROM 'TALLY_FETCH'
    THEN
        RETURN QUERY
        SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text,
               NULL::text;
        RETURN;
    END IF;

    -- Preserve exact V1 normalized fetch-result contract.
    IF pg_catalog.jsonb_typeof(p_result_payload->'guid')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(p_result_payload->'openingBalance')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(p_result_payload->'openingBalanceType')
          IS DISTINCT FROM 'string'
    THEN
        RETURN QUERY
        SELECT false, 'RESULT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    v_guid := pg_catalog.btrim(p_result_payload->>'guid');
    v_balance := p_result_payload->>'openingBalance';
    v_type := p_result_payload->>'openingBalanceType';

    IF v_guid IS DISTINCT FROM v_operation.ledger_guid
       OR v_balance !~ '^[0-9]+(\.[0-9]+)?$'
       OR v_type NOT IN ('DR', 'CR', '')
    THEN
        RETURN QUERY
        SELECT false, 'RESULT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    v_normalized_balance :=
        pg_catalog.regexp_replace(
            pg_catalog.split_part(v_balance, '.', 1),
            '^0+(?=[0-9])',
            ''
        );

    IF pg_catalog.strpos(v_balance, '.') > 0 THEN
        v_normalized_balance :=
            v_normalized_balance || '.' ||
            pg_catalog.split_part(v_balance, '.', 2);
    END IF;

    v_is_zero :=
        pg_catalog.replace(v_normalized_balance, '.', '') ~ '^0+$';

    IF (v_is_zero AND v_type <> '')
       OR (NOT v_is_zero AND v_type = '')
    THEN
        RETURN QUERY
        SELECT false, 'RESULT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    v_normalized_payload := pg_catalog.jsonb_build_object(
        'guid', v_guid,
        'openingBalance', v_normalized_balance,
        'openingBalanceType', v_type
    );

    -- Duplicate acknowledgement is not permission to dispatch again.
    IF v_operation.state = 'RESULT_VERIFIED' THEN
        IF v_operation.result_payload = v_normalized_payload THEN
            RETURN QUERY
            SELECT true, 'ALREADY_VERIFIED'::text,
                   v_operation.state;
        ELSE
            RETURN QUERY
            SELECT false, 'RESULT_CONFLICT'::text,
                   v_operation.state;
        END IF;

        RETURN;
    END IF;

    IF v_operation.state
       IS DISTINCT FROM 'DISPATCH_MAY_HAVE_OCCURRED'
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_OPERATION_STATE'::text,
               v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations AS o
    SET state = 'RESULT_VERIFIED',
        result_payload = v_normalized_payload,
        result_verified_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY
    SELECT true, 'RESULT_VERIFIED'::text, 'RESULT_VERIFIED'::text;
END;
$fn$;

ALTER FUNCTION public.verify_manual_reconcile_fetch_result_v2(
    uuid, uuid, uuid, uuid, jsonb, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.verify_manual_reconcile_fetch_result_v2(
        uuid, uuid, uuid, uuid, jsonb, uuid
    )
    FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
    public.verify_manual_reconcile_fetch_result_v2(
        uuid, uuid, uuid, uuid, jsonb, uuid
    )
    TO service_role;

-- ============================================================
-- PATCH 2A-2: RPC 4
-- Execution-fenced LEDGER_APPLY preparation.
-- Source fetch and apply must belong to the same execution.
-- ============================================================

CREATE FUNCTION public.prepare_manual_reconcile_apply_v2(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_operation_ordinal integer,
    p_source_fetch_operation_id uuid,
    p_execution_id uuid
)
RETURNS TABLE (
    prepared boolean,
    reason text,
    operation_state text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_fetch public.manual_reconcile_operations%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
BEGIN
    -- Preserve V1 validation and require execution identity.
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_source_fetch_operation_id IS NULL
       OR p_execution_id IS NULL
       OR p_operation_ordinal IS NULL
       OR p_operation_ordinal <= 0
       OR p_operation_id = p_source_fetch_operation_id
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Acquire company advisory lock, then reservation row lock.
    -- Must run before any journal operation row lock.
    SELECT e.*
    INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id,
        p_operation_token,
        p_execution_id
    ) AS e;

    -- Source fetch must be verified by this exact execution.
    SELECT o.*
    INTO v_fetch
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_source_fetch_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_fetch.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_fetch.reservation_id
          IS DISTINCT FROM v_execution.reservation_id
       OR v_fetch.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_fetch.execution_id
          IS DISTINCT FROM p_execution_id
       OR v_fetch.connector_id
          IS DISTINCT FROM v_execution.connector_id
       OR v_fetch.operation_type
          IS DISTINCT FROM 'TALLY_FETCH'
       OR v_fetch.state
          IS DISTINCT FROM 'RESULT_VERIFIED'
       OR v_fetch.result_payload IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'FETCH_NOT_VERIFIED'::text, NULL::text;
        RETURN;
    END IF;

    -- Existing apply operation: exact identity acknowledgement only.
    SELECT o.*
    INTO v_existing
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.attempt_id IS DISTINCT FROM p_attempt_id
           OR v_existing.reservation_id
              IS DISTINCT FROM v_execution.reservation_id
           OR v_existing.operation_token
              IS DISTINCT FROM p_operation_token
           OR v_existing.execution_id
              IS DISTINCT FROM p_execution_id
           OR v_existing.operation_ordinal
              IS DISTINCT FROM p_operation_ordinal
           OR v_existing.operation_type
              IS DISTINCT FROM 'LEDGER_APPLY'
           OR v_existing.source_fetch_operation_id
              IS DISTINCT FROM p_source_fetch_operation_id
        THEN
            RETURN QUERY
            SELECT false, 'OPERATION_ID_CONFLICT'::text,
                   v_existing.state;
        ELSE
            RETURN QUERY
            SELECT true, 'ALREADY_PREPARED'::text,
                   v_existing.state;
        END IF;

        RETURN;
    END IF;

    -- Preserve V1 ordinal and one-apply-per-fetch protection.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations AS o
        WHERE (
            o.attempt_id = p_attempt_id
            AND o.operation_ordinal = p_operation_ordinal
        )
        OR o.source_fetch_operation_id = p_source_fetch_operation_id
    ) THEN
        RETURN QUERY
        SELECT false, 'OPERATION_IDENTITY_CONFLICT'::text,
               NULL::text;
        RETURN;
    END IF;

    -- Persist the same immutable execution identity.
    INSERT INTO public.manual_reconcile_operations (
        operation_id,
        attempt_id,
        reservation_id,
        operation_token,
        execution_id,
        operation_ordinal,
        operation_type,
        ledger_guid,
        source_fetch_operation_id,
        input_payload,
        input_fingerprint
    )
    VALUES (
        p_operation_id,
        p_attempt_id,
        v_execution.reservation_id,
        p_operation_token,
        p_execution_id,
        p_operation_ordinal,
        'LEDGER_APPLY',
        v_fetch.ledger_guid,
        p_source_fetch_operation_id,
        v_fetch.result_payload,
        v_fetch.input_fingerprint
    );

    RETURN QUERY
    SELECT true, 'PREPARED'::text, 'PREPARED'::text;
END;
$fn$;

ALTER FUNCTION public.prepare_manual_reconcile_apply_v2(
    uuid, uuid, uuid, integer, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.prepare_manual_reconcile_apply_v2(
        uuid, uuid, uuid, integer, uuid, uuid
    )
    FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
    public.prepare_manual_reconcile_apply_v2(
        uuid, uuid, uuid, integer, uuid, uuid
    )
    TO service_role;

-- ============================================================
-- PATCH 2A-2: RPC 5
-- Execution-fenced atomic ledger opening apply.
-- ============================================================

CREATE FUNCTION public.apply_manual_reconcile_ledger_opening_v3(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_execution_id uuid
)
RETURNS TABLE (
    result text,
    previous_balance text,
    previous_type text,
    current_balance text,
    current_type text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_apply public.manual_reconcile_operations%ROWTYPE;
    v_fetch public.manual_reconcile_operations%ROWTYPE;
    v_balance numeric;
    v_type text;
    v_digits text;
    v_ledger_id bigint;
    v_ledger_match_count bigint;
    v_previous_balance numeric;
    v_previous_type text;
    v_result text;
    v_current_balance numeric;
    v_current_type text;
    v_payload jsonb;
BEGIN
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_execution_id IS NULL
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_INPUT_INVALID';
    END IF;

    -- Shared helper locks company advisory then reservation.
    -- No journal or ledger lock may precede this authorization.
    SELECT e.*
    INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id,
        p_operation_token,
        p_execution_id
    ) AS e;

    -- Retrieve the attempt for ledger company/owner scope.
    SELECT a.*
    INTO STRICT v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    -- Lock and validate the exact apply operation.
    SELECT o.*
    INTO v_apply
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_apply.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_apply.reservation_id
          IS DISTINCT FROM v_execution.reservation_id
       OR v_apply.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_apply.execution_id
          IS DISTINCT FROM p_execution_id
       OR v_apply.operation_type
          IS DISTINCT FROM 'LEDGER_APPLY'
    THEN
        RAISE EXCEPTION 'RECONCILE_APPLY_OPERATION_INVALID';
    END IF;

    -- Lock and validate the verified source fetch before replay
    -- acknowledgement or any ledger mutation.
    SELECT o.*
    INTO v_fetch
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = v_apply.source_fetch_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_fetch.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_fetch.reservation_id
          IS DISTINCT FROM v_execution.reservation_id
       OR v_fetch.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_fetch.execution_id
          IS DISTINCT FROM p_execution_id
       OR v_fetch.connector_id
          IS DISTINCT FROM v_execution.connector_id
       OR v_fetch.operation_type
          IS DISTINCT FROM 'TALLY_FETCH'
       OR v_fetch.state
          IS DISTINCT FROM 'RESULT_VERIFIED'
       OR v_fetch.result_payload IS NULL
       OR v_fetch.ledger_guid
          IS DISTINCT FROM v_apply.ledger_guid
       OR v_fetch.result_payload
          IS DISTINCT FROM v_apply.input_payload
    THEN
        RAISE EXCEPTION 'RECONCILE_VERIFIED_FETCH_MISMATCH';
    END IF;

    -- Idempotent acknowledgement of an already committed result.
    -- This never authorizes a second ledger mutation.
    IF v_apply.state = 'RESULT_VERIFIED' THEN
        IF v_apply.result_payload IS NULL THEN
            RAISE EXCEPTION 'RECONCILE_APPLY_RESULT_MISSING';
        END IF;

        RETURN QUERY SELECT
            v_apply.result_payload->>'result',
            v_apply.result_payload->>'previous_balance',
            v_apply.result_payload->>'previous_type',
            v_apply.result_payload->>'current_balance',
            v_apply.result_payload->>'current_type';
        RETURN;
    END IF;

    IF v_apply.state IS DISTINCT FROM 'PREPARED' THEN
        RAISE EXCEPTION 'RECONCILE_APPLY_STATE_INVALID';
    END IF;

    -- Preserve original V2 verified payload validation.
    v_type := v_fetch.result_payload->>'openingBalanceType';

    IF v_type IS NULL OR v_type NOT IN ('DR', 'CR', '') THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_TYPE_INVALID';
    END IF;

    IF (v_fetch.result_payload->>'guid')
       IS DISTINCT FROM v_fetch.ledger_guid
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_GUID_MISMATCH';
    END IF;

    IF (v_fetch.result_payload->>'openingBalance')
       !~ '^[0-9]+(\.[0-9]+)?$'
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_DECIMAL_INVALID';
    END IF;

    v_digits := pg_catalog.replace(
        v_fetch.result_payload->>'openingBalance',
        '.',
        ''
    );

    IF pg_catalog.length(v_digits) > 1000 THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_DECIMAL_TOO_LONG';
    END IF;

    v_balance :=
        (v_fetch.result_payload->>'openingBalance')::numeric;

    IF (v_balance = 0 AND v_type <> '')
       OR (v_balance <> 0 AND v_type = '')
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_TYPE_MISMATCH';
    END IF;

    -- Preserve original scoped ledger lookup.
    SELECT COUNT(*)
    INTO v_ledger_match_count
    FROM public.tally_sync_ledgers AS l
    WHERE l.company_code = v_attempt.company_code
      AND l.tally_owner = v_attempt.tally_owner
      AND l.guid = v_fetch.ledger_guid
      AND l.is_deleted IS DISTINCT FROM TRUE;

    IF v_ledger_match_count > 1 THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_AMBIGUOUS';
    END IF;

    IF v_ledger_match_count = 1 THEN
        SELECT
            l.id,
            l.opening_balance,
            l.opening_balance_type
        INTO STRICT
            v_ledger_id,
            v_previous_balance,
            v_previous_type
        FROM public.tally_sync_ledgers AS l
        WHERE l.company_code = v_attempt.company_code
          AND l.tally_owner = v_attempt.tally_owner
          AND l.guid = v_fetch.ledger_guid
          AND l.is_deleted IS DISTINCT FROM TRUE
        FOR UPDATE;
    END IF;

    IF v_ledger_match_count = 0 THEN
        v_result := 'NOT_FOUND';
        v_current_balance := NULL;
        v_current_type := NULL;

    ELSIF v_previous_balance IS NOT DISTINCT FROM v_balance
       AND v_previous_type IS NOT DISTINCT FROM v_type
    THEN
        v_result := 'UNCHANGED';
        v_current_balance := v_previous_balance;
        v_current_type := v_previous_type;

    ELSE
        UPDATE public.tally_sync_ledgers AS l
        SET opening_balance = v_balance,
            opening_balance_type = v_type
        WHERE l.id = v_ledger_id;

        v_result := 'UPDATED';
        v_current_balance := v_balance;
        v_current_type := v_type;
    END IF;

    -- Exact decimal evidence stored as JSON strings.
    v_payload := pg_catalog.jsonb_build_object(
        'result', v_result,
        'previous_balance', v_previous_balance::text,
        'previous_type', v_previous_type,
        'current_balance', v_current_balance::text,
        'current_type', v_current_type
    );

    UPDATE public.manual_reconcile_operations AS o
    SET state = 'RESULT_VERIFIED',
        result_payload = v_payload,
        result_verified_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY SELECT
        v_result,
        v_previous_balance::text,
        v_previous_type,
        v_current_balance::text,
        v_current_type;
END;
$fn$;

ALTER FUNCTION public.apply_manual_reconcile_ledger_opening_v3(
    uuid, uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.apply_manual_reconcile_ledger_opening_v3(
        uuid, uuid, uuid, uuid
    )
    FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
    public.apply_manual_reconcile_ledger_opening_v3(
        uuid, uuid, uuid, uuid
    )
    TO service_role;

COMMIT;
