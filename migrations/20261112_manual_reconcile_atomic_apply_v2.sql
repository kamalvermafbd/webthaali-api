
-- Phase 2: Atomic journal-aware ledger opening apply.
-- Depends on manual_reconcile_attempts_v2,
-- manual_reconcile_operations_v1,
-- manual_reconcile_verified_result_v1.
--
-- Monetary RPC outputs and journal evidence use TEXT
-- to preserve exact decimal values across JSON boundaries.
--
-- Do not deploy until integration and concurrency tests pass.

BEGIN;

ALTER TABLE public.manual_reconcile_operations
ADD COLUMN source_fetch_operation_id uuid;

ALTER TABLE public.manual_reconcile_operations
ADD CONSTRAINT manual_reconcile_apply_source_fk
FOREIGN KEY (source_fetch_operation_id)
REFERENCES public.manual_reconcile_operations(operation_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE public.manual_reconcile_operations
ADD CONSTRAINT manual_reconcile_apply_source_check
CHECK (
    (
        operation_type = 'LEDGER_APPLY'
        AND source_fetch_operation_id IS NOT NULL
    )
    OR
    (
        operation_type <> 'LEDGER_APPLY'
        AND source_fetch_operation_id IS NULL
    )
);

CREATE UNIQUE INDEX manual_reconcile_one_apply_per_fetch_uidx
ON public.manual_reconcile_operations(source_fetch_operation_id)
WHERE source_fetch_operation_id IS NOT NULL;

CREATE FUNCTION public.prepare_manual_reconcile_apply_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_operation_ordinal integer,
    p_source_fetch_operation_id uuid
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
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_fetch public.manual_reconcile_operations%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_source_fetch_operation_id IS NULL
       OR p_operation_ordinal IS NULL
       OR p_operation_ordinal <= 0
       OR p_operation_id = p_source_fetch_operation_id
    THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT a.* INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token IS DISTINCT FROM p_operation_token
    THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT s.* INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT o.* INTO v_fetch
    FROM public.manual_reconcile_operations o
    WHERE o.operation_id = p_source_fetch_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_fetch.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_fetch.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_fetch.operation_token IS DISTINCT FROM p_operation_token
       OR v_fetch.operation_type IS DISTINCT FROM 'TALLY_FETCH'
       OR v_fetch.state IS DISTINCT FROM 'RESULT_VERIFIED'
       OR v_fetch.result_payload IS NULL
    THEN
        RETURN QUERY SELECT false, 'FETCH_NOT_VERIFIED'::text, NULL::text;
        RETURN;
    END IF;

    SELECT o.* INTO v_existing
    FROM public.manual_reconcile_operations o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.attempt_id IS DISTINCT FROM p_attempt_id
           OR v_existing.reservation_id IS DISTINCT FROM v_attempt.reservation_id
           OR v_existing.operation_token IS DISTINCT FROM p_operation_token
           OR v_existing.operation_ordinal IS DISTINCT FROM p_operation_ordinal
           OR v_existing.operation_type IS DISTINCT FROM 'LEDGER_APPLY'
           OR v_existing.source_fetch_operation_id
              IS DISTINCT FROM p_source_fetch_operation_id
        THEN
            RETURN QUERY SELECT false, 'OPERATION_ID_CONFLICT'::text,
                                v_existing.state;
        ELSE
            RETURN QUERY SELECT true, 'ALREADY_PREPARED'::text,
                               v_existing.state;
        END IF;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations o
        WHERE (
            o.attempt_id = p_attempt_id
            AND o.operation_ordinal = p_operation_ordinal
        )
        OR o.source_fetch_operation_id = p_source_fetch_operation_id
    ) THEN
        RETURN QUERY SELECT false, 'OPERATION_IDENTITY_CONFLICT'::text,
                            NULL::text;
        RETURN;
    END IF;

    INSERT INTO public.manual_reconcile_operations (
        operation_id,
        attempt_id,
        reservation_id,
        operation_token,
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
        v_attempt.reservation_id,
        p_operation_token,
        p_operation_ordinal,
        'LEDGER_APPLY',
        v_fetch.ledger_guid,
        p_source_fetch_operation_id,
        v_fetch.result_payload,
        v_fetch.input_fingerprint
    );

    RETURN QUERY SELECT true, 'PREPARED'::text, 'PREPARED'::text;
END;
$fn$;

CREATE FUNCTION public.apply_manual_reconcile_ledger_opening_v2(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid
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
    v_status public.manual_reconcile_status%ROWTYPE;
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
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_INPUT_INVALID';
    END IF;

    SELECT a.* INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token IS DISTINCT FROM p_operation_token
    THEN
        RAISE EXCEPTION 'RECONCILE_ATTEMPT_NOT_CURRENT';
    END IF;

    -- Preserve existing company/owner advisory lock order.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_attempt.company_code || ':' || v_attempt.tally_owner,
            0
        )
    );

    SELECT s.* INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RAISE EXCEPTION 'RECONCILE_RESERVATION_NOT_OWNED';
    END IF;

    SELECT o.* INTO v_apply
    FROM public.manual_reconcile_operations o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_apply.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_apply.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_apply.operation_token IS DISTINCT FROM p_operation_token
       OR v_apply.operation_type IS DISTINCT FROM 'LEDGER_APPLY'
    THEN
        RAISE EXCEPTION 'RECONCILE_APPLY_OPERATION_INVALID';
    END IF;

    -- Idempotent replay using exact decimal strings.
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

    SELECT o.* INTO v_fetch
    FROM public.manual_reconcile_operations o
    WHERE o.operation_id = v_apply.source_fetch_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_fetch.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_fetch.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_fetch.operation_token IS DISTINCT FROM p_operation_token
       OR v_fetch.operation_type IS DISTINCT FROM 'TALLY_FETCH'
       OR v_fetch.state IS DISTINCT FROM 'RESULT_VERIFIED'
       OR v_fetch.result_payload IS NULL
       OR v_fetch.ledger_guid IS DISTINCT FROM v_apply.ledger_guid
       OR v_fetch.result_payload IS DISTINCT FROM v_apply.input_payload
    THEN
        RAISE EXCEPTION 'RECONCILE_VERIFIED_FETCH_MISMATCH';
    END IF;

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

    v_digits := replace(
        v_fetch.result_payload->>'openingBalance',
        '.',
        ''
    );

    IF length(v_digits) > 1000 THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_DECIMAL_TOO_LONG';
    END IF;

    v_balance :=
        (v_fetch.result_payload->>'openingBalance')::numeric;

    IF (v_balance = 0 AND v_type <> '')
       OR (v_balance <> 0 AND v_type = '')
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_TYPE_MISMATCH';
    END IF;

    -- Detect multiple active ledgers with the same scope/GUID.
    SELECT COUNT(*)
    INTO v_ledger_match_count
    FROM public.tally_sync_ledgers l
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
        FROM public.tally_sync_ledgers l
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
        UPDATE public.tally_sync_ledgers l
        SET opening_balance = v_balance,
            opening_balance_type = v_type
        WHERE l.id = v_ledger_id;

        v_result := 'UPDATED';
        v_current_balance := v_balance;
        v_current_type := v_type;
    END IF;

    -- Store monetary evidence as JSON strings, never JSON numbers.
    v_payload := jsonb_build_object(
        'result', v_result,
        'previous_balance', v_previous_balance::text,
        'previous_type', v_previous_type,
        'current_balance', v_current_balance::text,
        'current_type', v_current_type
    );

    UPDATE public.manual_reconcile_operations o
    SET state = 'RESULT_VERIFIED',
        result_payload = v_payload,
        result_verified_at = transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    -- RPC returns exact decimal strings across JSON boundary.
    RETURN QUERY SELECT
        v_result,
        v_previous_balance::text,
        v_previous_type,
        v_current_balance::text,
        v_current_type;
END;
$fn$;

ALTER FUNCTION public.prepare_manual_reconcile_apply_v1(
    uuid, uuid, uuid, integer, uuid
) OWNER TO postgres;

ALTER FUNCTION public.apply_manual_reconcile_ledger_opening_v2(
    uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.prepare_manual_reconcile_apply_v1(
    uuid, uuid, uuid, integer, uuid
) FROM PUBLIC, anon, authenticated;

REVOKE ALL ON FUNCTION public.apply_manual_reconcile_ledger_opening_v2(
    uuid, uuid, uuid
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.prepare_manual_reconcile_apply_v1(
    uuid, uuid, uuid, integer, uuid
) TO service_role;

GRANT EXECUTE ON FUNCTION public.apply_manual_reconcile_ledger_opening_v2(
    uuid, uuid, uuid
) TO service_role;

COMMIT;
