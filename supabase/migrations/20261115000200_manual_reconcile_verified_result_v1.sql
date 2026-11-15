
-- Phase 2: Durable verified Tally fetch result.
-- Depends on manual_reconcile_operations_v1.
-- Do not deploy before integration verification.

BEGIN;

CREATE FUNCTION public.verify_manual_reconcile_fetch_result_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid,
    p_result_payload jsonb
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
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
    v_guid text;
    v_balance text;
    v_type text;
    v_normalized_balance text;
    v_is_zero boolean;
    v_normalized_payload jsonb;
BEGIN
    IF p_operation_id IS NULL
       OR p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_request_id IS NULL
       OR p_result_payload IS NULL
       OR jsonb_typeof(p_result_payload) IS DISTINCT FROM 'object'
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

    SELECT o.* INTO v_operation
    FROM public.manual_reconcile_operations o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_operation.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_operation.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_operation.operation_token IS DISTINCT FROM p_operation_token
       OR v_operation.request_id IS DISTINCT FROM p_request_id
       OR v_operation.operation_type IS DISTINCT FROM 'TALLY_FETCH'
    THEN
        RETURN QUERY
        SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    -- The exact expected payload contract is the normalized
    -- fetchLedgerOpening() result, not arbitrary connector metadata.
    IF jsonb_typeof(p_result_payload->'guid') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_result_payload->'openingBalance') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_result_payload->'openingBalanceType') IS DISTINCT FROM 'string'
    THEN
        RETURN QUERY
        SELECT false, 'RESULT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    v_guid := btrim(p_result_payload->>'guid');
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
        regexp_replace(split_part(v_balance, '.', 1), '^0+(?=[0-9])', '');

    IF position('.' IN v_balance) > 0 THEN
        v_normalized_balance :=
            v_normalized_balance || '.' || split_part(v_balance, '.', 2);
    END IF;

    v_is_zero :=
        replace(v_normalized_balance, '.', '') ~ '^0+$';

    IF (v_is_zero AND v_type <> '')
       OR (NOT v_is_zero AND v_type = '')
    THEN
        RETURN QUERY
        SELECT false, 'RESULT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    v_normalized_payload := jsonb_build_object(
        'guid', v_guid,
        'openingBalance', v_normalized_balance,
        'openingBalanceType', v_type
    );

    IF v_operation.state = 'RESULT_VERIFIED' THEN
        IF v_operation.result_payload = v_normalized_payload THEN
            RETURN QUERY
            SELECT true, 'ALREADY_VERIFIED'::text, v_operation.state;
        ELSE
            RETURN QUERY
            SELECT false, 'RESULT_CONFLICT'::text, v_operation.state;
        END IF;
        RETURN;
    END IF;

    IF v_operation.state IS DISTINCT FROM 'DISPATCH_MAY_HAVE_OCCURRED' THEN
        RETURN QUERY
        SELECT false, 'INVALID_OPERATION_STATE'::text, v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations o
    SET state = 'RESULT_VERIFIED',
        result_payload = v_normalized_payload,
        result_verified_at = transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY
    SELECT true, 'RESULT_VERIFIED'::text, 'RESULT_VERIFIED'::text;
END;
$fn$;

ALTER FUNCTION public.verify_manual_reconcile_fetch_result_v1(
    uuid, uuid, uuid, uuid, jsonb
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.verify_manual_reconcile_fetch_result_v1(
    uuid, uuid, uuid, uuid, jsonb
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.verify_manual_reconcile_fetch_result_v1(
    uuid, uuid, uuid, uuid, jsonb
) TO service_role;

COMMIT;
