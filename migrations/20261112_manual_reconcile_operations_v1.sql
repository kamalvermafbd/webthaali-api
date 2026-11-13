
-- Phase 2: Manual reconciliation durable operation journal and admission gates.
-- Depends on 20261112_manual_reconcile_attempts_v2.sql.
-- NOT FOR PRODUCTION DEPLOYMENT until remaining result/apply/close RPCs
-- and disposable-DB integration/concurrency testing are complete.
BEGIN;

ALTER TABLE public.manual_reconcile_attempts
ADD CONSTRAINT manual_reconcile_attempts_identity_uq
UNIQUE (attempt_id, reservation_id, operation_token);

CREATE TABLE public.manual_reconcile_operations (
    operation_id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL,
    reservation_id bigint NOT NULL,
    operation_token uuid NOT NULL,
    operation_ordinal integer NOT NULL CHECK (operation_ordinal > 0),
    operation_type text NOT NULL CHECK (
        operation_type IN ('TALLY_FETCH', 'LEDGER_APPLY', 'FINAL_RECHECK', 'FINAL_CLOSE')
    ),
    ledger_guid text,
    request_id uuid,
    connector_id text,
    tally_company_guid text,
    input_payload jsonb NOT NULL,
    input_fingerprint text NOT NULL CHECK (length(btrim(input_fingerprint)) > 0),
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED', 'DISPATCH_MAY_HAVE_OCCURRED',
                  'RESULT_VERIFIED', 'OUTCOME_UNKNOWN')
    ),
    result_payload jsonb,
    error_details jsonb,
    prepared_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    dispatch_marked_at timestamptz,
    result_verified_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (attempt_id, operation_ordinal),
    FOREIGN KEY (attempt_id, reservation_id, operation_token)
        REFERENCES public.manual_reconcile_attempts
            (attempt_id, reservation_id, operation_token)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (operation_type = 'TALLY_FETCH'
         AND request_id IS NOT NULL
         AND ledger_guid IS NOT NULL AND length(btrim(ledger_guid)) > 0
         AND connector_id IS NOT NULL AND length(btrim(connector_id)) > 0
         AND tally_company_guid IS NOT NULL AND length(btrim(tally_company_guid)) > 0)
        OR (operation_type <> 'TALLY_FETCH' AND request_id IS NULL)
    )
);

CREATE UNIQUE INDEX manual_reconcile_operations_request_id_uidx
ON public.manual_reconcile_operations(request_id)
WHERE request_id IS NOT NULL;

CREATE INDEX manual_reconcile_operations_attempt_idx
ON public.manual_reconcile_operations(attempt_id, operation_ordinal);

ALTER TABLE public.manual_reconcile_operations ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.manual_reconcile_operations
FROM PUBLIC, anon, authenticated, service_role;

-- This initial gate supports TALLY_FETCH only. Other operation types
-- require additional, separately fenced RPCs in later migration work.
CREATE FUNCTION public.prepare_manual_reconcile_fetch_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_operation_ordinal integer,
    p_request_id uuid,
    p_ledger_guid text,
    p_connector_id text,
    p_tally_company_guid text,
    p_input_payload jsonb,
    p_input_fingerprint text
)
RETURNS TABLE(prepared boolean, reason text, operation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_request_id IS NULL
       OR p_operation_ordinal IS NULL OR p_operation_ordinal <= 0
       OR NULLIF(pg_catalog.btrim(p_ledger_guid), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_connector_id), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_tally_company_guid), '') IS NULL
       OR p_input_payload IS NULL OR p_input_fingerprint IS NULL
       OR NULLIF(pg_catalog.btrim(p_input_fingerprint), '') IS NULL
    THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Serialize prepare against reservation closure and other preparations.
    SELECT a.* INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND OR v_attempt.operation_token IS DISTINCT FROM p_operation_token THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT s.* INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::text;
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
           OR v_existing.operation_type IS DISTINCT FROM 'TALLY_FETCH'
           OR v_existing.request_id IS DISTINCT FROM p_request_id
           OR v_existing.ledger_guid IS DISTINCT FROM p_ledger_guid
           OR v_existing.connector_id IS DISTINCT FROM p_connector_id
           OR v_existing.tally_company_guid IS DISTINCT FROM p_tally_company_guid
           OR v_existing.input_payload IS DISTINCT FROM p_input_payload
           OR v_existing.input_fingerprint IS DISTINCT FROM p_input_fingerprint
        THEN
            RETURN QUERY SELECT false, 'OPERATION_ID_CONFLICT'::text, v_existing.state;
        ELSE
            RETURN QUERY SELECT true, 'ALREADY_PREPARED'::text, v_existing.state;
        END IF;
        RETURN;
    END IF;

    -- Prevent an unrelated ID from reusing an existing ordinal or request ID.
    IF EXISTS (
        SELECT 1 FROM public.manual_reconcile_operations o
        WHERE (o.attempt_id = p_attempt_id AND o.operation_ordinal = p_operation_ordinal)
           OR o.request_id = p_request_id
    ) THEN
        RETURN QUERY SELECT false, 'OPERATION_IDENTITY_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    INSERT INTO public.manual_reconcile_operations (
        operation_id, attempt_id, reservation_id, operation_token,
        operation_ordinal, operation_type, ledger_guid, request_id,
        connector_id, tally_company_guid, input_payload, input_fingerprint
    ) VALUES (
        p_operation_id, p_attempt_id, v_attempt.reservation_id, p_operation_token,
        p_operation_ordinal, 'TALLY_FETCH', p_ledger_guid, p_request_id,
        p_connector_id, p_tally_company_guid, p_input_payload, p_input_fingerprint
    );

    RETURN QUERY SELECT true, 'PREPARED'::text, 'PREPARED'::text;
END;
$fn$;

-- Marker is one-way. Repeated acknowledgement is not permission to re-emit.
CREATE FUNCTION public.mark_manual_reconcile_fetch_dispatch_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid
)
RETURNS TABLE(marked boolean, reason text, operation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_request_id IS NULL THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT a.* INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;
    IF NOT FOUND OR v_attempt.operation_token IS DISTINCT FROM p_operation_token THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT s.* INTO v_status
    FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;
    IF NOT FOUND OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner THEN
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
       OR v_operation.operation_type IS DISTINCT FROM 'TALLY_FETCH' THEN
        RETURN QUERY SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    IF v_operation.state IS DISTINCT FROM 'PREPARED' THEN
        RETURN QUERY SELECT false, 'DISPATCH_ALREADY_MARKED_OR_SETTLED'::text,
                            v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations o
    SET state = 'DISPATCH_MAY_HAVE_OCCURRED',
        dispatch_marked_at = transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY SELECT true, 'DISPATCH_MARKED'::text,
                        'DISPATCH_MAY_HAVE_OCCURRED'::text;
END;
$fn$;

ALTER FUNCTION public.prepare_manual_reconcile_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, text, jsonb, text
) OWNER TO postgres;
ALTER FUNCTION public.mark_manual_reconcile_fetch_dispatch_v1(
    uuid, uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.prepare_manual_reconcile_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, text, jsonb, text
) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.mark_manual_reconcile_fetch_dispatch_v1(
    uuid, uuid, uuid, uuid
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.prepare_manual_reconcile_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, text, jsonb, text
) TO service_role;
GRANT EXECUTE ON FUNCTION public.mark_manual_reconcile_fetch_dispatch_v1(
    uuid, uuid, uuid, uuid
) TO service_role;

COMMIT;
