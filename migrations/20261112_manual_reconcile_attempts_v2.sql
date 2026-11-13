-- Phase 1: Durable manual reconciliation admission
-- Do not deploy before verification.

BEGIN;

CREATE TABLE public.manual_reconcile_attempts (
    attempt_id uuid PRIMARY KEY,

    reservation_id bigint NOT NULL
        REFERENCES public.manual_reconcile_status(id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    operation_token uuid NOT NULL UNIQUE,

    company_code text NOT NULL
        CHECK (length(btrim(company_code)) > 0),

    tally_owner text NOT NULL
        CHECK (tally_owner IN ('USER', 'CA')),

    reconcile_type text NOT NULL
        CHECK (length(btrim(reconcile_type)) > 0),

    worker_id text NOT NULL
        CHECK (length(btrim(worker_id)) > 0),

    batch_id text,

    admitted_at timestamptz NOT NULL
        DEFAULT transaction_timestamp(),

    created_at timestamptz NOT NULL
        DEFAULT transaction_timestamp(),

    UNIQUE (reservation_id, operation_token)
);

CREATE FUNCTION public.guard_manual_reconcile_attempt_identity()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO ''
AS $fn$
BEGIN
    RAISE EXCEPTION
        'MANUAL_RECONCILE_ATTEMPT_IMMUTABLE'
        USING ERRCODE = '55000';
END;
$fn$;

CREATE TRIGGER manual_reconcile_attempts_immutable
BEFORE UPDATE OR DELETE
ON public.manual_reconcile_attempts
FOR EACH ROW
EXECUTE FUNCTION public.guard_manual_reconcile_attempt_identity();

ALTER TABLE public.manual_reconcile_attempts
ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON public.manual_reconcile_attempts
FROM PUBLIC, anon, authenticated, service_role;

REVOKE ALL ON FUNCTION
public.guard_manual_reconcile_attempt_identity()
FROM PUBLIC;

CREATE FUNCTION public.start_manual_reconcile_v2(
    p_attempt_id uuid,
    p_company_code text,
    p_tally_owner text,
    p_reconcile_type text,
    p_worker_id text,
    p_batch_id text DEFAULT NULL::text
)
RETURNS TABLE(
    acquired boolean,
    reason text,
    reservation_token uuid
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_admission record;
BEGIN
    IF p_attempt_id IS NULL
       OR NULLIF(pg_catalog.btrim(p_company_code), '') IS NULL
       OR p_tally_owner IS NULL
       OR p_tally_owner NOT IN ('USER', 'CA')
       OR NULLIF(pg_catalog.btrim(p_reconcile_type), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_worker_id), '') IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::uuid;
        RETURN;
    END IF;

    -- Lock order: attempt identity first.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        1297236306,
        pg_catalog.hashtext(p_attempt_id::text)
    );

    -- Check identity before acquiring company lock.
    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    IF FOUND THEN
        IF v_attempt.company_code IS DISTINCT FROM p_company_code
           OR v_attempt.tally_owner IS DISTINCT FROM p_tally_owner
           OR v_attempt.reconcile_type IS DISTINCT FROM p_reconcile_type
           OR v_attempt.worker_id IS DISTINCT FROM p_worker_id
           OR v_attempt.batch_id IS DISTINCT FROM p_batch_id
        THEN
            RETURN QUERY
            SELECT false, 'ATTEMPT_ID_CONFLICT'::text, NULL::uuid;
            RETURN;
        END IF;

        SELECT r.*
        INTO v_status
        FROM public.manual_reconcile_status AS r
        WHERE r.id = v_attempt.reservation_id
        FOR UPDATE;

        IF FOUND
           AND v_status.company_code = p_company_code
           AND v_status.tally_owner = p_tally_owner
           AND v_status.status = 'RUNNING'
           AND v_status.operation_token = v_attempt.operation_token
        THEN
            RETURN QUERY
            SELECT true, 'ALREADY_ACQUIRED'::text,
                   v_attempt.operation_token;
        ELSE
            RETURN QUERY
            SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::uuid;
        END IF;

        RETURN;
    END IF;

    -- Existing admission owns all original safety guards.
    -- Its company advisory lock is acquired inside this transaction.
    SELECT *
    INTO STRICT v_admission
    FROM public.start_manual_reconcile(
        p_company_code,
        p_tally_owner,
        p_reconcile_type,
        p_worker_id,
        p_batch_id
    );

    IF v_admission.acquired IS FALSE THEN
        RETURN QUERY
        SELECT false, v_admission.reason::text, NULL::uuid;
        RETURN;
    END IF;

    IF v_admission.acquired IS DISTINCT FROM true
       OR v_admission.reason IS DISTINCT FROM 'ACQUIRED'
       OR v_admission.reservation_token IS NULL
    THEN
        RAISE EXCEPTION
            'MANUAL_RECONCILE_ADMISSION_CONTRACT_CHANGED';
    END IF;

    SELECT r.*
    INTO STRICT v_status
    FROM public.manual_reconcile_status AS r
    WHERE r.company_code = p_company_code
      AND r.tally_owner = p_tally_owner
    FOR UPDATE;

    IF v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM
          v_admission.reservation_token
    THEN
        RAISE EXCEPTION
            'MANUAL_RECONCILE_ADMISSION_STATE_INVALID';
    END IF;

    INSERT INTO public.manual_reconcile_attempts (
        attempt_id,
        reservation_id,
        operation_token,
        company_code,
        tally_owner,
        reconcile_type,
        worker_id,
        batch_id
    )
    VALUES (
        p_attempt_id,
        v_status.id,
        v_admission.reservation_token,
        p_company_code,
        p_tally_owner,
        p_reconcile_type,
        p_worker_id,
        p_batch_id
    );

    RETURN QUERY
    SELECT true, 'ACQUIRED'::text,
           v_admission.reservation_token::uuid;
END;
$fn$;

ALTER FUNCTION public.start_manual_reconcile_v2(
    uuid, text, text, text, text, text
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.start_manual_reconcile_v2(
    uuid, text, text, text, text, text
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.start_manual_reconcile_v2(
    uuid, text, text, text, text, text
) TO service_role;

COMMIT;
