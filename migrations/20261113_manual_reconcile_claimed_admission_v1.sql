
-- DRAFT ONLY — NOT APPROVED FOR DEPLOYMENT.
-- Requires 20261112_manual_reconcile_attempts_v2.sql.
--
-- This migration introduces atomic claimed admission.
-- It does NOT yet provide:
-- 1. Ongoing batch mutation fencing.
-- 2. Legacy admission authorization cutover.
-- 3. HTTP route authorization.
-- 4. Trusted fresh BOOKSFROM resolution.
--
-- DO NOT EXECUTE IN PRODUCTION.

BEGIN;

-- =====================================================
-- 1. ORIGINAL EXECUTION CLAIM IDENTITY
-- =====================================================

ALTER TABLE public.manual_reconcile_attempts
    ADD COLUMN original_execution_claim_token uuid;

-- Historical and legacy v2 attempts retain NULL claim identity.
-- Do not fabricate execution tokens for token-less admission.
-- Claimed admission requires and verifies its token in the RPC below.
-- A stored claim token must always identify a batch attempt.

ALTER TABLE public.manual_reconcile_attempts
    ADD CONSTRAINT manual_reconcile_attempt_claim_pair_ck
    CHECK (
        original_execution_claim_token IS NULL
        OR batch_id IS NOT NULL
    ) NOT VALID;


-- =====================================================
-- 2. ATOMIC CLAIMED RECONCILIATION ADMISSION
-- =====================================================

CREATE FUNCTION public.start_manual_reconcile_claimed_v1(
    p_attempt_id uuid,
    p_company_code text,
    p_tally_owner text,
    p_reconcile_type text,
    p_worker_id text,
    p_batch_id text,
    p_execution_claim_token uuid
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
    v_batch public.sync_batches%ROWTYPE;
    v_admission record;

BEGIN

    -- =================================================
    -- INPUT VALIDATION
    -- =================================================

    IF p_attempt_id IS NULL
       OR NULLIF(
           pg_catalog.btrim(p_company_code), ''
       ) IS NULL
       OR p_tally_owner IS NULL
       OR p_tally_owner NOT IN ('USER', 'CA')
       OR NULLIF(
           pg_catalog.btrim(p_reconcile_type), ''
       ) IS NULL
       OR NULLIF(
           pg_catalog.btrim(p_worker_id), ''
       ) IS NULL
       OR NULLIF(
           pg_catalog.btrim(p_batch_id), ''
       ) IS NULL
       OR p_execution_claim_token IS NULL
    THEN
        RETURN QUERY
        SELECT
            false,
            'INVALID_INPUT'::text,
            NULL::uuid;

        RETURN;
    END IF;


    -- =================================================
    -- ATTEMPT IDENTITY LOCK
    -- =================================================

    PERFORM pg_catalog.pg_advisory_xact_lock(
        1297236306,
        pg_catalog.hashtext(p_attempt_id::text)
    );


    -- =================================================
    -- COMPANY / OWNER ADVISORY LOCK
    -- Same key as deployed legacy admission.
    -- =================================================

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            p_company_code || ':' || p_tally_owner,
            0
        )
    );


    -- =================================================
    -- HISTORICAL ATTEMPT REPLAY
    -- =================================================

    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    IF FOUND THEN

        IF v_attempt.company_code
                IS DISTINCT FROM p_company_code
           OR v_attempt.tally_owner
                IS DISTINCT FROM p_tally_owner
           OR v_attempt.reconcile_type
                IS DISTINCT FROM p_reconcile_type
           OR v_attempt.worker_id
                IS DISTINCT FROM p_worker_id
           OR v_attempt.batch_id
                IS DISTINCT FROM p_batch_id
           OR v_attempt.original_execution_claim_token
                IS DISTINCT FROM p_execution_claim_token
        THEN
            RETURN QUERY
            SELECT
                false,
                'ATTEMPT_ID_CONFLICT'::text,
                NULL::uuid;

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
           AND v_status.operation_token =
               v_attempt.operation_token
           AND v_status.batch_id
               IS NOT DISTINCT FROM p_batch_id
        THEN
            RETURN QUERY
            SELECT
                true,
                'ALREADY_ACQUIRED'::text,
                v_attempt.operation_token;

        ELSE
            RETURN QUERY
            SELECT
                false,
                'ATTEMPT_NOT_CURRENT'::text,
                NULL::uuid;
        END IF;

        RETURN;
    END IF;


    -- =================================================
    -- LOCK ORIGINAL SYNC BATCH
    -- =================================================

    SELECT b.*
    INTO v_batch
    FROM public.sync_batches AS b
    WHERE b.batch_id = p_batch_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY
        SELECT
            false,
            'BATCH_NOT_FOUND'::text,
            NULL::uuid;

        RETURN;
    END IF;


    -- =================================================
    -- ORIGINAL EXECUTION CLAIM VERIFICATION
    -- =================================================

    IF v_batch.company_code
            IS DISTINCT FROM p_company_code
       OR v_batch.tally_owner
            IS DISTINCT FROM p_tally_owner
       OR v_batch.execution_claim_token
            IS DISTINCT FROM p_execution_claim_token
    THEN
        RETURN QUERY
        SELECT
            false,
            'BATCH_CLAIM_INVALID'::text,
            NULL::uuid;

        RETURN;
    END IF;


    -- =================================================
    -- BATCH ELIGIBILITY
    -- =================================================

    IF v_batch.batch_status
            IS DISTINCT FROM 'COMPLETED'
       OR v_batch.worker_status
            IS DISTINCT FROM 'COMPLETED'
       OR v_batch.batch_closed
            IS DISTINCT FROM false
       OR v_batch.ledger_reconciliation_completed
            IS DISTINCT FROM false
       OR COALESCE(
           v_batch.error_message ~
               '^(CONNECTOR_OUTCOME_UNKNOWN:|HTTP_OUTCOME_UNKNOWN:)',
           false
       )
    THEN
        RETURN QUERY
        SELECT
            false,
            'BATCH_NOT_ELIGIBLE'::text,
            NULL::uuid;

        RETURN;
    END IF;


    -- =================================================
    -- RETRY / CLEANUP OWNERSHIP
    -- =================================================

    IF v_batch.retry_claim_token IS NOT NULL
       OR v_batch.cleanup_claim_token IS NOT NULL
       OR v_batch.retry_lease_until >
           pg_catalog.transaction_timestamp()
       OR v_batch.cleanup_lease_until >
           pg_catalog.transaction_timestamp()
    THEN
        RETURN QUERY
        SELECT
            false,
            'BATCH_OPERATION_ACTIVE'::text,
            NULL::uuid;

        RETURN;
    END IF;


    -- =================================================
    -- LEGACY RESERVATION ACQUISITION
    -- =================================================

    SELECT *
    INTO STRICT v_admission
    FROM public.start_manual_reconcile(
        p_company_code,
        p_tally_owner,
        p_reconcile_type,
        p_worker_id,
        p_batch_id
    );


    -- =================================================
    -- ADMISSION RESULT VALIDATION
    -- =================================================

    IF v_admission.acquired IS DISTINCT FROM true THEN

        IF v_admission.acquired IS NULL
           OR v_admission.reason IS NULL
           OR v_admission.reservation_token IS NOT NULL
        THEN
            RAISE EXCEPTION
                'MANUAL_RECONCILE_ADMISSION_RESULT_INVALID';
        END IF;

        RETURN QUERY
        SELECT
            false,
            v_admission.reason::text,
            NULL::uuid;

        RETURN;
    END IF;

    IF v_admission.reason IS DISTINCT FROM 'ACQUIRED'
       OR v_admission.reservation_token IS NULL
    THEN
        RAISE EXCEPTION
            'MANUAL_RECONCILE_ADMISSION_CONTRACT_CHANGED';
    END IF;


    -- =================================================
    -- VERIFY CURRENT MANUAL RESERVATION
    -- =================================================

    SELECT r.*
    INTO STRICT v_status
    FROM public.manual_reconcile_status AS r
    WHERE r.company_code = p_company_code
      AND r.tally_owner = p_tally_owner
    FOR UPDATE;

    IF v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token
            IS DISTINCT FROM v_admission.reservation_token
       OR v_status.batch_id
            IS DISTINCT FROM p_batch_id
    THEN
        RAISE EXCEPTION
            'MANUAL_RECONCILE_ADMISSION_STATE_INVALID';
    END IF;


    -- =================================================
    -- PERSIST DURABLE ATTEMPT + ORIGINAL CLAIM
    -- =================================================

    INSERT INTO public.manual_reconcile_attempts (
        attempt_id,
        reservation_id,
        operation_token,
        company_code,
        tally_owner,
        reconcile_type,
        worker_id,
        batch_id,
        original_execution_claim_token
    )
    VALUES (
        p_attempt_id,
        v_status.id,
        v_admission.reservation_token,
        p_company_code,
        p_tally_owner,
        p_reconcile_type,
        p_worker_id,
        p_batch_id,
        p_execution_claim_token
    );


    RETURN QUERY
    SELECT
        true,
        'ACQUIRED'::text,
        v_admission.reservation_token::uuid;

END;
$fn$;


-- =====================================================
-- 3. FUNCTION OWNER
-- =====================================================

ALTER FUNCTION public.start_manual_reconcile_claimed_v1(
    uuid,
    text,
    text,
    text,
    text,
    text,
    uuid
)
OWNER TO postgres;


-- =====================================================
-- 4. RPC PERMISSIONS
-- =====================================================

REVOKE ALL
ON FUNCTION public.start_manual_reconcile_claimed_v1(
    uuid,
    text,
    text,
    text,
    text,
    text,
    uuid
)
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE
ON FUNCTION public.start_manual_reconcile_claimed_v1(
    uuid,
    text,
    text,
    text,
    text,
    text,
    uuid
)
TO service_role;

COMMIT;


-- =====================================================
-- ACTIVATION BLOCKERS — NOT IMPLEMENTED HERE
-- =====================================================
--
-- 1. Fence all conflicting retry, cleanup, closure,
--    reconciliation and execution ownership mutations
--    while a matching manual reservation is RUNNING.
--
-- 2. Retire direct token-less admission access after
--    migrating all authorized callers.
--
-- 3. Verify function ownership, database permissions,
--    migration dependencies and active reservations.
--
-- 4. Resolve trusted fresh BOOKSFROM.
--
-- 5. Add authenticated company/owner authorization
--    before activating the HTTP route.
--
-- 6. Test admission/replay/mutation concurrency and
--    failure recovery in staging.
--
-- DO NOT DEPLOY UNTIL THESE CONTRACTS ARE VERIFIED.
