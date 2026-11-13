-- DRAFT ONLY: full replacement for
-- migrations/20261113_manual_reconcile_historical_replay_fix_DRAFT.sql
-- DO NOT DEPLOY until all prerequisite migrations and integration tests pass.
-- Depends on manual_reconcile_finalizations, manual_reconcile_executions,
-- manual_reconcile_pre_release_checkpoints and journal seal guard.
-- Replaces the existing 8-argument finalization recorder, preserving its API.
BEGIN;

CREATE OR REPLACE FUNCTION public.record_manual_reconcile_finalization_v1(
    p_attempt_id uuid,
    p_operation_token uuid,
    p_remaining_mismatch_ledger_guids jsonb,
    p_final_recheck_completed boolean,
    p_connector_release_verified boolean,
    p_connector_id text,
    p_release_owner_token text,
    p_release_evidence jsonb
)
RETURNS TABLE(recorded boolean, reason text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_checkpoint public.manual_reconcile_pre_release_checkpoints%ROWTYPE;
    v_existing public.manual_reconcile_finalizations%ROWTYPE;
    v_outcome text;
BEGIN
    v_outcome := public.manual_reconcile_final_outcome_v1(p_remaining_mismatch_ledger_guids);
    IF p_attempt_id IS NULL OR p_operation_token IS NULL
       OR p_final_recheck_completed IS DISTINCT FROM TRUE
       OR p_connector_release_verified IS DISTINCT FROM TRUE
       OR v_outcome IS NULL
       OR NULLIF(pg_catalog.btrim(p_connector_id), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_release_owner_token), '') IS NULL
       OR pg_catalog.jsonb_typeof(p_release_evidence) IS DISTINCT FROM 'object'
       OR p_release_evidence->'redisReleaseConfirmed' IS DISTINCT FROM 'true'::jsonb
    THEN
        RETURN QUERY SELECT false, 'INVALID_FINALIZATION_EVIDENCE'::text;
        RETURN;
    END IF;

    SELECT a.* INTO v_attempt
    FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;
    IF NOT FOUND OR v_attempt.operation_token IS DISTINCT FROM p_operation_token THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_FOUND'::text;
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(v_attempt.company_code || ':' || v_attempt.tally_owner, 0)
    );

    SELECT s.* INTO v_status FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id FOR UPDATE;

    SELECT e.* INTO v_execution FROM public.manual_reconcile_executions e
    WHERE e.attempt_id = p_attempt_id;
    IF NOT FOUND OR v_execution.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_execution.operation_token IS DISTINCT FROM p_operation_token THEN
        RETURN QUERY SELECT false, 'EXECUTION_NOT_BOUND'::text;
        RETURN;
    END IF;

    SELECT c.* INTO v_checkpoint FROM public.manual_reconcile_pre_release_checkpoints c
    WHERE c.attempt_id = p_attempt_id;
    IF NOT FOUND
       OR v_checkpoint.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_checkpoint.operation_token IS DISTINCT FROM p_operation_token
       OR v_checkpoint.execution_id IS DISTINCT FROM v_execution.execution_id
       OR v_checkpoint.connector_id IS DISTINCT FROM v_execution.connector_id
       OR v_checkpoint.owner_token IS DISTINCT FROM v_execution.owner_token
       OR v_checkpoint.remaining_mismatch_ledger_guids IS DISTINCT FROM p_remaining_mismatch_ledger_guids
       OR v_checkpoint.final_recheck_completed IS DISTINCT FROM TRUE
    THEN
        RETURN QUERY SELECT false, 'CHECKPOINT_NOT_VERIFIED'::text;
        RETURN;
    END IF;

    -- Compare every coordinator receipt identity with the immutable binding.
    -- JSON string comparisons avoid accepting malformed UUID casts as evidence.
    IF p_connector_id IS DISTINCT FROM v_execution.connector_id
       OR p_release_owner_token IS DISTINCT FROM v_execution.owner_token::text
       OR p_release_evidence->>'attemptId' IS DISTINCT FROM p_attempt_id::text
       OR p_release_evidence->>'executionId' IS DISTINCT FROM v_execution.execution_id::text
       OR p_release_evidence->>'serverInstanceId' IS DISTINCT FROM v_execution.server_instance_id::text
       OR p_release_evidence->>'connectorId' IS DISTINCT FROM v_execution.connector_id
       OR p_release_evidence->>'socketId' IS DISTINCT FROM v_execution.socket_id
       OR p_release_evidence->>'ownerToken' IS DISTINCT FROM v_execution.owner_token::text
    THEN
        RETURN QUERY SELECT false, 'RELEASE_IDENTITY_MISMATCH'::text;
        RETURN;
    END IF;

    -- Exact immutable replay is allowed even if this reservation was reused.
    SELECT f.* INTO v_existing FROM public.manual_reconcile_finalizations f
    WHERE f.attempt_id = p_attempt_id FOR UPDATE;
    IF FOUND THEN
        IF v_existing.reservation_id IS DISTINCT FROM v_attempt.reservation_id
           OR v_existing.operation_token IS DISTINCT FROM p_operation_token
           OR v_existing.remaining_mismatch_ledger_guids IS DISTINCT FROM p_remaining_mismatch_ledger_guids
           OR v_existing.outcome IS DISTINCT FROM v_outcome
           OR v_existing.final_recheck_completed IS DISTINCT FROM p_final_recheck_completed
           OR v_existing.connector_release_verified IS DISTINCT FROM p_connector_release_verified
           OR v_existing.connector_id IS DISTINCT FROM p_connector_id
           OR v_existing.release_owner_token IS DISTINCT FROM p_release_owner_token
           OR v_existing.release_evidence IS DISTINCT FROM p_release_evidence
        THEN
            RETURN QUERY SELECT false, 'FINALIZATION_EVIDENCE_CONFLICT'::text;
            RETURN;
        END IF;
        RETURN QUERY SELECT true, 'ALREADY_RECORDED'::text;
        RETURN;
    END IF;

    -- New finalization requires the currently owned reservation.
    IF v_status.id IS NULL
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY SELECT false, 'RESERVATION_NOT_OWNED'::text;
        RETURN;
    END IF;

    -- Checkpoint has already sealed the journal under the same reservation lock.
    INSERT INTO public.manual_reconcile_finalizations (
        attempt_id, reservation_id, operation_token,
        remaining_mismatch_ledger_guids, final_recheck_completed,
        connector_release_verified, connector_id, release_owner_token,
        release_evidence
    ) VALUES (
        p_attempt_id, v_attempt.reservation_id, p_operation_token,
        p_remaining_mismatch_ledger_guids, p_final_recheck_completed,
        p_connector_release_verified, p_connector_id, p_release_owner_token,
        p_release_evidence
    );

    RETURN QUERY SELECT true, 'EVIDENCE_RECORDED'::text;
END;
$fn$;

CREATE OR REPLACE FUNCTION public.close_manual_reconcile_v2(
    p_attempt_id uuid, p_operation_token uuid
)
RETURNS TABLE(closed boolean, reason text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_final public.manual_reconcile_finalizations%ROWTYPE;
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_checkpoint public.manual_reconcile_pre_release_checkpoints%ROWTYPE;
BEGIN
    IF p_attempt_id IS NULL OR p_operation_token IS NULL THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text;
        RETURN;
    END IF;

    SELECT a.* INTO v_attempt FROM public.manual_reconcile_attempts a
    WHERE a.attempt_id = p_attempt_id;
    IF NOT FOUND OR v_attempt.operation_token IS DISTINCT FROM p_operation_token THEN
        RETURN QUERY SELECT false, 'ATTEMPT_NOT_FOUND'::text;
        RETURN;
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(v_attempt.company_code || ':' || v_attempt.tally_owner, 0)
    );
    SELECT s.* INTO v_status FROM public.manual_reconcile_status s
    WHERE s.id = v_attempt.reservation_id FOR UPDATE;
    SELECT f.* INTO v_final FROM public.manual_reconcile_finalizations f
    WHERE f.attempt_id = p_attempt_id FOR UPDATE;

    -- Historical replay never changes the current reservation.
    IF v_final.attempt_id IS NOT NULL
       AND v_final.state = 'CLOSED'
       AND v_final.operation_token = p_operation_token
       AND v_final.reservation_id = v_attempt.reservation_id
    THEN
        RETURN QUERY SELECT true, 'ALREADY_CLOSED'::text;
        RETURN;
    END IF;

    IF v_status.id IS NULL
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token IS DISTINCT FROM p_operation_token
       OR v_status.company_code IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY SELECT false, 'RESERVATION_NOT_OWNED'::text;
        RETURN;
    END IF;

    SELECT e.* INTO v_execution FROM public.manual_reconcile_executions e
    WHERE e.attempt_id = p_attempt_id;
    SELECT c.* INTO v_checkpoint FROM public.manual_reconcile_pre_release_checkpoints c
    WHERE c.attempt_id = p_attempt_id;

    IF v_final.attempt_id IS NULL OR v_final.state IS DISTINCT FROM 'EVIDENCE_RECORDED'
       OR v_final.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_final.operation_token IS DISTINCT FROM p_operation_token
       OR v_final.final_recheck_completed IS DISTINCT FROM TRUE
       OR v_final.connector_release_verified IS DISTINCT FROM TRUE
       OR v_final.outcome IS DISTINCT FROM
          public.manual_reconcile_final_outcome_v1(v_final.remaining_mismatch_ledger_guids)
       OR v_execution.attempt_id IS NULL
       OR v_checkpoint.attempt_id IS NULL
       OR v_checkpoint.reservation_id IS DISTINCT FROM v_attempt.reservation_id
       OR v_checkpoint.operation_token IS DISTINCT FROM p_operation_token
       OR v_checkpoint.execution_id IS DISTINCT FROM v_execution.execution_id
       OR v_checkpoint.connector_id IS DISTINCT FROM v_execution.connector_id
       OR v_checkpoint.owner_token IS DISTINCT FROM v_execution.owner_token
       OR v_checkpoint.remaining_mismatch_ledger_guids IS DISTINCT FROM v_final.remaining_mismatch_ledger_guids
       OR v_final.connector_id IS DISTINCT FROM v_execution.connector_id
       OR v_final.release_owner_token IS DISTINCT FROM v_execution.owner_token::text
       OR v_final.release_evidence->'redisReleaseConfirmed' IS DISTINCT FROM 'true'::jsonb
       OR v_final.release_evidence->>'attemptId' IS DISTINCT FROM p_attempt_id::text
       OR v_final.release_evidence->>'executionId' IS DISTINCT FROM v_execution.execution_id::text
       OR v_final.release_evidence->>'serverInstanceId' IS DISTINCT FROM v_execution.server_instance_id::text
       OR v_final.release_evidence->>'connectorId' IS DISTINCT FROM v_execution.connector_id
       OR v_final.release_evidence->>'socketId' IS DISTINCT FROM v_execution.socket_id
       OR v_final.release_evidence->>'ownerToken' IS DISTINCT FROM v_execution.owner_token::text
    THEN
        RETURN QUERY SELECT false, 'FINALIZATION_NOT_VERIFIED'::text;
        RETURN;
    END IF;

    -- Journal was sealed before Redis release; no post-release journal writes.
    UPDATE public.manual_reconcile_finalizations f
    SET state = 'CLOSED', sealed_at = transaction_timestamp(),
        closed_at = transaction_timestamp()
    WHERE f.attempt_id = p_attempt_id AND f.state = 'EVIDENCE_RECORDED';
    IF NOT FOUND THEN RAISE EXCEPTION 'RECONCILE_FINALIZATION_SEAL_FAILED'; END IF;

    UPDATE public.manual_reconcile_status s
    SET status = 'CLOSED', operation_token = NULL,
        closed_at = transaction_timestamp(), error_message = NULL,
        updated_at = transaction_timestamp()
    WHERE s.id = v_attempt.reservation_id
      AND s.status = 'RUNNING' AND s.operation_token = p_operation_token;
    IF NOT FOUND THEN RAISE EXCEPTION 'RECONCILE_ATOMIC_CLOSE_FAILED'; END IF;

    RETURN QUERY SELECT true, 'CLOSED'::text;
END;
$fn$;

ALTER FUNCTION public.record_manual_reconcile_finalization_v1(
    uuid,uuid,jsonb,boolean,boolean,text,text,jsonb
) OWNER TO postgres;
ALTER FUNCTION public.close_manual_reconcile_v2(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.record_manual_reconcile_finalization_v1(
    uuid,uuid,jsonb,boolean,boolean,text,text,jsonb
) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.close_manual_reconcile_v2(uuid,uuid)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.record_manual_reconcile_finalization_v1(
    uuid,uuid,jsonb,boolean,boolean,text,text,jsonb
) TO service_role;
GRANT EXECUTE ON FUNCTION public.close_manual_reconcile_v2(uuid,uuid)
TO service_role;
COMMIT;
