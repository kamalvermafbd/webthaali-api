
-- =========================================================
-- BILLEY: MANUAL RECONCILIATION BATCH CLOSURE FENCE
-- DRAFT — DO NOT DEPLOY YET
--
-- Scope: close_verified_sync_batch only.
-- Does not fence all direct sync_batches writers.
-- =========================================================

BEGIN;

CREATE OR REPLACE FUNCTION public.close_verified_sync_batch(
    p_batch_id text,
    p_execution_claim_token uuid
)
RETURNS boolean
LANGUAGE plpgsql
SET search_path TO ''
AS $function$
DECLARE
    v_company_code text;
    v_tally_owner text;
    v_closed boolean := false;
BEGIN
    IF NULLIF(BTRIM(p_batch_id), '') IS NULL
       OR p_execution_claim_token IS NULL THEN
        RAISE EXCEPTION 'BATCH_CLOSURE_INPUT_INVALID'
            USING ERRCODE = 'P0001';
    END IF;

    -- Read identity before acquiring locks.
    SELECT
        b.company_code,
        b.tally_owner
    INTO
        v_company_code,
        v_tally_owner
    FROM public.sync_batches AS b
    WHERE b.batch_id = p_batch_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'BATCH_CLOSURE_NOT_CONFIRMED'
            USING ERRCODE = 'P0001';
    END IF;

    -- Match existing FIFO/admission advisory lock order.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_company_code || ':' || v_tally_owner,
            0
        )
    );

    -- Acquire batch row lock only AFTER company advisory lock.
    PERFORM 1
    FROM public.sync_batches AS b
    WHERE b.batch_id = p_batch_id
      AND b.company_code = v_company_code
      AND b.tally_owner = v_tally_owner
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'BATCH_CLOSURE_NOT_CONFIRMED'
            USING ERRCODE = 'P0001';
    END IF;

    -- Do not close while manual reconciliation owns the batch.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_status AS m
        WHERE m.company_code = v_company_code
          AND m.tally_owner = v_tally_owner
          AND m.status = 'RUNNING'
    ) THEN
        RAISE EXCEPTION 'MANUAL_RECONCILIATION_ACTIVE'
            USING ERRCODE = 'P0001';
    END IF;

    -- Preserve existing verified closure conditions.
    UPDATE public.sync_batches AS b
    SET
        batch_status = 'CLOSED',
        batch_closed = true,
        reconciliation_completed = true,
        reconciliation_completed_at =
            NOW() AT TIME ZONE 'UTC',
        last_activity_at =
            NOW() AT TIME ZONE 'UTC',
        error_message = CASE
            WHEN COALESCE(b.error_message, '')
                 LIKE 'HTTP_OUTCOME_UNKNOWN:%'
            THEN NULL
            ELSE b.error_message
        END
    WHERE b.batch_id = p_batch_id
      AND b.company_code = v_company_code
      AND b.tally_owner = v_tally_owner
      AND b.execution_claim_token = p_execution_claim_token
      AND b.batch_status = 'COMPLETED'
      AND b.worker_status = 'COMPLETED'
      AND b.batch_closed IS FALSE
      AND b.ledger_reconciliation_completed IS TRUE
      AND b.stock_reconciliation_completed IS TRUE
      AND COALESCE(b.error_message, '')
          NOT LIKE 'CONNECTOR_OUTCOME_UNKNOWN:%'
    RETURNING true INTO v_closed;

    IF v_closed IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'BATCH_CLOSURE_NOT_CONFIRMED'
            USING ERRCODE = 'P0001';
    END IF;

    RETURN true;
END;
$function$;

COMMIT;
