# Deployed Sync SQL Evidence

Reference-only audit material. No production SQL or application code is modified.

## PART 1 — Batch Claim, Retry and Closure Functions

```json
[
  {
    "signature": "assert_sync_batch_execution_claim(text,uuid)",
    "definition": "CREATE OR REPLACE FUNCTION public.assert_sync_batch_execution_claim(p_batch_id text, p_execution_claim_token uuid)\n RETURNS void\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_batch_status text;\r\n    v_worker_status text;\r\n    v_execution_claim_token uuid;\r\nBEGIN\r\n    IF p_batch_id IS NULL\r\n       OR p_execution_claim_token IS NULL\r\n    THEN\r\n        RAISE EXCEPTION 'EXECUTION_CLAIM_LOST'\r\n            USING ERRCODE = 'P0001';\r\n    END IF;\r\n\r\n    SELECT\r\n        b.batch_status,\r\n        b.worker_status,\r\n        b.execution_claim_token\r\n    INTO\r\n        v_batch_status,\r\n        v_worker_status,\r\n        v_execution_claim_token\r\n    FROM public.sync_batches AS b\r\n    WHERE b.batch_id = p_batch_id\r\n    FOR UPDATE;\r\n\r\n    IF NOT FOUND\r\n       OR v_batch_status IS DISTINCT FROM 'PROCESSING'\r\n       OR v_worker_status IS DISTINCT FROM 'RUNNING'\r\n       OR v_execution_claim_token\r\n            IS DISTINCT FROM p_execution_claim_token\r\n    THEN\r\n        RAISE EXCEPTION 'EXECUTION_CLAIM_LOST'\r\n            USING ERRCODE = 'P0001';\r\n    END IF;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "claim_latest_connector_loss_retry(text,text,text,text,uuid)",
    "definition": "CREATE OR REPLACE FUNCTION public.claim_latest_connector_loss_retry(p_company_code text, p_tally_owner text, p_expected_batch_id text, p_worker_name text, p_retry_claim_token uuid)\n RETURNS TABLE(claimed boolean, reason text, claimed_batch_id text)\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\r\nDECLARE\r\n    v_batch public.sync_batches%ROWTYPE;\r\nBEGIN\r\n\r\n    -- Serialize retry claims for same company + Tally owner.\r\n    PERFORM pg_advisory_xact_lock(\r\n        hashtextextended(\r\n            p_company_code || ':' || p_tally_owner,\r\n            0\r\n        )\r\n    );\r\n\r\n    -- Lock exact batch.\r\n    SELECT b.*\r\n    INTO v_batch\r\n    FROM public.sync_batches AS b\r\n    WHERE b.company_code = p_company_code\r\n      AND b.tally_owner = p_tally_owner\r\n      AND b.batch_id = p_expected_batch_id\r\n    FOR UPDATE;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY\r\n        SELECT false, 'BATCH_NOT_FOUND'::text, NULL::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Only FAILED batches can be reserved for retry.\r\n    IF v_batch.batch_status IS DISTINCT FROM 'FAILED'\r\n       OR v_batch.worker_status IS DISTINCT FROM 'FAILED'\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'NOT_ELIGIBLE'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Never retry a batch with uncertain connector or HTTP outcome.\r\n    IF public.is_sync_outcome_unknown(v_batch.error_message)\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'OUTCOME_UNKNOWN'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Maximum 3 automatic retries.\r\n    IF COALESCE(v_batch.retry_count, 0) >= 3\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'MAX_RETRIES_REACHED'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Retry claim token is mandatory.\r\n    IF p_retry_claim_token IS NULL\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'RETRY_TOKEN_REQUIRED'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Retry only after watchdog confirms connector readiness.\r\n    IF v_batch.connector_retry_ready IS DISTINCT FROM true\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'CONNECTOR_NOT_RETRY_READY'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- FIFO: older eligible FAILED batch gets priority.\r\n    -- Uncertain batches must not block eligible retries.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'FAILED'\r\n          AND b.worker_status = 'FAILED'\r\n          AND b.connector_retry_ready = true\r\n          AND COALESCE(b.retry_count, 0) < 3\r\n          AND NOT public.is_sync_outcome_unknown(b.error_message)\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'EARLIER_FAILED_RETRY_READY'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- FIFO: older eligible PENDING batch gets priority.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'PENDING'\r\n          AND b.worker_status = 'PENDING'\r\n          AND b.worker_id IS NOT NULL\r\n          AND b.current_stage IS DISTINCT FROM 'WAITING_CONNECTOR'\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'EARLIER_BATCH_PENDING'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Do not retry while another batch is running.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = p_company_code\r\n          AND b.tally_owner = p_tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'PROCESSING'\r\n          AND b.worker_status = 'RUNNING'\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'ANOTHER_BATCH_RUNNING'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Reserve retry.\r\n    -- Do not increment retry_count here.\r\n    -- The worker claim RPC increments it on actual execution.\r\n    UPDATE public.sync_batches AS b\r\n    SET\r\n        batch_status = 'PENDING',\r\n        worker_status = 'PENDING',\r\n        worker_id = p_worker_name,\r\n        locked_at = now(),\r\n        retry_claim_token = p_retry_claim_token,\r\n        retry_lease_until = now() + INTERVAL '3 minutes',\r\n        heartbeat_at = NULL,\r\n        waiting_connector_since = NULL,\r\n        completed_at = NULL,\r\n        error_message = NULL,\r\n        current_stage = 'PENDING',\r\n        current_module = 'MASTERS',\r\n        current_action = 'PENDING'\r\n    WHERE b.id = v_batch.id\r\n      AND b.batch_status = 'FAILED'\r\n      AND b.worker_status = 'FAILED'\r\n      AND b.connector_retry_ready = true\r\n      AND COALESCE(b.retry_count, 0) < 3\r\n      AND NOT public.is_sync_outcome_unknown(b.error_message);\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'CLAIM_FAILED'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    RETURN QUERY\r\n    SELECT true, 'CLAIMED'::text, v_batch.batch_id;\r\n\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "claim_latest_connector_loss_retry(text,text,text,text)",
    "definition": "CREATE OR REPLACE FUNCTION public.claim_latest_connector_loss_retry(p_company_code text, p_tally_owner text, p_expected_batch_id text, p_worker_name text)\n RETURNS TABLE(claimed boolean, reason text, claimed_batch_id text)\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\r\nDECLARE\r\n    v_batch public.sync_batches%ROWTYPE;\r\nBEGIN\r\n    PERFORM pg_advisory_xact_lock(\r\n        hashtextextended(\r\n            p_company_code || ':' || p_tally_owner,\r\n            0\r\n        )\r\n    );\r\n\r\n    SELECT b.*\r\n    INTO v_batch\r\n    FROM public.sync_batches AS b\r\n    WHERE b.company_code = p_company_code\r\n      AND b.tally_owner = p_tally_owner\r\n      AND b.batch_id = p_expected_batch_id\r\n    FOR UPDATE;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY\r\n        SELECT false, 'BATCH_NOT_FOUND'::text, NULL::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.batch_status IS DISTINCT FROM 'FAILED'\r\n       OR v_batch.worker_status IS DISTINCT FROM 'FAILED'\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'NOT_ELIGIBLE'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF COALESCE(v_batch.error_message, '')\r\n       NOT LIKE 'CONNECTOR_LOSS:%'\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false, 'NOT_CONNECTOR_LOSS'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n        -- Retry only after watchdog confirms connector recovery.\r\n    IF v_batch.connector_retry_ready IS DISTINCT FROM true\r\n    THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'CONNECTOR_NOT_RETRY_READY'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- FIFO: older retry-ready CONNECTOR_LOSS batch first.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n\r\n          AND b.batch_status = 'FAILED'\r\n          AND b.worker_status = 'FAILED'\r\n          AND b.error_message LIKE 'CONNECTOR_LOSS:%'\r\n          AND b.connector_retry_ready = true\r\n\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'EARLIER_CONNECTOR_LOSS_RETRY_READY'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n        -- FIFO: older eligible PENDING batch gets priority.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n\r\n          AND b.batch_status = 'PENDING'\r\n          AND b.worker_status = 'PENDING'\r\n          AND b.worker_id IS NOT NULL\r\n          AND b.current_stage IS DISTINCT FROM 'WAITING_CONNECTOR'\r\n\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false,\r\n               'EARLIER_BATCH_PENDING'::text,\r\n               v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n    -- Do not retry while another batch is running.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = p_company_code\r\n          AND b.tally_owner = p_tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'PROCESSING'\r\n          AND b.worker_status = 'RUNNING'\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT false, 'ANOTHER_BATCH_RUNNING'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n        UPDATE public.sync_batches AS b\r\n    SET batch_status = 'PENDING',\r\n        worker_status = 'PENDING',\r\n        worker_id = p_worker_name,\r\n        locked_at = now(),\r\n        retry_claim_token = gen_random_uuid(),\r\n        retry_lease_until = now() + INTERVAL '3 minutes',\r\n        heartbeat_at = NULL,\r\nwaiting_connector_since = NULL,\r\n        completed_at = NULL,\r\n        error_message = NULL,\r\n        current_stage = 'PENDING',\r\n        current_module = 'MASTERS',\r\n        current_action = 'PENDING'\r\n    WHERE b.id = v_batch.id\r\n      AND b.batch_status = 'FAILED'\r\n      AND b.worker_status = 'FAILED'\r\n      AND b.error_message LIKE 'CONNECTOR_LOSS:%';\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY\r\n        SELECT false, 'CLAIM_FAILED'::text, v_batch.batch_id;\r\n        RETURN;\r\n    END IF;\r\n\r\n    RETURN QUERY\r\n    SELECT true, 'CLAIMED'::text, v_batch.batch_id;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "claim_or_create_http_sync_batch(text,text,text,text,text)",
    "definition": "CREATE OR REPLACE FUNCTION public.claim_or_create_http_sync_batch(p_company_code text, p_tally_owner text, p_request_id text, p_sync_mode text, p_sync_period text)\n RETURNS TABLE(batch_id text, claimed boolean, reason text, worker_id text)\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\r\n\r\nDECLARE\r\n    v_batch public.sync_batches%ROWTYPE;\r\n    v_batch_id text;\r\n    v_worker_type text;\r\n\r\nBEGIN\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * COMPANY + TALLY OWNER LOCK\r\n     * ---------------------------------------------------------\r\n     *\r\n     * Same company/tally ke liye ek time par\r\n     * sirf ek batch create/claim decision hoga.\r\n     */\r\n\r\n    PERFORM pg_advisory_xact_lock(\r\n        hashtextextended(\r\n            p_company_code || ':' || p_tally_owner,\r\n            0\r\n        )\r\n    );\r\n\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * CHECK ACTIVE BATCH\r\n     * ---------------------------------------------------------\r\n     *\r\n     * BullMQ architecture mein HTTP request kisi\r\n     * worker ko directly own nahi karegi.\r\n     *\r\n     * Existing active batch ko reuse kiya jayega.\r\n     */\r\n\r\n    SELECT b.*\r\n    INTO v_batch\r\n    FROM public.sync_batches AS b\r\n    WHERE b.company_code = p_company_code\r\n      AND b.tally_owner = p_tally_owner\r\n      AND b.batch_status IN (\r\n          'PENDING',\r\n          'PROCESSING',\r\n          'RUNNING'\r\n      )\r\n    ORDER BY b.created_at DESC, b.id DESC\r\n    LIMIT 1\r\n    FOR UPDATE;\r\n\r\n    IF FOUND THEN\r\n\r\n        RETURN QUERY\r\n        SELECT\r\n            v_batch.batch_id,\r\n            false,\r\n            'ALREADY_ACTIVE'::text,\r\n            v_batch.worker_id;\r\n\r\n        RETURN;\r\n\r\n    END IF;\r\n\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * BLOCK NEW SYNC WHEN TALLY OUTCOME IS UNCERTAIN\r\n     * ---------------------------------------------------------\r\n     *\r\n     * Agar same company + Tally owner ka koi batch\r\n     * uncertain outcome mein hai, toh naya HTTP sync\r\n     * create nahi hoga.\r\n     *\r\n     * Manual review required.\r\n     */\r\n\r\n    SELECT b.*\r\n    INTO v_batch\r\n    FROM public.sync_batches AS b\r\n    WHERE b.company_code = p_company_code\r\n      AND b.tally_owner = p_tally_owner\r\n      AND public.is_sync_outcome_unknown(b.error_message)\r\n    ORDER BY b.created_at DESC, b.id DESC\r\n    LIMIT 1\r\n    FOR UPDATE;\r\n\r\n    IF FOUND THEN\r\n\r\n        RETURN QUERY\r\n        SELECT\r\n            v_batch.batch_id,\r\n            false,\r\n            'OUTCOME_UNKNOWN'::text,\r\n            v_batch.worker_id;\r\n\r\n        RETURN;\r\n\r\n    END IF;\r\n\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * DETERMINE SYNC TYPE\r\n     * ---------------------------------------------------------\r\n     *\r\n     * CLOSED = successful previous sync\r\n     *\r\n     * No CLOSED batch:\r\n     * INITIAL_SYNC\r\n     *\r\n     * Existing CLOSED batch:\r\n     * NORMAL_SYNC\r\n     */\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches\r\n        WHERE company_code = p_company_code\r\n          AND tally_owner = p_tally_owner\r\n          AND batch_status = 'CLOSED'\r\n    ) THEN\r\n\r\n        v_worker_type := 'NORMAL_SYNC';\r\n\r\n    ELSE\r\n\r\n        v_worker_type := 'INITIAL_SYNC';\r\n\r\n    END IF;\r\n\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * CREATE NEW QUEUED BATCH\r\n     * ---------------------------------------------------------\r\n     *\r\n     * IMPORTANT:\r\n     *\r\n     * worker_id      = NULL\r\n     * worker_status  = PENDING\r\n     * batch_status   = PENDING\r\n     * current_stage  = PENDING\r\n     *\r\n     * Worker assignment BullMQ dispatcher karega.\r\n     */\r\n\r\n    INSERT INTO public.sync_batches (\r\n        batch_id,\r\n        company_code,\r\n        tally_owner,\r\n        batch_status,\r\n        current_stage,\r\n        current_module,\r\n        current_action,\r\n        worker_status,\r\n        worker_id,\r\n        locked_at,\r\n        heartbeat_at,\r\n        worker_type,\r\n        sync_mode,\r\n        sync_period\r\n    )\r\n    VALUES (\r\n        gen_random_uuid()::text,\r\n        p_company_code,\r\n        p_tally_owner,\r\n        'PENDING',\r\n        'PENDING',\r\n        'MASTERS',\r\n        'PENDING',\r\n        'PENDING',\r\n        NULL,\r\n        NULL,\r\n        NULL,\r\n        v_worker_type,\r\n        p_sync_mode,\r\n        p_sync_period\r\n    )\r\n    RETURNING\r\n        sync_batches.batch_id\r\n    INTO\r\n        v_batch_id;\r\n\r\n\r\n    /*\r\n     * ---------------------------------------------------------\r\n     * RETURN NEW BATCH\r\n     * ---------------------------------------------------------\r\n     */\r\n\r\n    RETURN QUERY\r\n    SELECT\r\n        v_batch_id,\r\n        true,\r\n        'CREATED'::text,\r\n        NULL::text;\r\n\r\nEND;\r\n\r\n$function$\n"
  },
  {
    "signature": "claim_sync_batch_fifo_v2(bigint,text,uuid)",
    "definition": "CREATE OR REPLACE FUNCTION public.claim_sync_batch_fifo_v2(p_batch_db_id bigint, p_worker_name text, p_retry_claim_token uuid)\n RETURNS TABLE(claimed boolean, reason text, execution_claim_token uuid)\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\r\nDECLARE\r\n    v_batch public.sync_batches%ROWTYPE;\r\n    v_execution_token uuid;\r\nBEGIN\r\n    SELECT *\r\n    INTO v_batch\r\n    FROM public.sync_batches\r\n    WHERE id = p_batch_db_id;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY SELECT false, 'BATCH_NOT_FOUND'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    PERFORM pg_advisory_xact_lock(\r\n        hashtextextended(\r\n            v_batch.company_code || ':' || v_batch.tally_owner,\r\n            0\r\n        )\r\n    );\r\n\r\n    SELECT *\r\n    INTO v_batch\r\n    FROM public.sync_batches\r\n    WHERE id = p_batch_db_id\r\n    FOR UPDATE;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY SELECT false, 'BATCH_NOT_FOUND'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Block both uncertain outcomes before claiming.\r\n    IF public.is_sync_outcome_unknown(v_batch.error_message) THEN\r\n        RETURN QUERY SELECT false, 'OUTCOME_UNKNOWN'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- B03 FIX 1: Require an explicitly PENDING stage.\r\n    IF v_batch.batch_status IS DISTINCT FROM 'PENDING'\r\n       OR v_batch.worker_status IS DISTINCT FROM 'PENDING'\r\n       OR v_batch.current_stage IS DISTINCT FROM 'PENDING'\r\n       OR v_batch.worker_id IS DISTINCT FROM p_worker_name\r\n    THEN\r\n        RETURN QUERY SELECT false, 'NOT_ELIGIBLE'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.retry_claim_token IS DISTINCT FROM p_retry_claim_token\r\n    THEN\r\n        RETURN QUERY SELECT false, 'RETRY_TOKEN_MISMATCH'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.retry_claim_token IS NOT NULL\r\n       AND (\r\n           v_batch.retry_lease_until IS NULL\r\n           OR v_batch.retry_lease_until <= now()\r\n       )\r\n    THEN\r\n        RETURN QUERY SELECT false, 'RETRY_LEASE_EXPIRED'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.retry_claim_token IS NOT NULL\r\n       AND COALESCE(v_batch.retry_count, 0) >= 3\r\n    THEN\r\n        RETURN QUERY SELECT false, 'MAX_RETRIES_REACHED'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'PROCESSING'\r\n          AND b.worker_status = 'RUNNING'\r\n    ) THEN\r\n        RETURN QUERY SELECT false, 'ANOTHER_BATCH_RUNNING'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'FAILED'\r\n          AND b.worker_status = 'FAILED'\r\n          AND b.connector_retry_ready = true\r\n          AND COALESCE(b.retry_count, 0) < 3\r\n          AND NOT public.is_sync_outcome_unknown(b.error_message)\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY\r\n        SELECT\r\n            false,\r\n            'EARLIER_CONNECTOR_LOSS_RETRY_READY'::text,\r\n            NULL::uuid;\r\n\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.sync_batches AS b\r\n        WHERE b.company_code = v_batch.company_code\r\n          AND b.tally_owner = v_batch.tally_owner\r\n          AND b.id <> v_batch.id\r\n          AND b.batch_status = 'PENDING'\r\n          AND b.worker_status = 'PENDING'\r\n          AND b.worker_id IS NOT NULL\r\n          AND b.current_stage IS DISTINCT FROM 'WAITING_CONNECTOR'\r\n          AND (\r\n              b.created_at < v_batch.created_at\r\n              OR (\r\n                  b.created_at = v_batch.created_at\r\n                  AND b.id < v_batch.id\r\n              )\r\n          )\r\n    ) THEN\r\n        RETURN QUERY SELECT false, 'EARLIER_BATCH_PENDING'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    UPDATE public.sync_batches AS b\r\n    SET\r\n        batch_status = 'PROCESSING',\r\n        worker_status = 'RUNNING',\r\n        worker_id = p_worker_name,\r\n        locked_at = now(),\r\n        started_at = now(),\r\n        heartbeat_at = now(),\r\n\r\n        execution_claim_token = gen_random_uuid(),\r\n\r\n        -- Reset reconciliation results for the new execution.\r\n        ledger_reconciliation_completed = false,\r\n        stock_reconciliation_completed = false,\r\n\r\n        retry_count = CASE\r\n            WHEN b.retry_claim_token IS NOT NULL\r\n            THEN COALESCE(b.retry_count, 0) + 1\r\n            ELSE COALESCE(b.retry_count, 0)\r\n        END\r\n    WHERE b.id = p_batch_db_id\r\n      AND b.batch_status = 'PENDING'\r\n      AND b.worker_status = 'PENDING'\r\n\r\n      -- B03 FIX 2: Enforce stage at the atomic UPDATE.\r\n      AND b.current_stage = 'PENDING'\r\n\r\n      AND b.worker_id = p_worker_name\r\n      AND b.retry_claim_token IS NOT DISTINCT FROM p_retry_claim_token\r\n      AND NOT public.is_sync_outcome_unknown(b.error_message)\r\n      AND (\r\n          b.retry_claim_token IS NULL\r\n          OR (\r\n              b.retry_lease_until > now()\r\n              AND COALESCE(b.retry_count, 0) < 3\r\n          )\r\n      )\r\n    RETURNING b.execution_claim_token INTO v_execution_token;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY SELECT false, 'CLAIM_FAILED'::text, NULL::uuid;\r\n        RETURN;\r\n    END IF;\r\n\r\n    RETURN QUERY SELECT true, 'CLAIMED'::text, v_execution_token;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "close_verified_sync_batch(text,uuid)",
    "definition": "CREATE OR REPLACE FUNCTION public.close_verified_sync_batch(p_batch_id text, p_execution_claim_token uuid)\n RETURNS boolean\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_closed boolean := false;\r\nBEGIN\r\n    UPDATE public.sync_batches AS b\r\n    SET\r\n        batch_status = 'CLOSED',\r\n        batch_closed = true,\r\n        reconciliation_completed = true,\r\n        reconciliation_completed_at = NOW() AT TIME ZONE 'UTC',\r\n        last_activity_at = NOW() AT TIME ZONE 'UTC',\r\n        error_message = CASE\r\n            WHEN COALESCE(b.error_message, '')\r\n                 LIKE 'HTTP_OUTCOME_UNKNOWN:%'\r\n            THEN NULL\r\n            ELSE b.error_message\r\n        END\r\n    WHERE b.batch_id = p_batch_id\r\n      AND b.execution_claim_token = p_execution_claim_token\r\n      AND b.batch_status = 'COMPLETED'\r\n      AND b.worker_status = 'COMPLETED'\r\n      AND b.batch_closed IS FALSE\r\n      AND b.ledger_reconciliation_completed IS TRUE\r\n      AND b.stock_reconciliation_completed IS TRUE\r\n      AND COALESCE(b.error_message, '')\r\n          NOT LIKE 'CONNECTOR_OUTCOME_UNKNOWN:%'\r\n    RETURNING true INTO v_closed;\r\n\r\n    IF v_closed IS DISTINCT FROM TRUE THEN\r\n        RAISE EXCEPTION 'BATCH_CLOSURE_NOT_CONFIRMED'\r\n            USING ERRCODE = 'P0001';\r\n    END IF;\r\n\r\n    RETURN true;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "recover_expired_connector_retry(text,uuid)",
    "definition": "CREATE OR REPLACE FUNCTION public.recover_expired_connector_retry(p_batch_id text, p_retry_claim_token uuid)\n RETURNS TABLE(recovered boolean, reason text)\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\r\nDECLARE\r\n    v_batch public.sync_batches%ROWTYPE;\r\n    v_company_code text;\r\n    v_tally_owner text;\r\nBEGIN\r\n    -- Read identity for the same advisory lock used by retry claim.\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO v_company_code, v_tally_owner\r\n    FROM public.sync_batches AS b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY SELECT false, 'BATCH_NOT_FOUND'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    PERFORM pg_advisory_xact_lock(\r\n        hashtextextended(\r\n            v_company_code || ':' || v_tally_owner,\r\n            0\r\n        )\r\n    );\r\n\r\n    SELECT b.*\r\n    INTO v_batch\r\n    FROM public.sync_batches AS b\r\n    WHERE b.batch_id = p_batch_id\r\n    FOR UPDATE;\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY SELECT false, 'BATCH_NOT_FOUND'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    -- Never recover either uncertain outcome automatically.\r\n    IF public.is_sync_outcome_unknown(v_batch.error_message) THEN\r\n        RETURN QUERY SELECT false, 'OUTCOME_UNKNOWN'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.batch_status IS DISTINCT FROM 'PENDING'\r\n       OR v_batch.worker_status IS DISTINCT FROM 'PENDING'\r\n       OR v_batch.current_stage IS DISTINCT FROM 'PENDING'\r\n    THEN\r\n        RETURN QUERY SELECT false, 'STATE_CHANGED'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.retry_claim_token IS DISTINCT FROM p_retry_claim_token\r\n       OR p_retry_claim_token IS NULL\r\n    THEN\r\n        RETURN QUERY SELECT false, 'TOKEN_MISMATCH'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    IF v_batch.retry_lease_until IS NULL\r\n       OR v_batch.retry_lease_until >= now()\r\n    THEN\r\n        RETURN QUERY SELECT false, 'LEASE_NOT_EXPIRED'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    UPDATE public.sync_batches AS b\r\n    SET\r\n        batch_status = 'FAILED',\r\n        worker_status = 'FAILED',\r\n        worker_id = NULL,\r\n        locked_at = NULL,\r\n        heartbeat_at = NULL,\r\n        retry_claim_token = NULL,\r\n        retry_lease_until = NULL,\r\n        connector_retry_ready = false,\r\n        error_message =\r\n            'CONNECTOR_LOSS: Retry reservation expired before worker claim',\r\n        current_stage = 'FAILED',\r\n        current_action = 'FAILED',\r\n        completed_at = now()\r\n    WHERE b.id = v_batch.id\r\n      AND b.batch_status = 'PENDING'\r\n      AND b.worker_status = 'PENDING'\r\n      AND b.current_stage = 'PENDING'\r\n      AND b.retry_claim_token = p_retry_claim_token\r\n      AND b.retry_lease_until < now()\r\n      AND NOT public.is_sync_outcome_unknown(b.error_message);\r\n\r\n    IF NOT FOUND THEN\r\n        RETURN QUERY\r\n        SELECT false, 'RECOVERY_CONDITION_CHANGED'::text;\r\n        RETURN;\r\n    END IF;\r\n\r\n    RETURN QUERY SELECT true, 'RECOVERED'::text;\r\nEND;\r\n$function$\n"
  }
]
```

## PART 2 — Fenced Mutation Functions

```json
[
  {
    "signature": "fenced_opening_balance_deactivate(text,uuid,bigint[],text,timestamp with time zone)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_opening_balance_deactivate(p_batch_id text, p_execution_claim_token uuid, p_ids bigint[], p_sync_batch_id text, p_updated_at timestamp with time zone)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_count integer := 0;\r\n    v_company_code text;\r\n    v_tally_owner text;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO v_company_code, v_tally_owner\r\n    FROM public.sync_batches b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    IF NOT FOUND THEN\r\n        RAISE EXCEPTION 'Opening balance batch scope not found';\r\n    END IF;\r\n\r\n    IF p_sync_batch_id IS DISTINCT FROM p_batch_id THEN\r\n        RAISE EXCEPTION 'Opening balance sync batch mismatch';\r\n    END IF;\r\n\r\n    IF p_ids IS NULL THEN\r\n        RAISE EXCEPTION 'Opening balance IDs cannot be null';\r\n    END IF;\r\n\r\n    IF cardinality(p_ids) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.opening_balance_allocations a\r\n        WHERE a.id = ANY(p_ids)\r\n          AND (\r\n              a.company_code IS DISTINCT FROM v_company_code\r\n              OR a.tally_owner IS DISTINCT FROM v_tally_owner\r\n              OR a.source_type IS DISTINCT FROM 'TALLY'\r\n          )\r\n    ) THEN\r\n        RAISE EXCEPTION 'Opening balance deactivate scope mismatch';\r\n    END IF;\r\n\r\n    UPDATE public.opening_balance_allocations\r\n    SET\r\n        is_active = false,\r\n        updated_at = p_updated_at,\r\n        sync_batch_id = p_sync_batch_id\r\n    WHERE id = ANY(p_ids)\r\n      AND company_code = v_company_code\r\n      AND tally_owner = v_tally_owner\r\n      AND source_type = 'TALLY';\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_opening_balance_upsert(text,uuid,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_opening_balance_upsert(p_batch_id text, p_execution_claim_token uuid, p_rows jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_count integer := 0;\r\nBEGIN\r\n\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    IF p_rows IS NULL\r\n       OR jsonb_typeof(p_rows) <> 'array'\r\n    THEN\r\n        RAISE EXCEPTION 'Invalid opening balance rows';\r\n    END IF;\r\n\r\n    IF jsonb_array_length(p_rows) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM jsonb_to_recordset(p_rows) AS r(\r\n            company_code text,\r\n            tally_owner text,\r\n            source_type text\r\n        )\r\n        WHERE r.source_type IS DISTINCT FROM 'TALLY'\r\n           OR NOT EXISTS (\r\n               SELECT 1\r\n               FROM public.sync_batches b\r\n               WHERE b.batch_id = p_batch_id\r\n                 AND b.company_code = r.company_code\r\n                 AND b.tally_owner = r.tally_owner\r\n           )\r\n    ) THEN\r\n        RAISE EXCEPTION\r\n            'Opening balance batch scope mismatch';\r\n    END IF;\r\n\r\n    INSERT INTO public.opening_balance_allocations (\r\n        company_code,\r\n        tally_owner,\r\n        ledger_guid,\r\n        bill_name,\r\n        bill_date,\r\n        due_date,\r\n        amount,\r\n        balance_type,\r\n        source_type,\r\n        is_active,\r\n        sync_batch_id\r\n    )\r\n    SELECT\r\n        r.company_code,\r\n        r.tally_owner,\r\n        r.ledger_guid,\r\n        r.bill_name,\r\n        r.bill_date,\r\n        r.due_date,\r\n        r.amount,\r\n        r.balance_type,\r\n        r.source_type,\r\n        r.is_active,\r\n        r.sync_batch_id\r\n    FROM jsonb_to_recordset(p_rows) AS r(\r\n        company_code text,\r\n        tally_owner text,\r\n        ledger_guid text,\r\n        bill_name text,\r\n        bill_date date,\r\n        due_date date,\r\n        amount numeric,\r\n        balance_type text,\r\n        source_type text,\r\n        is_active boolean,\r\n        sync_batch_id text\r\n    )\r\n    ON CONFLICT (\r\n        company_code,\r\n        tally_owner,\r\n        ledger_guid,\r\n        bill_name,\r\n        source_type\r\n    )\r\n    DO UPDATE SET\r\n        company_code = EXCLUDED.company_code,\r\n        tally_owner = EXCLUDED.tally_owner,\r\n        ledger_guid = EXCLUDED.ledger_guid,\r\n        bill_name = EXCLUDED.bill_name,\r\n        source_type = EXCLUDED.source_type,\r\n        bill_date = EXCLUDED.bill_date,\r\n        due_date = EXCLUDED.due_date,\r\n        amount = EXCLUDED.amount,\r\n        balance_type = EXCLUDED.balance_type,\r\n        is_active = EXCLUDED.is_active,\r\n        sync_batch_id = EXCLUDED.sync_batch_id;\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\n\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_snapshot_remove_missing(text,uuid,text,text,text,text)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_snapshot_remove_missing(p_batch_id text, p_execution_claim_token uuid, p_company_code text, p_tally_owner text, p_module text, p_entity_type text)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_count integer;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    SELECT company_code, tally_owner\r\n    INTO STRICT v_company_code, v_tally_owner\r\n    FROM public.sync_batches\r\n    WHERE batch_id = p_batch_id;\r\n\r\n    IF p_company_code IS DISTINCT FROM v_company_code\r\n       OR p_tally_owner IS DISTINCT FROM v_tally_owner\r\n       OR NULLIF(BTRIM(p_module), '') IS NULL\r\n       OR NULLIF(BTRIM(p_entity_type), '') IS NULL\r\n    THEN\r\n        RAISE EXCEPTION 'Snapshot cleanup scope mismatch';\r\n    END IF;\r\n\r\n    UPDATE public.tally_sync_snapshot\r\n    SET\r\n        is_deleted = true,\r\n        status = 'DELETED',\r\n        updated_at = NOW()\r\n    WHERE company_code = p_company_code\r\n      AND tally_owner = p_tally_owner\r\n      AND module = p_module\r\n      AND entity_type = p_entity_type\r\n      AND last_sync_batch_id <> p_batch_id\r\n      AND is_deleted = false;\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_snapshot_upsert(text,uuid,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_snapshot_upsert(p_batch_id text, p_execution_claim_token uuid, p_rows jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_count integer;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    SELECT company_code, tally_owner\r\n    INTO STRICT v_company_code, v_tally_owner\r\n    FROM public.sync_batches\r\n    WHERE batch_id = p_batch_id;\r\n\r\n    IF p_rows IS NULL\r\n       OR jsonb_typeof(p_rows) <> 'array' THEN\r\n        RAISE EXCEPTION 'Invalid snapshot rows';\r\n    END IF;\r\n\r\n    IF jsonb_array_length(p_rows) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM jsonb_to_recordset(p_rows) AS r(\r\n            company_code text,\r\n            tally_owner text,\r\n            sync_batch_id text,\r\n            last_sync_batch_id text,\r\n            module text,\r\n            entity_type text,\r\n            guid text\r\n        )\r\n        WHERE r.company_code IS DISTINCT FROM v_company_code\r\n           OR r.tally_owner IS DISTINCT FROM v_tally_owner\r\n           OR r.sync_batch_id IS DISTINCT FROM p_batch_id\r\n           OR r.last_sync_batch_id IS DISTINCT FROM p_batch_id\r\n           OR NULLIF(BTRIM(r.module), '') IS NULL\r\n           OR NULLIF(BTRIM(r.entity_type), '') IS NULL\r\n           OR NULLIF(BTRIM(r.guid), '') IS NULL\r\n    ) THEN\r\n        RAISE EXCEPTION 'Snapshot batch or scope mismatch';\r\n    END IF;\r\n\r\n    WITH input_rows AS (\r\n        SELECT *\r\n        FROM jsonb_to_recordset(p_rows) AS r(\r\n            sync_batch_id text,\r\n            last_sync_batch_id text,\r\n            company_code text,\r\n            tally_owner text,\r\n            module text,\r\n            entity_type text,\r\n            guid text,\r\n            alter_id bigint,\r\n            master_id text,\r\n            status text,\r\n            is_deleted boolean\r\n        )\r\n    ),\r\n    upserted AS (\r\n        INSERT INTO public.tally_sync_snapshot (\r\n            sync_batch_id,\r\n            last_sync_batch_id,\r\n            company_code,\r\n            tally_owner,\r\n            module,\r\n            entity_type,\r\n            guid,\r\n            alter_id,\r\n            master_id,\r\n            status,\r\n            is_deleted,\r\n            updated_at\r\n        )\r\n        SELECT\r\n            sync_batch_id,\r\n            last_sync_batch_id,\r\n            company_code,\r\n            tally_owner,\r\n            module,\r\n            entity_type,\r\n            guid,\r\n            alter_id,\r\n            master_id,\r\n            status,\r\n            is_deleted,\r\n            NOW()\r\n        FROM input_rows\r\n        ON CONFLICT (\r\n            company_code,\r\n            tally_owner,\r\n            module,\r\n            entity_type,\r\n            guid\r\n        )\r\n        DO UPDATE SET\r\n            sync_batch_id = EXCLUDED.sync_batch_id,\r\n            last_sync_batch_id = EXCLUDED.last_sync_batch_id,\r\n            alter_id = EXCLUDED.alter_id,\r\n            master_id = EXCLUDED.master_id,\r\n            status = EXCLUDED.status,\r\n            is_deleted = EXCLUDED.is_deleted,\r\n            updated_at = NOW()\r\n        RETURNING 1\r\n    )\r\n    SELECT COUNT(*) INTO v_count\r\n    FROM upserted;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_stock_opening_balance_deactivate(text,uuid,bigint[],text,timestamp with time zone)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_stock_opening_balance_deactivate(p_batch_id text, p_execution_claim_token uuid, p_ids bigint[], p_sync_batch_id text, p_updated_at timestamp with time zone)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_count integer := 0;\r\n    v_company_code text;\r\n    v_tally_owner text;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO v_company_code, v_tally_owner\r\n    FROM public.sync_batches b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    IF NOT FOUND THEN\r\n        RAISE EXCEPTION 'Stock opening balance batch scope not found';\r\n    END IF;\r\n\r\n    IF p_sync_batch_id IS DISTINCT FROM p_batch_id THEN\r\n        RAISE EXCEPTION 'Stock opening balance sync batch mismatch';\r\n    END IF;\r\n\r\n    IF p_ids IS NULL THEN\r\n        RAISE EXCEPTION 'Stock opening balance IDs cannot be null';\r\n    END IF;\r\n\r\n    IF cardinality(p_ids) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM public.tally_stock_opening_balances s\r\n        WHERE s.id = ANY(p_ids)\r\n          AND (\r\n              s.company_code IS DISTINCT FROM v_company_code\r\n              OR s.tally_owner IS DISTINCT FROM v_tally_owner\r\n              OR s.source_type IS DISTINCT FROM 'TALLY'\r\n          )\r\n    ) THEN\r\n        RAISE EXCEPTION 'Stock opening balance deactivate scope mismatch';\r\n    END IF;\r\n\r\n    UPDATE public.tally_stock_opening_balances\r\n    SET\r\n        is_active = false,\r\n        updated_at = p_updated_at,\r\n        sync_batch_id = p_sync_batch_id\r\n    WHERE id = ANY(p_ids)\r\n      AND company_code = v_company_code\r\n      AND tally_owner = v_tally_owner\r\n      AND source_type = 'TALLY';\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_stock_opening_balance_upsert(text,uuid,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_stock_opening_balance_upsert(p_batch_id text, p_execution_claim_token uuid, p_rows jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_count integer := 0;\r\nBEGIN\r\n    -- Claim validation and write happen in one transaction.\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    IF p_rows IS NULL OR jsonb_typeof(p_rows) <> 'array' THEN\r\n        RAISE EXCEPTION 'Invalid stock opening balance rows';\r\n    END IF;\r\n\r\n    IF jsonb_array_length(p_rows) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    -- Prevent writes outside the claimed batch's company/owner.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM jsonb_to_recordset(p_rows) AS r(\r\n            company_code text,\r\n            tally_owner text,\r\n            source_type text,\r\n            sync_batch_id text\r\n        )\r\n        WHERE r.source_type IS DISTINCT FROM 'TALLY'\r\n           OR r.sync_batch_id IS DISTINCT FROM p_batch_id\r\n           OR NOT EXISTS (\r\n               SELECT 1\r\n               FROM public.sync_batches b\r\n               WHERE b.batch_id = p_batch_id\r\n                 AND b.company_code = r.company_code\r\n                 AND b.tally_owner = r.tally_owner\r\n           )\r\n    ) THEN\r\n        RAISE EXCEPTION 'Stock opening balance batch scope mismatch';\r\n    END IF;\r\n\r\n    INSERT INTO public.tally_stock_opening_balances (\r\n        company_code,\r\n        tally_owner,\r\n        stock_guid,\r\n        stock_masterid,\r\n        stock_alterid,\r\n        godown_name,\r\n        batch_name,\r\n        opening_balance,\r\n        opening_rate,\r\n        opening_value,\r\n        source_type,\r\n        sync_batch_id,\r\n        is_active\r\n    )\r\n    SELECT\r\n        r.company_code,\r\n        r.tally_owner,\r\n        r.stock_guid,\r\n        r.stock_masterid,\r\n        r.stock_alterid,\r\n        r.godown_name,\r\n        r.batch_name,\r\n        r.opening_balance,\r\n        r.opening_rate,\r\n        r.opening_value,\r\n        r.source_type,\r\n        r.sync_batch_id,\r\n        r.is_active\r\n    FROM jsonb_to_recordset(p_rows) AS r(\r\n        company_code text,\r\n        tally_owner text,\r\n        stock_guid text,\r\n        stock_masterid text,\r\n        stock_alterid bigint,\r\n        godown_name text,\r\n        batch_name text,\r\n        opening_balance numeric,\r\n        opening_rate numeric,\r\n        opening_value numeric,\r\n        source_type text,\r\n        sync_batch_id text,\r\n        is_active boolean\r\n    )\r\n    ON CONFLICT (\r\n        company_code,\r\n        tally_owner,\r\n        stock_guid,\r\n        godown_name,\r\n        batch_name,\r\n        source_type\r\n    )\r\n    DO UPDATE SET\r\n        company_code = EXCLUDED.company_code,\r\n        tally_owner = EXCLUDED.tally_owner,\r\n        stock_guid = EXCLUDED.stock_guid,\r\n        stock_masterid = EXCLUDED.stock_masterid,\r\n        stock_alterid = EXCLUDED.stock_alterid,\r\n        godown_name = EXCLUDED.godown_name,\r\n        batch_name = EXCLUDED.batch_name,\r\n        opening_balance = EXCLUDED.opening_balance,\r\n        opening_rate = EXCLUDED.opening_rate,\r\n        opening_value = EXCLUDED.opening_value,\r\n        source_type = EXCLUDED.source_type,\r\n        sync_batch_id = EXCLUDED.sync_batch_id,\r\n        is_active = EXCLUDED.is_active;\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_sync_delete(text,uuid,text,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_sync_delete(p_batch_id text, p_execution_claim_token uuid, p_table text, p_filters jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_filter jsonb;\r\n    v_column text;\r\n    v_type text;\r\n    v_value jsonb;\r\n    v_filter_count integer := 0;\r\n    v_company_seen boolean := false;\r\n    v_owner_seen boolean := false;\r\n    v_guid_seen boolean := false;\r\n    v_guids text[];\r\n    v_sql text;\r\n    v_deleted integer;\r\nBEGIN\r\n    -- 1. Validate execution claim before any DELETE.\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    -- 2. Get authoritative batch scope.\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO STRICT v_company_code, v_tally_owner\r\n    FROM public.sync_batches AS b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    -- 3. Only voucher child tables can be deleted.\r\n    IF p_table NOT IN (\r\n        'tally_voucher_ledgers',\r\n        'tally_voucher_inventory',\r\n        'tally_voucher_inventory_godowns',\r\n        'tally_stock_vouchers',\r\n        'tally_bill_allocations',\r\n        'tally_costcentre_allocations'\r\n    ) THEN\r\n        RAISE EXCEPTION 'Unsupported DELETE table: %', p_table;\r\n    END IF;\r\n\r\n    -- 4. Require exactly three scoped filters.\r\n    IF jsonb_typeof(p_filters) IS DISTINCT FROM 'array'\r\n       OR jsonb_array_length(p_filters) <> 3 THEN\r\n        RAISE EXCEPTION 'DELETE requires exactly three filters';\r\n    END IF;\r\n\r\n    FOR v_filter IN\r\n        SELECT value FROM jsonb_array_elements(p_filters)\r\n    LOOP\r\n        v_filter_count := v_filter_count + 1;\r\n\r\n        IF jsonb_typeof(v_filter) IS DISTINCT FROM 'object'\r\n           OR (SELECT count(*) FROM jsonb_object_keys(v_filter)) <> 3\r\n           OR NOT (\r\n               v_filter ? 'column'\r\n               AND v_filter ? 'type'\r\n               AND v_filter ? 'value'\r\n           ) THEN\r\n            RAISE EXCEPTION 'Invalid DELETE filter';\r\n        END IF;\r\n\r\n        v_column := v_filter->>'column';\r\n        v_type := v_filter->>'type';\r\n        v_value := v_filter->'value';\r\n\r\n        IF v_column = 'company_code'\r\n           AND v_type = 'eq'\r\n           AND NOT v_company_seen THEN\r\n\r\n            IF jsonb_typeof(v_value) <> 'string'\r\n               OR v_value #>> '{}' IS DISTINCT FROM v_company_code THEN\r\n                RAISE EXCEPTION 'DELETE company scope mismatch';\r\n            END IF;\r\n\r\n            v_company_seen := true;\r\n\r\n        ELSIF v_column = 'tally_owner'\r\n           AND v_type = 'eq'\r\n           AND NOT v_owner_seen THEN\r\n\r\n            IF jsonb_typeof(v_value) <> 'string'\r\n               OR v_value #>> '{}' IS DISTINCT FROM v_tally_owner THEN\r\n                RAISE EXCEPTION 'DELETE owner scope mismatch';\r\n            END IF;\r\n\r\n            v_owner_seen := true;\r\n\r\n        ELSIF v_column = 'voucher_guid'\r\n           AND v_type = 'in'\r\n           AND NOT v_guid_seen THEN\r\n\r\n            IF jsonb_typeof(v_value) <> 'array' THEN\r\n                RAISE EXCEPTION 'DELETE voucher_guid must be an array';\r\n            END IF;\r\n\r\n            IF EXISTS (\r\n                SELECT 1\r\n                FROM jsonb_array_elements(v_value) AS e(value)\r\n                WHERE jsonb_typeof(e.value) <> 'string'\r\n                   OR e.value #>> '{}' = ''\r\n            ) THEN\r\n                RAISE EXCEPTION 'Invalid voucher GUID';\r\n            END IF;\r\n\r\n            SELECT array_agg(e.value #>> '{}')\r\n            INTO v_guids\r\n            FROM jsonb_array_elements(v_value) AS e(value);\r\n\r\n            v_guids := COALESCE(v_guids, ARRAY[]::text[]);\r\n            v_guid_seen := true;\r\n\r\n        ELSE\r\n            RAISE EXCEPTION\r\n                'Unsupported or duplicate DELETE filter: % %',\r\n                v_column, v_type;\r\n        END IF;\r\n    END LOOP;\r\n\r\n    IF NOT (v_company_seen AND v_owner_seen AND v_guid_seen) THEN\r\n        RAISE EXCEPTION 'DELETE scope filters missing';\r\n    END IF;\r\n\r\n    IF cardinality(v_guids) = 0 THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    -- 5. Execute scoped DELETE.\r\n    v_sql := format(\r\n        'DELETE FROM public.%I\r\n         WHERE company_code = $1\r\n           AND tally_owner = $2\r\n           AND voucher_guid = ANY($3)',\r\n        p_table\r\n    );\r\n\r\n    EXECUTE v_sql\r\n    USING v_company_code, v_tally_owner, v_guids;\r\n\r\n    GET DIAGNOSTICS v_deleted = ROW_COUNT;\r\n\r\n    RETURN v_deleted;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_sync_delete_eq(text,uuid,text,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_sync_delete_eq(p_batch_id text, p_execution_claim_token uuid, p_table text, p_filters jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_filters jsonb;\r\n    v_filter jsonb;\r\n    v_guid_count integer;\r\nBEGIN\r\n    IF jsonb_typeof(p_filters) IS DISTINCT FROM 'array'\r\n       OR jsonb_array_length(p_filters) <> 3 THEN\r\n        RAISE EXCEPTION 'DELETE requires exactly three filters';\r\n    END IF;\r\n\r\n    SELECT count(*)\r\n    INTO v_guid_count\r\n    FROM jsonb_array_elements(p_filters) AS f(value)\r\n    WHERE f.value->>'column' = 'voucher_guid'\r\n      AND f.value->>'type' = 'eq'\r\n      AND jsonb_typeof(f.value->'value') = 'string'\r\n      AND length(f.value->>'value') > 0;\r\n\r\n    IF v_guid_count <> 1 THEN\r\n        RAISE EXCEPTION 'DELETE requires one voucher_guid eq filter';\r\n    END IF;\r\n\r\n    SELECT jsonb_agg(\r\n        CASE\r\n            WHEN f.value->>'column' = 'voucher_guid'\r\n             AND f.value->>'type' = 'eq'\r\n            THEN jsonb_build_object(\r\n                'column', 'voucher_guid',\r\n                'type', 'in',\r\n                'value', jsonb_build_array(f.value->'value')\r\n            )\r\n            ELSE f.value\r\n        END\r\n    )\r\n    INTO v_filters\r\n    FROM jsonb_array_elements(p_filters) AS f(value);\r\n\r\n    RETURN public.fenced_sync_delete(\r\n        p_batch_id,\r\n        p_execution_claim_token,\r\n        p_table,\r\n        v_filters\r\n    );\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_sync_insert(text,uuid,text,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_sync_insert(p_batch_id text, p_execution_claim_token uuid, p_table text, p_rows jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_inserted integer := 0;\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_columns text;\r\n    v_values text;\r\n    v_keys text[];\r\n    v_key text;\r\n    v_sql text;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    IF p_table IS NULL OR p_table NOT IN (\r\n        'tally_voucher_ledgers',\r\n        'tally_voucher_inventory',\r\n        'tally_voucher_inventory_godowns',\r\n        'tally_stock_vouchers',\r\n        'tally_bill_allocations',\r\n        'tally_costcentre_allocations'\r\n    ) THEN\r\n        RAISE EXCEPTION 'INVALID_FENCED_INSERT_TABLE';\r\n    END IF;\r\n\r\n    IF p_rows IS NULL\r\n       OR jsonb_typeof(p_rows) IS DISTINCT FROM 'array'\r\n       OR jsonb_array_length(p_rows) = 0\r\n    THEN\r\n        RAISE EXCEPTION 'INVALID_FENCED_INSERT_ROWS';\r\n    END IF;\r\n\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO v_company_code, v_tally_owner\r\n    FROM public.sync_batches AS b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM jsonb_array_elements(p_rows) AS r(value)\r\n        WHERE jsonb_typeof(r.value) IS DISTINCT FROM 'object'\r\n           OR r.value->>'company_code' IS DISTINCT FROM v_company_code\r\n           OR r.value->>'tally_owner' IS DISTINCT FROM v_tally_owner\r\n    ) THEN\r\n        RAISE EXCEPTION 'FENCED_INSERT_SCOPE_MISMATCH';\r\n    END IF;\r\n\r\n    -- Require identical JSON keys across all rows in this chunk.\r\n    SELECT array_agg(k ORDER BY k)\r\n    INTO v_keys\r\n    FROM jsonb_object_keys(p_rows->0) AS k;\r\n\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM jsonb_array_elements(p_rows) AS r(value)\r\n        WHERE (\r\n            SELECT array_agg(k ORDER BY k)\r\n            FROM jsonb_object_keys(r.value) AS k\r\n        ) IS DISTINCT FROM v_keys\r\n    ) THEN\r\n        RAISE EXCEPTION 'FENCED_INSERT_COLUMN_MISMATCH';\r\n    END IF;\r\n\r\n    -- Never allow caller-supplied identity values.\r\n    IF 'id' = ANY(v_keys) THEN\r\n        RAISE EXCEPTION 'FENCED_INSERT_ID_NOT_ALLOWED';\r\n    END IF;\r\n\r\n    -- Only real, writable columns are accepted.\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM unnest(v_keys) AS k\r\n        WHERE NOT EXISTS (\r\n            SELECT 1\r\n            FROM information_schema.columns AS c\r\n            WHERE c.table_schema = 'public'\r\n              AND c.table_name = p_table\r\n              AND c.column_name = k\r\n              AND c.is_generated = 'NEVER'\r\n              AND c.identity_generation IS NULL\r\n        )\r\n    ) THEN\r\n        RAISE EXCEPTION 'FENCED_INSERT_INVALID_COLUMN';\r\n    END IF;\r\n\r\n    SELECT\r\n        string_agg(format('%I', k), ', '),\r\n        string_agg(format('x.%I', k), ', ')\r\n    INTO v_columns, v_values\r\n    FROM unnest(v_keys) AS k;\r\n\r\n    v_sql := format(\r\n        'INSERT INTO public.%I (%s)\r\n         SELECT %s\r\n         FROM jsonb_populate_recordset(\r\n             NULL::public.%I,\r\n             $1\r\n         ) AS x',\r\n        p_table,\r\n        v_columns,\r\n        v_values,\r\n        p_table\r\n    );\r\n\r\n    EXECUTE v_sql USING p_rows;\r\n\r\n    GET DIAGNOSTICS v_inserted = ROW_COUNT;\r\n\r\n    RETURN v_inserted;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_sync_update(text,uuid,text,jsonb,jsonb)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_sync_update(p_batch_id text, p_execution_claim_token uuid, p_table text, p_values jsonb, p_filters jsonb)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO 'public', 'pg_temp'\nAS $function$\r\nDECLARE\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_alter_column text;\r\n    v_alter_type text;\r\n    v_timestamp_type text;\r\n    v_last_synced_type text;\r\n    v_guid text;\r\n    v_guids text[];\r\n    v_filter jsonb;\r\n    v_filter_columns text[] := ARRAY[]::text[];\r\n    v_column text;\r\n    v_type text;\r\n    v_value jsonb;\r\n    v_has_in boolean := false;\r\n    v_has_deleted_filter boolean := false;\r\n    v_is_soft_delete boolean;\r\n    v_set_clause text;\r\n    v_where_clause text;\r\n    v_sql text;\r\n    v_count integer;\r\nBEGIN\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    SELECT b.company_code, b.tally_owner\r\n    INTO STRICT v_company_code, v_tally_owner\r\n    FROM public.sync_batches b\r\n    WHERE b.batch_id = p_batch_id;\r\n\r\n    IF p_table IS NULL OR p_table NOT IN (\r\n        'tally_sync_groups',\r\n        'tally_sync_stock_groups',\r\n        'tally_sync_ledgers',\r\n        'tally_sync_stocks',\r\n        'tally_sync_units',\r\n        'tally_sync_godowns',\r\n        'tally_sync_cost_centres',\r\n        'tally_vouchers'\r\n    ) THEN\r\n        RAISE EXCEPTION 'Unsupported fenced UPDATE table';\r\n    END IF;\r\n\r\n    v_alter_column :=\r\n        CASE\r\n            WHEN p_table IN (\r\n                'tally_sync_groups',\r\n                'tally_sync_stock_groups',\r\n                'tally_sync_ledgers'\r\n            ) THEN 'alter_id'\r\n            ELSE 'alterid'\r\n        END;\r\n\r\n    v_alter_type :=\r\n        CASE\r\n            WHEN p_table IN (\r\n                'tally_sync_stocks',\r\n                'tally_sync_units'\r\n            ) THEN 'integer'\r\n            ELSE 'bigint'\r\n        END;\r\n\r\n    v_timestamp_type := 'timestamp';\r\n\r\n    v_last_synced_type :=\r\n        CASE\r\n            WHEN p_table IN (\r\n                'tally_sync_cost_centres',\r\n                'tally_sync_godowns'\r\n            ) THEN 'timestamptz'\r\n            ELSE 'timestamp'\r\n        END;\r\n\r\n    IF p_values IS NULL\r\n       OR jsonb_typeof(p_values) IS DISTINCT FROM 'object'\r\n       OR p_values = '{}'::jsonb THEN\r\n        RAISE EXCEPTION 'Invalid UPDATE values';\r\n    END IF;\r\n\r\n    v_is_soft_delete := p_values ? 'is_deleted';\r\n\r\n    IF v_is_soft_delete THEN\r\n        IF p_values->'is_deleted' IS DISTINCT FROM 'true'::jsonb\r\n           OR NOT (\r\n               p_values ? 'sync_batch_id'\r\n               AND p_values ? 'updated_at'\r\n           )\r\n           OR (\r\n               p_values - 'is_deleted'\r\n                        - 'sync_batch_id'\r\n                        - 'updated_at'\r\n           ) <> '{}'::jsonb THEN\r\n            RAISE EXCEPTION 'Invalid soft-delete values';\r\n        END IF;\r\n    ELSE\r\n        IF NOT (\r\n            p_values ? v_alter_column\r\n            AND p_values ? 'sync_batch_id'\r\n            AND p_values ? 'updated_at'\r\n        )\r\n        OR (\r\n            p_values - v_alter_column\r\n                     - 'sync_batch_id'\r\n                     - 'updated_at'\r\n                     - 'last_synced_at'\r\n        ) <> '{}'::jsonb THEN\r\n            RAISE EXCEPTION 'Invalid ALTER UPDATE values';\r\n        END IF;\r\n\r\n        IF p_table = 'tally_vouchers'\r\n           AND p_values ? 'last_synced_at' THEN\r\n            RAISE EXCEPTION 'Unexpected voucher last_synced_at';\r\n        END IF;\r\n\r\n        IF p_table <> 'tally_vouchers'\r\n           AND NOT (p_values ? 'last_synced_at') THEN\r\n            RAISE EXCEPTION 'Missing master last_synced_at';\r\n        END IF;\r\n    END IF;\r\n\r\n    IF p_values->>'sync_batch_id' IS DISTINCT FROM p_batch_id THEN\r\n        RAISE EXCEPTION 'UPDATE batch mismatch';\r\n    END IF;\r\n\r\n    IF jsonb_typeof(p_filters) IS DISTINCT FROM 'array' THEN\r\n        RAISE EXCEPTION 'Invalid UPDATE filters';\r\n    END IF;\r\n\r\n    FOR v_filter IN\r\n        SELECT value FROM jsonb_array_elements(p_filters)\r\n    LOOP\r\n        IF jsonb_typeof(v_filter) IS DISTINCT FROM 'object' THEN\r\n            RAISE EXCEPTION 'Invalid UPDATE filter object';\r\n        END IF;\r\n\r\n        v_column := v_filter->>'column';\r\n        v_type := v_filter->>'type';\r\n        v_value := v_filter->'value';\r\n\r\n        IF v_column IS NULL\r\n           OR v_column = ANY(v_filter_columns) THEN\r\n            RAISE EXCEPTION 'Duplicate or missing filter column';\r\n        END IF;\r\n\r\n        v_filter_columns := array_append(v_filter_columns, v_column);\r\n\r\n        IF v_column = 'company_code' AND v_type = 'eq' THEN\r\n            IF v_value IS DISTINCT FROM to_jsonb(v_company_code) THEN\r\n                RAISE EXCEPTION 'Company scope mismatch';\r\n            END IF;\r\n\r\n        ELSIF v_column = 'tally_owner' AND v_type = 'eq' THEN\r\n            IF v_value IS DISTINCT FROM to_jsonb(v_tally_owner) THEN\r\n                RAISE EXCEPTION 'Owner scope mismatch';\r\n            END IF;\r\n\r\n        ELSIF v_column = 'guid' AND v_type = 'eq' THEN\r\n            IF jsonb_typeof(v_value) IS DISTINCT FROM 'string'\r\n               OR NULLIF(v_filter->>'value', '') IS NULL THEN\r\n                RAISE EXCEPTION 'Invalid GUID filter';\r\n            END IF;\r\n\r\n            v_guid := v_filter->>'value';\r\n\r\n        ELSIF v_column = 'guid' AND v_type = 'in' THEN\r\n            IF p_table <> 'tally_vouchers'\r\n               OR NOT v_is_soft_delete\r\n               OR jsonb_typeof(v_value) IS DISTINCT FROM 'array' THEN\r\n                RAISE EXCEPTION 'Unsupported IN filter';\r\n            END IF;\r\n\r\n            IF EXISTS (\r\n                SELECT 1\r\n                FROM jsonb_array_elements(v_value) AS x(value)\r\n                WHERE jsonb_typeof(x.value) IS DISTINCT FROM 'string'\r\n                   OR NULLIF(x.value #>> '{}', '') IS NULL\r\n            ) THEN\r\n                RAISE EXCEPTION 'Invalid GUID array';\r\n            END IF;\r\n\r\n            SELECT COALESCE(array_agg(x.value), ARRAY[]::text[])\r\n            INTO v_guids\r\n            FROM jsonb_array_elements_text(v_value) AS x(value);\r\n\r\n            v_has_in := true;\r\n\r\n        ELSIF v_column = 'is_deleted'\r\n              AND v_type = 'eq'\r\n              AND v_value = 'false'::jsonb THEN\r\n            v_has_deleted_filter := true;\r\n\r\n        ELSE\r\n            RAISE EXCEPTION 'Unsupported UPDATE filter: %', v_filter;\r\n        END IF;\r\n    END LOOP;\r\n\r\n    IF NOT (\r\n        v_filter_columns @> ARRAY['company_code','tally_owner','guid']\r\n        AND cardinality(v_filter_columns) =\r\n            CASE WHEN v_is_soft_delete THEN 4 ELSE 3 END\r\n    ) THEN\r\n        RAISE EXCEPTION 'Invalid UPDATE filter scope';\r\n    END IF;\r\n\r\n    IF v_is_soft_delete IS DISTINCT FROM v_has_deleted_filter THEN\r\n        RAISE EXCEPTION 'Soft-delete filter mismatch';\r\n    END IF;\r\n\r\n    IF v_has_in AND v_guids = ARRAY[]::text[] THEN\r\n        RETURN 0;\r\n    END IF;\r\n\r\n    IF v_is_soft_delete THEN\r\n        v_set_clause := format(\r\n            'is_deleted = true,\r\n             sync_batch_id = $1,\r\n             updated_at = ($2)::%s',\r\n            v_timestamp_type\r\n        );\r\n    ELSE\r\n        v_set_clause := format(\r\n            '%I = ($3)::%s,\r\n             sync_batch_id = $1,\r\n             updated_at = ($2)::%s',\r\n            v_alter_column,\r\n            v_alter_type,\r\n            v_timestamp_type\r\n        );\r\n\r\n        IF p_values ? 'last_synced_at' THEN\r\n            v_set_clause := v_set_clause || format(\r\n                ', last_synced_at = ($4)::%s',\r\n                v_last_synced_type\r\n            );\r\n        END IF;\r\n    END IF;\r\n\r\n    v_where_clause :=\r\n        'company_code = $5 AND tally_owner = $6';\r\n\r\n    IF v_has_in THEN\r\n        v_where_clause := v_where_clause || ' AND guid = ANY($7)';\r\n    ELSE\r\n        v_where_clause := v_where_clause || ' AND guid = $8';\r\n    END IF;\r\n\r\n    IF v_has_deleted_filter THEN\r\n        v_where_clause := v_where_clause || ' AND is_deleted = false';\r\n    END IF;\r\n\r\n    v_sql := format(\r\n        'UPDATE public.%I SET %s WHERE %s',\r\n        p_table,\r\n        v_set_clause,\r\n        v_where_clause\r\n    );\r\n\r\n    EXECUTE v_sql\r\n    USING\r\n        p_batch_id,\r\n        p_values->>'updated_at',\r\n        p_values->>v_alter_column,\r\n        p_values->>'last_synced_at',\r\n        v_company_code,\r\n        v_tally_owner,\r\n        v_guids,\r\n        v_guid;\r\n\r\n    GET DIAGNOSTICS v_count = ROW_COUNT;\r\n\r\n    RETURN v_count;\r\nEND;\r\n$function$\n"
  },
  {
    "signature": "fenced_sync_upsert(text,uuid,text,jsonb,text)",
    "definition": "CREATE OR REPLACE FUNCTION public.fenced_sync_upsert(p_batch_id text, p_execution_claim_token uuid, p_table text, p_rows jsonb, p_on_conflict text)\n RETURNS integer\n LANGUAGE plpgsql\n SET search_path TO ''\nAS $function$\r\nDECLARE\r\n    v_company_code text;\r\n    v_tally_owner text;\r\n    v_row_count integer;\r\n    v_columns text;\r\n    v_select_columns text;\r\n    v_updates text;\r\n    v_keys text[];\r\nBEGIN\r\n\r\n    -- 1. Atomically verify and lock execution claim\r\n    PERFORM public.assert_sync_batch_execution_claim(\r\n        p_batch_id,\r\n        p_execution_claim_token\r\n    );\r\n\r\n    -- 2. Restrict target tables\r\n    IF p_table IS NULL OR p_table NOT IN (\r\n        'tally_sync_groups',\r\n        'tally_sync_stock_groups',\r\n        'tally_sync_ledgers',\r\n        'tally_sync_stocks',\r\n        'tally_sync_units',\r\n        'tally_sync_godowns',\r\n        'tally_sync_cost_centres',\r\n        'tally_vouchers'\r\n    ) THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_TABLE_NOT_ALLOWED: %', p_table;\r\n    END IF;\r\n\r\n    -- 3. Validate conflict target\r\n    IF p_on_conflict IS DISTINCT FROM\r\n        'company_code,tally_owner,guid'\r\n    THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_CONFLICT_NOT_ALLOWED';\r\n    END IF;\r\n\r\n    -- 4. Validate JSON array\r\n    IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array'\r\n       OR jsonb_array_length(p_rows) = 0\r\n    THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_ROWS_INVALID';\r\n    END IF;\r\n\r\n    -- 5. Fetch batch scope\r\n    SELECT\r\n        company_code,\r\n        tally_owner\r\n    INTO STRICT\r\n        v_company_code,\r\n        v_tally_owner\r\n    FROM public.sync_batches\r\n    WHERE batch_id = p_batch_id;\r\n\r\n    -- 6. Validate every row and its scope\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM pg_catalog.jsonb_array_elements(p_rows)\r\n            AS r(value)\r\n        WHERE\r\n            pg_catalog.jsonb_typeof(r.value)\r\n                IS DISTINCT FROM 'object'\r\n            OR r.value->>'company_code'\r\n                IS DISTINCT FROM v_company_code\r\n            OR r.value->>'tally_owner'\r\n                IS DISTINCT FROM v_tally_owner\r\n            OR pg_catalog.nullif(\r\n                pg_catalog.btrim(r.value->>'guid'),\r\n                ''\r\n            ) IS NULL\r\n    ) THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_SCOPE_OR_ROW_INVALID';\r\n    END IF;\r\n\r\n    -- 7. Collect column names\r\n    SELECT\r\n        pg_catalog.array_agg(\r\n            k.column_name\r\n            ORDER BY k.column_name\r\n        )\r\n    INTO v_keys\r\n    FROM pg_catalog.jsonb_object_keys(p_rows->0)\r\n        AS k(column_name);\r\n\r\n    -- 8. Every row must have identical columns\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM pg_catalog.jsonb_array_elements(p_rows)\r\n            AS r(value)\r\n        WHERE (\r\n            SELECT\r\n                pg_catalog.array_agg(\r\n                    k.column_name\r\n                    ORDER BY k.column_name\r\n                )\r\n            FROM pg_catalog.jsonb_object_keys(r.value)\r\n                AS k(column_name)\r\n        ) IS DISTINCT FROM v_keys\r\n    ) THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_ROW_COLUMNS_MISMATCH';\r\n    END IF;\r\n\r\n    -- 9. Required keys and protected id\r\n    IF v_keys IS NULL\r\n       OR 'id' = ANY(v_keys)\r\n       OR NOT (\r\n           ARRAY[\r\n               'company_code',\r\n               'tally_owner',\r\n               'guid'\r\n           ]::text[] <@ v_keys\r\n       )\r\n    THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_COLUMNS_INVALID';\r\n    END IF;\r\n\r\n    -- 10. Reject unknown, generated or identity columns\r\n    IF EXISTS (\r\n        SELECT 1\r\n        FROM pg_catalog.unnest(v_keys)\r\n            AS k(column_name)\r\n        WHERE NOT EXISTS (\r\n            SELECT 1\r\n            FROM pg_catalog.pg_attribute a\r\n            JOIN pg_catalog.pg_class c\r\n                ON c.oid = a.attrelid\r\n            JOIN pg_catalog.pg_namespace n\r\n                ON n.oid = c.relnamespace\r\n            WHERE\r\n                n.nspname = 'public'\r\n                AND c.relname = p_table\r\n                AND a.attname = k.column_name\r\n                AND a.attnum > 0\r\n                AND NOT a.attisdropped\r\n                AND a.attgenerated = ''\r\n                AND a.attidentity = ''\r\n        )\r\n    ) THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_UNKNOWN_OR_PROTECTED_COLUMN';\r\n    END IF;\r\n\r\n    -- 11. Build safe quoted column lists\r\n    SELECT\r\n        pg_catalog.string_agg(\r\n            pg_catalog.format('%I', k.column_name),\r\n            ', '\r\n            ORDER BY k.column_name\r\n        ),\r\n        pg_catalog.string_agg(\r\n            pg_catalog.format(\r\n                'src.%I',\r\n                k.column_name\r\n            ),\r\n            ', '\r\n            ORDER BY k.column_name\r\n        )\r\n    INTO\r\n        v_columns,\r\n        v_select_columns\r\n    FROM pg_catalog.unnest(v_keys)\r\n        AS k(column_name);\r\n\r\n    -- 12. Build conflict update assignments\r\n    SELECT\r\n        pg_catalog.string_agg(\r\n            pg_catalog.format(\r\n                '%1$I = EXCLUDED.%1$I',\r\n                k.column_name\r\n            ),\r\n            ', '\r\n            ORDER BY k.column_name\r\n        )\r\n    INTO v_updates\r\n    FROM pg_catalog.unnest(v_keys)\r\n        AS k(column_name)\r\n    WHERE k.column_name NOT IN (\r\n        'company_code',\r\n        'tally_owner',\r\n        'guid'\r\n    );\r\n\r\n    IF v_updates IS NULL THEN\r\n        RAISE EXCEPTION\r\n            'UPSERT_NO_UPDATE_COLUMNS';\r\n    END IF;\r\n\r\n    -- 13. Atomic fenced UPSERT\r\n    EXECUTE pg_catalog.format(\r\n        'INSERT INTO public.%1$I (%2$s)\r\n         SELECT %3$s\r\n         FROM pg_catalog.jsonb_populate_recordset(\r\n             NULL::public.%1$I,\r\n             $1\r\n         ) AS src\r\n         ON CONFLICT (\r\n             company_code,\r\n             tally_owner,\r\n             guid\r\n         )\r\n         DO UPDATE SET %4$s',\r\n        p_table,\r\n        v_columns,\r\n        v_select_columns,\r\n        v_updates\r\n    )\r\n    USING p_rows;\r\n\r\n    GET DIAGNOSTICS v_row_count = ROW_COUNT;\r\n\r\n    RETURN v_row_count;\r\n\r\nEND;\r\n$function$\n"
  }
]
```

## PART 3 — sync_batches Columns and Triggers

```json
[
  {
    "record_type": "COLUMN",
    "name": "batch_closed",
    "details": "boolean",
    "nullable": "YES",
    "extra": "false"
  },
  {
    "record_type": "COLUMN",
    "name": "batch_id",
    "details": "text",
    "nullable": "NO",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "batch_status",
    "details": "text",
    "nullable": "NO",
    "extra": "'PENDING'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "batch_type",
    "details": "text",
    "nullable": "YES",
    "extra": "'INCREMENTAL_SYNC'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "cleanup_attempts",
    "details": "integer",
    "nullable": "NO",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "cleanup_completed_at",
    "details": "timestamp with time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "cleanup_error",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "cleanup_started_at",
    "details": "timestamp with time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "cleanup_status",
    "details": "text",
    "nullable": "NO",
    "extra": "'PENDING'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "company_code",
    "details": "text",
    "nullable": "NO",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "completed_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "connector_retry_ready",
    "details": "boolean",
    "nullable": "NO",
    "extra": "false"
  },
  {
    "record_type": "COLUMN",
    "name": "created_at",
    "details": "timestamp without time zone",
    "nullable": "NO",
    "extra": "now()"
  },
  {
    "record_type": "COLUMN",
    "name": "current_action",
    "details": "text",
    "nullable": "YES",
    "extra": "'PENDING'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "current_entity",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "current_module",
    "details": "text",
    "nullable": "YES",
    "extra": "'INIT'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "current_operation",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "current_stage",
    "details": "text",
    "nullable": "YES",
    "extra": "'INIT'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "error_message",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "execution_claim_token",
    "details": "uuid",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "failed_vouchers",
    "details": "integer",
    "nullable": "NO",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "from_alterid",
    "details": "bigint",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "guid_scan_completed",
    "details": "boolean",
    "nullable": "YES",
    "extra": "false"
  },
  {
    "record_type": "COLUMN",
    "name": "heartbeat_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "id",
    "details": "bigint",
    "nullable": "NO",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "incremental_post_completed",
    "details": "boolean",
    "nullable": "YES",
    "extra": "false"
  },
  {
    "record_type": "COLUMN",
    "name": "job_type",
    "details": "text",
    "nullable": "YES",
    "extra": "'TALLY_SYNC'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "last_activity_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "last_successful_module",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "ledger_reconciliation_completed",
    "details": "boolean",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "locked_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "operation_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "payload",
    "details": "jsonb",
    "nullable": "YES",
    "extra": "'{}'::jsonb"
  },
  {
    "record_type": "COLUMN",
    "name": "priority",
    "details": "integer",
    "nullable": "YES",
    "extra": "5"
  },
  {
    "record_type": "COLUMN",
    "name": "processed_vouchers",
    "details": "integer",
    "nullable": "NO",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "reconciliation_completed",
    "details": "boolean",
    "nullable": "YES",
    "extra": "false"
  },
  {
    "record_type": "COLUMN",
    "name": "reconciliation_completed_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "retry_claim_token",
    "details": "uuid",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "retry_count",
    "details": "integer",
    "nullable": "NO",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "retry_lease_until",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "scheduled_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "started_at",
    "details": "timestamp without time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "stock_reconciliation_completed",
    "details": "boolean",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "sync_mode",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "sync_period",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "sync_progress",
    "details": "jsonb",
    "nullable": "YES",
    "extra": "'{}'::jsonb"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_cost_centres_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_godowns_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_groups_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_ledgers_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_owner",
    "details": "text",
    "nullable": "NO",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "tally_stock_groups_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_stocks_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "tally_units_count",
    "details": "integer",
    "nullable": "YES",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "to_alterid",
    "details": "bigint",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "total_vouchers",
    "details": "integer",
    "nullable": "NO",
    "extra": "0"
  },
  {
    "record_type": "COLUMN",
    "name": "waiting_connector_since",
    "details": "timestamp with time zone",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "worker_id",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "COLUMN",
    "name": "worker_status",
    "details": "text",
    "nullable": "YES",
    "extra": "'PENDING'::text"
  },
  {
    "record_type": "COLUMN",
    "name": "worker_type",
    "details": "text",
    "nullable": "YES",
    "extra": null
  },
  {
    "record_type": "TRIGGER",
    "name": "block_uncertain_sync_batch_retry",
    "details": "CREATE TRIGGER block_uncertain_sync_batch_retry BEFORE UPDATE ON public.sync_batches FOR EACH ROW WHEN (is_sync_outcome_unknown(old.error_message)) EXECUTE FUNCTION block_uncertain_sync_batch_retry()",
    "nullable": null,
    "extra": null
  },
  {
    "record_type": "TRIGGER",
    "name": "trg_bump_company_version_on_batch_closed",
    "details": "CREATE TRIGGER trg_bump_company_version_on_batch_closed AFTER UPDATE OF batch_status ON public.sync_batches FOR EACH ROW WHEN (((new.batch_status = 'CLOSED'::text) AND (old.batch_status IS DISTINCT FROM 'CLOSED'::text))) EXECUTE FUNCTION bump_company_version_on_batch_closed()",
    "nullable": null,
    "extra": null
  },
  {
    "record_type": "TRIGGER",
    "name": "trg_protect_sync_batch_outcome",
    "details": "CREATE TRIGGER trg_protect_sync_batch_outcome BEFORE UPDATE ON public.sync_batches FOR EACH ROW EXECUTE FUNCTION protect_sync_batch_outcome()",
    "nullable": null,
    "extra": null
  },
  {
    "record_type": "TRIGGER",
    "name": "trg_reset_connector_retry_ready_on_claim",
    "details": "CREATE TRIGGER trg_reset_connector_retry_ready_on_claim BEFORE UPDATE OF batch_status, worker_status ON public.sync_batches FOR EACH ROW EXECUTE FUNCTION reset_connector_retry_ready_on_claim()",
    "nullable": null,
    "extra": null
  }
]
```

## PART 4 — Trigger Function Definitions

The following definitions are transcribed from the SQL JSON pasted in the conversation; JSON whitespace and line-ending escape representation are normalized.

```json
[
  {
    "signature": "block_uncertain_sync_batch_retry()",
    "definition": "CREATE OR REPLACE FUNCTION public.block_uncertain_sync_batch_retry()\n RETURNS trigger\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\nBEGIN\n    IF public.is_sync_outcome_unknown(OLD.error_message)\n       AND (\n           NEW.batch_status IS DISTINCT FROM OLD.batch_status\n           OR NEW.worker_status IS DISTINCT FROM OLD.worker_status\n           OR NEW.current_stage IS DISTINCT FROM OLD.current_stage\n           OR NEW.worker_id IS DISTINCT FROM OLD.worker_id\n           OR NEW.error_message IS DISTINCT FROM OLD.error_message\n       )\n       AND NOT (\n           OLD.error_message LIKE 'HTTP_OUTCOME_UNKNOWN:%'\n           AND OLD.batch_status = 'PROCESSING'\n           AND OLD.worker_status = 'RUNNING'\n           AND NEW.batch_status = 'COMPLETED'\n           AND NEW.worker_status = 'COMPLETED'\n           AND NEW.error_message IS NULL\n       )\n    THEN\n        RAISE EXCEPTION\n            'UNCERTAIN_SYNC_BATCH_REQUIRES_REVIEW';\n    END IF;\n\n    RETURN NEW;\nEND;\n$function$\n"
  },
  {
    "signature": "bump_company_version_on_batch_closed()",
    "definition": "CREATE OR REPLACE FUNCTION public.bump_company_version_on_batch_closed()\n RETURNS trigger\n LANGUAGE plpgsql\n SECURITY DEFINER\n SET search_path TO ''\nAS $function$\nBEGIN\n    IF NEW.batch_status = 'CLOSED'\n       AND OLD.batch_status IS DISTINCT FROM 'CLOSED' THEN\n\n        PERFORM public.increment_company_data_version(\n            NEW.company_code,\n            NEW.tally_owner\n        );\n    END IF;\n\n    RETURN NEW;\nEND;\n$function$\n"
  },
  {
    "signature": "is_sync_outcome_unknown(text)",
    "definition": "CREATE OR REPLACE FUNCTION public.is_sync_outcome_unknown(p_error_message text)\n RETURNS boolean\n LANGUAGE sql\n IMMUTABLE\n SET search_path TO 'public'\nAS $function$\n    SELECT\n        COALESCE(p_error_message, '') LIKE 'CONNECTOR_OUTCOME_UNKNOWN:%'\n        OR\n        COALESCE(p_error_message, '') LIKE 'HTTP_OUTCOME_UNKNOWN:%';\n$function$\n"
  },
  {
    "signature": "protect_sync_batch_outcome()",
    "definition": "CREATE OR REPLACE FUNCTION public.protect_sync_batch_outcome()\n RETURNS trigger\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\nBEGIN\n    IF public.is_sync_outcome_unknown(OLD.error_message)\n       AND NEW.batch_status = 'PROCESSING'\n       AND NEW.worker_status = 'RUNNING'\n       AND (\n           OLD.batch_status IS DISTINCT FROM NEW.batch_status\n           OR OLD.worker_status IS DISTINCT FROM NEW.worker_status\n       )\n    THEN\n        RAISE EXCEPTION\n            'SYNC_OUTCOME_UNKNOWN: Batch % cannot be claimed',\n            OLD.batch_id;\n    END IF;\n\n    RETURN NEW;\nEND;\n$function$\n"
  },
  {
    "signature": "reset_connector_retry_ready_on_claim()",
    "definition": "CREATE OR REPLACE FUNCTION public.reset_connector_retry_ready_on_claim()\n RETURNS trigger\n LANGUAGE plpgsql\n SET search_path TO 'public'\nAS $function$\nBEGIN\n    IF NEW.batch_status IS DISTINCT FROM 'FAILED'\n       OR NEW.worker_status IS DISTINCT FROM 'FAILED'\n    THEN\n        NEW.connector_retry_ready := false;\n    END IF;\n\n    RETURN NEW;\nEND;\n$function$\n"
  }
]
```
