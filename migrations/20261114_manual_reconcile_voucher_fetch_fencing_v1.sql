-- DRAFT / NOT FOR DEPLOYMENT.
-- Requires manual_reconcile_operations_v1 and zz_journal_execution_fencing_v1.
-- Execution-fenced voucher fetch prepare, dispatch and result verification.
-- Signed AMOUNT is mandatory in new verified voucher evidence.
-- Voucher settlement and checkpoint integration are NOT implemented here.
BEGIN;

CREATE FUNCTION public.prepare_manual_reconcile_voucher_fetch_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_operation_ordinal integer,
    p_request_id uuid,
    p_connector_id text,
    p_tally_company_guid text,
    p_input_payload jsonb,
    p_input_fingerprint text,
    p_execution_id uuid
)
RETURNS TABLE(prepared boolean, reason text, operation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_request_id IS NULL
       OR p_execution_id IS NULL OR p_operation_ordinal IS NULL
       OR p_operation_ordinal <= 0
       OR NULLIF(pg_catalog.btrim(p_connector_id), '') IS NULL
       OR NULLIF(pg_catalog.btrim(p_tally_company_guid), '') IS NULL
       OR p_input_payload IS NULL
       OR pg_catalog.jsonb_typeof(p_input_payload) IS DISTINCT FROM 'object'
       OR p_input_fingerprint IS NULL
       OR NULLIF(pg_catalog.btrim(p_input_fingerprint), '') IS NULL
    THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Shared execution authorization acquires company advisory then reservation lock.
    SELECT e.* INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id, p_operation_token, p_execution_id
    ) AS e;

    IF p_connector_id IS DISTINCT FROM v_execution.connector_id THEN
        RETURN QUERY SELECT false, 'EXECUTION_CONNECTOR_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    SELECT o.* INTO v_existing
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.attempt_id IS DISTINCT FROM p_attempt_id
           OR v_existing.reservation_id IS DISTINCT FROM v_execution.reservation_id
           OR v_existing.operation_token IS DISTINCT FROM p_operation_token
           OR v_existing.execution_id IS DISTINCT FROM p_execution_id
           OR v_existing.operation_ordinal IS DISTINCT FROM p_operation_ordinal
           OR v_existing.operation_type IS DISTINCT FROM 'VOUCHER_FETCH'
           OR v_existing.ledger_guid IS NOT NULL
           OR v_existing.request_id IS DISTINCT FROM p_request_id
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

    IF EXISTS (
        SELECT 1 FROM public.manual_reconcile_operations AS o
        WHERE (o.attempt_id = p_attempt_id AND o.operation_ordinal = p_operation_ordinal)
           OR o.request_id = p_request_id
    ) THEN
        RETURN QUERY SELECT false, 'OPERATION_IDENTITY_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    INSERT INTO public.manual_reconcile_operations (
        operation_id, attempt_id, reservation_id, operation_token,
        execution_id, operation_ordinal, operation_type, ledger_guid,
        request_id, connector_id, tally_company_guid,
        input_payload, input_fingerprint
    ) VALUES (
        p_operation_id, p_attempt_id, v_execution.reservation_id,
        p_operation_token, p_execution_id, p_operation_ordinal,
        'VOUCHER_FETCH', NULL, p_request_id, p_connector_id,
        p_tally_company_guid, p_input_payload, p_input_fingerprint
    );

    RETURN QUERY SELECT true, 'PREPARED'::text, 'PREPARED'::text;
END;
$fn$;

ALTER FUNCTION public.prepare_manual_reconcile_voucher_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, jsonb, text, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.prepare_manual_reconcile_voucher_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, jsonb, text, uuid
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION public.prepare_manual_reconcile_voucher_fetch_v1(
    uuid, uuid, uuid, integer, uuid, text, text, jsonb, text, uuid
) TO service_role;


-- Stage 2: mark voucher fetch dispatch before any connector emission.
CREATE FUNCTION public.mark_manual_reconcile_voucher_dispatch_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid,
    p_execution_id uuid
)
RETURNS TABLE(marked boolean, reason text, operation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_request_id IS NULL
       OR p_execution_id IS NULL THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Lock order: company advisory -> reservation -> journal operation.
    SELECT e.* INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id, p_operation_token, p_execution_id
    ) AS e;

    SELECT o.* INTO v_operation
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_operation.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_operation.reservation_id IS DISTINCT FROM v_execution.reservation_id
       OR v_operation.operation_token IS DISTINCT FROM p_operation_token
       OR v_operation.execution_id IS DISTINCT FROM p_execution_id
       OR v_operation.connector_id IS DISTINCT FROM v_execution.connector_id
       OR v_operation.request_id IS DISTINCT FROM p_request_id
       OR v_operation.operation_type IS DISTINCT FROM 'VOUCHER_FETCH'
       OR v_operation.ledger_guid IS NOT NULL THEN
        RETURN QUERY SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    IF v_operation.state IS DISTINCT FROM 'PREPARED' THEN
        RETURN QUERY SELECT false, 'DISPATCH_ALREADY_MARKED_OR_SETTLED'::text,
                            v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations AS o
    SET state = 'DISPATCH_MAY_HAVE_OCCURRED',
        dispatch_marked_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY SELECT true, 'DISPATCH_MARKED'::text,
                        'DISPATCH_MAY_HAVE_OCCURRED'::text;
END;
$fn$;

ALTER FUNCTION public.mark_manual_reconcile_voucher_dispatch_v1(
    uuid, uuid, uuid, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.mark_manual_reconcile_voucher_dispatch_v1(
    uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION public.mark_manual_reconcile_voucher_dispatch_v1(
    uuid, uuid, uuid, uuid, uuid
) TO service_role;


-- Stage 3: persist a structurally validated, scope-bound voucher result.
-- Caller must supply the connector's normalized `data.entries` array as
-- p_result_payload.entries; raw vouchers are NOT accepted into this journal.
CREATE FUNCTION public.verify_manual_reconcile_voucher_result_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_request_id uuid,
    p_result_payload jsonb,
    p_execution_id uuid
)
RETURNS TABLE(verified boolean, reason text, operation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_operation public.manual_reconcile_operations%ROWTYPE;
    v_requested jsonb;
    v_entry jsonb;
    v_guid text;
    v_amount text;
    v_debit text;
    v_credit text;
    v_result jsonb;
    v_count integer;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_request_id IS NULL
       OR p_execution_id IS NULL OR p_result_payload IS NULL
       OR pg_catalog.jsonb_typeof(p_result_payload) IS DISTINCT FROM 'object'
    THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text;
        RETURN;
    END IF;

    -- Shared authorization preserves company advisory -> reservation -> journal order.
    SELECT e.* INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id, p_operation_token, p_execution_id
    ) AS e;

    SELECT o.* INTO v_operation
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id FOR UPDATE;

    IF NOT FOUND
       OR v_operation.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_operation.reservation_id IS DISTINCT FROM v_execution.reservation_id
       OR v_operation.operation_token IS DISTINCT FROM p_operation_token
       OR v_operation.execution_id IS DISTINCT FROM p_execution_id
       OR v_operation.connector_id IS DISTINCT FROM v_execution.connector_id
       OR v_operation.request_id IS DISTINCT FROM p_request_id
       OR v_operation.operation_type IS DISTINCT FROM 'VOUCHER_FETCH'
       OR v_operation.ledger_guid IS NOT NULL
    THEN
        RETURN QUERY SELECT false, 'OPERATION_NOT_FOUND_OR_CONFLICT'::text, NULL::text;
        RETURN;
    END IF;

    -- Fail closed unless the prepare input records explicit scope and GUID set.
    v_requested := v_operation.input_payload->'voucherGuids';
    IF pg_catalog.jsonb_typeof(v_requested) IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(v_requested) = 0
       OR pg_catalog.jsonb_typeof(p_result_payload->'entries') IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(p_result_payload->'entries') = 0
       OR pg_catalog.jsonb_typeof(v_operation.input_payload->'companyCode') IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(v_operation.input_payload->'tallyOwner') IS DISTINCT FROM 'string'
       OR v_operation.input_payload->>'tallyOwner' NOT IN ('USER', 'CA')
       OR NULLIF(pg_catalog.btrim(v_operation.input_payload->>'companyCode'), '') IS NULL
    THEN
        RETURN QUERY SELECT false, 'RESULT_CONTEXT_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

    -- Every requested voucher must be represented, with no duplicates in the
    -- requested GUID set. Entries may repeat ledger GUIDs: occurrence matching
    -- is a separate requirement for the later UPDATE RPC.
       IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(v_requested) AS x(value)
        WHERE pg_catalog.jsonb_typeof(x.value) IS DISTINCT FROM 'string'
           OR ((x.value #>> '{}') COLLATE "C")
              !~ '^[A-Za-z0-9][A-Za-z0-9-]{0,127}$'
    ) OR (
        SELECT pg_catalog.count(*)
        FROM pg_catalog.jsonb_array_elements_text(v_requested)
    ) IS DISTINCT FROM (
        SELECT pg_catalog.count(
            DISTINCT pg_catalog.lower(t.value COLLATE "C")
        )
        FROM pg_catalog.jsonb_array_elements_text(v_requested) AS t(value)
    ) THEN
        RETURN QUERY SELECT false, 'REQUEST_GUIDS_INVALID'::text, v_operation.state;
        RETURN;
    END IF;

        FOR v_entry IN
        SELECT value
        FROM pg_catalog.jsonb_array_elements(
            p_result_payload->'entries'
        ) AS t(value)
    LOOP
        IF pg_catalog.jsonb_typeof(v_entry) IS DISTINCT FROM 'object'
           OR pg_catalog.jsonb_typeof(v_entry->'companyCode') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'tallyOwner') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'voucherGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'ledgerGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'amount') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'debit') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(v_entry->'credit') IS DISTINCT FROM 'string'
        THEN
            RETURN QUERY SELECT false, 'ENTRY_INVALID'::text, v_operation.state;
            RETURN;
        END IF;

        v_guid := v_entry->>'voucherGuid';
        v_amount := v_entry->>'amount';
        v_debit := v_entry->>'debit';
        v_credit := v_entry->>'credit';

        IF v_entry->>'companyCode'
               IS DISTINCT FROM v_operation.input_payload->>'companyCode'
           OR v_entry->>'tallyOwner'
               IS DISTINCT FROM v_operation.input_payload->>'tallyOwner'
           OR (v_guid COLLATE "C")
               !~ '^[A-Za-z0-9][A-Za-z0-9-]{0,127}$'
           OR ((v_entry->>'ledgerGuid') COLLATE "C")
               !~ '^[A-Za-z0-9][A-Za-z0-9-]{0,127}$'
           OR NOT EXISTS (
               SELECT 1
               FROM pg_catalog.jsonb_array_elements_text(v_requested) AS t(value)
               WHERE (t.value COLLATE "C") = (v_guid COLLATE "C")
           )
           OR (v_amount COLLATE "C")
               !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (v_debit COLLATE "C")
               !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (v_credit COLLATE "C")
               !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR pg_catalog.length(v_amount) > 18
           OR pg_catalog.length(v_debit) > 17
           OR pg_catalog.length(v_credit) > 17
        THEN
            RETURN QUERY SELECT false, 'ENTRY_SCOPE_OR_AMOUNT_INVALID'::text,
                                v_operation.state;
            RETURN;
        END IF;

        IF v_amount::numeric < -90071992547409.91
           OR v_amount::numeric > 90071992547409.91
           OR v_debit::numeric > 90071992547409.91
           OR v_credit::numeric > 90071992547409.91
           OR (v_debit::numeric > 0 AND v_credit::numeric > 0)
           OR pg_catalog.abs(v_amount::numeric)
   <> (v_debit::numeric + v_credit::numeric)
        THEN
            RETURN QUERY SELECT false, 'ENTRY_SCOPE_OR_AMOUNT_INVALID'::text,
                                v_operation.state;
            RETURN;
        END IF;
    END LOOP;

       -- Reject case-only ledger GUID conflicts.
    -- Preserve repeated occurrences with identical spelling.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            p_result_payload->'entries'
        ) AS e(value)
        GROUP BY pg_catalog.lower(
            (e.value->>'ledgerGuid') COLLATE "C"
        )
        HAVING pg_catalog.count(
            DISTINCT (e.value->>'ledgerGuid') COLLATE "C"
        ) > 1
    ) THEN
        RETURN QUERY SELECT false, 'ENTRY_SCOPE_OR_AMOUNT_INVALID'::text,
                            v_operation.state;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements_text(v_requested) AS requested(value)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.jsonb_array_elements(p_result_payload->'entries') AS entry(value)
            WHERE ((entry.value->>'voucherGuid') COLLATE "C")
                = (requested.value COLLATE "C")
        )
    ) THEN
        RETURN QUERY SELECT false, 'VOUCHER_COVERAGE_INCOMPLETE'::text, v_operation.state;
        RETURN;
    END IF;

    -- Store only normalized entries; no DB voucher rows are changed here.
    v_result := pg_catalog.jsonb_build_object('entries', p_result_payload->'entries');

    IF v_operation.state = 'RESULT_VERIFIED' THEN
        IF v_operation.result_payload = v_result THEN
            RETURN QUERY SELECT true, 'ALREADY_VERIFIED'::text, v_operation.state;
        ELSE
            RETURN QUERY SELECT false, 'RESULT_CONFLICT'::text, v_operation.state;
        END IF;
        RETURN;
    END IF;

    IF v_operation.state IS DISTINCT FROM 'DISPATCH_MAY_HAVE_OCCURRED' THEN
        RETURN QUERY SELECT false, 'INVALID_OPERATION_STATE'::text, v_operation.state;
        RETURN;
    END IF;

    UPDATE public.manual_reconcile_operations AS o
    SET state = 'RESULT_VERIFIED', result_payload = v_result,
        result_verified_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY SELECT true, 'RESULT_VERIFIED'::text, 'RESULT_VERIFIED'::text;
END;
$fn$;

ALTER FUNCTION public.verify_manual_reconcile_voucher_result_v1(
    uuid, uuid, uuid, uuid, jsonb, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION public.verify_manual_reconcile_voucher_result_v1(
    uuid, uuid, uuid, uuid, jsonb, uuid
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION public.verify_manual_reconcile_voucher_result_v1(
    uuid, uuid, uuid, uuid, jsonb, uuid
) TO service_role;

COMMIT;
