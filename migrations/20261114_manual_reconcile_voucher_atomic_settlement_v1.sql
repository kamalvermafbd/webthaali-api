-- Manual Reconciliation V2: conservative atomic voucher amount settlement.
-- LOCAL DRAFT ONLY. Do not deploy until concurrent normal-sync exclusion and
-- disposable-DB tests have been verified.
-- Prerequisites: operations_v1, atomic_apply_v2, execution fencing,
-- voucher_fetch_fencing_v1; operation_type CHECK must include VOUCHER_APPLY.
BEGIN;

CREATE OR REPLACE FUNCTION public.apply_manual_reconcile_voucher_amounts_v1(
    p_operation_id uuid,
    p_attempt_id uuid,
    p_operation_token uuid,
    p_execution_id uuid,
    p_operation_ordinal integer,
    p_source_fetch_operation_id uuid,
    p_expected_rows jsonb
)
RETURNS TABLE(applied boolean, reason text, operation_state text, updated_rows integer)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $fn$
DECLARE
    v_execution public.manual_reconcile_executions%ROWTYPE;
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_source public.manual_reconcile_operations%ROWTYPE;
    v_existing public.manual_reconcile_operations%ROWTYPE;
    v_scope_company text;
    v_scope_owner text;
    v_expected_count integer;
    v_target_count integer;
    v_tally_count integer;
    v_changed integer := 0;
    v_row jsonb;
    v_entry jsonb;
    v_id bigint;
    v_current public.tally_voucher_ledgers%ROWTYPE;
    v_amount numeric;
    v_debit numeric;
    v_credit numeric;
    v_fingerprint text;
    v_input jsonb;
    v_result jsonb;
BEGIN
    IF p_operation_id IS NULL OR p_attempt_id IS NULL
       OR p_operation_token IS NULL OR p_execution_id IS NULL
       OR p_source_fetch_operation_id IS NULL
       OR p_operation_ordinal IS NULL OR p_operation_ordinal < 1
       OR p_expected_rows IS NULL
       OR pg_catalog.jsonb_typeof(p_expected_rows) IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(p_expected_rows) = 0
       OR pg_catalog.jsonb_array_length(p_expected_rows) > 10000
       OR p_operation_id = p_source_fetch_operation_id
    THEN
        RETURN QUERY SELECT false, 'INVALID_INPUT'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- Locks company advisory key and RUNNING reservation in established order.
    SELECT e.* INTO STRICT v_execution
    FROM public.require_manual_reconcile_execution_v1(
        p_attempt_id, p_operation_token, p_execution_id
    ) AS e;

    SELECT a.* INTO STRICT v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;
    v_scope_company := v_attempt.company_code;
    v_scope_owner := v_attempt.tally_owner;

    -- Lock source before settlement journal; source must be immutable and verified.
    SELECT o.* INTO v_source
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_source_fetch_operation_id
    FOR UPDATE;
    IF NOT FOUND OR v_source.operation_type IS DISTINCT FROM 'VOUCHER_FETCH'
       OR v_source.state IS DISTINCT FROM 'RESULT_VERIFIED'
       OR v_source.attempt_id IS DISTINCT FROM p_attempt_id
       OR v_source.reservation_id IS DISTINCT FROM v_execution.reservation_id
       OR v_source.operation_token IS DISTINCT FROM p_operation_token
       OR v_source.execution_id IS DISTINCT FROM p_execution_id
       OR v_source.operation_ordinal >= p_operation_ordinal
       OR v_source.input_payload->>'companyCode' IS DISTINCT FROM v_scope_company
       OR v_source.input_payload->>'tallyOwner' IS DISTINCT FROM v_scope_owner
       OR pg_catalog.jsonb_typeof(v_source.result_payload->'entries') IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(v_source.result_payload->'entries') = 0
    THEN
        RETURN QUERY SELECT false, 'SOURCE_FETCH_INVALID'::text, NULL::text, 0;
        RETURN;
    END IF;

    v_input := pg_catalog.jsonb_build_object(
        'sourceFetchOperationId', p_source_fetch_operation_id::text,
        'expectedRows', p_expected_rows,
        'companyCode', v_scope_company,
        'tallyOwner', v_scope_owner
    );
    v_fingerprint := pg_catalog.md5(v_input::text);

    SELECT o.* INTO v_existing
    FROM public.manual_reconcile_operations AS o
    WHERE o.operation_id = p_operation_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.operation_type IS DISTINCT FROM 'VOUCHER_APPLY'
           OR v_existing.attempt_id IS DISTINCT FROM p_attempt_id
           OR v_existing.reservation_id IS DISTINCT FROM v_execution.reservation_id
           OR v_existing.operation_token IS DISTINCT FROM p_operation_token
           OR v_existing.execution_id IS DISTINCT FROM p_execution_id
           OR v_existing.operation_ordinal IS DISTINCT FROM p_operation_ordinal
           OR v_existing.source_fetch_operation_id IS DISTINCT FROM p_source_fetch_operation_id
           OR v_existing.input_payload IS DISTINCT FROM v_input
           OR v_existing.input_fingerprint IS DISTINCT FROM v_fingerprint
        THEN
            RETURN QUERY SELECT false, 'OPERATION_CONFLICT'::text, v_existing.state, 0;
            RETURN;
        END IF;
             IF v_existing.state = 'RESULT_VERIFIED' THEN
            RETURN QUERY SELECT
                true,
                'RESULT_VERIFIED'::text,
                'RESULT_VERIFIED'::text,
                COALESCE(
                    (v_existing.result_payload->>'updatedRows')::integer,
                    0
                );
            RETURN;
        END IF;
        -- A previously created, unsettled operation is never silently re-applied.
        RETURN QUERY SELECT false, 'OPERATION_NOT_SETTLED'::text, v_existing.state, 0;
        RETURN;
    END IF;

    -- Fail closed if this fetch or ordinal was already consumed.
    IF EXISTS (
        SELECT 1 FROM public.manual_reconcile_operations AS o
        WHERE o.source_fetch_operation_id = p_source_fetch_operation_id
           OR (o.attempt_id = p_attempt_id
               AND o.operation_ordinal = p_operation_ordinal)
    ) THEN
        RETURN QUERY SELECT false, 'OPERATION_ALREADY_RESERVED'::text, NULL::text, 0;
        RETURN;
    END IF;

    v_expected_count := pg_catalog.jsonb_array_length(p_expected_rows);
    v_tally_count := pg_catalog.jsonb_array_length(v_source.result_payload->'entries');
    IF v_expected_count <> v_tally_count THEN
        RETURN QUERY SELECT false, 'ENTRY_COUNT_MISMATCH'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- No guessed occurrence matching: every (voucher,ledger) must be unique.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(v_source.result_payload->'entries') AS t(value)
        GROUP BY t.value->>'voucherGuid', t.value->>'ledgerGuid'
        HAVING pg_catalog.count(*) <> 1
    ) THEN
        RETURN QUERY SELECT false, 'TALLY_OCCURRENCE_AMBIGUOUS'::text, NULL::text, 0;
        RETURN;
    END IF;
    -- Independently validate verified Tally monetary values.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            v_source.result_payload->'entries'
        ) AS t(value)
        WHERE pg_catalog.jsonb_typeof(t.value) IS DISTINCT FROM 'object'
           OR pg_catalog.jsonb_typeof(t.value->'amount') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'debit') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'credit') IS DISTINCT FROM 'string'
           OR (t.value->>'amount') COLLATE "C"
                !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (t.value->>'debit') COLLATE "C"
                !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (t.value->>'credit') COLLATE "C"
                !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR pg_catalog.length(t.value->>'amount') > 18
           OR pg_catalog.length(t.value->>'debit') > 17
           OR pg_catalog.length(t.value->>'credit') > 17
    ) THEN
        RETURN QUERY SELECT false, 'TALLY_AMOUNTS_INVALID'::text, NULL::text, 0;
        RETURN;
    END IF;
        -- Validate Tally monetary limits and debit/credit consistency.
    -- This runs only after the monetary string-format validation.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            v_source.result_payload->'entries'
        ) AS t(value)
        WHERE pg_catalog.abs((t.value->>'amount')::numeric)
                > 90071992547409.91
           OR (t.value->>'debit')::numeric
                > 90071992547409.91
           OR (t.value->>'credit')::numeric
                > 90071992547409.91
           OR (
                (t.value->>'debit')::numeric > 0
                AND (t.value->>'credit')::numeric > 0
           )
           OR pg_catalog.abs((t.value->>'amount')::numeric)
                <> (
                    (t.value->>'debit')::numeric
                    + (t.value->>'credit')::numeric
                )
    ) THEN
        RETURN QUERY SELECT
            false,
            'TALLY_AMOUNTS_INVALID'::text,
            NULL::text,
            0;
        RETURN;
    END IF;
        -- Every verified Tally entry must belong to this reconciliation scope.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            v_source.result_payload->'entries'
        ) AS t(value)
        WHERE pg_catalog.jsonb_typeof(t.value) IS DISTINCT FROM 'object'
           OR t.value->>'companyCode' IS DISTINCT FROM v_scope_company
           OR t.value->>'tallyOwner' IS DISTINCT FROM v_scope_owner
           OR pg_catalog.jsonb_typeof(t.value->'voucherGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'ledgerGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.length(t.value->>'voucherGuid') = 0
           OR pg_catalog.length(t.value->>'ledgerGuid') = 0
    ) THEN
        RETURN QUERY SELECT
            false,
            'TALLY_ENTRY_SCOPE_INVALID'::text,
            NULL::text,
            0;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
        WHERE pg_catalog.jsonb_typeof(t.value) IS DISTINCT FROM 'object'
           OR pg_catalog.jsonb_typeof(t.value->'id') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'voucherGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'ledgerGuid') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'amount') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'debit') IS DISTINCT FROM 'string'
           OR pg_catalog.jsonb_typeof(t.value->'credit') IS DISTINCT FROM 'string'
           OR (t.value->>'id') COLLATE "C" !~ '^[1-9][0-9]{0,18}$'
           OR CASE
                WHEN (t.value->>'id') COLLATE "C" ~ '^[1-9][0-9]{0,18}$'
                THEN (t.value->>'id')::numeric > 9223372036854775807
                ELSE false
              END
           OR (t.value->>'amount') COLLATE "C" !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (t.value->>'debit') COLLATE "C" !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR (t.value->>'credit') COLLATE "C" !~ '^(0|[1-9][0-9]*)\.[0-9]{2}$'
           OR pg_catalog.length(t.value->>'amount') > 18
           OR pg_catalog.length(t.value->>'debit') > 17
           OR pg_catalog.length(t.value->>'credit') > 17
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
        GROUP BY t.value->>'id'
        HAVING pg_catalog.count(*) <> 1
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
        GROUP BY t.value->>'voucherGuid', t.value->>'ledgerGuid'
        HAVING pg_catalog.count(*) <> 1
    ) THEN
        RETURN QUERY SELECT false, 'EXPECTED_ROWS_INVALID'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- Reject malformed monetary snapshots, including values outside safe contract.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
        WHERE pg_catalog.abs((t.value->>'amount')::numeric) > 90071992547409.91
           OR (t.value->>'debit')::numeric > 90071992547409.91
           OR (t.value->>'credit')::numeric > 90071992547409.91
           OR ((t.value->>'debit')::numeric > 0 AND (t.value->>'credit')::numeric > 0)
           OR pg_catalog.abs((t.value->>'amount')::numeric)
              <> (t.value->>'debit')::numeric + (t.value->>'credit')::numeric
    ) THEN
        RETURN QUERY SELECT false, 'EXPECTED_AMOUNTS_INVALID'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- Exact bijection between SQL snapshot and verified Tally entries.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS x(value)
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_array_elements(v_source.result_payload->'entries') AS t(value)
            WHERE t.value->>'voucherGuid' = x.value->>'voucherGuid'
              AND t.value->>'ledgerGuid' = x.value->>'ledgerGuid'
        )
    ) THEN
        RETURN QUERY SELECT false, 'TALLY_SQL_IDENTITY_MISMATCH'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- Validate source fetch voucher coverage before locking target rows.
    IF pg_catalog.jsonb_typeof(
        v_source.input_payload->'voucherGuids'
    ) IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(
           v_source.input_payload->'voucherGuids'
       ) = 0
    THEN
        RETURN QUERY SELECT false, 'SOURCE_VOUCHER_GUIDS_INVALID'::text, NULL::text, 0;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(
            v_source.input_payload->'voucherGuids'
        ) AS g(value)
        WHERE pg_catalog.jsonb_typeof(g.value) IS DISTINCT FROM 'string'
           OR (g.value #>> '{}') COLLATE "C"
                !~ '^[A-Za-z0-9][A-Za-z0-9-]{0,127}$'
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements_text(
            v_source.input_payload->'voucherGuids'
        ) AS g(guid)
        GROUP BY pg_catalog.lower(g.guid)
        HAVING pg_catalog.count(*) > 1
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS x(value)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.jsonb_array_elements_text(
                v_source.input_payload->'voucherGuids'
            ) AS g(guid)
            WHERE g.guid = x.value->>'voucherGuid'
        )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.jsonb_array_elements_text(
            v_source.input_payload->'voucherGuids'
        ) AS g(guid)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS x(value)
            WHERE x.value->>'voucherGuid' = g.guid
        )
    ) THEN
        RETURN QUERY SELECT
            false,
            'SOURCE_VOUCHER_GUIDS_INVALID'::text,
            NULL::text,
            0;
        RETURN;
    END IF;
    -- Serialize writes to this table during this transaction only.
    -- This does not exclude a multi-transaction normal-sync replacement lifecycle.
    LOCK TABLE public.tally_voucher_ledgers IN SHARE ROW EXCLUSIVE MODE;

    -- Lock *all* current rows in scope before any updates. Re-read under lock.
    PERFORM 1 FROM public.tally_voucher_ledgers AS l
    WHERE l.company_code = v_scope_company
      AND l.tally_owner = v_scope_owner
      AND l.voucher_guid IN (
          SELECT t.value #>> '{}'
          FROM pg_catalog.jsonb_array_elements(v_source.input_payload->'voucherGuids') AS t(value)
      )
    ORDER BY l.id FOR UPDATE;

    SELECT pg_catalog.count(*)::integer INTO v_target_count
    FROM public.tally_voucher_ledgers AS l
    WHERE l.company_code = v_scope_company
      AND l.tally_owner = v_scope_owner
      AND l.voucher_guid IN (
          SELECT t.value #>> '{}'
          FROM pg_catalog.jsonb_array_elements(v_source.input_payload->'voucherGuids') AS t(value)
      );
    IF v_target_count <> v_expected_count THEN
        RETURN QUERY SELECT false, 'CURRENT_ROW_SET_CHANGED'::text, NULL::text, 0;
        RETURN;
    END IF;

    -- Validate every current row before changing any of them.
    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
    LOOP
        v_id := (v_row->>'id')::bigint;
        SELECT l.* INTO v_current
        FROM public.tally_voucher_ledgers AS l
        WHERE l.id = v_id FOR UPDATE;
        IF NOT FOUND
           OR v_current.company_code IS DISTINCT FROM v_scope_company
           OR v_current.tally_owner IS DISTINCT FROM v_scope_owner
           OR v_current.voucher_guid IS DISTINCT FROM v_row->>'voucherGuid'
           OR v_current.ledger_guid IS DISTINCT FROM v_row->>'ledgerGuid'
           OR v_current.is_deleted IS DISTINCT FROM false
           OR v_current.amount IS NULL OR v_current.debit IS NULL OR v_current.credit IS NULL
           OR v_current.amount <> (v_row->>'amount')::numeric
           OR v_current.debit <> (v_row->>'debit')::numeric
           OR v_current.credit <> (v_row->>'credit')::numeric
           OR pg_catalog.jsonb_typeof(v_current.bill_allocations) IS DISTINCT FROM 'array'
           OR pg_catalog.jsonb_typeof(v_current.costcentre_allocations) IS DISTINCT FROM 'array'
           OR pg_catalog.jsonb_array_length(v_current.bill_allocations) <> 0
           OR pg_catalog.jsonb_array_length(v_current.costcentre_allocations) <> 0
        THEN
            RETURN QUERY SELECT false, 'CURRENT_ROW_UNSAFE_OR_STALE'::text, NULL::text, 0;
            RETURN;
        END IF;
    END LOOP;

    -- All checks passed. Insert journal and update rows in this same transaction.
    INSERT INTO public.manual_reconcile_operations (
        operation_id, attempt_id, reservation_id, operation_token,
        execution_id, operation_ordinal, operation_type,
        source_fetch_operation_id, input_payload, input_fingerprint, state
    ) VALUES (
        p_operation_id, p_attempt_id, v_execution.reservation_id, p_operation_token,
        p_execution_id, p_operation_ordinal, 'VOUCHER_APPLY',
        p_source_fetch_operation_id, v_input, v_fingerprint, 'PREPARED'
    );

    FOR v_row IN SELECT value FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
    LOOP
        SELECT t.value INTO STRICT v_entry
        FROM pg_catalog.jsonb_array_elements(v_source.result_payload->'entries') AS t(value)
        WHERE t.value->>'voucherGuid' = v_row->>'voucherGuid'
          AND t.value->>'ledgerGuid' = v_row->>'ledgerGuid';
        v_amount := (v_entry->>'amount')::numeric;
        v_debit := (v_entry->>'debit')::numeric;
        v_credit := (v_entry->>'credit')::numeric;
        UPDATE public.tally_voucher_ledgers AS l
        SET amount = v_amount, debit = v_debit, credit = v_credit
        WHERE l.id = (v_row->>'id')::bigint
          AND (l.amount, l.debit, l.credit)
              IS DISTINCT FROM (v_amount, v_debit, v_credit);
        GET DIAGNOSTICS v_target_count = ROW_COUNT;
        v_changed := v_changed + v_target_count;
    END LOOP;
    -- Verify every row after the UPDATE.
    -- Any mismatch raises an exception and rolls back
    -- both the voucher updates and operation journal.
    FOR v_row IN
        SELECT value
        FROM pg_catalog.jsonb_array_elements(p_expected_rows) AS t(value)
    LOOP
        SELECT t.value INTO STRICT v_entry
        FROM pg_catalog.jsonb_array_elements(
            v_source.result_payload->'entries'
        ) AS t(value)
        WHERE t.value->>'voucherGuid' = v_row->>'voucherGuid'
          AND t.value->>'ledgerGuid' = v_row->>'ledgerGuid';

        SELECT l.* INTO STRICT v_current
        FROM public.tally_voucher_ledgers AS l
        WHERE l.id = (v_row->>'id')::bigint
        FOR UPDATE;

        IF v_current.amount IS DISTINCT FROM
               (v_entry->>'amount')::numeric
           OR v_current.debit IS DISTINCT FROM
               (v_entry->>'debit')::numeric
           OR v_current.credit IS DISTINCT FROM
               (v_entry->>'credit')::numeric
        THEN
            RAISE EXCEPTION
                'RECONCILE_VOUCHER_POST_WRITE_VERIFICATION_FAILED';
        END IF;
    END LOOP;
    v_result := pg_catalog.jsonb_build_object(
        'sourceFetchOperationId', p_source_fetch_operation_id::text,
        'matchedRows', v_expected_count,
        'updatedRows', v_changed
    );
    UPDATE public.manual_reconcile_operations AS o
    SET state = 'RESULT_VERIFIED', result_payload = v_result,
        result_verified_at = pg_catalog.transaction_timestamp()
    WHERE o.operation_id = p_operation_id;

    RETURN QUERY SELECT true, 'RESULT_VERIFIED'::text, 'RESULT_VERIFIED'::text, v_changed;
END;
$fn$;

ALTER FUNCTION public.apply_manual_reconcile_voucher_amounts_v1(
    uuid, uuid, uuid, uuid, integer, uuid, jsonb
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.apply_manual_reconcile_voucher_amounts_v1(
    uuid, uuid, uuid, uuid, integer, uuid, jsonb
) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.apply_manual_reconcile_voucher_amounts_v1(
    uuid, uuid, uuid, uuid, integer, uuid, jsonb
) TO service_role;
COMMIT;
