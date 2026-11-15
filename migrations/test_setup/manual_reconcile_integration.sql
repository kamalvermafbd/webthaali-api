-- LOCAL ONLY: SQL RPC integration tests, synthetic fixtures only.
-- Requires PostgreSQL 17, baseline + all 18 migrations, psql, local postgres
-- administrator (fixture setup and inspection bypass RLS).
-- Does not call Supabase, Redis, Socket.IO, Tally, or application APIs.
-- Synthetic release receipts test SQL evidence validation only.
-- VOUCHER_SETTLEMENT_ENABLED must remain false; app code is not touched.
-- Not a multi-connection concurrency test or ManualReconciliationSession.js E2E.
-- All rows/helpers roll back. Identity sequence increments do NOT roll back.
-- On failure ON_ERROR_STOP exits psql and disconnect rolls back the transaction.
-- Run from repository root:
-- psql -X -h 127.0.0.1 -p 5433 -U postgres -d billey_reconcile_test -v ON_ERROR_STOP=1 -f migrations/test_setup/manual_reconcile_integration.sql
\set ON_ERROR_STOP on
\connect "host=127.0.0.1 port=5433 dbname=billey_reconcile_test user=postgres"

BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL client_min_messages = notice;

DO $guard$
BEGIN
    IF current_database() <> 'billey_reconcile_test'
       OR inet_server_addr() IS DISTINCT FROM '127.0.0.1'::inet
       OR inet_server_port() IS DISTINCT FROM 5433
       OR current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999
    THEN
        RAISE EXCEPTION 'LOCAL_POSTGRES_17_TARGET_REQUIRED';
    END IF;
    IF current_user <> 'postgres' THEN
        RAISE EXCEPTION 'Run as local postgres for synthetic fixtures and RLS inspection';
    END IF;
END;
$guard$;

CREATE TEMP TABLE test_passes (label text NOT NULL) ON COMMIT DROP;

CREATE FUNCTION pg_temp.check_ok(ok boolean, label text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    IF ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'FAIL: %', label;
    END IF;
    INSERT INTO pg_temp.test_passes VALUES (label);
    RAISE NOTICE 'PASS: %', label;
END;
$fn$;

CREATE FUNCTION pg_temp.expect_error(
    command text, expected_message text, label text,
    expected_state text DEFAULT 'P0001'
) RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
    caught boolean := false;
BEGIN
    BEGIN
        EXECUTE command;
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM IS DISTINCT FROM expected_message
           OR SQLSTATE IS DISTINCT FROM expected_state THEN
            RAISE EXCEPTION 'FAIL: %; expected % / %, got % / %',
                label, expected_state, expected_message, SQLSTATE, SQLERRM;
        END IF;
        caught := true;
    END;
    PERFORM pg_temp.check_ok(caught, label);
END;
$fn$;

DO $tests$
DECLARE
    company text := 'LOCAL_RECONCILE_TEST_' || gen_random_uuid()::text;
    batch text := gen_random_uuid()::text;
    claim uuid := gen_random_uuid();
    ledger text := gen_random_uuid()::text;
    worker text := 'local-synthetic-worker';
    connector text := 'local-synthetic-connector';
    socket text := 'local-synthetic-socket';
    tally_company text := gen_random_uuid()::text;
    attempt uuid;
    execution uuid;
    owner_token uuid;
    server_id uuid;
    token uuid;
    fetch_id uuid;
    apply_id uuid;
    request_id uuid;
    receipt jsonb;
    mismatches jsonb;
    input jsonb;
    previous_attempt uuid;
    previous_execution uuid;
    previous_owner uuid;
    previous_token uuid;
    previous_receipt jsonb;
    previous_mismatches jsonb;
    r record;
    signature text;
    stage text := 'RPC prerequisites';
    cycle integer;
BEGIN
    FOREACH signature IN ARRAY ARRAY[
        'public.start_manual_reconcile_claimed_v1(uuid,text,text,text,text,text,uuid)',
        'public.bind_manual_reconcile_execution_v1(uuid,uuid,uuid,text,text,uuid,uuid)',
        'public.prepare_manual_reconcile_fetch_v2(uuid,uuid,uuid,integer,uuid,text,text,text,jsonb,text,uuid)',
        'public.mark_manual_reconcile_fetch_dispatch_v2(uuid,uuid,uuid,uuid,uuid)',
        'public.verify_manual_reconcile_fetch_result_v2(uuid,uuid,uuid,uuid,jsonb,uuid)',
        'public.prepare_manual_reconcile_apply_v2(uuid,uuid,uuid,integer,uuid,uuid)',
        'public.apply_manual_reconcile_ledger_opening_v3(uuid,uuid,uuid,uuid)',
        'public.record_manual_reconcile_pre_release_checkpoint_v1(uuid,uuid,uuid,text,uuid,jsonb)',
        'public.record_manual_reconcile_finalization_v1(uuid,uuid,jsonb,boolean,boolean,text,text,jsonb)',
        'public.close_manual_reconcile_v2(uuid,uuid)'
    ] LOOP
        PERFORM pg_temp.check_ok(to_regprocedure(signature) IS NOT NULL,
            'required RPC exists: ' || signature);
        PERFORM pg_temp.check_ok(has_function_privilege('service_role', signature, 'EXECUTE')
            AND NOT has_function_privilege('anon', signature, 'EXECUTE')
            AND NOT has_function_privilege('authenticated', signature, 'EXECUTE'),
            'RPC execution grants: ' || signature);
    END LOOP;

    stage := 'synthetic fixtures';
    INSERT INTO public.sync_batches (
        batch_id, company_code, tally_owner, batch_status, worker_status,
        batch_closed, ledger_reconciliation_completed, stock_reconciliation_completed,
        execution_claim_token, error_message
    ) VALUES (
        batch, company, 'USER', 'COMPLETED', 'COMPLETED',
        false, false, true, claim, NULL
    );
    INSERT INTO public.tally_sync_ledgers (
        company_code, tally_owner, guid, name, opening_balance,
        opening_balance_type, is_deleted
    ) VALUES (company, 'USER', ledger, 'Synthetic reconciliation ledger', 10, 'DR', false);

    -- Cycle 2 reuses the same reservation with a new attempt and tests the
    -- investigation outcome. It also proves old close replay cannot close it.
    FOR cycle IN 1..2 LOOP
        attempt := gen_random_uuid();
        execution := gen_random_uuid();
        owner_token := gen_random_uuid();
        server_id := gen_random_uuid();
        fetch_id := gen_random_uuid();
        apply_id := gen_random_uuid();
        request_id := gen_random_uuid();
        mismatches := CASE WHEN cycle = 1 THEN '[]'::jsonb
                           ELSE jsonb_build_array(ledger) END;
        stage := 'admission cycle ' || cycle;

        SELECT * INTO STRICT r FROM public.start_manual_reconcile_claimed_v1(
            attempt, company, 'USER', 'LEDGER', worker, batch, gen_random_uuid());
        PERFORM pg_temp.check_ok(NOT r.acquired AND r.reason = 'BATCH_CLAIM_INVALID'
            AND r.reservation_token IS NULL, 'wrong batch claim rejected');

        SELECT * INTO STRICT r FROM public.start_manual_reconcile_claimed_v1(
            attempt, company, 'USER', 'LEDGER', worker, batch, claim);
        PERFORM pg_temp.check_ok(r.acquired AND r.reason = 'ACQUIRED'
            AND r.reservation_token IS NOT NULL, 'claimed session admitted');
        token := r.reservation_token;

        SELECT * INTO STRICT r FROM public.start_manual_reconcile_claimed_v1(
            attempt, company, 'USER', 'LEDGER', worker, batch, claim);
        PERFORM pg_temp.check_ok(r.acquired AND r.reason = 'ALREADY_ACQUIRED'
            AND r.reservation_token = token, 'exact admission replay acknowledged');

        SELECT * INTO STRICT r FROM public.start_manual_reconcile_claimed_v1(
            attempt, company, 'USER', 'LEDGER', 'different-worker', batch, claim);
        PERFORM pg_temp.check_ok(NOT r.acquired AND r.reason = 'ATTEMPT_ID_CONFLICT',
            'changed admission identity rejected');

        SELECT * INTO STRICT r FROM public.start_manual_reconcile_claimed_v1(
            gen_random_uuid(), company, 'USER', 'LEDGER', worker, batch, claim);
        PERFORM pg_temp.check_ok(NOT r.acquired AND r.reason = 'RECONCILE_ALREADY_RUNNING',
            'competing admission rejected');

        IF cycle = 2 THEN
            SELECT * INTO STRICT r FROM public.close_manual_reconcile_v2(previous_attempt, previous_token);
            PERFORM pg_temp.check_ok(r.closed AND r.reason = 'ALREADY_CLOSED',
                'historical close replay acknowledged during newer reservation');
            SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
                previous_attempt, previous_token, previous_mismatches, true, true,
                connector, previous_owner::text, previous_receipt);
            PERFORM pg_temp.check_ok(r.recorded AND r.reason = 'ALREADY_RECORDED',
                'historical evidence replay acknowledged during newer reservation');
            PERFORM pg_temp.check_ok(EXISTS (
                SELECT 1 FROM public.manual_reconcile_status
                WHERE company_code = company AND tally_owner = 'USER'
                  AND status = 'RUNNING' AND operation_token = token
            ), 'historical replay preserves newer RUNNING reservation');
            PERFORM pg_temp.expect_error(format(
                'SELECT public.require_manual_reconcile_execution_v1(%L::uuid,%L::uuid,%L::uuid)',
                previous_attempt, previous_token, previous_execution),
                'RECONCILE_RESERVATION_NOT_OWNED', 'stale execution rejected after reservation reuse');

            SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
                attempt, token, previous_execution, connector, socket, owner_token, server_id);
            PERFORM pg_temp.check_ok(NOT r.bound AND r.reason = 'EXECUTION_IDENTITY_CONFLICT',
                'execution UUID reused across attempts rejected');
            SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
                attempt, token, execution, connector, socket, previous_owner, server_id);
            PERFORM pg_temp.check_ok(NOT r.bound AND r.reason = 'EXECUTION_IDENTITY_CONFLICT',
                'owner token reused across attempts rejected');
        END IF;

        stage := 'execution binding cycle ' || cycle;
        SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
            attempt, gen_random_uuid(), execution, connector, socket, owner_token, server_id);
        PERFORM pg_temp.check_ok(NOT r.bound AND r.reason = 'ATTEMPT_NOT_CURRENT',
            'wrong reservation token cannot bind');
        SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
            attempt, token, execution, connector, socket, owner_token, server_id);
        PERFORM pg_temp.check_ok(r.bound AND r.reason = 'BOUND' AND r.execution_id = execution,
            'first execution bound');
        SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
            attempt, token, execution, connector, socket, owner_token, server_id);
        PERFORM pg_temp.check_ok(r.bound AND r.reason = 'ALREADY_BOUND',
            'duplicate binding acknowledges identity only');
        SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
            attempt, token, gen_random_uuid(), connector, socket, owner_token, server_id);
        PERFORM pg_temp.check_ok(NOT r.bound AND r.reason = 'EXECUTION_BINDING_CONFLICT',
            'replacement execution on same attempt rejected');

        input := jsonb_build_object('ledgerGuid', ledger, 'booksBeginningFrom', '20260401');
        PERFORM pg_temp.expect_error(format(
            'SELECT * FROM public.prepare_manual_reconcile_fetch_v2(%L::uuid,%L::uuid,%L::uuid,1,%L::uuid,%L,%L,%L,%L::jsonb,%L,%L::uuid)',
            fetch_id, attempt, token, request_id, ledger, connector, tally_company,
            input, md5(input::text), gen_random_uuid()),
            'RECONCILE_EXECUTION_NOT_BOUND', 'fetch rejects foreign execution');
        PERFORM pg_temp.expect_error(format(
            'SELECT * FROM public.prepare_manual_reconcile_fetch_v2(%L::uuid,%L::uuid,%L::uuid,1,%L::uuid,%L,%L,%L,%L::jsonb,%L,%L::uuid)',
            fetch_id, attempt, gen_random_uuid(), request_id, ledger, connector, tally_company,
            input, md5(input::text), execution),
            'RECONCILE_ATTEMPT_NOT_CURRENT', 'fetch rejects wrong reservation token');

        stage := 'durable ledger operation cycle ' || cycle;
        SELECT * INTO STRICT r FROM public.prepare_manual_reconcile_fetch_v2(
            fetch_id, attempt, token, 1, request_id, ledger, connector,
            tally_company, input, md5(input::text), execution);
        PERFORM pg_temp.check_ok(r.prepared AND r.reason = 'PREPARED', 'fetch prepared');
        SELECT * INTO STRICT r FROM public.prepare_manual_reconcile_fetch_v2(
            fetch_id, attempt, token, 1, request_id, ledger, connector,
            tally_company, input, md5(input::text), execution);
        PERFORM pg_temp.check_ok(r.prepared AND r.reason = 'ALREADY_PREPARED',
            'duplicate prepare acknowledged without dispatch authority');

        PERFORM pg_temp.expect_error(format(
            'SELECT public.record_manual_reconcile_pre_release_checkpoint_v1(%L::uuid,%L::uuid,%L::uuid,%L,%L::uuid,%L::jsonb)',
            attempt, token, execution, connector, owner_token, mismatches),
            'RECONCILE_CHECKPOINT_JOURNAL_NOT_VERIFIED', 'checkpoint rejects unfinished fetch');

        SELECT * INTO STRICT r FROM public.mark_manual_reconcile_fetch_dispatch_v2(
            fetch_id, attempt, token, request_id, execution);
        PERFORM pg_temp.check_ok(r.marked AND r.reason = 'DISPATCH_MARKED', 'dispatch marked once');
        SELECT * INTO STRICT r FROM public.mark_manual_reconcile_fetch_dispatch_v2(
            fetch_id, attempt, token, request_id, execution);
        PERFORM pg_temp.check_ok(NOT r.marked AND r.reason = 'DISPATCH_ALREADY_MARKED_OR_SETTLED',
            'duplicate dispatch rejected');

        -- Synthetic connector payload, not a real Tally response.
        SELECT * INTO STRICT r FROM public.verify_manual_reconcile_fetch_result_v2(
            fetch_id, attempt, token, request_id,
            jsonb_build_object('guid', ledger, 'openingBalance', '25.00', 'openingBalanceType', 'DR'),
            execution);
        PERFORM pg_temp.check_ok(r.verified AND r.reason = 'RESULT_VERIFIED', 'synthetic result verified');
        PERFORM pg_temp.expect_error(format(
            'SELECT public.record_manual_reconcile_pre_release_checkpoint_v1(%L::uuid,%L::uuid,%L::uuid,%L,%L::uuid,%L::jsonb)',
            attempt, token, execution, connector, owner_token, mismatches),
            'RECONCILE_CHECKPOINT_FETCH_APPLY_MISMATCH', 'checkpoint rejects fetch without apply');

        SELECT * INTO STRICT r FROM public.prepare_manual_reconcile_apply_v2(
            apply_id, attempt, token, 2, fetch_id, execution);
        PERFORM pg_temp.check_ok(r.prepared AND r.reason = 'PREPARED', 'ledger apply prepared');
        SELECT * INTO STRICT r FROM public.apply_manual_reconcile_ledger_opening_v3(
            apply_id, attempt, token, execution);
        PERFORM pg_temp.check_ok(r.result = CASE WHEN cycle = 1 THEN 'UPDATED' ELSE 'UNCHANGED' END
            AND r.current_balance::numeric = 25 AND r.current_type = 'DR',
            'ledger apply returns expected exact monetary result');
        SELECT * INTO STRICT r FROM public.apply_manual_reconcile_ledger_opening_v3(
            apply_id, attempt, token, execution);
        PERFORM pg_temp.check_ok(r.result = CASE WHEN cycle = 1 THEN 'UPDATED' ELSE 'UNCHANGED' END
            AND r.current_balance::numeric = 25, 'ledger apply replay returns stored result');
        PERFORM pg_temp.check_ok((
            SELECT count(*) = 2 AND bool_and(state = 'RESULT_VERIFIED')
            FROM public.manual_reconcile_operations WHERE attempt_id = attempt
        ), 'one verified fetch/apply pair persisted');

        stage := 'finalization cycle ' || cycle;
        receipt := jsonb_build_object(
            'redisReleaseConfirmed', true, 'attemptId', attempt::text,
            'executionId', execution::text, 'serverInstanceId', server_id::text,
            'connectorId', connector, 'socketId', socket, 'ownerToken', owner_token::text
        );
        SELECT * INTO STRICT r FROM public.close_manual_reconcile_v2(attempt, token);
        PERFORM pg_temp.check_ok(NOT r.closed AND r.reason = 'FINALIZATION_NOT_VERIFIED',
            'premature close rejected');
        SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
            attempt, token, mismatches, true, true, connector, owner_token::text, receipt);
        PERFORM pg_temp.check_ok(NOT r.recorded AND r.reason = 'CHECKPOINT_NOT_VERIFIED',
            'finalization without checkpoint rejected');

        PERFORM pg_temp.check_ok(public.record_manual_reconcile_pre_release_checkpoint_v1(
            attempt, token, execution, connector, owner_token, mismatches), 'checkpoint sealed');
        PERFORM pg_temp.check_ok(public.record_manual_reconcile_pre_release_checkpoint_v1(
            attempt, token, execution, connector, owner_token, mismatches), 'exact checkpoint replay accepted');
        PERFORM pg_temp.expect_error(format(
            'UPDATE public.manual_reconcile_operations SET input_fingerprint = input_fingerprint WHERE operation_id = %L::uuid',
            fetch_id), 'RECONCILE_JOURNAL_SEALED', 'journal mutation after checkpoint rejected');
        PERFORM pg_temp.expect_error(format(
            'UPDATE public.manual_reconcile_pre_release_checkpoints SET connector_id = connector_id WHERE attempt_id = %L::uuid',
            attempt), 'RECONCILE_CHECKPOINT_IMMUTABLE', 'checkpoint mutation rejected', '55000');

        SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
            attempt, token, mismatches, true, true, connector, owner_token::text,
            receipt || jsonb_build_object('socketId', 'foreign-socket'));
        PERFORM pg_temp.check_ok(NOT r.recorded AND r.reason = 'RELEASE_IDENTITY_MISMATCH',
            'release receipt identity mismatch rejected');
        SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
            attempt, token, mismatches, true, true, connector, owner_token::text, receipt);
        PERFORM pg_temp.check_ok(r.recorded AND r.reason = 'EVIDENCE_RECORDED', 'finalization recorded');
        SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
            attempt, token, mismatches, true, true, connector, owner_token::text, receipt);
        PERFORM pg_temp.check_ok(r.recorded AND r.reason = 'ALREADY_RECORDED', 'finalization replay accepted');
        SELECT * INTO STRICT r FROM public.record_manual_reconcile_finalization_v1(
            attempt, token, mismatches, true, true, connector, owner_token::text,
            receipt || jsonb_build_object('syntheticExtra', true));
        PERFORM pg_temp.check_ok(NOT r.recorded AND r.reason = 'FINALIZATION_EVIDENCE_CONFLICT',
            'changed finalization receipt rejected');
        SELECT * INTO STRICT r FROM public.close_manual_reconcile_v2(attempt, token);
        PERFORM pg_temp.check_ok(r.closed AND r.reason = 'CLOSED', 'reservation closed');
        SELECT * INTO STRICT r FROM public.close_manual_reconcile_v2(attempt, token);
        PERFORM pg_temp.check_ok(r.closed AND r.reason = 'ALREADY_CLOSED', 'duplicate close accepted');
        PERFORM pg_temp.check_ok(EXISTS (
            SELECT 1 FROM public.manual_reconcile_finalizations
            WHERE attempt_id = attempt AND state = 'CLOSED'
              AND sealed_at IS NOT NULL AND closed_at IS NOT NULL
              AND outcome = CASE WHEN cycle = 1 THEN 'RECONCILED'
                            ELSE 'TRANSACTION_INVESTIGATION_REQUIRED' END
        ) AND EXISTS (
            SELECT 1 FROM public.manual_reconcile_status
            WHERE company_code = company AND tally_owner = 'USER'
              AND status = 'CLOSED' AND operation_token IS NULL
        ), 'finalization outcome and atomic close state verified');
        SELECT * INTO STRICT r FROM public.bind_manual_reconcile_execution_v1(
            attempt, token, execution, connector, socket, owner_token, server_id);
        PERFORM pg_temp.check_ok(NOT r.bound AND r.reason = 'ATTEMPT_NOT_CURRENT',
            'closed execution cannot bind again');

        previous_attempt := attempt;
        previous_execution := execution;
        previous_owner := owner_token;
        previous_token := token;
        previous_receipt := receipt;
        previous_mismatches := mismatches;
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL at [%]: % (SQLSTATE %)', stage, SQLERRM, SQLSTATE;
END;
$tests$;

SELECT count(*) AS passed_assertions FROM pg_temp.test_passes;
ROLLBACK;
\echo 'PASS: integration assertions completed; synthetic rows and helpers rolled back.'
\echo 'Identity sequences may have advanced. Concurrency and connector E2E remain untested.'