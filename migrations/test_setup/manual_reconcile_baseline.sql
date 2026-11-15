-- Billey Manual Reconciliation: schema-only local PostgreSQL 17 baseline.
-- Target: 127.0.0.1:5433/billey_reconcile_test. NOT a production migration.
-- Reconstructed from read-only PostgreSQL system catalogs; function bodies
-- are pg_get_functiondef output, not placeholders. No production rows or
-- sequence current values are included.
-- Prerequisites: a fresh database; local administrator/postgres ownership;
-- existing roles anon, authenticated, service_role (BYPASSRLS);
-- PostgreSQL 17 pgcrypto extension files, version 1.3.
-- Includes the four requested tables and two requested functions, plus:
-- sync_http_executions and is_sync_outcome_unknown (admission dependencies);
-- existing sync_batches triggers and their real helper functions;
-- company_data_versions (required by the batch-close trigger).
-- RLS enablement is preserved. Application policies/table ACLs are outside
-- this owner-run SQL migration test baseline; this is not an app-role restore.
-- attempts_v2 is intentionally NOT included: apply it once after this baseline
-- on a fresh local database. Do not replay it against existing Supabase.
-- This file has NOT been executed.

SET search_path = public, pg_catalog;

CREATE SCHEMA IF NOT EXISTS extensions;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions VERSION '1.3';

CREATE TABLE public."manual_reconcile_status" (
    "id" bigint GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME "public"."manual_reconcile_status_id_seq" START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE) NOT NULL,
    "company_code" text NOT NULL,
    "tally_owner" text NOT NULL,
    "reconcile_type" text NOT NULL,
    "status" text DEFAULT 'CLOSED'::text NOT NULL,
    "operation_token" uuid,
    "worker_id" text,
    "batch_id" text,
    "started_at" timestamp with time zone,
    "closed_at" timestamp with time zone,
    "error_message" text,
    "created_at" timestamp with time zone DEFAULT now() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE public."manual_reconcile_status" OWNER TO "postgres";

CREATE TABLE public."sync_batches" (
    "id" bigint GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME "public"."sync_batches_id_seq" START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE) NOT NULL,
    "batch_id" text NOT NULL,
    "company_code" text NOT NULL,
    "tally_owner" text NOT NULL,
    "batch_status" text DEFAULT 'PENDING'::text NOT NULL,
    "total_vouchers" integer DEFAULT 0 NOT NULL,
    "processed_vouchers" integer DEFAULT 0 NOT NULL,
    "failed_vouchers" integer DEFAULT 0 NOT NULL,
    "retry_count" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp without time zone DEFAULT now() NOT NULL,
    "completed_at" timestamp without time zone,
    "error_message" text,
    "worker_id" text,
    "worker_status" text DEFAULT 'PENDING'::text,
    "locked_at" timestamp without time zone,
    "heartbeat_at" timestamp without time zone,
    "from_alterid" bigint,
    "to_alterid" bigint,
    "reconciliation_completed" boolean DEFAULT false,
    "current_module" text DEFAULT 'INIT'::text,
    "current_action" text DEFAULT 'PENDING'::text,
    "sync_progress" jsonb DEFAULT '{}'::jsonb,
    "batch_type" text DEFAULT 'INCREMENTAL_SYNC'::text,
    "reconciliation_completed_at" timestamp without time zone,
    "started_at" timestamp without time zone,
    "last_activity_at" timestamp without time zone,
    "last_successful_module" text,
    "incremental_post_completed" boolean DEFAULT false,
    "guid_scan_completed" boolean DEFAULT false,
    "job_type" text DEFAULT 'TALLY_SYNC'::text,
    "priority" integer DEFAULT 5,
    "payload" jsonb DEFAULT '{}'::jsonb,
    "scheduled_at" timestamp without time zone,
    "current_entity" text,
    "current_operation" text,
    "operation_count" integer DEFAULT 0,
    "current_stage" text DEFAULT 'INIT'::text,
    "ledger_reconciliation_completed" boolean,
    "stock_reconciliation_completed" boolean,
    "batch_closed" boolean DEFAULT false,
    "sync_mode" text,
    "worker_type" text,
    "sync_period" text,
    "tally_groups_count" integer DEFAULT 0,
    "tally_ledgers_count" integer DEFAULT 0,
    "tally_stock_groups_count" integer DEFAULT 0,
    "tally_stocks_count" integer DEFAULT 0,
    "tally_godowns_count" integer DEFAULT 0,
    "tally_cost_centres_count" integer DEFAULT 0,
    "tally_units_count" integer DEFAULT 0,
    "waiting_connector_since" timestamp with time zone,
    "connector_retry_ready" boolean DEFAULT false NOT NULL,
    "retry_claim_token" uuid,
    "retry_lease_until" timestamp without time zone,
    "cleanup_status" text DEFAULT 'PENDING'::text NOT NULL,
    "cleanup_attempts" integer DEFAULT 0 NOT NULL,
    "cleanup_error" text,
    "cleanup_completed_at" timestamp with time zone,
    "cleanup_started_at" timestamp with time zone,
    "execution_claim_token" uuid,
    "cleanup_claim_token" uuid,
    "cleanup_lease_until" timestamp with time zone,
    "cleanup_completed_steps" integer DEFAULT 0 NOT NULL,
    "cleanup_deleted_counts" jsonb DEFAULT '{}'::jsonb NOT NULL,
    "cleanup_owner_instance_id" uuid,
    "retry_reservation_origin" text,
    "retry_original_error_message" text
);
ALTER TABLE public."sync_batches" OWNER TO "postgres";

CREATE TABLE public."tally_sync_ledgers" (
    "id" bigint GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME "public"."tally_sync_ledgers_id_seq" START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE) NOT NULL,
    "company_code" text NOT NULL,
    "name" text NOT NULL,
    "parent" text,
    "gst_applicable" text,
    "gst_registration_type" text,
    "state_name" text,
    "country" text,
    "pin_code" text,
    "mailing_name" text,
    "opening_balance" numeric DEFAULT 0,
    "is_bill_wise" boolean,
    "is_revenue" boolean,
    "is_deemed_positive" boolean,
    "created_at" timestamp without time zone DEFAULT now(),
    "updated_at" timestamp without time zone DEFAULT now(),
    "tally_owner" text,
    "guid" text,
    "master_id" text,
    "alter_id" bigint,
    "reserved_name" text,
    "gstin" text,
    "address" text,
    "phone" text,
    "email" text,
    "contact_person" text,
    "root_group" text,
    "last_synced_at" timestamp without time zone,
    "sync_batch_id" text,
    "is_deleted" boolean DEFAULT false,
    "opening_balance_type" text,
    "parent_group_guid" text,
    "parent_group_master_id" text,
    "parent_group_alter_id" bigint,
    "gst_duty_type" text,
    "gst_tax_type" text,
    "gst_rate" numeric,
    "tax_percentage_of_calculation" numeric,
    "tally_balance" numeric DEFAULT 0,
    "db_balance" numeric DEFAULT 0,
    "balance_difference" numeric DEFAULT 0,
    "nature" text,
    "parent_group_reserved_name" text,
    "credit_limit" numeric,
    "credit_period_days" text
);
ALTER TABLE public."tally_sync_ledgers" OWNER TO "postgres";

CREATE TABLE public."tally_voucher_ledgers" (
    "id" bigint GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME "public"."tally_voucher_ledgers_id_seq" START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE) NOT NULL,
    "voucher_guid" text NOT NULL,
    "company_code" text NOT NULL,
    "tally_owner" text NOT NULL,
    "ledger_name" text,
    "ledger_masterid" text,
    "amount" numeric,
    "debit" numeric,
    "credit" numeric,
    "bill_allocations" jsonb,
    "costcentre_allocations" jsonb,
    "created_at" timestamp without time zone DEFAULT now(),
    "ledger_guid" text,
    "ledger_alterid" text,
    "sync_batch_id" text,
    "is_deleted" boolean DEFAULT false,
    "updated_at" timestamp without time zone DEFAULT now()
);
ALTER TABLE public."tally_voucher_ledgers" OWNER TO "postgres";

CREATE TABLE public."sync_http_executions" (
    "invocation_id" uuid DEFAULT gen_random_uuid() NOT NULL,
    "batch_db_id" bigint NOT NULL,
    "batch_id" text NOT NULL,
    "execution_claim_token" uuid NOT NULL,
    "guard_owner_id" text NOT NULL,
    "server_instance_id" text NOT NULL,
    "state" text DEFAULT 'STARTED'::text NOT NULL,
    "started_at" timestamp with time zone DEFAULT now() NOT NULL,
    "terminated_at" timestamp with time zone,
    "terminal_outcome" text,
    "created_at" timestamp with time zone DEFAULT now() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE public."sync_http_executions" OWNER TO "postgres";

CREATE TABLE public."company_data_versions" (
    "company_code" text NOT NULL,
    "tally_owner" text NOT NULL,
    "version" bigint DEFAULT 1 NOT NULL,
    "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE public."company_data_versions" OWNER TO "postgres";

ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_company_owner_unique" UNIQUE (company_code, tally_owner);
ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_owner_check" CHECK ((tally_owner = ANY (ARRAY['USER'::text, 'CA'::text])));
ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_state_check" CHECK ((status = ANY (ARRAY['RUNNING'::text, 'CLOSED'::text])));
ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_status_pkey" PRIMARY KEY (id);
ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_token_check" CHECK ((((status = 'RUNNING'::text) AND (operation_token IS NOT NULL) AND (started_at IS NOT NULL) AND (closed_at IS NULL)) OR ((status = 'CLOSED'::text) AND (operation_token IS NULL))));
ALTER TABLE ONLY public."manual_reconcile_status" ADD CONSTRAINT "manual_reconcile_type_check" CHECK ((length(btrim(reconcile_type)) > 0));
ALTER TABLE ONLY public."sync_batches" ADD CONSTRAINT "sync_batches_batch_id_key" UNIQUE (batch_id);
ALTER TABLE ONLY public."sync_batches" ADD CONSTRAINT "sync_batches_pkey" PRIMARY KEY (id);
ALTER TABLE ONLY public."sync_http_executions" ADD CONSTRAINT "sync_http_execution_terminal_consistency" CHECK ((((state = 'TERMINATED'::text) AND (terminated_at IS NOT NULL) AND (terminal_outcome IS NOT NULL)) OR ((state <> 'TERMINATED'::text) AND (terminated_at IS NULL))));
ALTER TABLE ONLY public."sync_http_executions" ADD CONSTRAINT "sync_http_executions_batch_db_id_fkey" FOREIGN KEY (batch_db_id) REFERENCES sync_batches(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public."sync_http_executions" ADD CONSTRAINT "sync_http_executions_pkey" PRIMARY KEY (invocation_id);
ALTER TABLE ONLY public."sync_http_executions" ADD CONSTRAINT "sync_http_executions_state_check" CHECK ((state = ANY (ARRAY['STARTED'::text, 'TERMINATED'::text, 'FENCED_UNRESOLVED'::text])));
ALTER TABLE ONLY public."tally_sync_ledgers" ADD CONSTRAINT "tally_sync_ledgers_pkey" PRIMARY KEY (id);
ALTER TABLE ONLY public."tally_voucher_ledgers" ADD CONSTRAINT "tally_voucher_ledgers_pkey" PRIMARY KEY (id);
ALTER TABLE ONLY public."company_data_versions" ADD CONSTRAINT "company_data_versions_pkey" PRIMARY KEY (company_code, tally_owner);
ALTER TABLE ONLY public."company_data_versions" ADD CONSTRAINT "company_data_versions_tally_owner_check" CHECK ((tally_owner = ANY (ARRAY['USER'::text, 'CA'::text])));

CREATE UNIQUE INDEX idx_sync_batches_one_running_company_owner ON public.sync_batches USING btree (company_code, tally_owner) WHERE (batch_status = 'RUNNING'::text);
CREATE INDEX idx_tally_sync_ledgers_name ON public.tally_sync_ledgers USING btree (name);
CREATE INDEX idx_tvl_company_owner_ledger ON public.tally_voucher_ledgers USING btree (company_code, tally_owner, ledger_masterid);
CREATE UNIQUE INDEX idx_tally_sync_ledgers_company_owner_guid ON public.tally_sync_ledgers USING btree (company_code, tally_owner, guid);
CREATE INDEX sync_http_executions_batch_history ON public.sync_http_executions USING btree (batch_db_id, started_at DESC);
CREATE UNIQUE INDEX sync_http_executions_one_active_per_batch ON public.sync_http_executions USING btree (batch_db_id) WHERE (state = ANY (ARRAY['STARTED'::text, 'FENCED_UNRESOLVED'::text]));
CREATE INDEX idx_sync_batches_pickup ON public.sync_batches USING btree (batch_status, created_at);
CREATE INDEX idx_tvl_ledger ON public.tally_voucher_ledgers USING btree (ledger_masterid);
CREATE INDEX idx_tally_sync_ledgers_parent ON public.tally_sync_ledgers USING btree (parent);
CREATE INDEX idx_tvl_ledger_name ON public.tally_voucher_ledgers USING btree (ledger_name);
CREATE INDEX idx_sync_batches_company_owner ON public.sync_batches USING btree (company_code, tally_owner);
CREATE UNIQUE INDEX idx_sync_batches_one_processing_company_owner ON public.sync_batches USING btree (company_code, tally_owner) WHERE ((batch_status = 'PROCESSING'::text) AND (worker_status = 'RUNNING'::text));
CREATE INDEX idx_tvl_company_owner ON public.tally_voucher_ledgers USING btree (company_code, tally_owner);
CREATE INDEX idx_manual_reconcile_running ON public.manual_reconcile_status USING btree (company_code, tally_owner) WHERE (status = 'RUNNING'::text);
CREATE INDEX idx_tally_voucher_ledgers_sync_batch_id ON public.tally_voucher_ledgers USING btree (sync_batch_id);
CREATE INDEX idx_sync_batches_cleanup_owner_instance ON public.sync_batches USING btree (cleanup_owner_instance_id) WHERE (cleanup_claim_token IS NOT NULL);
CREATE INDEX idx_tvl_voucher ON public.tally_voucher_ledgers USING btree (voucher_guid);
CREATE INDEX idx_sync_batches_resume ON public.sync_batches USING btree (company_code, tally_owner, batch_status);
CREATE INDEX idx_tally_sync_ledgers_root_group ON public.tally_sync_ledgers USING btree (root_group);
CREATE INDEX idx_sync_batches_created ON public.sync_batches USING btree (created_at DESC);
CREATE INDEX idx_sync_batches_running ON public.sync_batches USING btree (worker_status, heartbeat_at);
CREATE UNIQUE INDEX sync_http_executions_claim_invocation ON public.sync_http_executions USING btree (batch_db_id, execution_claim_token, invocation_id);

ALTER TABLE public."manual_reconcile_status" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."sync_batches" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."tally_sync_ledgers" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."tally_voucher_ledgers" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."company_data_versions" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."sync_http_executions" ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION public.is_sync_outcome_unknown(p_error_message text)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
    SELECT
        COALESCE(p_error_message, '') LIKE 'CONNECTOR_OUTCOME_UNKNOWN:%'
        OR
        COALESCE(p_error_message, '') LIKE 'HTTP_OUTCOME_UNKNOWN:%';
$function$;

CREATE OR REPLACE FUNCTION public.increment_company_data_version(p_company_code text, p_tally_owner text)
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
DECLARE
    new_version BIGINT;
BEGIN
    IF NULLIF(BTRIM(p_company_code), '') IS NULL THEN
        RAISE EXCEPTION 'company_code is required';
    END IF;

    IF UPPER(BTRIM(p_tally_owner)) NOT IN ('USER', 'CA') THEN
        RAISE EXCEPTION 'Invalid tally_owner';
    END IF;

    INSERT INTO public.company_data_versions AS v (
        company_code,
        tally_owner,
        version,
        updated_at
    )
    VALUES (
        BTRIM(p_company_code),
        UPPER(BTRIM(p_tally_owner)),
        1,
        NOW()
    )
    ON CONFLICT (company_code, tally_owner)
    DO UPDATE SET
        version = v.version + 1,
        updated_at = NOW()
    RETURNING version INTO new_version;

    RETURN new_version;
END;
$function$;

CREATE OR REPLACE FUNCTION public.bump_company_version_on_batch_closed()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
BEGIN
    IF NEW.batch_status = 'CLOSED'
       AND OLD.batch_status IS DISTINCT FROM 'CLOSED' THEN

        PERFORM public.increment_company_data_version(
            NEW.company_code,
            NEW.tally_owner
        );

    END IF;

    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.reset_connector_retry_ready_on_claim()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
    IF NEW.batch_status IS DISTINCT FROM 'FAILED'
       OR NEW.worker_status IS DISTINCT FROM 'FAILED'
    THEN
        NEW.connector_retry_ready := false;
    END IF;

    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.block_uncertain_sync_batch_retry()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
    IF public.is_sync_outcome_unknown(OLD.error_message)
       AND (
           NEW.batch_status IS DISTINCT FROM OLD.batch_status
           OR NEW.worker_status IS DISTINCT FROM OLD.worker_status
           OR NEW.current_stage IS DISTINCT FROM OLD.current_stage
           OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
           OR NEW.error_message IS DISTINCT FROM OLD.error_message
       )
       AND NOT (
           OLD.error_message LIKE 'HTTP_OUTCOME_UNKNOWN:%'
           AND OLD.batch_status = 'PROCESSING'
           AND OLD.worker_status = 'RUNNING'
           AND NEW.batch_status = 'COMPLETED'
           AND NEW.worker_status = 'COMPLETED'
           AND NEW.error_message IS NULL
       )
    THEN
        RAISE EXCEPTION
            'UNCERTAIN_SYNC_BATCH_REQUIRES_REVIEW';
    END IF;

    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.protect_sync_batch_outcome()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
    IF public.is_sync_outcome_unknown(OLD.error_message)
       AND NEW.batch_status = 'PROCESSING'
       AND NEW.worker_status = 'RUNNING'
       AND (
           OLD.batch_status IS DISTINCT FROM NEW.batch_status
           OR OLD.worker_status IS DISTINCT FROM NEW.worker_status
       )
    THEN
        RAISE EXCEPTION
            'SYNC_OUTCOME_UNKNOWN: Batch % cannot be claimed',
            OLD.batch_id;
    END IF;

    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.apply_manual_reconcile_ledger_opening(p_company_code text, p_tally_owner text, p_operation_token uuid, p_ledger_guid text, p_opening_balance text, p_opening_balance_type text)
 RETURNS TABLE(result text, previous_balance numeric, previous_type text, current_balance numeric, current_type text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
DECLARE
    v_balance numeric;
    v_previous_balance numeric;
    v_previous_type text;
    v_ledger_id bigint;
    v_digits text;
BEGIN
    IF NULLIF(BTRIM(p_company_code), '') IS NULL
       OR p_tally_owner NOT IN ('USER', 'CA')
       OR p_operation_token IS NULL
       OR NULLIF(BTRIM(p_ledger_guid), '') IS NULL
       OR p_opening_balance IS NULL
       OR p_opening_balance_type IS NULL
       OR p_opening_balance_type NOT IN ('DR', 'CR', '')
    THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_INPUT_INVALID';
    END IF;

    -- Validate decimal string before numeric conversion.
    IF p_opening_balance !~ '^[0-9]+(\.[0-9]+)?$' THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_DECIMAL_INVALID';
    END IF;

    -- Prevent pathological numeric input.
    v_digits := REPLACE(p_opening_balance, '.', '');

    IF LENGTH(v_digits) > 1000 THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_DECIMAL_TOO_LONG';
    END IF;

    v_balance := p_opening_balance::numeric;

    IF (
        v_balance = 0
        AND p_opening_balance_type <> ''
    ) OR (
        v_balance <> 0
        AND p_opening_balance_type = ''
    ) THEN
        RAISE EXCEPTION 'RECONCILE_LEDGER_TYPE_MISMATCH';
    END IF;

    -- Same advisory lock order as normal sync admission.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            p_company_code || ':' || p_tally_owner,
            0
        )
    );

    -- Fence every write against the exact RUNNING reservation.
    PERFORM 1
    FROM public.manual_reconcile_status AS r
    WHERE r.company_code = p_company_code
      AND r.tally_owner = p_tally_owner
      AND r.status = 'RUNNING'
      AND r.operation_token = p_operation_token
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RECONCILE_RESERVATION_NOT_OWNED';
    END IF;

    -- Lock only an existing, non-deleted ledger.
    SELECT
        l.id,
        l.opening_balance,
        l.opening_balance_type
    INTO
        v_ledger_id,
        v_previous_balance,
        v_previous_type
    FROM public.tally_sync_ledgers AS l
    WHERE l.company_code = p_company_code
      AND l.tally_owner = p_tally_owner
      AND l.guid = p_ledger_guid
      AND l.is_deleted IS DISTINCT FROM TRUE
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT
            'NOT_FOUND'::text,
            NULL::numeric,
            NULL::text,
            NULL::numeric,
            NULL::text;
        RETURN;
    END IF;

    IF v_previous_balance IS NOT DISTINCT FROM v_balance
       AND v_previous_type IS NOT DISTINCT FROM p_opening_balance_type
    THEN
        RETURN QUERY SELECT
            'UNCHANGED'::text,
            v_previous_balance,
            v_previous_type,
            v_previous_balance,
            v_previous_type;
        RETURN;
    END IF;

    UPDATE public.tally_sync_ledgers AS l
    SET
        opening_balance = v_balance,
        opening_balance_type = p_opening_balance_type
    WHERE l.id = v_ledger_id;

    RETURN QUERY SELECT
        'UPDATED'::text,
        v_previous_balance,
        v_previous_type,
        v_balance,
        p_opening_balance_type;
END;
$function$;

CREATE OR REPLACE FUNCTION public.start_manual_reconcile(p_company_code text, p_tally_owner text, p_reconcile_type text, p_worker_id text, p_batch_id text DEFAULT NULL::text)
 RETURNS TABLE(acquired boolean, reason text, reservation_token uuid)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
DECLARE
    v_token uuid;
    v_batch public.sync_batches%ROWTYPE;
BEGIN
    -- Validate inputs.
    IF NULLIF(BTRIM(p_company_code), '') IS NULL
       OR p_tally_owner NOT IN ('USER', 'CA')
       OR NULLIF(BTRIM(p_reconcile_type), '') IS NULL
       OR NULLIF(BTRIM(p_worker_id), '') IS NULL
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::uuid;
        RETURN;
    END IF;

    -- Same company/owner lock used by normal sync.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            p_company_code || ':' || p_tally_owner,
            0
        )
    );

    -- Another manual reconciliation is running.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_status AS r
        WHERE r.company_code = p_company_code
          AND r.tally_owner = p_tally_owner
          AND r.status = 'RUNNING'
    ) THEN
        RETURN QUERY
        SELECT false, 'RECONCILE_ALREADY_RUNNING'::text,
               NULL::uuid;
        RETURN;
    END IF;

    -- Normal sync is queued or executing.
    IF EXISTS (
        SELECT 1
        FROM public.sync_batches AS b
        WHERE b.company_code = p_company_code
          AND b.tally_owner = p_tally_owner
          AND (
              b.batch_status IN (
                  'PENDING',
                  'PROCESSING',
                  'RUNNING'
              )
              OR b.worker_status = 'RUNNING'
              OR b.cleanup_claim_token IS NOT NULL
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'SYNC_IN_PROGRESS'::text, NULL::uuid;
        RETURN;
    END IF;

    -- Any unresolved HTTP execution for this company/owner.
    IF EXISTS (
        SELECT 1
        FROM public.sync_http_executions AS h
        JOIN public.sync_batches AS b
          ON b.id = h.batch_db_id
        WHERE b.company_code = p_company_code
          AND b.tally_owner = p_tally_owner
          AND h.state IN (
              'STARTED',
              'FENCED_UNRESOLVED'
          )
    ) THEN
        RETURN QUERY
        SELECT false, 'HTTP_EXECUTION_UNRESOLVED'::text,
               NULL::uuid;
        RETURN;
    END IF;

    -- Prevent reconciliation on uncertain sync outcomes.
    IF EXISTS (
        SELECT 1
        FROM public.sync_batches AS b
        WHERE b.company_code = p_company_code
          AND b.tally_owner = p_tally_owner
          AND public.is_sync_outcome_unknown(b.error_message)
    ) THEN
        RETURN QUERY
        SELECT false, 'SYNC_OUTCOME_UNKNOWN'::text,
               NULL::uuid;
        RETURN;
    END IF;

    -- Optional batch reference must belong to company/owner.
    IF p_batch_id IS NOT NULL THEN
        SELECT *
        INTO v_batch
        FROM public.sync_batches AS b
        WHERE b.batch_id = p_batch_id
          AND b.company_code = p_company_code
          AND b.tally_owner = p_tally_owner;

        IF NOT FOUND THEN
            RETURN QUERY
            SELECT false, 'BATCH_NOT_FOUND'::text,
                   NULL::uuid;
            RETURN;
        END IF;
    END IF;

    v_token := extensions.gen_random_uuid();

    -- One row per company + owner, reused across runs.
    INSERT INTO public.manual_reconcile_status AS r (
        company_code,
        tally_owner,
        reconcile_type,
        status,
        operation_token,
        worker_id,
        batch_id,
        started_at,
        closed_at,
        error_message,
        updated_at
    )
    VALUES (
        p_company_code,
        p_tally_owner,
        UPPER(BTRIM(p_reconcile_type)),
        'RUNNING',
        v_token,
        p_worker_id,
        p_batch_id,
        now(),
        NULL,
        NULL,
        now()
    )
    ON CONFLICT (company_code, tally_owner)
    DO UPDATE SET
        reconcile_type = EXCLUDED.reconcile_type,
        status = 'RUNNING',
        operation_token = EXCLUDED.operation_token,
        worker_id = EXCLUDED.worker_id,
        batch_id = EXCLUDED.batch_id,
        started_at = EXCLUDED.started_at,
        closed_at = NULL,
        error_message = NULL,
        updated_at = now()
    WHERE r.status = 'CLOSED';

    IF NOT FOUND THEN
        RETURN QUERY
        SELECT false, 'RECONCILE_ALREADY_RUNNING'::text,
               NULL::uuid;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT true, 'ACQUIRED'::text, v_token;
END;
$function$;

ALTER FUNCTION public.increment_company_data_version(p_company_code text, p_tally_owner text) OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.increment_company_data_version(p_company_code text, p_tally_owner text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.increment_company_data_version(p_company_code text, p_tally_owner text) TO "postgres";
GRANT EXECUTE ON FUNCTION public.increment_company_data_version(p_company_code text, p_tally_owner text) TO "service_role";
ALTER FUNCTION public.bump_company_version_on_batch_closed() OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.bump_company_version_on_batch_closed() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bump_company_version_on_batch_closed() TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.bump_company_version_on_batch_closed() TO "postgres";
GRANT EXECUTE ON FUNCTION public.bump_company_version_on_batch_closed() TO "anon";
GRANT EXECUTE ON FUNCTION public.bump_company_version_on_batch_closed() TO "authenticated";
GRANT EXECUTE ON FUNCTION public.bump_company_version_on_batch_closed() TO "service_role";
ALTER FUNCTION public.reset_connector_retry_ready_on_claim() OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.reset_connector_retry_ready_on_claim() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.reset_connector_retry_ready_on_claim() TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.reset_connector_retry_ready_on_claim() TO "postgres";
GRANT EXECUTE ON FUNCTION public.reset_connector_retry_ready_on_claim() TO "anon";
GRANT EXECUTE ON FUNCTION public.reset_connector_retry_ready_on_claim() TO "authenticated";
GRANT EXECUTE ON FUNCTION public.reset_connector_retry_ready_on_claim() TO "service_role";
ALTER FUNCTION public.block_uncertain_sync_batch_retry() OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.block_uncertain_sync_batch_retry() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.block_uncertain_sync_batch_retry() TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.block_uncertain_sync_batch_retry() TO "postgres";
GRANT EXECUTE ON FUNCTION public.block_uncertain_sync_batch_retry() TO "anon";
GRANT EXECUTE ON FUNCTION public.block_uncertain_sync_batch_retry() TO "authenticated";
GRANT EXECUTE ON FUNCTION public.block_uncertain_sync_batch_retry() TO "service_role";
ALTER FUNCTION public.is_sync_outcome_unknown(p_error_message text) OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) TO "postgres";
GRANT EXECUTE ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) TO "anon";
GRANT EXECUTE ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) TO "authenticated";
GRANT EXECUTE ON FUNCTION public.is_sync_outcome_unknown(p_error_message text) TO "service_role";
ALTER FUNCTION public.protect_sync_batch_outcome() OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.protect_sync_batch_outcome() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.protect_sync_batch_outcome() TO PUBLIC;
GRANT EXECUTE ON FUNCTION public.protect_sync_batch_outcome() TO "postgres";
GRANT EXECUTE ON FUNCTION public.protect_sync_batch_outcome() TO "anon";
GRANT EXECUTE ON FUNCTION public.protect_sync_batch_outcome() TO "authenticated";
GRANT EXECUTE ON FUNCTION public.protect_sync_batch_outcome() TO "service_role";
ALTER FUNCTION public.start_manual_reconcile(p_company_code text, p_tally_owner text, p_reconcile_type text, p_worker_id text, p_batch_id text) OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.start_manual_reconcile(p_company_code text, p_tally_owner text, p_reconcile_type text, p_worker_id text, p_batch_id text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.start_manual_reconcile(p_company_code text, p_tally_owner text, p_reconcile_type text, p_worker_id text, p_batch_id text) TO "postgres";
GRANT EXECUTE ON FUNCTION public.start_manual_reconcile(p_company_code text, p_tally_owner text, p_reconcile_type text, p_worker_id text, p_batch_id text) TO "service_role";
ALTER FUNCTION public.apply_manual_reconcile_ledger_opening(p_company_code text, p_tally_owner text, p_operation_token uuid, p_ledger_guid text, p_opening_balance text, p_opening_balance_type text) OWNER TO "postgres";
REVOKE ALL ON FUNCTION public.apply_manual_reconcile_ledger_opening(p_company_code text, p_tally_owner text, p_operation_token uuid, p_ledger_guid text, p_opening_balance text, p_opening_balance_type text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.apply_manual_reconcile_ledger_opening(p_company_code text, p_tally_owner text, p_operation_token uuid, p_ledger_guid text, p_opening_balance text, p_opening_balance_type text) TO "postgres";
GRANT EXECUTE ON FUNCTION public.apply_manual_reconcile_ledger_opening(p_company_code text, p_tally_owner text, p_operation_token uuid, p_ledger_guid text, p_opening_balance text, p_opening_balance_type text) TO "service_role";

CREATE TRIGGER block_uncertain_sync_batch_retry BEFORE UPDATE ON public.sync_batches FOR EACH ROW WHEN (is_sync_outcome_unknown(old.error_message)) EXECUTE FUNCTION block_uncertain_sync_batch_retry();
CREATE TRIGGER trg_bump_company_version_on_batch_closed AFTER UPDATE OF batch_status ON public.sync_batches FOR EACH ROW WHEN (((new.batch_status = 'CLOSED'::text) AND (old.batch_status IS DISTINCT FROM 'CLOSED'::text))) EXECUTE FUNCTION bump_company_version_on_batch_closed();
CREATE TRIGGER trg_protect_sync_batch_outcome BEFORE UPDATE ON public.sync_batches FOR EACH ROW EXECUTE FUNCTION protect_sync_batch_outcome();
CREATE TRIGGER trg_reset_connector_retry_ready_on_claim BEFORE UPDATE OF batch_status, worker_status ON public.sync_batches FOR EACH ROW EXECUTE FUNCTION reset_connector_retry_ready_on_claim();
