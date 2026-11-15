
-- Manual Reconciliation V2: immutable execution binding.
--
-- Local draft. Do not deploy before isolated PostgreSQL verification.
--
-- Required objects:
--   public.manual_reconcile_status
--   public.manual_reconcile_attempts
--   public.manual_reconcile_operations
--   public.manual_reconcile_finalizations
--
-- Requires the composite attempt identity constraint introduced by
-- 20261112_manual_reconcile_operations_v1.sql.
--
-- This migration does not authorize execution replay, verify Redis
-- ownership, or change existing admission/journal/finalization RPCs.

BEGIN;

CREATE TABLE public.manual_reconcile_executions (
    attempt_id uuid NOT NULL,
    reservation_id bigint NOT NULL,
    operation_token uuid NOT NULL,

    execution_id uuid NOT NULL,
    connector_id text NOT NULL,
    socket_id text NOT NULL,
    owner_token uuid NOT NULL,
    server_instance_id uuid NOT NULL,

    bound_at timestamptz NOT NULL
        DEFAULT transaction_timestamp(),

    CONSTRAINT manual_reconcile_executions_pkey
        PRIMARY KEY (attempt_id),

    CONSTRAINT manual_reconcile_executions_execution_uq
        UNIQUE (execution_id),

    CONSTRAINT manual_reconcile_executions_owner_uq
        UNIQUE (owner_token),

    CONSTRAINT manual_reconcile_executions_identity_uq
        UNIQUE (
            attempt_id,
            reservation_id,
            operation_token,
            execution_id
        ),

    CONSTRAINT manual_reconcile_executions_reservation_fk
        FOREIGN KEY (reservation_id)
        REFERENCES public.manual_reconcile_status (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT manual_reconcile_executions_attempt_fk
        FOREIGN KEY (
            attempt_id,
            reservation_id,
            operation_token
        )
        REFERENCES public.manual_reconcile_attempts (
            attempt_id,
            reservation_id,
            operation_token
        )
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT manual_reconcile_executions_connector_check
        CHECK (connector_id !~ '^[[:space:]]*$'),

    CONSTRAINT manual_reconcile_executions_socket_check
        CHECK (socket_id !~ '^[[:space:]]*$')
);

ALTER TABLE public.manual_reconcile_executions
    OWNER TO postgres;

ALTER TABLE public.manual_reconcile_executions
    ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.manual_reconcile_executions
    FROM PUBLIC, anon, authenticated, service_role;


CREATE FUNCTION public.guard_manual_reconcile_execution_identity_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $fn$
BEGIN
    RAISE EXCEPTION 'MANUAL_RECONCILE_EXECUTION_IMMUTABLE'
        USING ERRCODE = '55000';
END;
$fn$;

ALTER FUNCTION
    public.guard_manual_reconcile_execution_identity_v1()
    OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.guard_manual_reconcile_execution_identity_v1()
    FROM PUBLIC, anon, authenticated, service_role;

CREATE TRIGGER manual_reconcile_executions_immutable
BEFORE UPDATE OR DELETE
ON public.manual_reconcile_executions
FOR EACH ROW
EXECUTE FUNCTION
    public.guard_manual_reconcile_execution_identity_v1();


CREATE FUNCTION public.bind_manual_reconcile_execution_v1(
    p_attempt_id uuid,
    p_operation_token uuid,
    p_execution_id uuid,
    p_connector_id text,
    p_socket_id text,
    p_owner_token uuid,
    p_server_instance_id uuid
)
RETURNS TABLE (
    bound boolean,
    reason text,
    execution_id uuid
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO ''
AS $fn$
DECLARE
    v_attempt public.manual_reconcile_attempts%ROWTYPE;
    v_status public.manual_reconcile_status%ROWTYPE;
    v_existing public.manual_reconcile_executions%ROWTYPE;

    v_constraint_name text;
    v_table_name text;
    v_schema_name text;
BEGIN
    IF p_attempt_id IS NULL
       OR p_operation_token IS NULL
       OR p_execution_id IS NULL
       OR p_owner_token IS NULL
       OR p_server_instance_id IS NULL
       OR p_connector_id IS NULL
       OR p_connector_id ~ '^[[:space:]]*$'
       OR p_socket_id IS NULL
       OR p_socket_id ~ '^[[:space:]]*$'
    THEN
        RETURN QUERY
        SELECT false, 'INVALID_INPUT'::text, NULL::uuid;
        RETURN;
    END IF;

    -- Reuse the existing V2 admission attempt lock.
    -- Never acquire this lock after taking the company/row locks.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        1297236306,
        pg_catalog.hashtext(p_attempt_id::text)
    );

    SELECT a.*
    INTO v_attempt
    FROM public.manual_reconcile_attempts AS a
    WHERE a.attempt_id = p_attempt_id;

    IF NOT FOUND
       OR v_attempt.operation_token
          IS DISTINCT FROM p_operation_token
    THEN
        RETURN QUERY
        SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::uuid;
        RETURN;
    END IF;

    -- Exact company/owner expression used by deployed admission
    -- and existing manual apply/finalization RPCs.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_attempt.company_code || ':' || v_attempt.tally_owner,
            0
        )
    );

    SELECT s.*
    INTO v_status
    FROM public.manual_reconcile_status AS s
    WHERE s.id = v_attempt.reservation_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_status.status IS DISTINCT FROM 'RUNNING'
       OR v_status.operation_token
          IS DISTINCT FROM p_operation_token
       OR v_status.company_code
          IS DISTINCT FROM v_attempt.company_code
       OR v_status.tally_owner
          IS DISTINCT FROM v_attempt.tally_owner
    THEN
        RETURN QUERY
        SELECT false, 'ATTEMPT_NOT_CURRENT'::text, NULL::uuid;
        RETURN;
    END IF;

    SELECT e.*
    INTO v_existing
    FROM public.manual_reconcile_executions AS e
    WHERE e.attempt_id = p_attempt_id;

    IF FOUND THEN
        IF v_existing.reservation_id
              IS DISTINCT FROM v_attempt.reservation_id
           OR v_existing.operation_token
              IS DISTINCT FROM p_operation_token
           OR v_existing.execution_id
              IS DISTINCT FROM p_execution_id
           OR v_existing.connector_id
              IS DISTINCT FROM p_connector_id
           OR v_existing.socket_id
              IS DISTINCT FROM p_socket_id
           OR v_existing.owner_token
              IS DISTINCT FROM p_owner_token
           OR v_existing.server_instance_id
              IS DISTINCT FROM p_server_instance_id
        THEN
            RETURN QUERY
            SELECT false, 'EXECUTION_BINDING_CONFLICT'::text,
                   NULL::uuid;
            RETURN;
        END IF;

        -- Identity acknowledgement only.
        -- This does not authorize another executor or dispatch.
        RETURN QUERY
        SELECT true, 'ALREADY_BOUND'::text,
               v_existing.execution_id;
        RETURN;
    END IF;

    -- Do not retroactively bind previously started work.
    -- Existing journal RPCs serialize on this reservation row.
    IF EXISTS (
        SELECT 1
        FROM public.manual_reconcile_operations AS o
        WHERE o.attempt_id = p_attempt_id
    )
    OR EXISTS (
        SELECT 1
        FROM public.manual_reconcile_finalizations AS f
        WHERE f.attempt_id = p_attempt_id
    )
    THEN
        RETURN QUERY
        SELECT false, 'EXECUTION_ALREADY_STARTED'::text,
               NULL::uuid;
        RETURN;
    END IF;

    -- Only explicitly identified execution/owner uniqueness
    -- collisions become structured rejections.
    -- All other database failures propagate.
    BEGIN
        INSERT INTO public.manual_reconcile_executions (
            attempt_id,
            reservation_id,
            operation_token,
            execution_id,
            connector_id,
            socket_id,
            owner_token,
            server_instance_id
        )
        VALUES (
            p_attempt_id,
            v_attempt.reservation_id,
            p_operation_token,
            p_execution_id,
            p_connector_id,
            p_socket_id,
            p_owner_token,
            p_server_instance_id
        );
    EXCEPTION
        WHEN unique_violation THEN
            GET STACKED DIAGNOSTICS
                v_constraint_name = CONSTRAINT_NAME,
                v_table_name = TABLE_NAME,
                v_schema_name = SCHEMA_NAME;

            IF v_schema_name IS NOT DISTINCT FROM 'public'
               AND v_table_name
                   IS NOT DISTINCT FROM 'manual_reconcile_executions'
               AND v_constraint_name IN (
                   'manual_reconcile_executions_execution_uq',
                   'manual_reconcile_executions_owner_uq'
               )
            THEN
                RETURN QUERY
                SELECT false, 'EXECUTION_IDENTITY_CONFLICT'::text,
                       NULL::uuid;
                RETURN;
            END IF;

            RAISE;
    END;

    RETURN QUERY
    SELECT true, 'BOUND'::text, p_execution_id;
END;
$fn$;

ALTER FUNCTION public.bind_manual_reconcile_execution_v1(
    uuid, uuid, uuid, text, text, uuid, uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION
    public.bind_manual_reconcile_execution_v1(
        uuid, uuid, uuid, text, text, uuid, uuid
    )
    FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
    public.bind_manual_reconcile_execution_v1(
        uuid, uuid, uuid, text, text, uuid, uuid
    )
    TO service_role;

COMMENT ON FUNCTION public.bind_manual_reconcile_execution_v1(
    uuid, uuid, uuid, text, text, uuid, uuid
) IS
'Binds one immutable coordinator execution to a current manual attempt. BOUND acknowledges first insertion. ALREADY_BOUND acknowledges identity only and never authorizes execution replay. Does not verify Redis ownership.';

COMMIT;
