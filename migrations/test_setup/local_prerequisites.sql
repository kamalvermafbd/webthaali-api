-- LOCAL ONLY. Run with psql as a local PostgreSQL administrator.
-- No passwords or credentials are stored in this file.
\set ON_ERROR_STOP on
\connect "host=127.0.0.1 port=5433 dbname=billey_reconcile_test user=postgres"

BEGIN;

DO $local$
BEGIN
    IF current_database() <> 'billey_reconcile_test'
       OR inet_server_addr() IS DISTINCT FROM '127.0.0.1'::inet
       OR inet_server_port() IS DISTINCT FROM 5433 THEN
        RAISE EXCEPTION 'Refusing prerequisites setup outside the local test database';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        CREATE ROLE anon NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role NOLOGIN BYPASSRLS;
    END IF;
END;
$local$;

CREATE SCHEMA IF NOT EXISTS extensions;
CREATE EXTENSION IF NOT EXISTS pgcrypto
    WITH SCHEMA extensions VERSION '1.3';

DO $verify$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_extension AS e
        JOIN pg_namespace AS n ON n.oid = e.extnamespace
        WHERE e.extname = 'pgcrypto'
          AND e.extversion = '1.3'
          AND n.nspname = 'extensions'
    ) THEN
        RAISE EXCEPTION 'pgcrypto must be version 1.3 in schema extensions';
    END IF;
END;
$verify$;

COMMIT;