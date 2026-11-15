#!/usr/bin/env python3
"""LOCAL ONLY PostgreSQL 17 concurrency tests; Python standard library + psql.
Requires baseline + all 18 migrations, local postgres superuser authentication,
and psql on PATH. No passwords are requested, stored, or printed.
No Supabase/Redis/Tally clients. Does not change app code or voucher gate.
Normal completion/errors: closes workers, cleans this run's committed fixtures.
Cleanup uses SET LOCAL session_replication_role=replica in ONE transaction:
immutable-row triggers otherwise prohibit deletion. No ALTER/DISABLE TRIGGER.
Hard kill/power loss: rerun with --cleanup-run UUID printed at startup.
Identity sequences may advance. No claim of connector or concurrent voucher tests.
"""
import argparse
import concurrent.futures
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import uuid

HOST, PORT, DATABASE, USER = "127.0.0.1", "5433", "billey_reconcile_test", "postgres"
CLIENT_TIMEOUT = 18
BARRIER_TIMEOUT = 6
GATE_CLASS = 1748123456


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def uid():
    return str(uuid.uuid4())


def check(condition, message):
    if not condition:
        raise AssertionError(message)


class Connection:
    """One persistent psql process = one independent PostgreSQL backend."""
    def __init__(self, label, run_id):
        self.label = label
        self.pid = None
        self.process = None
        self.app = "billey-mrc-" + run_id.hex + "-" + label
        self.lines = queue.Queue()
        executable = shutil.which("psql")
        if not executable:
            raise RuntimeError("psql is required on PATH")
        env = os.environ.copy()  # existing libpq authentication; never printed
        # Prevent inherited libpq routing/service settings overriding loopback.
        for setting in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGHOST",
                        "PGPORT", "PGDATABASE", "PGUSER"):
            env.pop(setting, None)
        env["PGAPPNAME"] = self.app
        env["PGCONNECT_TIMEOUT"] = "5"
        env["PGOPTIONS"] = (
            "-c statement_timeout=12000 -c lock_timeout=10000 "
            "-c idle_in_transaction_session_timeout=20000"
        )
        self.process = subprocess.Popen(
            [executable, "-X", "-q", "-A", "-t", "-w",
             "-h", HOST, "-p", PORT, "-d", DATABASE, "-U", USER,
             "-v", "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, encoding="utf-8", bufsize=1,
            env=env,
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            metadata = self.json("""
SELECT json_build_object(
 'host', pg_catalog.host(inet_server_addr()), 'port', inet_server_port(),
 'database', current_database(),
 'version', current_setting('server_version_num')::integer,
 'superuser', (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
 'pid', pg_backend_pid(), 'application', current_setting('application_name'));
""")
            check(isinstance(metadata, dict),
                  "Refusing target: parsed metadata received=" + repr(metadata)
                  + " type=" + type(metadata).__name__ + "; expected JSON object")
            # Preserve all six original predicates; report every field, even when
            # an earlier condition fails. Never print environment/authentication.
            conditions = [
                ("host", HOST, lambda value: value == HOST),
                ("port", int(PORT), lambda value: value == int(PORT)),
                ("database", DATABASE, lambda value: value == DATABASE),
                ("version", "170000 <= server_version_num < 180000",
                 lambda value: 170000 <= value < 180000),
                ("superuser", True, lambda value: value is True),
                ("application", self.app, lambda value: value == self.app),
            ]
            diagnostics = []
            failures = []
            for field, expected, predicate in conditions:
                received = metadata.get(field)
                try:
                    passed = field in metadata and predicate(received)
                except (TypeError, ValueError):
                    passed = False
                detail = (
                    field + ": received=" + repr(received)
                    + " type=" + type(received).__name__
                    + "; expected=" + repr(expected)
                )
                diagnostics.append(("PASS " if passed else "FAIL ") + detail)
                if not passed:
                    failures.append(field)
            for diagnostic in diagnostics:
                print(self.label + " target check " + diagnostic, flush=True)
            check(not failures, "Refusing target: failed checks="
                  + ", ".join(failures) + "; " + " | ".join(diagnostics))
            self.pid = metadata["pid"]
            self.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        except BaseException:
            # Teardown diagnostics must never replace the constructor failure.
            close_safely(self)
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                self.lines.put(line.rstrip("\r\n"))
        finally:
            self.lines.put(None)

    def execute(self, sql):
        marker = "LOCAL_DONE_" + uuid.uuid4().hex
        self.process.stdin.write(sql + "\n\\echo " + marker + "\n")
        self.process.stdin.flush()
        result = []
        deadline = time.monotonic() + CLIENT_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(self.label + ": bounded psql response timeout")
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty as exc:
                raise TimeoutError(self.label + ": bounded psql response timeout") from exc
            if line is None:
                # Queries contain synthetic IDs only; no connection credentials.
                raise RuntimeError(self.label + ": psql stopped: " + "\n".join(result[-8:]))
            if line == marker:
                return [line for line in result if line.strip()]
            result.append(line)

    def json(self, sql):
        lines = self.execute(sql)
        values = [json.loads(line) for line in lines
                  if line.startswith("{") or line.startswith("[")]
        check(len(values) == 1, self.label + ": expected one JSON result")
        return values[0]

    def close(self):
        """Best-effort bounded teardown; return diagnostics instead of raising."""
        errors = []

        def attempt(label, action):
            try:
                action()
            except BaseException as exc:
                errors.append(label + ": " + type(exc).__name__ + ": " + str(exc))

        if self.process is None:
            return errors
        process = self.process
        if process.poll() is None:
            attempt("terminate", process.terminate)
            attempt("terminate wait", lambda: process.wait(timeout=5))
        if process.poll() is None:
            attempt("kill", process.kill)
            attempt("kill wait", lambda: process.wait(timeout=5))
        reader = getattr(self, "reader", None)
        if reader is not None:
            attempt("reader join", lambda: reader.join(timeout=2))
            if reader.is_alive():
                errors.append("reader did not stop within 2 seconds")

        # Buffered pipe close can block on a reader lock or fail with BrokenPipe.
        # Isolate each close in a daemon thread, independently bounded.
        for label, stream in (("stdin", process.stdin), ("stdout", process.stdout)):
            if stream is None:
                continue
            closer = threading.Thread(
                target=attempt, args=(label + " close", stream.close), daemon=True
            )
            closer.start()
            closer.join(timeout=1)
            if closer.is_alive():
                errors.append(label + " close did not finish within 1 second")
        return errors


def close_safely(connection):
    """Independently protect each connection so fixture cleanup is still tried."""
    try:
        errors = connection.close()
    except BaseException as exc:
        errors = [type(exc).__name__ + ": " + str(exc)]
    for error in errors:
        print("FAIL close " + connection.label + ": " + error,
              file=sys.stderr, flush=True)
    return not errors


def one(function, arguments):
    return "SELECT row_to_json(r) FROM public." + function + "(" + arguments + ") AS r;"


def expect_error(command, message):
    # A subtransaction catches ONLY the precise expected RPC failure.
    return """
DO $expected$
BEGIN
    BEGIN
        %s;
        RAISE EXCEPTION 'EXPECTED_RPC_ERROR_WAS_NOT_RAISED';
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE <> 'P0001' OR SQLERRM <> %s THEN
            RAISE;
        END IF;
    END;
END;
$expected$;
SELECT json_build_object('rejected', true);
""" % (command.rstrip(";"), literal(message))


def admission(attempt, company, batch, claim):
    return one("start_manual_reconcile_claimed_v1", ",".join(map(literal, [
        attempt, company, "USER", "LEDGER", "local-concurrency", batch, claim
    ])))


def binding(attempt, token, identity):
    return one("bind_manual_reconcile_execution_v1", ",".join(map(literal, [
        attempt, token, identity["execution"], identity["connector"],
        identity["socket"], identity["owner"], identity["server"]
    ])))


def identity():
    return dict(execution=uid(), connector="local-connector-" + uid(),
                socket="local-socket-" + uid(), owner=uid(), server=uid())


def fenced_prepare(attempt, token, execution):
    # An obsolete execution must be rejected before any journal row is inserted.
    return "SELECT * FROM public.prepare_manual_reconcile_fetch_v2(" + ",".join([
        literal(uid()), literal(attempt), literal(token), "1", literal(uid()),
        literal("synthetic-ledger"), literal("local-connector"),
        literal("synthetic-tally-company"), "'{}'::jsonb", literal("synthetic-fingerprint"),
        literal(execution)
    ]) + ")"


def race(observer, workers, sqls):
    """BOTH workers must wait on the exact transaction-scoped observer gate.
    Observer rollback releases both shared-lock waiters together. The server
    aborts an abandoned gate transaction after at most 10 seconds idle.
    Each worker commits before its result is returned to Python.
    """
    gate = uuid.uuid4().int % 2000000000 + 1
    pool = None
    futures = []
    gate_active = True  # also roll back if acquisition only partially completed
    primary_error = None
    try:
        observer.execute(
            "BEGIN; SET LOCAL idle_in_transaction_session_timeout = '10s'; "
            "SELECT pg_advisory_xact_lock(%d,%d);" % (GATE_CLASS, gate)
        )
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        for worker, sql in zip(workers, sqls):
            futures.append(pool.submit(
                worker.json,
                "BEGIN; SELECT pg_advisory_xact_lock_shared(%d,%d);\n%s\nCOMMIT;"
                % (GATE_CLASS, gate, sql)
            ))
        deadline = time.monotonic() + BARRIER_TIMEOUT
        while True:
            ready = observer.json("""
SELECT json_build_object('waiting', count(*))
FROM pg_locks WHERE locktype='advisory' AND NOT granted
AND classid=%d::oid AND objid=%d::oid AND objsubid=2
AND pid IN (%d,%d);
""" % (GATE_CLASS, gate, workers[0].pid, workers[1].pid))
            if ready["waiting"] == 2:
                break
            check(not any(f.done() for f in futures), "competitor exited before barrier")
            if time.monotonic() >= deadline:
                raise TimeoutError("Both PostgreSQL backends did not reach the barrier")
            time.sleep(0.02)  # state polling, never a timing-based release criterion
        observer.execute("ROLLBACK;")
        gate_active = False
        return [f.result(timeout=CLIENT_TIMEOUT + 1) for f in futures]
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        teardown_errors = []
        if gate_active:
            try:
                observer.execute("ROLLBACK;")
            except BaseException as exc:
                teardown_errors.append("gate rollback: " + str(exc))
                # Disconnect is the fallback; the server idle timeout also bounds
                # an abandoned transaction if process/pipe closure fails.
                if not close_safely(observer):
                    teardown_errors.append("observer disconnect reported errors")
        if pool is not None:
            for future in futures:
                future.cancel()
            try:
                # Active worker queries already have client/server timeouts.
                # Do not let an executor wait block main's connection cleanup.
                pool.shutdown(wait=False, cancel_futures=True)
            except BaseException as exc:
                teardown_errors.append("executor shutdown: " + str(exc))
        if teardown_errors:
            if primary_error is None:
                raise RuntimeError("; ".join(teardown_errors))
            for error in teardown_errors:
                print("Additional race teardown failure: " + error,
                      file=sys.stderr, flush=True)


def reservation(connection, company):
    return connection.json("""
SELECT row_to_json(r) FROM (
 SELECT id, status, operation_token, company_code, tally_owner, batch_id
 FROM public.manual_reconcile_status WHERE company_code=%s AND tally_owner='USER'
) AS r;
""" % literal(company))


def cleanup(observer, company):
    ids = observer.json("""
SELECT json_build_object('ids', COALESCE(json_agg(attempt_id), '[]'::json))
FROM public.manual_reconcile_attempts WHERE company_code=%s AND tally_owner='USER';
""" % literal(company))["ids"]
    uuid_array = "ARRAY[" + ",".join(literal(i) for i in ids) + "]::uuid[]"
    # All deletion keys are scoped by this exact run UUID, never wildcard prefixes.
    sql = "BEGIN; SET LOCAL session_replication_role = replica;\n"
    for table in [
        "manual_reconcile_operations", "manual_reconcile_pre_release_checkpoints",
        "manual_reconcile_finalizations", "manual_reconcile_executions"
    ]:
        sql += "DELETE FROM public.%s WHERE attempt_id=ANY(%s);\n" % (table, uuid_array)
    sql += """
DELETE FROM public.manual_reconcile_attempts WHERE company_code=%s AND tally_owner='USER';
DELETE FROM public.manual_reconcile_status WHERE company_code=%s AND tally_owner='USER';
DELETE FROM public.sync_http_executions WHERE batch_db_id IN (
 SELECT id FROM public.sync_batches WHERE company_code=%s AND tally_owner='USER');
DELETE FROM public.sync_batches WHERE company_code=%s AND tally_owner='USER';
DELETE FROM public.company_data_versions WHERE company_code=%s AND tally_owner='USER';
COMMIT;
""" % tuple(literal(company) for _ in range(5))
    observer.execute(sql)
    checks = [
        "SELECT count(*) AS n FROM public.%s WHERE attempt_id=ANY(%s)" % (t, uuid_array)
        for t in ["manual_reconcile_operations", "manual_reconcile_pre_release_checkpoints",
                  "manual_reconcile_finalizations", "manual_reconcile_executions"]
    ] + [
        "SELECT count(*) AS n FROM public.%s WHERE company_code=%s AND tally_owner='USER'"
        % (t, literal(company))
        for t in ["manual_reconcile_attempts", "manual_reconcile_status",
                  "sync_batches", "company_data_versions"]
    ]
    remaining = observer.json(
        "SELECT json_build_object('rows', sum(n)) FROM (" +
        " UNION ALL ".join(checks) + ") AS counts;"
    )
    check(remaining["rows"] == 0, "Synthetic fixture cleanup incomplete")
    print("PASS cleanup: scoped committed fixtures deleted; trigger suppression ended", flush=True)


def tests(observer, workers, company):
    # Inspect installed definitions/signatures, not placeholders or copied RPCs.
    signatures = [
        "start_manual_reconcile_claimed_v1(uuid,text,text,text,text,text,uuid)",
        "bind_manual_reconcile_execution_v1(uuid,uuid,uuid,text,text,uuid,uuid)",
        "require_manual_reconcile_execution_v1(uuid,uuid,uuid)",
        "prepare_manual_reconcile_fetch_v2(uuid,uuid,uuid,integer,uuid,text,text,text,jsonb,text,uuid)",
        "record_manual_reconcile_pre_release_checkpoint_v1(uuid,uuid,uuid,text,uuid,jsonb)",
        "record_manual_reconcile_finalization_v1(uuid,uuid,jsonb,boolean,boolean,text,text,jsonb)",
        "close_manual_reconcile_v2(uuid,uuid)"
    ]
    for signature in signatures:
        definition = observer.json(
            "SELECT json_build_object('definition', pg_get_functiondef(to_regprocedure(%s)));"
            % literal("public." + signature)
        )["definition"]
        check(definition is not None, "Missing installed RPC: " + signature)
    print("PASS installed RPC definitions/signatures found", flush=True)
    check(len({observer.pid, workers[0].pid, workers[1].pid}) == 3,
          "Three independent verified PostgreSQL backends required")

    batch, claim = uid(), uid()
    observer.execute("""
INSERT INTO public.sync_batches(
 batch_id, company_code, tally_owner, batch_status, worker_status,
 batch_closed, ledger_reconciliation_completed, stock_reconciliation_completed,
 execution_claim_token
) VALUES (%s,%s,'USER','COMPLETED','COMPLETED',false,false,true,%s);
""" % (literal(batch), literal(company), literal(claim)))

    attempts = [uid(), uid()]
    results = race(observer, workers, [
        admission(attempt, company, batch, claim) for attempt in attempts
    ])
    winners = [i for i, r in enumerate(results) if r["acquired"] is True]
    check(len(winners) == 1, "Simultaneous admission must have exactly one winner")
    winner = winners[0]
    loser = 1 - winner
    check(results[winner]["reason"] == "ACQUIRED"
          and results[winner]["reservation_token"] is not None, "Winner admission contract")
    check(results[loser] == dict(acquired=False, reason="RECONCILE_ALREADY_RUNNING",
                                reservation_token=None), "Losing admission contract")
    attempt, token = attempts[winner], results[winner]["reservation_token"]
    count = observer.json(
        "SELECT json_build_object('n',count(*)) FROM public.manual_reconcile_attempts "
        "WHERE company_code=%s;" % literal(company)
    )
    check(count["n"] == 1, "Exactly one durable admitted attempt")
    print("PASS simultaneous claimed admission: exactly one winner", flush=True)

    identities = [identity(), identity()]
    bound = race(observer, workers, [binding(attempt, token, i) for i in identities])
    winners = [i for i, r in enumerate(bound) if r["bound"] is True]
    check(len(winners) == 1, "Competing execution binding must have exactly one winner")
    winner = winners[0]
    check(bound[winner]["reason"] == "BOUND"
          and bound[winner]["execution_id"] == identities[winner]["execution"],
          "Bound execution identity")
    check(bound[1-winner]["bound"] is False
          and bound[1-winner]["reason"] == "EXECUTION_BINDING_CONFLICT",
          "Competing execution binding rejected")
    old = identities[winner]
    print("PASS competing execution binding: exactly one winner", flush=True)

    stale = workers[0].json(binding(attempt, uid(), identity()))
    check(stale["bound"] is False and stale["reason"] == "ATTEMPT_NOT_CURRENT",
          "Wrong reservation token rejected by binding")
    workers[1].json(expect_error(
        fenced_prepare(attempt, uid(), old["execution"]), "RECONCILE_ATTEMPT_NOT_CURRENT"))
    workers[1].json(expect_error(
        fenced_prepare(attempt, token, identities[1-winner]["execution"]),
        "RECONCILE_EXECUTION_NOT_BOUND"))
    print("PASS stale-token and losing-execution fencing", flush=True)

    checkpoint = observer.json(
        "SELECT json_build_object('ok', public.record_manual_reconcile_pre_release_checkpoint_v1("
        + ",".join(map(literal, [attempt, token, old["execution"], old["connector"], old["owner"]]))
        + ",'[]'::jsonb));"
    )
    check(checkpoint["ok"] is True, "Zero-operation checkpoint")
    receipt = dict(redisReleaseConfirmed=True, attemptId=attempt,
                   executionId=old["execution"], serverInstanceId=old["server"],
                   connectorId=old["connector"], socketId=old["socket"], ownerToken=old["owner"])
    # Synthetic evidence only: no Redis release is asserted as externally verified.
    record_sql = one("record_manual_reconcile_finalization_v1", ",".join([
        literal(attempt), literal(token), "'[]'::jsonb", "true", "true",
        literal(old["connector"]), literal(old["owner"]), literal(json.dumps(receipt)) + "::jsonb"
    ]))
    check(observer.json(record_sql) == dict(recorded=True, reason="EVIDENCE_RECORDED"),
          "Old evidence recorded")
    close_sql = one("close_manual_reconcile_v2", literal(attempt) + "," + literal(token))
    check(observer.json(close_sql) == dict(closed=True, reason="CLOSED"), "Old reservation closed")
    prior_id = reservation(observer, company)["id"]

    newer_attempt, newer_identity = uid(), identity()
    newer = workers[0].json(admission(newer_attempt, company, batch, claim))
    check(newer["acquired"] is True and newer["reason"] == "ACQUIRED", "New reservation admitted")
    newer_token = newer["reservation_token"]
    check(newer_token != token, "New reservation has a distinct token")
    check(workers[0].json(binding(newer_attempt, newer_token, newer_identity))["reason"] == "BOUND",
          "New execution bound")
    before = reservation(observer, company)
    check(before["id"] == prior_id and before["status"] == "RUNNING", "Reservation row reused")

    active_sql = one("require_manual_reconcile_execution_v1", ",".join(map(literal, [
        newer_attempt, newer_token, newer_identity["execution"]
    ])))
    concurrent = race(observer, workers, [
        active_sql,
        expect_error(fenced_prepare(attempt, token, old["execution"]),
                     "RECONCILE_RESERVATION_NOT_OWNED")
    ])
    check(concurrent[0]["execution_id"] == newer_identity["execution"]
          and concurrent[1]["rejected"] is True, "Old execution rejected alongside valid new execution")
    check(workers[1].json(close_sql) == dict(closed=True, reason="ALREADY_CLOSED"),
          "Historical close replay")
    check(workers[1].json(record_sql) == dict(recorded=True, reason="ALREADY_RECORDED"),
          "Historical finalization replay")
    check(workers[1].json(binding(attempt, token, old))["reason"] == "ATTEMPT_NOT_CURRENT",
          "Old execution cannot rebind")
    check(reservation(observer, company) == before,
          "Old execution/replays must not change the newer reservation")
    journals = observer.json(
        "SELECT json_build_object('n',count(*)) FROM public.manual_reconcile_operations o "
        "JOIN public.manual_reconcile_attempts a USING(attempt_id) WHERE a.company_code=%s;"
        % literal(company)
    )
    check(journals["n"] == 0, "Rejected old/foreign executions created no journal operations")
    print("PASS old execution cannot affect a newer reservation; historical replay is read-only",
          flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup-run", type=uuid.UUID,
                        help="clean only fixtures for this previously printed run UUID")
    args = parser.parse_args()
    run_id = args.cleanup_run or uuid.uuid4()
    company = "LOCAL_RECONCILE_CONCURRENCY_" + run_id.hex
    print("LOCAL run UUID: " + str(run_id), flush=True)
    print("Target: " + HOST + ":" + PORT + "/" + DATABASE, flush=True)
    observer = None
    workers = []
    failed = False
    try:
        observer = Connection("observer", run_id)
        if not args.cleanup_run:
            for label in ("worker-a", "worker-b"):
                workers.append(Connection(label, run_id))
            tests(observer, workers, company)
    except BaseException as exc:
        failed = True
        print("FAIL: " + str(exc), file=sys.stderr, flush=True)
    finally:
        # Close all test sessions FIRST; a fresh verified connection cleans up
        # even if the observer's SQL/transport failed midway through the test.
        for worker in workers:
            if not close_safely(worker):
                failed = True
        if observer is not None:
            if not close_safely(observer):
                failed = True
            # A close failure above never skips the independent cleanup attempt.
            cleaner = None
            try:
                cleaner = Connection("cleanup", run_id)
                applications = ",".join(literal(
                    "billey-mrc-" + run_id.hex + "-" + label
                ) for label in ("worker-a", "worker-b", "observer"))
                # Also supports recovery after a killed client process.
                cleaner.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE application_name IN (" + applications + ") "
                    "AND datname=" + literal(DATABASE) + " AND client_addr='127.0.0.1'::inet;"
                )
                deadline = time.monotonic() + 6
                while True:
                    live = cleaner.json(
                        "SELECT json_build_object('n',count(*)) FROM pg_stat_activity "
                        "WHERE application_name IN (" + applications + ") "
                        "AND datname=" + literal(DATABASE) + ";"
                    )["n"]
                    if live == 0:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Competitor backends did not exit; cleanup deferred")
                    time.sleep(0.02)
                cleanup(cleaner, company)
            except BaseException as exc:
                failed = True
                print("FAIL cleanup: " + str(exc), file=sys.stderr, flush=True)
                print("Recovery: python migrations/test_setup/manual_reconcile_concurrency.py "
                      "--cleanup-run " + str(run_id), file=sys.stderr, flush=True)
            finally:
                if cleaner is not None:
                    if not close_safely(cleaner):
                        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())