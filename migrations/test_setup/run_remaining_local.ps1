# LOCAL ONLY. Save/run with PowerShell and psql on PATH.
# Uses existing libpq authentication; never stores a password.
# The first six migrations (attempts, active-ledger uniqueness, operations,
# verified result, atomic apply, base finalization) MUST already be applied.
[CmdletBinding()]
param(
    [string]$DatabaseUser = 'postgres'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$localHostName = '127.0.0.1'
$localPort = '5433'
$localDatabase = 'billey_reconcile_test'
$migrationsDirectory = Split-Path -Parent $PSScriptRoot

# Explicit allowlist: never enumerate/sort SQL files for execution.
$remainingMigrations = @(
    '20261112_manual_reconcile_journal_seal_guard_v1.sql'
    '20261112_manual_reconcile_z_execution_binding_v1.sql'
    '20261112_manual_reconcile_zz_journal_execution_fencing_v1.sql'
    '20261112_manual_reconcile_z_legacy_apply_revoke_v1.sql'
    '20261113_manual_reconcile_claimed_admission_v1.sql'
    '20261113_manual_reconcile_batch_mutation_fence_v1.sql'
    '20261113_manual_reconcile_pre_release_checkpoint_DRAFT.sql'
    '20261113_manual_reconcile_historical_replay_fix_DRAFT.sql'
    '20261114_manual_reconcile_voucher_fetch_fencing_v1.sql'
    '20261114_manual_reconcile_voucher_atomic_settlement_v1.sql'
    '20261113_manual_reconcile_isolation_guard_v1.sql'
    '20261114_manual_reconcile_finalization_voucher_guard_v1.sql'
)

try {
    $psqlPath = (Get-Command psql -CommandType Application -ErrorAction Stop).Source
    $connectionArguments = @(
        '-X'
        '--host', $localHostName
        '--port', $localPort
        '--dbname', $localDatabase
        '--username', $DatabaseUser
        '--set', 'ON_ERROR_STOP=1'
    )

    # Resolve all files before making any database changes.
    foreach ($migration in $remainingMigrations) {
        $migrationPath = Join-Path $migrationsDirectory $migration
        if (-not (Test-Path -LiteralPath $migrationPath -PathType Leaf)) {
            Write-Host "FAIL $migration (file missing)"
            throw "Missing migration: $migration"
        }
    }

    $verificationSql = @"
SELECT
    inet_server_addr() = '127.0.0.1'::inet
    AND inet_server_port() = 5433
    AND current_database() = 'billey_reconcile_test'
    AND current_setting('server_version_num')::integer >= 170000
    AND current_setting('server_version_num')::integer < 180000
    AND current_setting('transaction_isolation') = 'read committed'
    AND to_regclass('public.manual_reconcile_attempts') IS NOT NULL
    AND to_regclass('public.tally_sync_ledgers_active_scope_guid_uidx') IS NOT NULL
    AND to_regclass('public.manual_reconcile_operations') IS NOT NULL
    AND to_regclass('public.manual_reconcile_finalizations') IS NOT NULL
    AND to_regprocedure('public.start_manual_reconcile_v2(uuid,text,text,text,text,text)') IS NOT NULL
    AND to_regprocedure('public.verify_manual_reconcile_fetch_result_v1(uuid,uuid,uuid,uuid,jsonb)') IS NOT NULL
    AND to_regprocedure('public.apply_manual_reconcile_ledger_opening_v2(uuid,uuid,uuid)') IS NOT NULL;
"@

    $verificationResult = & $psqlPath @connectionArguments '-A' '-t' '-c' $verificationSql
    $verificationExitCode = $LASTEXITCODE
    if ($verificationExitCode -ne 0 -or
        (($verificationResult -join "`n").Trim() -cne 't')) {
        Write-Host 'FAIL local target / PostgreSQL 17 / first-six baseline verification'
        throw 'Local target or baseline verification failed; no migrations executed.'
    }
    Write-Host "PASS local target and baseline: ${localHostName}:${localPort}/$localDatabase"

    foreach ($migration in $remainingMigrations) {
        $migrationPath = Join-Path $migrationsDirectory $migration
        try {
            & $psqlPath @connectionArguments '--file' $migrationPath
            $migrationExitCode = $LASTEXITCODE
            if ($migrationExitCode -ne 0) {
                throw "psql exited with code $migrationExitCode"
            }
        } catch {
            Write-Host "FAIL $migration"
            throw
        }
        Write-Host "PASS $migration"
    }
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}