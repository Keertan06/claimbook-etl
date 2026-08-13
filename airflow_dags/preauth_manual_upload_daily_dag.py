"""
=============================================================================
preauth_manual_upload_daily_dag.py  -  MULTI-TENANT version

Replaces the Talend job end to end:

    Talend                          ->  This DAG
    ------------------------------------------------------------------
    hand-edited date literal        ->  {{ ds }} injected as run_date
    tLoop_1 over tenant schemas     ->  get_tenant_schemas() macro
    tDBInput (read claimbook)       ->  dbt run  (staging model)
    tDBOutput (write cb_reports)    ->  load_manual_report.py
    manual re-run on failure        ->  retries + rollback

WHY TWO STEPS INSTEAD OF ONE:
    dbt reads from the `claimbook` database but the target table lives in a
    separate `cb_reports` database. Postgres cannot write across databases in
    one connection, and dbt's Postgres adapter is one-connection-per-target.
    So dbt stages inside claimbook, then the loader moves the rows across.

STAGED ROLLOUT:
    TENANT_LIMIT controls how many tenants are processed. Validated at
    5 -> 25 -> 100 -> all 483 active tenants (see
    docs/PROJECT_CONTEXT_ABI_Health_MASTER.md Section 16 for results at each
    stage). Still set to 5 here deliberately - raise only after confirming
    real write access and re-validating. Set to 0 for "all active tenants".

  !! The load task WRITES. Keep CBREPORTS_DBNAME pointed at a sandbox until
  parallel-run validation against Talend is signed off, and real dev/staging
  write access to claimbook has been granted (still the #1 blocker as of
  2026-08-12 - see docs/PROJECT_CONTEXT_ABI_Health_MASTER.md Section 1). !!

NOTE ON WHERE THIS FILE ACTUALLY LIVES:
    This repo keeps a copy here for version control, but Airflow only reads
    DAGs from its own configured dags folder ($AIRFLOW_HOME/dags). Editing
    this repo copy does NOT change what Airflow runs - copy it across after
    every change. See SETUP.md.
=============================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# --- paths: quote these in every bash_command; a Windows path containing a
# --- space (e.g. "Keertan Kumar") would otherwise be split by bash --------
# --- SETUP.md explains how to set these for a new machine. ----------------
DBT_PROJECT_DIR = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project"
DBT_PROFILES_DIR = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project"
LOADER_PATH = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project/load_manual_report.py"
PYTHON_BIN = "/home/keertan_kumar/airflow_venv/bin/python"
DBT_BIN = "/home/keertan_kumar/airflow_venv/bin/dbt"

TENANT_LIMIT = 5             # <-- raise this deliberately as rollout proceeds
TENANT_ACTIVE_FLAG = "true"  # mtdm_tenant_tb.is_tenant: true = active tenant

default_args = {
    "owner": "keertan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="preauth_manual_upload_daily",
    description="Preauth manual upload daily report - multi-tenant (replaces Talend)",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="0 6 * * *",          # 06:00 daily - confirmed against the live
                                    # Airflow UI 2026-08-12, NOT the 0 2 * * *
                                    # an earlier local copy of this file had.
    catchup=False,
    max_active_runs=1,             # never let two runs write concurrently
    tags=["claimbook", "migration", "preauth"],
) as dag:

    # -- 1. build the staged table inside claimbook ------------------------
    dbt_run = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f'cd "{DBT_PROJECT_DIR}" && '
            f'"{DBT_BIN}" run '
            f'--profiles-dir "{DBT_PROFILES_DIR}" '
            f'--select manual_report_staged '
            f"--vars \"{{run_date: '{{{{ ds }}}}', "
            f"tenant_limit: {TENANT_LIMIT}, "
            f"tenant_active_flag: {TENANT_ACTIVE_FLAG}}}\""
        ),
    )

    # -- 2. sanity-check the staged data before it moves anywhere ----------
    # As of 2026-08-12 this genuinely tests something - see
    # models/reports/schema.yml and tests/test_date_window_sanity.sql.
    # Before that date, zero tests were defined and this step passed
    # vacuously in every prior run - see docs Section 17 Part D.
    dbt_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f'cd "{DBT_PROJECT_DIR}" && '
            f'"{DBT_BIN}" test '
            f'--profiles-dir "{DBT_PROFILES_DIR}" '
            f'--select manual_report_staged '
            f"--vars \"{{run_date: '{{{{ ds }}}}', "
            f"tenant_limit: {TENANT_LIMIT}, "
            f"tenant_active_flag: {TENANT_ACTIVE_FLAG}}}\""
        ),
    )

    # -- 3. show what the load would change (audit trail in the task log) --
    load_dry_run = BashOperator(
        task_id="load_dry_run",
        bash_command=f'"{PYTHON_BIN}" "{LOADER_PATH}" --dry-run',
    )

    # -- 4. the actual cross-database write -------------------------------
    load_to_cb_reports = BashOperator(
        task_id="load_to_cb_reports",
        bash_command=f'"{PYTHON_BIN}" "{LOADER_PATH}"',
    )

    dbt_run >> dbt_test >> load_dry_run >> load_to_cb_reports
