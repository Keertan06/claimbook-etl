# Setup Guide

Written for someone joining this project with zero prior context. Follow
in order — later steps assume earlier ones worked. Where this project hit
a real problem before, it's called out explicitly rather than left for you
to rediscover.

## 0. What you'll actually have access to, realistically

Worth knowing upfront so you're not confused later:

- **You will not have a working Talend Studio project.** The real Talend
  project — the one with all ~100+ production jobs — is run by someone
  else, in a different city. You can install Talend Open Studio locally
  (useful for reference, needs JDK 11 / Zulu 11 specifically), but it
  won't be connected to anything real. Getting any job's actual query
  requires asking that person directly.
- **You will (probably) not have write access to real `claimbook` or
  `cb_reports`.** This has been the #1 blocker for this entire project so
  far. Everything real-data-related you can do without it is read-only —
  which is most of what matters, since correctness can be fully validated
  read-only (see `validation/`).
- **You will have full read/write access to a local sandbox** — two
  Postgres databases (`claimbook_sandbox`, `cb_reports_sandbox`) that
  mirror the real structure with synthetic data. This is where you build
  and test.

## 1. Prerequisites

- Python 3.12 (via deadsnakes PPA if on an older Ubuntu/WSL2 base)
- WSL2 Ubuntu, if on Windows (confirm with `wsl --list --verbose` — must
  say version 2, not 1)
- PostgreSQL client tools, and access to a Postgres instance for the
  sandbox (this project uses Postgres running on Windows itself, reached
  from WSL2 — see the networking note below)
- pgAdmin4 (useful for ad-hoc queries; several workflows in this project
  use it directly rather than `psql`, since `psql` isn't always on the
  Windows PATH by default)

## 2. Clone and set up the environment

```bash
git clone <this repo>
cd claimbook-etl

python3 -m venv airflow_venv
source airflow_venv/bin/activate

pip install -r requirements.txt
```

If that produces dependency conflicts (common — dbt and Airflow have
historically clashed on Flask/Werkzeug/marshmallow versions), use
Airflow's own constraints file instead — see the comment block at the
bottom of `requirements.txt` for the exact command.

## 3. Environment variables

```bash
cp .env.example .env
# edit .env with real values
```

**Read this carefully — it's caused real confusion before:**
`profiles.yml` has three named targets (`sandbox`, `dev`, `validate`), but
**all three read the exact same environment variable names**
(`CLAIMBOOK_HOST`, `CLAIMBOOK_PORT`, etc.). Passing `--target dev` does
**not** switch you to a different database — only changing the actual
`CLAIMBOOK_*` values does that. This means it's entirely possible to
believe you're pointed at real production while your environment
variables still hold sandbox values, or vice versa.

**Before running anything — especially anything that writes — confirm
directly:**

```bash
echo "HOST=$CLAIMBOOK_HOST DB=$CLAIMBOOK_DBNAME USER=$CLAIMBOOK_USER"
```

Environment variables set with `export` are **session-scoped only** —
they vanish when the terminal closes, and are not shared between
terminals. If you're running multiple terminals (scheduler, webserver,
and a working shell, say), you need to set them in *each one*, and
Airflow's `BashOperator` tasks specifically inherit whatever the
**scheduler process's own terminal** had exported — not whatever terminal
you happen to be typing commands in.

### WSL2 networking, if applicable

If your sandbox Postgres runs on Windows and you're working from WSL2,
`localhost` from WSL will not reach it — WSL2's mirrored networking is
off by default in this kind of setup. Use the gateway IP instead:

```bash
cat /etc/resolv.conf | grep nameserver
# use that IP as CLAIMBOOK_HOST, not localhost or 127.0.0.1
```

This is genuinely required, not a workaround to remove later.

## 4. Set up the local sandbox

You need two databases: `claimbook_sandbox` and `cb_reports_sandbox`,
mirroring the real `claimbook`/`cb_reports` split. At minimum, the
sandbox needs:

- `mtdm.mtdm_tenant_tb` with a few tenant rows (`is_tenant = true`,
  `sche_name` pointing at a real schema)
- Per-tenant schemas with `oltp_pre_authorisation`, `oltp_preauth_status`,
  `oltp_preauth_email`, and the lookup tables the model joins against
  (`oltp_patient_tb`, `oltp_person`, `oltp_insurance_policy`,
  `oltp_workflow_state`, `oltp_workflow_request_type`, and
  `mtdm.mtdm_tpa_organization_tb`) — see `models/reports/manual_report_staged.sql`
  for the exact join structure if building this from scratch.
- `cb_report.manual_report` in the reports sandbox, matching the real
  target table's columns (see `load_manual_report.py`'s `COLUMNS` list).

Grant your sandbox user proper privileges — this project hit two real
permission gaps worth knowing about in advance:

```sql
-- the tenant registry lives in a shared schema, easy to forget to grant on
GRANT USAGE ON SCHEMA mtdm TO etl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA mtdm TO etl_user;

-- the staging schema itself, for dbt to CREATE TABLE into
GRANT USAGE, CREATE ON SCHEMA cb_staging TO etl_user;
```

If a table already exists and was created by a different role (e.g. your
Postgres superuser instead of your app user), `dbt run` will fail with
`must be owner of table` — in the sandbox, the simplest fix is just
`DROP TABLE` and let dbt recreate it under the right owner.

## 5. Confirm the connection

```bash
dbt debug --profiles-dir .
```

Should report a successful connection to your sandbox. If it doesn't,
stop here and fix it before continuing — every following step assumes
this works.

## 6. Run the model manually

```bash
dbt run --profiles-dir . --select manual_report_staged \
  --vars "{run_date: '2026-07-29', tenant_limit: 5, tenant_active_flag: true}"
```

(`2026-07-29` is a date this project's own sandbox has synthetic data
for — use whatever date your sandbox actually has data for. Omit
`--vars run_date` entirely to use the dynamic "yesterday" default in
`dbt_project.yml`, but note your sandbox almost certainly won't have
anything dated *actual* yesterday unless you seed it that way.)

Then:

```bash
dbt test --profiles-dir . --select manual_report_staged \
  --vars "{run_date: '2026-07-29', tenant_limit: 5, tenant_active_flag: true}"
```

Should show 9 tests, all passing. **If you ever see "Nothing to do"
instead of a real test count, something is wrong** — this project
shipped with zero tests defined for months without anyone noticing,
because a vacuous pass looks identical to a real one from the outside.
Don't let that happen again.

If a fix to `schema.yml` doesn't seem to be taking effect even after
confirming the file on disk is correct, clear dbt's parse cache — it can
survive way longer than you'd expect (including across a machine restart)
and mask a genuinely-updated file:

```bash
rm -rf target/
```

## 7. Run the loader

```bash
python load_manual_report.py --dry-run   # always first
python load_manual_report.py             # the real write
```

Both should report "0 staged rows" if your `dbt run` above matched no
real dates, and correctly decline to touch the target table at all in
that case — that's intended, safe behavior, not a bug.

## 8. Set up Airflow

```bash
export AIRFLOW_HOME=~/airflow_home
airflow db init
airflow users create --username admin --firstname <you> --lastname <you> \
  --role Admin --email admin@example.com --password <pick one>
```

**Copy the DAG into place — this repo's copy is not what Airflow runs:**

```bash
mkdir -p $AIRFLOW_HOME/dags
cp airflow_dags/preauth_manual_upload_daily_dag.py $AIRFLOW_HOME/dags/
```

Edit the path constants at the top of that file
(`DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `LOADER_PATH`, `PYTHON_BIN`,
`DBT_BIN`) to match your actual machine before copying — they're
hardcoded absolute paths, not portable as-is.

Start the scheduler and webserver in **separate terminals**, each with
the same environment variables exported (see the warning in step 3):

```bash
airflow scheduler   # terminal 1
airflow webserver --port 8081   # terminal 2 (default 8080 may clash with dbt docs)
```

**Known issue, not yet fixed**: this project has twice hit a manually-
triggered DAG run getting stuck indefinitely in `queued`, with zero
scheduler activity logged for it — no import errors, no blocked prior
run, just a stall. This matches Airflow's own UI warning about SQLite +
`SequentialExecutor` concurrency when the webserver, scheduler, and CLI
all hit one database file at once. If you hit this: don't burn much time
on it. Workaround that's proven to work — run the DAG's 4 steps by hand,
in order, using the exact commands each task runs (see the `bash_command`
strings in `airflow_dags/preauth_manual_upload_daily_dag.py`). A real fix
would mean moving to `LocalExecutor` + a real Postgres metadata database
instead of SQLite — worth doing eventually, not attempted yet.

## 9. Validate against real data (read-only, always safe)

The scripts in `validation/` never write anywhere — every connection is
opened with `conn.set_session(readonly=True)`, which Postgres itself
enforces (confirmed directly: a write attempt on such a connection raises
`ReadOnlySqlTransaction`, not just script discipline).

```bash
# point your env vars at real claimbook/cb_reports first (see .env.example)
python validation/full_count_test.py --limit 483 --timeout 60s --csv results.csv
```

Defaults to yesterday's date automatically. See the docstring at the top
of each script in `validation/` for what it checks and how it differs
from the others (coverage vs. parity vs. combined count).

## 10. Where to go from here

Read `docs/PROJECT_CONTEXT_ABI_Health_MASTER.md`. It's long, but it's the
actual memory of this project — every bug, every root cause, every wrong
theory that got corrected, in order. Skimming Section 1 (current state)
and the most recent session section at the bottom will orient you fastest.
