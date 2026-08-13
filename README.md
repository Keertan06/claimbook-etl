# ClaimbookDB Reporting Migration (Talend → dbt + Airflow)

Replacing a discontinued reporting tool (Talend Open Studio 8.0.1 — Qlik
acquired Talend in 2024 and stopped supporting this version) with an
actively-maintained stack: **dbt** for the report logic, **Airflow** for
scheduling. First job converted and extensively validated against real
production data: **`preauth manual upload daily`**.

**New here? Read [`SETUP.md`](SETUP.md) first** — it's the actual
step-by-step path to a working environment, including the specific
mistakes this project has already made so you don't repeat them. This
README is an overview, not a tutorial.

## What this actually does

Every hospital using this system has its own isolated set of database
tables (its own Postgres *schema*) — there is no single shared table
across all 483 active hospitals. This one fact shapes the whole
architecture: the pipeline has to loop over every tenant schema
individually, and it does this dynamically against the live tenant
registry (`mtdm.mtdm_tenant_tb`), not a hardcoded list.

There are also two separate physical databases: `claimbook` (source,
per-tenant schemas) and `cb_reports` (report output, one shared
`cb_report` schema). Postgres can't write across databases in one
connection, so the pipeline is two steps: dbt builds the report inside
`claimbook`, then a Python script moves the finished rows into
`cb_reports`.

```
mtdm.mtdm_tenant_tb (tenant registry)
        │
        ▼
get_tenant_schemas() macro  ──loops over──▶  every active tenant's schema
        │
        ▼
models/reports/manual_report_staged.sql   (dbt, writes inside claimbook)
        │
        ▼
load_manual_report.py   (Python, moves rows claimbook → cb_reports)
        │
        ▼
cb_report.manual_report   (the real target table, same one Talend writes to)
```

All four steps are orchestrated daily by
[`airflow_dags/preauth_manual_upload_daily_dag.py`](airflow_dags/preauth_manual_upload_daily_dag.py):
`dbt_run_staging → dbt_test_staging → load_dry_run → load_to_cb_reports`,
each step only running if the previous one succeeded.

## Current status (as of 2026-08-12)

**Validated, with real numbers, against real production data (read-only):**

| Check | Result | What it means |
|---|---|---|
| Field-level match, one hospital | 66/66 (100%) | Every column, one real day, one real hospital |
| Claim coverage, all 483 active tenants | 461/483 (95.4%) | Right claims found, at full scale |
| Row count, all 483 tenants, near-zero lag | 465/483 (96.3%) | Same test, using yesterday's date instead of an old one |
| Multi-event claim row count | 211/222 (95%) | Claims with 2+ events/day get the right count, not just presence |
| Row order, one tenant, full comparison | 72/72 exact, position-for-position | Not just same rows — same order as Talend |

**Every mismatch above is explained**, not unexplained noise — see
[`docs/PROJECT_CONTEXT_ABI_Health_MASTER.md`](docs/PROJECT_CONTEXT_ABI_Health_MASTER.md)
Section 16 Part G and Section 17 Part C for the two real root causes found
(a mutable timestamp field, and — separately — two real hospitals that
appear to have never been in Talend's reporting at all, flagged as a
likely gap in the *current* system).

**Not yet done:**
- **Real dev/staging write access to `claimbook`** — the #1 blocker.
  Everything validated so far is either read-only against real data, or
  fully write-tested against a local sandbox. Nothing has been written to
  real `claimbook`/`cb_reports`.
- **The next job** (`functionality = 'CLAIMS'` in `cb_report.manual_report`,
  313,736 rows, actively running) — identified, query requested from the
  person who manages the Talend jobs, not yet received.
- **Row order proven at scale** — currently proven correct on real data for
  one tenant; the mechanism is identical for every tenant, so this is good
  evidence, not yet a full-scale proof like the count-based checks got.

## Repo structure

```
.
├── dbt_project.yml              dbt project config
├── profiles.yml                 dbt connection targets (env-var driven, no credentials)
├── load_manual_report.py        the cross-database loader (claimbook → cb_reports)
├── models/reports/
│   ├── manual_report_staged.sql the report logic itself (multi-tenant)
│   └── schema.yml               dbt tests (see the file for why preauth_claim_id
│                                 is deliberately NOT tested for uniqueness)
├── macros/
│   └── get_tenant_schemas.sql   resolves which tenant schemas to loop over
├── tests/
│   └── test_date_window_sanity.sql   a real behavioral test, not just shape checks
├── airflow_dags/
│   └── preauth_manual_upload_daily_dag.py   the 4-task daily pipeline
├── validation/                  read-only scripts used to prove correctness against
│   │                            real production data without ever writing to it
│   ├── scale_test_readonly.py   does the right claim show up? (coverage)
│   ├── parity_check.py          does it show up the right number of times? (parity)
│   └── full_count_test.py       both at once, whole-tenant row count, yesterday's date
├── docs/
│   └── PROJECT_CONTEXT_ABI_Health_MASTER.md   the complete project history — every
│                                 finding, every root cause, every mistake made and
│                                 fixed, session by session. Long, but authoritative.
├── requirements.txt
├── .env.example
└── SETUP.md                     start here
```

## A few things worth knowing before you dig in

- **`profiles.yml`'s three targets read the same env vars.** Switching
  `--target sandbox` vs `--target dev` does *not* switch which database you
  hit — the `CLAIMBOOK_*` environment variables do, regardless of which
  target name you pass. This has caused real confusion in this project
  (running commands against the wrong database without realizing it).
  Always `echo $CLAIMBOOK_HOST` before anything that writes. Full detail
  in `SETUP.md`.
- **dbt and Airflow share one virtualenv in this project** (`airflow_venv`
  everywhere in these examples), because that's what's actually deployed.
  It works with the pinned versions in `requirements.txt`, but is fragile
  to casual upgrades — see the comments there.
- **The local Airflow scheduler is known-unreliable** for triggering fresh
  runs in this setup (SQLite + `SequentialExecutor` concurrency issue,
  which Airflow's own UI warns about). Documented workaround: run the
  DAG's 4 steps by hand in order. See
  `docs/PROJECT_CONTEXT_ABI_Health_MASTER.md` Section 17 Part E.
- **The Airflow DAG file in this repo is a copy, not what actually runs.**
  Airflow only reads from its own configured `dags/` folder — you must
  copy `airflow_dags/preauth_manual_upload_daily_dag.py` there after every
  change. `SETUP.md` covers this.

For literally everything else — every bug hit, every root cause found,
every KT question asked and answered, the full validation history — read
`docs/PROJECT_CONTEXT_ABI_Health_MASTER.md`. It's long on purpose: this
project has learned that "we tested it" and "here's exactly what we tested,
against what, with what result" are very different claims, and the second
one is what actually earns trust.
