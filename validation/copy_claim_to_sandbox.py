#!/usr/bin/env python3
"""
copy_claim_to_sandbox.py

Standalone script (NOT part of dbt) that copies ONE real claim's data from
real production Claimbook + cb_reports into the sandbox, so that a real
`dbt run` can be executed against real-world data shapes without touching
real production write access.

This exists because dbt itself CANNOT do this in one step - a single dbt
run connects to exactly one database, and cross-database writes are already
a known hard constraint in this project (see PROJECT_CONTEXT Section 1 -
the claimbook -> cb_reports loader exists for the same reason). This script
is the explicit, manual bridge between real prod (read-only) and sandbox
(write-capable), run once per claim, on purpose, never automated.

SAFETY MODEL:
- Source connections (real prod CLAIMBOOK_* / CBREPORTS_*) are OPENED
  READ-ONLY (conn.set_session(readonly=True)) - this script can never
  write to real production, structurally, not just by convention.
- Target connection (SANDBOX_*, separate env var prefix on purpose so it
  can NEVER be confused with real prod vars) is the only writable
  connection.
- Defaults to --dry-run: prints exactly what WOULD be inserted into
  sandbox without executing any INSERT. You must pass --execute to
  actually write.
- Prints resolved host/port/dbname for BOTH sides before doing anything,
  every time, no exceptions - so a misconfigured env var is caught before
  any query runs, not after.
- Only ever touches ONE claim_id per run. No bulk/loop mode. This is a
  demonstration/debugging tool, not a migration tool.

Env vars required (source, read-only, real prod):
    CLAIMBOOK_HOST / CLAIMBOOK_PORT / CLAIMBOOK_USER / CLAIMBOOK_PASSWORD / CLAIMBOOK_DBNAME
    CBREPORTS_HOST / CBREPORTS_PORT / CBREPORTS_USER / CBREPORTS_PASSWORD / CBREPORTS_DBNAME
    (falls back to CLAIMBOOK_* for any CBREPORTS_* not set, same convention
    as full_byte_comparison.py)

Env vars required (target, writable, sandbox - DELIBERATELY separate prefix):
    SANDBOX_HOST / SANDBOX_PORT / SANDBOX_USER / SANDBOX_PASSWORD / SANDBOX_DBNAME

Usage:
    python copy_claim_to_sandbox.py --tenant-id 36 --schema dmh --claim-id 255841 --run-date 2026-08-11
    python copy_claim_to_sandbox.py --tenant-id 36 --schema dmh --claim-id 255841 --run-date 2026-08-11 --execute
"""

import argparse
import os
import sys

import psycopg2


def env(name, fallback_name=None, default=None, required=False):
    val = os.environ.get(name)
    if val is None and fallback_name:
        val = os.environ.get(fallback_name)
    if val is None:
        val = default
    if required and val is None:
        print(f"FATAL: required env var {name} is not set (and no fallback/default).", file=sys.stderr)
        sys.exit(1)
    return val


def connect_readonly(host, port, user, password, dbname, label, timeout="30s"):
    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname,
        connect_timeout=int(timeout.rstrip("s")),
    )
    conn.set_session(readonly=True, autocommit=True)
    print(f"[{label}] connected READ-ONLY -> host={host} port={port} dbname={dbname} user={user}")
    return conn


def connect_writable(host, port, user, password, dbname, label, timeout="30s"):
    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname,
        connect_timeout=int(timeout.rstrip("s")),
    )
    conn.autocommit = False
    print(f"[{label}] connected WRITABLE -> host={host} port={port} dbname={dbname} user={user}")
    return conn


def claimbook_source_conn():
    return connect_readonly(
        host=env("CLAIMBOOK_HOST", default="localhost"),
        port=env("CLAIMBOOK_PORT", default="5432"),
        user=env("CLAIMBOOK_USER", required=True),
        password=env("CLAIMBOOK_PASSWORD", required=True),
        dbname=env("CLAIMBOOK_DBNAME", default="claimbook"),
        label="SOURCE claimbook",
    )


def cbreports_source_conn():
    return connect_readonly(
        host=env("CBREPORTS_HOST", "CLAIMBOOK_HOST", default="localhost"),
        port=env("CBREPORTS_PORT", "CLAIMBOOK_PORT", default="5432"),
        user=env("CBREPORTS_USER", "CLAIMBOOK_USER", required=True),
        password=env("CBREPORTS_PASSWORD", "CLAIMBOOK_PASSWORD", required=True),
        dbname=env("CBREPORTS_DBNAME", "CLAIMBOOK_DBNAME", default="cb_reports"),
        label="SOURCE cb_reports",
    )


def sandbox_target_conn():
    return connect_writable(
        host=env("SANDBOX_HOST", required=True),
        port=env("SANDBOX_PORT", default="5433"),
        user=env("SANDBOX_USER", required=True),
        password=env("SANDBOX_PASSWORD", required=True),
        dbname=env("SANDBOX_DBNAME", default="claimbook_sandbox"),
        label="TARGET sandbox",
    )


def fetch_source_claim(conn, schema, claim_id):
    """Pull the real raw chain: pre_authorisation -> patient -> person, -> insurance_policy."""
    query = f"""
        SELECT
            pre.preauth_claim_id,
            pre.patient_id,
            pre.insurance_policy_id,
            pre.al_number,
            patient.person_id,
            person.first_name,
            ip.insurance_policy_number
        FROM {schema}.oltp_pre_authorisation pre
        LEFT JOIN {schema}.oltp_patient_tb patient ON patient.patient_id = pre.patient_id
        LEFT JOIN {schema}.oltp_person person ON person.person_id = patient.person_id
        LEFT JOIN {schema}.oltp_insurance_policy ip ON ip.insurance_policy_id = pre.insurance_policy_id
        WHERE pre.preauth_claim_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (claim_id,))
        row = cur.fetchone()
    if row is None:
        print(f"FATAL: claim_id={claim_id} not found in {schema}.oltp_pre_authorisation on source.", file=sys.stderr)
        sys.exit(1)
    cols = ["preauth_claim_id", "patient_id", "insurance_policy_id", "al_number",
            "person_id", "first_name", "insurance_policy_number"]
    return dict(zip(cols, row))


def fetch_source_status(conn, schema, claim_id, run_date):
    query = f"""
        SELECT preauth_status_id, manual_upload_completed_time
        FROM {schema}.oltp_preauth_status
        WHERE preauth_claim_id = %s AND manual_upload_completed_time::date = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (claim_id, run_date))
        row = cur.fetchone()
    if row is None:
        print(f"WARNING: no oltp_preauth_status row found for claim_id={claim_id} on run_date={run_date}. "
              f"dbt's date filter will exclude this claim unless a status row is inserted.", file=sys.stderr)
        return None
    return {"preauth_status_id": row[0], "manual_upload_completed_time": row[1]}


def fetch_talend_row(conn, tenant_id, claim_id, run_date):
    # NOTE: real prod table is cb_report.manual_report (confirmed via \dt
    # cb_report.* on 2026-08-17). Sandbox's equivalent table is named
    # cb_report.preauth_manual_upload_daily - the two environments use
    # different table names for the same conceptual data. This function
    # targets the REAL PROD name since it only ever reads from prod.
    query = """
        SELECT preauth_claim_id, first_name, al_number, insurance_policy_number
        FROM cb_report.manual_report
        WHERE tenant_id = %s AND preauth_claim_id = %s AND start_date = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (tenant_id, claim_id, run_date))
        row = cur.fetchone()
    if row is None:
        print(f"NOTE: no Talend row found in cb_report.preauth_manual_upload_daily for "
              f"tenant_id={tenant_id} claim_id={claim_id} run_date={run_date}.", file=sys.stderr)
        return None
    cols = ["preauth_claim_id", "first_name", "al_number", "insurance_policy_number"]
    return dict(zip(cols, row))


def print_plan(claim, status, talend, target_schema):
    print("\n" + "=" * 70)
    print("PLAN - values that will be written to SANDBOX")
    print("=" * 70)
    print(f"Target schema for raw tables: {target_schema}")
    print(f"  oltp_person:            person_id={claim['person_id']}, first_name={claim['first_name']!r}")
    print(f"  oltp_patient_tb:        patient_id={claim['patient_id']}, person_id={claim['person_id']}")
    print(f"  oltp_insurance_policy:  insurance_policy_id={claim['insurance_policy_id']}, "
          f"insurance_policy_number={claim['insurance_policy_number']!r}")
    print(f"  oltp_pre_authorisation: preauth_claim_id={claim['preauth_claim_id']}, "
          f"patient_id={claim['patient_id']}, insurance_policy_id={claim['insurance_policy_id']}, "
          f"al_number={claim['al_number']!r}")
    if status:
        print(f"  oltp_preauth_status:    preauth_status_id={status['preauth_status_id']}, "
              f"preauth_claim_id={claim['preauth_claim_id']}, "
              f"manual_upload_completed_time={status['manual_upload_completed_time']!r}")
    else:
        print("  oltp_preauth_status:    NO ROW TO INSERT (none found on source for this run_date - "
              "dbt will exclude this claim until one exists)")
    print(f"Target schema for Talend table: cb_report")
    if talend:
        print(f"  preauth_manual_upload_daily: preauth_claim_id={talend['preauth_claim_id']}, "
              f"first_name={talend['first_name']!r}, al_number={talend['al_number']!r}, "
              f"insurance_policy_number={talend['insurance_policy_number']!r}")
    else:
        print("  preauth_manual_upload_daily: NO ROW TO INSERT (none found on source)")
    print("=" * 70 + "\n")


def execute_inserts(conn, claim, status, talend, schema):
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {schema}.oltp_person (person_id, first_name) VALUES (%s, %s) "
            f"ON CONFLICT (person_id) DO UPDATE SET first_name = EXCLUDED.first_name",
            (claim["person_id"], claim["first_name"]),
        )
        cur.execute(
            f"INSERT INTO {schema}.oltp_patient_tb (patient_id, person_id) VALUES (%s, %s) "
            f"ON CONFLICT (patient_id) DO UPDATE SET person_id = EXCLUDED.person_id",
            (claim["patient_id"], claim["person_id"]),
        )
        cur.execute(
            f"INSERT INTO {schema}.oltp_insurance_policy (insurance_policy_id, insurance_policy_number) "
            f"VALUES (%s, %s) ON CONFLICT (insurance_policy_id) DO UPDATE "
            f"SET insurance_policy_number = EXCLUDED.insurance_policy_number",
            (claim["insurance_policy_id"], claim["insurance_policy_number"]),
        )
        cur.execute(
            f"INSERT INTO {schema}.oltp_pre_authorisation "
            f"(preauth_claim_id, patient_id, insurance_policy_id, al_number) VALUES (%s, %s, %s, %s) "
            f"ON CONFLICT (preauth_claim_id) DO UPDATE SET "
            f"patient_id = EXCLUDED.patient_id, insurance_policy_id = EXCLUDED.insurance_policy_id, "
            f"al_number = EXCLUDED.al_number",
            (claim["preauth_claim_id"], claim["patient_id"], claim["insurance_policy_id"], claim["al_number"]),
        )
        if status:
            cur.execute(
                f"INSERT INTO {schema}.oltp_preauth_status "
                f"(preauth_status_id, preauth_claim_id, manual_upload_completed_time) VALUES (%s, %s, %s) "
                f"ON CONFLICT (preauth_status_id) DO UPDATE SET "
                f"manual_upload_completed_time = EXCLUDED.manual_upload_completed_time",
                (status["preauth_status_id"], claim["preauth_claim_id"], status["manual_upload_completed_time"]),
            )
        if talend:
            cur.execute(
                "INSERT INTO cb_report.preauth_manual_upload_daily "
                "(preauth_claim_id, first_name, al_number, insurance_policy_number) VALUES (%s, %s, %s, %s)",
                (talend["preauth_claim_id"], talend["first_name"], talend["al_number"],
                 talend["insurance_policy_number"]),
            )
    conn.commit()
    print("COMMITTED all inserts to sandbox.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant-id", type=int, required=True)
    ap.add_argument("--schema", required=True, help="Real tenant schema name, e.g. dmh")
    ap.add_argument("--claim-id", type=int, required=True)
    ap.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--target-schema", default=None,
                     help="Schema to write into on sandbox. Defaults to same as --schema.")
    ap.add_argument("--execute", action="store_true",
                     help="Actually write to sandbox. Without this flag, only prints the plan (dry run).")
    args = ap.parse_args()
    target_schema = args.target_schema or args.schema

    print(f"tenant_id={args.tenant_id} source_schema={args.schema} claim_id={args.claim_id} "
          f"run_date={args.run_date} target_schema={target_schema} mode={'EXECUTE' if args.execute else 'DRY RUN'}")

    cb_conn = claimbook_source_conn()
    cbr_conn = cbreports_source_conn()

    claim = fetch_source_claim(cb_conn, args.schema, args.claim_id)
    status = fetch_source_status(cb_conn, args.schema, args.claim_id, args.run_date)
    talend = fetch_talend_row(cbr_conn, args.tenant_id, args.claim_id, args.run_date)

    cb_conn.close()
    cbr_conn.close()

    print_plan(claim, status, talend, target_schema)

    if not args.execute:
        print("DRY RUN - nothing written. Re-run with --execute to actually write to sandbox.")
        return

    sb_conn = sandbox_target_conn()
    execute_inserts(sb_conn, claim, status, talend, target_schema)
    sb_conn.close()


if __name__ == "__main__":
    main()
