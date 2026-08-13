#!/usr/bin/env python3
"""
Moves dbt-staged rows from claimbook -> cb_reports, because Postgres cannot
write across databases in a single connection.

    claimbook.cb_staging.manual_report_staged -> cb_reports.cb_report.manual_report

  !! WRITE OPERATION !! Point CBREPORTS_DBNAME at a sandbox until parallel-run
  validation is signed off. Use --dry-run first.

Idempotent: deletes rows matching the staged data's
(tenant_id, start_date, end_date, functionality) keys, then inserts - all in
one transaction, rolls back on any failure. manual_report_id is deliberately
excluded from the column list (identity PK, generated on insert).

Usage:  python load_manual_report.py --dry-run
        python load_manual_report.py

Env (source): CLAIMBOOK_HOST/PORT/USER/PASSWORD/DBNAME
Env (target): CBREPORTS_HOST/PORT/USER/PASSWORD/DBNAME
              (each falls back to the CLAIMBOOK_* value if unset)
"""

import argparse
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

COLUMNS = [
    "preauth_claim_id", "mrn", "first_name", "tpa_name",
    "insurance_policy_number", "tpa_member_id", "al_number", "request_type",
    "workflow_state", "claim_submission_time", "automation_received_time",
    "automation_tat", "automation_status", "manual_upload_completed_time",
    "ops_user_name", "upload_completed_source",
    "manual_upload_completed_actual_tat", "automation_type", "proxy_remarks",
    "automation_failure_reason", "tenant_id", "start_date", "end_date",
    "functionality", "cl_number", "claims_id",
]

KEY_COLUMNS = ["tenant_id", "start_date", "end_date", "functionality"]

SOURCE_TABLE = "cb_staging.manual_report_staged"
TARGET_TABLE = "cb_report.manual_report"


def env(name, fallback_name=None, default=None):
    val = os.environ.get(name)
    if val:
        return val
    if fallback_name:
        val = os.environ.get(fallback_name)
        if val:
            return val
    return default


def source_conn():
    return psycopg2.connect(
        host=env("CLAIMBOOK_HOST", default="localhost"),
        port=env("CLAIMBOOK_PORT", default="5432"),
        user=env("CLAIMBOOK_USER"),
        password=env("CLAIMBOOK_PASSWORD"),
        dbname=env("CLAIMBOOK_DBNAME", default="claimbook"),
    )


def target_conn():
    return psycopg2.connect(
        host=env("CBREPORTS_HOST", "CLAIMBOOK_HOST", "localhost"),
        port=env("CBREPORTS_PORT", "CLAIMBOOK_PORT", "5432"),
        user=env("CBREPORTS_USER", "CLAIMBOOK_USER"),
        password=env("CBREPORTS_PASSWORD", "CLAIMBOOK_PASSWORD"),
        dbname=env("CBREPORTS_DBNAME", default="cb_reports"),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch-size", type=int, default=5000)
    args = ap.parse_args()

    col_list = ", ".join(COLUMNS)

    with source_conn() as src:
        with src.cursor() as cur:
            cur.execute(f"SELECT {col_list} FROM {SOURCE_TABLE}")
            rows = cur.fetchall()
            cur.execute(f"SELECT DISTINCT {', '.join(KEY_COLUMNS)} FROM {SOURCE_TABLE}")
            keys = cur.fetchall()

    print(f"[source] {len(rows)} staged row(s) across {len(keys)} key group(s)")
    for k in keys:
        print(f"         tenant_id={k[0]} start={k[1]} end={k[2]} functionality={k[3]}")

    if not rows:
        print("[source] nothing staged - exiting without touching the target.")
        return 0

    tgt = target_conn()
    try:
        with tgt.cursor() as cur:
            key_pred = " OR ".join(
                ["(" + " AND ".join(f"{c} = %s" for c in KEY_COLUMNS) + ")"] * len(keys)
            )
            flat = [v for k in keys for v in k]

            cur.execute(f"SELECT count(*) FROM {TARGET_TABLE} WHERE {key_pred}", flat)
            to_delete = cur.fetchone()[0]
            print(f"[target] {to_delete} existing row(s) match those keys and will be replaced")

            if args.dry_run:
                print(f"[dry-run] would DELETE {to_delete} row(s), then INSERT {len(rows)} row(s). No changes made.")
                tgt.rollback()
                return 0

            cur.execute(f"DELETE FROM {TARGET_TABLE} WHERE {key_pred}", flat)
            print(f"[target] deleted {cur.rowcount} row(s)")

            execute_values(
                cur,
                f"INSERT INTO {TARGET_TABLE} ({col_list}) VALUES %s",
                rows,
                page_size=args.batch_size,
            )
            print(f"[target] inserted {len(rows)} row(s)")

            cur.execute(f"SELECT count(*) FROM {TARGET_TABLE} WHERE {key_pred}", flat)
            print(f"[target] post-load count for those keys: {cur.fetchone()[0]}")

        tgt.commit()
        print("[target] COMMITTED")
    except Exception:
        tgt.rollback()
        print("[target] ROLLED BACK - no changes written", file=sys.stderr)
        raise
    finally:
        tgt.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
