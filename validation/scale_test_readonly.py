#!/usr/bin/env python3
"""
READ-ONLY scale-test: extends the 5-tenant parallel-run validation
(2, 30, 42, 44, 52) to N real active tenants, ordered by tenant_id -
same selection logic as get_tenant_schemas() (macros/get_tenant_schemas.sql),
just as plain SQL instead of a dbt macro.

Does NOT use dbt. dbt run against real claimbook would CREATE TABLE
claimbook.cb_staging.manual_report_staged - a write, and writes to real
claimbook are still blocked. This script only ever SELECTs.

Both connections are opened with set_session(readonly=True), which makes
Postgres itself reject any write attempt (ReadOnlySqlTransaction) - this is
enforced by the server, not just script discipline.

Compares, per tenant:
  - converted   = distinct preauth_claim_id from the SUBMISSION+QUERY union
                  logic (same as models/reports/manual_report_staged.sql),
                  run directly against real claimbook
  - talend_raw  = raw row count from real cb_report.manual_report (may
                  include Talend's known duplicate-insert rows)
  - talend_dedup = distinct preauth_claim_id count from the same table

Only preauth_claim_id is compared (not the full 20-column payload) -
field-level correctness was already exhaustively proven for tenant 36
(66/66, Section 6). This test is about identity/coverage at scale, not
re-proving field mapping.

Usage:
    python scale_test_readonly.py --run-date 2026-07-29 --limit 25
    python scale_test_readonly.py --run-date 2026-07-29 --limit 25 --list-tenants-only
    python scale_test_readonly.py --run-date 2026-07-29 --limit 25 --csv results.csv

Env (claimbook, real - source):   CLAIMBOOK_HOST/PORT/USER/PASSWORD/DBNAME
Env (cb_reports, real - Talend):  CBREPORTS_HOST/PORT/USER/PASSWORD/DBNAME
    (each falls back to the CLAIMBOOK_* value if unset - same convention
    as load_manual_report.py)
"""

import argparse
import csv
import os
import sys

import psycopg2
from psycopg2 import sql


def env(name, fallback_name=None, default=None):
    val = os.environ.get(name)
    if val:
        return val
    if fallback_name:
        val = os.environ.get(fallback_name)
        if val:
            return val
    return default


def _connect_readonly(host, port, user, password, dbname, timeout):
    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname,
    )
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET statement_timeout = {}").format(sql.Literal(timeout)))
    return conn


def claimbook_conn(timeout="30s"):
    return _connect_readonly(
        host=env("CLAIMBOOK_HOST", default="localhost"),
        port=env("CLAIMBOOK_PORT", default="5432"),
        user=env("CLAIMBOOK_USER"),
        password=env("CLAIMBOOK_PASSWORD"),
        dbname=env("CLAIMBOOK_DBNAME", default="claimbook"),
        timeout=timeout,
    )


def cbreports_conn(timeout="30s"):
    return _connect_readonly(
        host=env("CBREPORTS_HOST", "CLAIMBOOK_HOST", default="localhost"),
        port=env("CBREPORTS_PORT", "CLAIMBOOK_PORT", default="5432"),
        user=env("CBREPORTS_USER", "CLAIMBOOK_USER"),
        password=env("CBREPORTS_PASSWORD", "CLAIMBOOK_PASSWORD"),
        dbname=env("CBREPORTS_DBNAME", "CLAIMBOOK_DBNAME", default="cb_reports"),
        timeout=timeout,
    )


# Same 3 guards as get_tenant_schemas(): is_tenant, schema exists,
# oltp_pre_authorisation table exists. Ordered/limited identically too, so
# this picks exactly the tenants the real pipeline would pick at this limit.
TENANT_SQL = """
    select t.tenant_id, t.sche_name
    from mtdm.mtdm_tenant_tb t
    join information_schema.schemata s on s.schema_name = t.sche_name
    where t.sche_name is not null
      and btrim(t.sche_name) <> ''
      and t.is_tenant is true
      and exists (
          select 1 from information_schema.tables it
          where it.table_schema = t.sche_name
            and it.table_name = 'oltp_pre_authorisation'
      )
    order by t.tenant_id
    limit %s;
"""

# Same SUBMISSION + QUERY union as manual_report_staged.sql, trimmed to just
# preauth_claim_id (identity only - the joins that were dropped are all
# LEFT JOINs on mrn/tpa/etc. that don't affect which claim_ids qualify).
CONVERTED_SQL_TEMPLATE = sql.SQL("""
    select pre.preauth_claim_id
    from {schema}.oltp_pre_authorisation pre
    left join {schema}.oltp_preauth_status ps
        on pre.preauth_claim_id = ps.preauth_claim_id
    where ps.manual_upload_completed_time::date = %s

    union

    select pre.preauth_claim_id
    from {schema}.oltp_pre_authorisation pre
    left join {schema}.oltp_preauth_email pe
        on pre.preauth_claim_id = pe.preauth_id
    where pe.manual_upload_completed_time::date = %s
""")

TALEND_SQL = """
    select preauth_claim_id
    from cb_report.manual_report
    where tenant_id = %s
      and functionality = 'PREAUTH'
      and start_date = %s;
"""


def get_tenants(cb_conn, limit):
    with cb_conn.cursor() as cur:
        cur.execute(TENANT_SQL, (limit,))
        return cur.fetchall()  # [(tenant_id, schema), ...]


def converted_claim_ids(cb_conn, schema, run_date):
    query = CONVERTED_SQL_TEMPLATE.format(schema=sql.Identifier(schema))
    with cb_conn.cursor() as cur:
        cur.execute(query, (run_date, run_date))
        return {row[0] for row in cur.fetchall()}


def talend_claim_ids_raw(rep_conn, tenant_id, run_date):
    with rep_conn.cursor() as cur:
        cur.execute(TALEND_SQL, (tenant_id, run_date))
        return [row[0] for row in cur.fetchall()]  # keep duplicates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=25,
                     help="number of tenants, ordered by tenant_id (default 25 - "
                          "next step after the 5 already tested)")
    ap.add_argument("--list-tenants-only", action="store_true",
                     help="print which tenants would be tested, run no comparison queries")
    ap.add_argument("--csv", help="optional path to write full results as CSV")
    ap.add_argument("--timeout", default="30s",
                     help="per-statement timeout, e.g. '30s', '60s' (default 30s)")
    args = ap.parse_args()

    cb = claimbook_conn(timeout=args.timeout)
    print(f"[claimbook]  connected READ-ONLY to "
          f"{env('CLAIMBOOK_HOST', default='localhost')} / "
          f"{env('CLAIMBOOK_DBNAME', default='claimbook')}  (timeout={args.timeout})")

    tenants = get_tenants(cb, args.limit)
    print(f"[claimbook]  {len(tenants)} tenant(s) selected (limit={args.limit}):")
    print("             " + ", ".join(f"{tid}:{schema}" for tid, schema in tenants))

    if args.list_tenants_only:
        cb.close()
        return 0

    rep = cbreports_conn(timeout=args.timeout)
    print(f"[cb_reports] connected READ-ONLY to "
          f"{env('CBREPORTS_HOST', 'CLAIMBOOK_HOST', default='localhost')} / "
          f"{env('CBREPORTS_DBNAME', 'CLAIMBOOK_DBNAME', default='cb_reports')}")
    print()

    header = (f"{'tenant':>7}  {'schema':<20}  {'converted':>9}  "
              f"{'talend_raw':>10}  {'talend_dedup':>12}  {'match':>7}")
    print(header)
    print("-" * len(header))

    results = []
    failed = []
    for tenant_id, schema in tenants:
        try:
            conv_ids = converted_claim_ids(cb, schema, args.run_date)
            talend_raw = talend_claim_ids_raw(rep, tenant_id, args.run_date)
        except psycopg2.Error as e:
            # Timeout/error on one tenant shouldn't kill the whole batch -
            # both connections are autocommit, so a canceled statement only
            # aborts that one implicit transaction; the connection itself
            # stays usable for the next tenant (verified against a fixture
            # before shipping this fix).
            print(f"{tenant_id:>7}  {schema:<20}  {'ERROR: ' + type(e).__name__:>44}")
            failed.append({"tenant_id": tenant_id, "schema": schema,
                            "error": f"{type(e).__name__}: {str(e).strip()}"})
            continue

        talend_dedup = set(talend_raw)
        match = "YES" if conv_ids == talend_dedup else "NO"
        missing = sorted(talend_dedup - conv_ids)   # in Talend, not in converted
        extra = sorted(conv_ids - talend_dedup)      # in converted, not in Talend

        print(f"{tenant_id:>7}  {schema:<20}  {len(conv_ids):>9}  "
              f"{len(talend_raw):>10}  {len(talend_dedup):>12}  {match:>7}")

        results.append({
            "tenant_id": tenant_id, "schema": schema,
            "converted": len(conv_ids), "talend_raw": len(talend_raw),
            "talend_dedup": len(talend_dedup), "match": match,
            "missing_from_converted": ";".join(map(str, missing)),
            "extra_in_converted": ";".join(map(str, extra)),
        })

    cb.close()
    rep.close()

    mismatches = [r for r in results if r["match"] == "NO"]
    print()
    print(f"{len(results) - len(mismatches)}/{len(results)} tenants match exactly "
          f"(converted set == distinct Talend set).")
    if failed:
        print(f"\n{len(failed)} tenant(s) FAILED (not counted above, re-run these separately):")
        for f in failed:
            print(f"  tenant {f['tenant_id']} ({f['schema']}): {f['error']}")

    if mismatches:
        print("\nMismatches:")
        for r in mismatches:
            print(f"  tenant {r['tenant_id']} ({r['schema']}): "
                  f"missing_from_converted=[{r['missing_from_converted']}]  "
                  f"extra_in_converted=[{r['extra_in_converted']}]")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
        print(f"\nFull results written to {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
