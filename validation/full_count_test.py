#!/usr/bin/env python3
"""
READ-ONLY full end-to-end row-COUNT test, per (tenant_id, start_date,
end_date) - the same key load_manual_report.py actually uses.

Different from the earlier two scripts:
  - scale_test_readonly.py checks claim-ID SET coverage (did we find the
    right claims), using the simplified claim-ID-only query.
  - parity_check.py checks row COUNT for a pre-selected sample of known
    multi-event claims, one claim at a time.
  - THIS script checks raw row COUNT for the WHOLE tenant+date, using the
    real ~20-column query exactly as manual_report_staged.sql runs it -
    coverage and parity in one number per tenant, no claim-ID
    identification needed.

Defaults run-date to YESTERDAY (computed fresh each run), not a fixed
historical date. This is deliberate: the Part G finding was that residual
mismatches are a retroactive-testing artifact (a mutable timestamp column
gets overwritten between when Talend ran and when we check it weeks
later). Testing against yesterday's date - almost no time lag - is the
direct test of that theory: if it's right, this should come back close to
100%, much higher than the 95.4%/95% seen against 2026-07-29 (13+ days
old at the time it was tested).

Read-only, enforced by Postgres itself, same as the other two scripts.

Usage:
    python full_count_test.py --limit 483 --timeout 60s --csv full_count_results.csv
    python full_count_test.py --run-date 2026-08-05  # override the default if needed

Env: same convention as the other scripts - CLAIMBOOK_HOST/PORT/USER/
PASSWORD/DBNAME, CBREPORTS_DBNAME (falls back to CLAIMBOOK_* if unset).
"""

import argparse
import csv
import os
import sys
from datetime import date, timedelta

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
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
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

# Full ~20-column query, exactly matching manual_report_staged.sql - whole
# tenant, not scoped to one claim, counted rather than fetched (COUNT(*)
# on a subquery keeps this cheap even for a busy tenant).
FULL_CONVERTED_COUNT_SQL = sql.SQL("""
    select count(*) from (
        select
            pre.preauth_claim_id, patient.mrn, person.first_name,
            mto.name as tpa_name, ip.insurance_policy_number, ip.tpa_member_id,
            pre.al_number, wrt.code as request_type, st.name as workflow_state,
            to_char(ps.status_update_date_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as claim_submission_time,
            to_char(ps.automation_received_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as automation_received_time,
            ps.automation_tat, ps.automation_status, ps.automation_failure_reason,
            to_char(ps.manual_upload_completed_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as manual_upload_completed_time,
            ps.manual_upload_created_by as ops_user_name, ps.upload_completed_source,
            (age(ps.manual_upload_completed_time::timestamp,
                 ps.automation_received_time::timestamp) * (3600/60)/60)::text
                as manual_upload_completed_actual_tat,
            'SUBMISSION' as automation_type, ps.proxy_remarks
        from {schema}.oltp_pre_authorisation pre
        left join {schema}.oltp_preauth_status        ps      on pre.preauth_claim_id   = ps.preauth_claim_id
        left join {schema}.oltp_patient_tb            patient on patient.patient_id     = pre.patient_id
        left join {schema}.oltp_person                person  on person.person_id       = patient.person_id
        left join {schema}.oltp_insurance_policy      ip      on ip.insurance_policy_id = pre.insurance_policy_id
        left join mtdm.mtdm_tpa_organization_tb       mto     on mto.tpa_organization_id = ip.tpa_organization_id
        left join {schema}.oltp_workflow_state        st      on st.workflow_state_id   = ps.workflow_state_id
        left join {schema}.oltp_workflow_request_type wrt     on wrt.request_type_id    = ps.request_type_id
        where ps.manual_upload_completed_time::date = %s

        union

        select
            pre.preauth_claim_id, patient.mrn, person.first_name,
            mto.name as tpa_name, ip.insurance_policy_number, ip.tpa_member_id,
            pre.al_number, pe.request_type, pe.state as workflow_state,
            to_char(pe.received_date_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as claim_submission_time,
            to_char(pe.automation_received_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as automation_received_time,
            pe.automation_tat, pe.automation_status, null as automation_failure_reason,
            to_char(pe.manual_upload_completed_time::timestamp without time zone,
                    'DD/MM/YYYY HH24:MI:SS'::text) as manual_upload_completed_time,
            pe.manual_upload_created_by, pe.source as upload_completed_source,
            (age(pe.manual_upload_completed_time::timestamp,
                 pe.automation_received_time::timestamp) * (3600/60)/60)::text
                as manual_upload_completed_actual_tat,
            'QUERY' as automation_type, pe.proxy_remarks
        from {schema}.oltp_pre_authorisation pre
        left join {schema}.oltp_preauth_email    pe      on pre.preauth_claim_id   = pe.preauth_id
        left join {schema}.oltp_patient_tb       patient on patient.patient_id     = pre.patient_id
        left join {schema}.oltp_person           person  on person.person_id       = patient.person_id
        left join {schema}.oltp_insurance_policy ip      on ip.insurance_policy_id = pre.insurance_policy_id
        left join mtdm.mtdm_tpa_organization_tb  mto     on mto.tpa_organization_id = ip.tpa_organization_id
        where pe.manual_upload_completed_time::date = %s
    ) as foo;
""")

# Real key load_manual_report.py's loader actually uses - tenant_id,
# start_date, end_date, functionality. Our test model sets start_date =
# end_date = run_date, so filtering on both here (not just start_date, as
# earlier scripts did) matches the real key exactly.
TALEND_COUNT_SQL = """
    select count(*)
    from cb_report.manual_report
    where tenant_id = %s
      and functionality = 'PREAUTH'
      and start_date = %s
      and end_date = %s;
"""


def get_tenants(conn, limit):
    with conn.cursor() as cur:
        cur.execute(TENANT_SQL, (limit,))
        return cur.fetchall()


def converted_count(conn, schema, run_date):
    query = FULL_CONVERTED_COUNT_SQL.format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(query, (run_date, run_date))
        return cur.fetchone()[0]


def talend_count(conn, tenant_id, run_date):
    with conn.cursor() as cur:
        cur.execute(TALEND_COUNT_SQL, (tenant_id, run_date, run_date))
        return cur.fetchone()[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", default=None,
                     help="YYYY-MM-DD. Default: yesterday (computed fresh each run).")
    ap.add_argument("--limit", type=int, default=483)
    ap.add_argument("--timeout", default="30s")
    ap.add_argument("--csv", help="optional path to write full results")
    args = ap.parse_args()

    run_date = args.run_date or (date.today() - timedelta(days=1)).isoformat()
    print(f"Testing run_date = {run_date}" + ("" if args.run_date else "  (yesterday, computed automatically)"))

    cb = claimbook_conn(timeout=args.timeout)
    rep = cbreports_conn(timeout=args.timeout)
    print(f"[claimbook]  READ-ONLY {env('CLAIMBOOK_HOST')} / {env('CLAIMBOOK_DBNAME', 'claimbook')}")
    print(f"[cb_reports] READ-ONLY {env('CBREPORTS_HOST', 'CLAIMBOOK_HOST')} / {env('CBREPORTS_DBNAME', 'CLAIMBOOK_DBNAME', 'cb_reports')}")

    tenants = get_tenants(cb, args.limit)
    print(f"{len(tenants)} tenant(s) selected.\n")

    header = f"{'tenant':>7}  {'schema':<22}  {'talend':>7}  {'converted':>9}  {'match':>6}"
    print(header)
    print("-" * len(header))

    results, failed, match_count = [], [], 0
    for tenant_id, schema in tenants:
        try:
            t_count = talend_count(rep, tenant_id, run_date)
            c_count = converted_count(cb, schema, run_date)
        except psycopg2.Error as e:
            print(f"{tenant_id:>7}  {schema:<22}  {'ERROR: ' + type(e).__name__}")
            failed.append({"tenant_id": tenant_id, "schema": schema, "error": str(e).strip()})
            continue

        match = "YES" if t_count == c_count else "NO"
        if match == "YES":
            match_count += 1
        if t_count or c_count:  # skip printing the (very common) 0/0 rows to keep output readable
            print(f"{tenant_id:>7}  {schema:<22}  {t_count:>7}  {c_count:>9}  {match:>6}")
        results.append({"tenant_id": tenant_id, "schema": schema, "run_date": run_date,
                         "talend_count": t_count, "converted_count": c_count, "match": match})

    cb.close()
    rep.close()

    print(f"\n{match_count}/{len(results)} tenants match exactly on row COUNT for {run_date}.")
    if failed:
        print(f"{len(failed)} tenant(s) failed to check.")

    mismatches = [r for r in results if r["match"] == "NO"]
    if mismatches:
        print("\nMismatches:")
        for r in mismatches:
            print(f"  tenant {r['tenant_id']} ({r['schema']}): talend={r['talend_count']} converted={r['converted_count']}")

    if args.csv and results:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
        print(f"\nFull results written to {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
