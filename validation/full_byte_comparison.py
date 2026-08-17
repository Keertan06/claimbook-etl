#!/usr/bin/env python3
"""
READ-ONLY byte-by-byte comparison: every column, not just counts or
claim-ID coverage. Answers directly: for a given tenant/date, is our
converted row genuinely identical to Talend's real row, column for
column - not just "same number of rows" (full_count_test.py) or "same
claim IDs in the same order" (the manual pgAdmin order check).

Pulls the EXACT columns and expressions manual_report_staged.sql uses -
copied directly from that file, not approximated - and compares against
the same columns from real cb_report.manual_report.

Read-only, enforced by Postgres itself (conn.set_session(readonly=True)),
same as every other validation script in this project.

Usage:
    python full_byte_comparison.py --tenant-id 36 --schema dmh --run-date 2026-08-12

Env: same convention as the other validation scripts - CLAIMBOOK_HOST/
PORT/USER/PASSWORD/DBNAME, CBREPORTS_DBNAME (falls back to CLAIMBOOK_*).
"""

import argparse
import os
import sys

import psycopg2
from psycopg2 import sql

# Exact column order load_manual_report.py writes, and therefore the
# column order to compare in - copied directly from that file's COLUMNS.
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


def claimbook_conn(timeout="60s"):
    return _connect_readonly(
        host=env("CLAIMBOOK_HOST", default="localhost"),
        port=env("CLAIMBOOK_PORT", default="5432"),
        user=env("CLAIMBOOK_USER"),
        password=env("CLAIMBOOK_PASSWORD"),
        dbname=env("CLAIMBOOK_DBNAME", default="claimbook"),
        timeout=timeout,
    )


def cbreports_conn(timeout="60s"):
    return _connect_readonly(
        host=env("CBREPORTS_HOST", "CLAIMBOOK_HOST", default="localhost"),
        port=env("CBREPORTS_PORT", "CLAIMBOOK_PORT", default="5432"),
        user=env("CBREPORTS_USER", "CLAIMBOOK_USER"),
        password=env("CBREPORTS_PASSWORD", "CLAIMBOOK_PASSWORD"),
        dbname=env("CBREPORTS_DBNAME", "CLAIMBOOK_DBNAME", default="cb_reports"),
        timeout=timeout,
    )


# Exact copy of manual_report_staged.sql's SELECT list and joins, just
# with the tenant_id/schema/run_date hardcoded per-call instead of looped
# by the dbt macro - same logic, same column expressions, nothing
# simplified or approximated this time.
CONVERTED_FULL_SQL = sql.SQL("""
select
    foo.preauth_claim_id, foo.mrn, foo.first_name, foo.tpa_name,
    foo.insurance_policy_number, foo.tpa_member_id, foo.al_number,
    foo.request_type, foo.workflow_state, foo.claim_submission_time,
    foo.automation_received_time, foo.automation_tat, foo.automation_status,
    foo.manual_upload_completed_time, foo.ops_user_name,
    foo.upload_completed_source, foo.manual_upload_completed_actual_tat,
    foo.automation_type, foo.proxy_remarks, foo.automation_failure_reason,
    {tenant_id}::integer as tenant_id,
    {run_date}::date as start_date,
    {run_date}::date as end_date,
    'PREAUTH'::varchar as functionality,
    null::varchar as cl_number,
    null::integer as claims_id
from (
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
    where ps.manual_upload_completed_time::date = {run_date}

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
    where pe.manual_upload_completed_time::date = {run_date}
) as foo
order by foo.preauth_claim_id;
""")

TALEND_FULL_SQL = sql.SQL("""
select {cols}
from cb_report.manual_report
where tenant_id = {tenant_id}
  and functionality = 'PREAUTH'
  and start_date = {run_date}
  and end_date = {run_date}
order by preauth_claim_id, manual_report_id;
""")


def fetch_converted(conn, tenant_id, schema, run_date):
    query = CONVERTED_FULL_SQL.format(
        tenant_id=sql.Literal(tenant_id),
        schema=sql.Identifier(schema),
        run_date=sql.Literal(run_date),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def fetch_talend(conn, tenant_id, run_date):
    query = TALEND_FULL_SQL.format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in COLUMNS),
        tenant_id=sql.Literal(tenant_id),
        run_date=sql.Literal(run_date),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant-id", type=int, required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--timeout", default="60s")
    args = ap.parse_args()

    cb = claimbook_conn(timeout=args.timeout)
    rep = cbreports_conn(timeout=args.timeout)

    converted = fetch_converted(cb, args.tenant_id, args.schema, args.run_date)
    talend = fetch_talend(rep, args.tenant_id, args.run_date)

    cb.close()
    rep.close()

    print(f"tenant {args.tenant_id} ({args.schema}), run_date {args.run_date}")
    print(f"converted rows: {len(converted)}   talend rows: {len(talend)}")

    if len(converted) != len(talend):
        print("\nROW COUNT MISMATCH - cannot do a clean row-by-row byte "
              "comparison until counts match. Showing counts only.")
        return 1

    exact_matches = 0
    mismatched_rows = []

    for i, (c_row, t_row) in enumerate(zip(converted, talend)):
        if c_row == t_row:
            exact_matches += 1
        else:
            diffs = []
            for col, c_val, t_val in zip(COLUMNS, c_row, t_row):
                if c_val != t_val:
                    diffs.append((col, c_val, t_val))
            mismatched_rows.append((i, c_row[0], diffs))  # (position, claim_id, diffs)

    print(f"\n{exact_matches}/{len(converted)} rows are a full, exact, "
          f"every-column match.")

    if mismatched_rows:
        print(f"\n{len(mismatched_rows)} row(s) have at least one column difference:")
        for pos, claim_id, diffs in mismatched_rows:
            print(f"\n  Row {pos} (claim_id={claim_id}):")
            for col, c_val, t_val in diffs:
                print(f"    {col}: converted={c_val!r}  talend={t_val!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
