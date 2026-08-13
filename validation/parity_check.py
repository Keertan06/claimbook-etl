#!/usr/bin/env python3
"""
READ-ONLY row-count parity check for multi-event claims.

scale_test_readonly.py answers "does this claim show up at all" (distinct
claim_id coverage). This answers a sharper question: for claims that
genuinely have MULTIPLE events on the same day (a claim with both a
SUBMISSION and a QUERY completion, or multiple resubmissions), does the
converted model produce the SAME NUMBER of rows as Talend's real output -
not just the same claim_id?

Sample: the 222 (tenant_id, preauth_claim_id) pairs already identified as
multi-event in cb_report.manual_report for 2026-07-29 (from the earlier
duplicate-theory investigation, Section 16 Part C) - reused here rather
than re-querying cb_reports, since we already have Talend's counts for
every one of them.

For each pair, runs the FULL SUBMISSION+QUERY UNION query (all ~20
columns, exactly matching models/reports/manual_report_staged.sql - NOT
the claim-ID-only version used by scale_test_readonly.py) scoped to that
one claim, counts the rows it returns, and compares to Talend's known
count for that same claim.

Read-only, enforced by Postgres itself (conn.set_session(readonly=True)),
same as scale_test_readonly.py.

Usage:
    python parity_check.py --run-date 2026-07-29 --sample talend_multievent_sample.csv --csv parity_results.csv

Env: same convention as scale_test_readonly.py - CLAIMBOOK_HOST/PORT/USER/
PASSWORD/DBNAME (real prod: 4.213.181.70 / 5433 / claimbook_ranjithad /
claimbook).
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

import psycopg2
from psycopg2 import sql


def env(name, default=None):
    return os.environ.get(name, default)


def claimbook_conn(timeout="30s"):
    conn = psycopg2.connect(
        host=env("CLAIMBOOK_HOST", "localhost"),
        port=env("CLAIMBOOK_PORT", "5432"),
        user=env("CLAIMBOOK_USER"),
        password=env("CLAIMBOOK_PASSWORD"),
        dbname=env("CLAIMBOOK_DBNAME", "claimbook"),
    )
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET statement_timeout = {}").format(sql.Literal(timeout)))
    return conn


TENANT_LOOKUP_SQL = """
    select tenant_id, sche_name
    from mtdm.mtdm_tenant_tb
    where tenant_id = ANY(%s);
"""

# Exact same SUBMISSION + QUERY union as manual_report_staged.sql - full
# ~20 column set, scoped to one claim_id so each check is cheap and
# targeted. Using the full column set (not just claim_id) matters here:
# UNION dedups on the complete row, and we already know from the Section
# 16 Part C investigation that genuinely-different events always differ
# in at least one of these columns (times, request_type, ops_user_name,
# etc.) - a claim-ID-only count would silently collapse genuine multi-
# event claims and defeat the point of this check.
FULL_CONVERTED_SQL_ALL_COLS = sql.SQL("""
    select * from (
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
          and pre.preauth_claim_id = %s

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
          and pre.preauth_claim_id = %s
    ) as foo;
""")


def load_sample(path):
    sample = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            sample.append((int(row["tenant_id"]), int(row["preauth_claim_id"]), int(row["talend_count"])))
    return sample


def get_schemas(conn, tenant_ids):
    with conn.cursor() as cur:
        cur.execute(TENANT_LOOKUP_SQL, (list(tenant_ids),))
        return dict(cur.fetchall())


def converted_row_count(conn, schema, claim_id, run_date):
    query = FULL_CONVERTED_SQL_ALL_COLS.format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(query, (run_date, claim_id, run_date, claim_id))
        return cur.rowcount if cur.rowcount is not None else len(cur.fetchall())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--sample", required=True, help="CSV with tenant_id,preauth_claim_id,talend_count")
    ap.add_argument("--timeout", default="30s")
    ap.add_argument("--csv", help="optional path to write full results")
    args = ap.parse_args()

    sample = load_sample(args.sample)
    print(f"Loaded {len(sample)} multi-event claims from {args.sample}")

    conn = claimbook_conn(timeout=args.timeout)
    print(f"[claimbook] connected READ-ONLY to {env('CLAIMBOOK_HOST')} / {env('CLAIMBOOK_DBNAME', 'claimbook')}")

    tenant_ids = sorted(set(t for t, _, _ in sample))
    schemas = get_schemas(conn, tenant_ids)
    missing = [t for t in tenant_ids if t not in schemas]
    if missing:
        print(f"WARNING: {len(missing)} tenant_id(s) not found in mtdm.mtdm_tenant_tb, skipping: {missing}")

    print(f"\n{'tenant':>7}  {'claim_id':>10}  {'talend':>7}  {'converted':>9}  {'match':>6}")
    print("-" * 50)

    results = []
    failed = []
    match_count = 0
    for tenant_id, claim_id, talend_count in sample:
        schema = schemas.get(tenant_id)
        if not schema:
            continue
        try:
            conv_count = converted_row_count(conn, schema, claim_id, args.run_date)
        except psycopg2.Error as e:
            print(f"{tenant_id:>7}  {claim_id:>10}  {talend_count:>7}  {'ERROR':>9}  {type(e).__name__}")
            failed.append({"tenant_id": tenant_id, "claim_id": claim_id, "error": str(e).strip()})
            continue

        match = "YES" if conv_count == talend_count else "NO"
        if match == "YES":
            match_count += 1
        print(f"{tenant_id:>7}  {claim_id:>10}  {talend_count:>7}  {conv_count:>9}  {match:>6}")
        results.append({"tenant_id": tenant_id, "schema": schema, "claim_id": claim_id,
                         "talend_count": talend_count, "converted_count": conv_count, "match": match})

    conn.close()

    print(f"\n{match_count}/{len(results)} claims match exactly on row COUNT (not just presence).")
    if failed:
        print(f"{len(failed)} claim(s) failed to check (see above).")

    mismatches = [r for r in results if r["match"] == "NO"]
    if mismatches:
        print("\nMismatches:")
        for r in mismatches:
            print(f"  tenant {r['tenant_id']} ({r['schema']}), claim {r['claim_id']}: "
                  f"talend={r['talend_count']} converted={r['converted_count']}")

    if args.csv and results:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
        print(f"\nFull results written to {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
