{{
    config(
        materialized = 'table',
        schema       = none,
        alias        = 'manual_report_staged'
    )
}}

{#-
=============================================================================
manual_report  -  preauth manual upload daily (MULTI-TENANT)

Replaces the Talend job's tLoop_1 + per-tenant schema switching. The macro
resolves the tenant list from mtdm.mtdm_tenant_tb, and this model generates
one query block per tenant, UNION ALL'd into a single result set.

MATERIALIZATION (fixed 2026-08-11 - docstring previously described the
wrong layer's behavior, see below):
  materialized = 'table' - every `dbt run` fully replaces this table's
  entire contents with only the current run's tenants + run_date. This
  table does NOT accumulate rows across runs and is NOT keyed/incremental.

  This is intentional, not a gap: this table is a transient staging area,
  fully consumed by load_manual_report.py within the same DAG run,
  immediately after dbt_run/dbt_test complete and before another run could
  touch it. It is not safe to inspect after a later run has occurred - by
  then it only holds that later run's rows.

  Real idempotency - the actual guarantee that matters for correctness -
  lives one layer down, in load_manual_report.py's write to the real
  cb_reports target: DELETE any existing rows matching
  (tenant_id, start_date, end_date, functionality), then INSERT the
  freshly-staged rows, in one transaction. That is where "re-running for
  the same tenants+date is safe and will not duplicate" is actually true
  and actually enforced - not here.

  !! The loader's delete step affects the real cb_report.manual_report,
  including rows Talend wrote. That is intended for cutover, but is why
  the loader must not point at the real table until parallel-run
  validation is signed off.

DATE WINDOW:
  start_date = end_date = run_date, matching the real table's convention
  (the report run's date window, not a per-row business date).

COLUMN NOTES vs the real cb_report.manual_report (27 cols):
  - claimbook_submission_time is renamed to claim_submission_time here
  - functionality is hardcoded 'PREAUTH' (this table is shared by 4 job types)
  - cl_number / claims_id are NULL - they belong to the claims-side jobs
  - manual_report_id is the identity PK and is deliberately not written
=============================================================================
-#}

{%- set tenants  = get_tenant_schemas() -%}
{%- set run_date = var('run_date') -%}

{%- for t in tenants %}

-- ============================ tenant {{ t.tenant_id }} ({{ t.schema }}) ============================
select
    foo.preauth_claim_id,
    foo.mrn,
    foo.first_name,
    foo.tpa_name,
    foo.insurance_policy_number,
    foo.tpa_member_id,
    foo.al_number,
    foo.request_type,
    foo.workflow_state,
    foo.claim_submission_time,
    foo.automation_received_time,
    foo.automation_tat,
    foo.automation_status,
    foo.manual_upload_completed_time,
    foo.ops_user_name,
    foo.upload_completed_source,
    foo.manual_upload_completed_actual_tat,
    foo.automation_type,
    foo.proxy_remarks,
    foo.automation_failure_reason,
    -- ---- fields the Talend job adds outside the SELECT ----
    {{ t.tenant_id }}::integer          as tenant_id,
    '{{ run_date }}'::date              as start_date,
    '{{ run_date }}'::date              as end_date,
    'PREAUTH'::varchar                  as functionality,
    null::varchar                       as cl_number,
    null::integer                       as claims_id
from (

    -- ---------------- SUBMISSION branch (status-driven) ----------------
    select
        pre.preauth_claim_id,
        patient.mrn,
        person.first_name,
        mto.name                                                    as tpa_name,
        ip.insurance_policy_number,
        ip.tpa_member_id,
        pre.al_number,
        wrt.code                                                    as request_type,
        st.name                                                     as workflow_state,
        to_char(ps.status_update_date_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as claim_submission_time,
        to_char(ps.automation_received_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as automation_received_time,
        ps.automation_tat,
        ps.automation_status,
        ps.automation_failure_reason,
        to_char(ps.manual_upload_completed_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as manual_upload_completed_time,
        ps.manual_upload_created_by                                 as ops_user_name,
        ps.upload_completed_source,
        (age(ps.manual_upload_completed_time::timestamp,
             ps.automation_received_time::timestamp) * (3600/60)/60)::text
                                                                    as manual_upload_completed_actual_tat,
        'SUBMISSION'                                                as automation_type,
        ps.proxy_remarks
    from {{ t.schema }}.oltp_pre_authorisation pre
    left join {{ t.schema }}.oltp_preauth_status        ps      on pre.preauth_claim_id   = ps.preauth_claim_id
    left join {{ t.schema }}.oltp_patient_tb            patient on patient.patient_id     = pre.patient_id
    left join {{ t.schema }}.oltp_person                person  on person.person_id       = patient.person_id
    left join {{ t.schema }}.oltp_insurance_policy      ip      on ip.insurance_policy_id = pre.insurance_policy_id
    left join mtdm.mtdm_tpa_organization_tb             mto     on mto.tpa_organization_id = ip.tpa_organization_id
    left join {{ t.schema }}.oltp_workflow_state        st      on st.workflow_state_id   = ps.workflow_state_id
    left join {{ t.schema }}.oltp_workflow_request_type wrt     on wrt.request_type_id    = ps.request_type_id
    where ps.manual_upload_completed_time::date = '{{ run_date }}'

    union

    -- ---------------- QUERY branch (email-driven) ----------------
    select
        pre.preauth_claim_id,
        patient.mrn,
        person.first_name,
        mto.name                                                    as tpa_name,
        ip.insurance_policy_number,
        ip.tpa_member_id,
        pre.al_number,
        pe.request_type                                             as request_type,
        pe.state                                                    as workflow_state,
        to_char(pe.received_date_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as claim_submission_time,
        to_char(pe.automation_received_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as automation_received_time,
        pe.automation_tat,
        pe.automation_status,
        null                                                        as automation_failure_reason,
        to_char(pe.manual_upload_completed_time::timestamp without time zone,
                'DD/MM/YYYY HH24:MI:SS'::text)                      as manual_upload_completed_time,
        pe.manual_upload_created_by                                 as ops_user_name,
        pe.source                                                   as upload_completed_source,
        (age(pe.manual_upload_completed_time::timestamp,
             pe.automation_received_time::timestamp) * (3600/60)/60)::text
                                                                    as manual_upload_completed_actual_tat,
        'QUERY'                                                     as automation_type,
        pe.proxy_remarks
    from {{ t.schema }}.oltp_pre_authorisation pre
    left join {{ t.schema }}.oltp_preauth_email    pe      on pre.preauth_claim_id   = pe.preauth_id
    left join {{ t.schema }}.oltp_patient_tb       patient on patient.patient_id     = pre.patient_id
    left join {{ t.schema }}.oltp_person           person  on person.person_id       = patient.person_id
    left join {{ t.schema }}.oltp_insurance_policy ip      on ip.insurance_policy_id = pre.insurance_policy_id
    left join mtdm.mtdm_tpa_organization_tb        mto     on mto.tpa_organization_id = ip.tpa_organization_id
    where pe.manual_upload_completed_time::date = '{{ run_date }}'

) as foo

{% if not loop.last %}
union all
{% endif %}
{%- endfor %}

-- ROW ORDER (added 2026-08-12): confirmed against real Talend output
-- (tenant 30, 2026-07-29, cb_reports.manual_report ordered by its own
-- manual_report_id) that Talend's insertion order is preauth_claim_id
-- ascending within each tenant - NOT chronological by completion time
-- (times jump around freely; one duplicate-claim pair even appears with
-- its later row's timestamp earlier than its first, consistent with the
-- mutable-timestamp finding, Section 16 Part G - a row's time can change
-- after insertion without changing its position in the table).
-- This ORDER BY replicates that primary sort. It does NOT attempt to
-- replicate secondary order among multiple rows sharing one claim_id -
-- that didn't follow a clean, reproducible rule in the real data (see
-- the out-of-order example above), so no secondary sort key is claimed
-- here that we can't actually guarantee matches.
-- Before this was added, the model had NO explicit ordering at all -
-- output order was whatever Postgres's query plan happened to produce,
-- not a guarantee. That's a separate, real fragility this also closes.
order by tenant_id, preauth_claim_id
