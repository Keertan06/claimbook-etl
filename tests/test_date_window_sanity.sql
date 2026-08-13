-- Fails if any staged row's manual_upload_completed_time falls outside
-- the run's own start_date/end_date window. This is a direct check on
-- the model's core filtering logic (the WHERE ...::date = run_date
-- clauses) - if that logic ever broke, this is what would catch it.
-- No package dependency (no dbt_utils) - safe to run standalone.

select
    tenant_id,
    preauth_claim_id,
    manual_upload_completed_time,
    start_date,
    end_date
from {{ ref('manual_report_staged') }}
where manual_upload_completed_time is not null
  and to_date(manual_upload_completed_time, 'DD/MM/YYYY HH24:MI:SS') not between start_date and end_date
