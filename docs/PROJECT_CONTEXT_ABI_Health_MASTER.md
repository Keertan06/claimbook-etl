# ABI HEALTH — TALEND → dbt/AIRFLOW ETL MIGRATION
## Master Context File (read in full before answering anything)

> **If you are a fresh Claude instance:** this file replaces the need for me to
> re-explain the project. Read Section 0 and Section 1 first — they change how
> you should answer. Everything else is reference you can search when needed.
> Do not ask me questions this file already answers.

> **Provenance:** this is a merge of two earlier context files — the original
> `PROJECT_CONTEXT_ABI_Health.md` and a newer one written after the
> multi-tenant build session (2026-08-07). Where the two conflicted, the
> conflict is called out explicitly rather than silently resolved — see
> Section 9 for the full list of live contradictions. This file supersedes
> both; delete the old ones from project knowledge.

---

# SECTION 0 — HOW TO ANSWER ME (most important section)

## Who I am
Keertan, developer at **ABI Health Pvt Ltd**, Bangalore. ABI Health sits
between hospitals and insurance TPAs (Third Party Administrators), managing
insurance claims. I am the **sole technical person** on ETL infrastructure
going forward — there is no second engineer sanity-checking decisions, so
being genuinely careful (especially about anything that writes) matters more
than usual. I am comfortable with Python and SQL. I am not a data engineer by
background — I am learning this stack by building it.

## Non-negotiable answering rules

1. **Give real, runnable code. Always.** Copy-pasteable SQL, dbt models,
   Airflow DAGs, PowerShell/bash commands. Never pseudocode, never "you would
   do something like...". If I ask "how do I X", the answer contains the exact
   command, not a description of the command.

2. **One step at a time, then pause.** Do not dump ten steps and assume they
   all work. Give me a step (or a tight group), tell me what success looks
   like, and **wait for my actual pasted output** before the next one. This
   project has hit 35+ real errors — batching steps makes them impossible to
   isolate. Ask for real terminal output, not "did that work?".

3. **Explain the root cause before the fix.** When something breaks, tell me
   *why* in one or two plain sentences, then give the fix. Keep it tight —
   not a lecture.

4. **Assume production caution by default.** I work against real, live,
   in-use databases holding real patient PII and claims data. Before giving
   me any command with write potential (`CREATE`, `INSERT`, `UPDATE`, `DROP`,
   `dbt run` without an explicitly safe target), confirm which environment
   it's hitting. If ambiguous, ask before giving the command. Flag
   destructive operations explicitly and loudly.

5. **Correct yourself openly and immediately when you're wrong.** This has
   happened repeatedly (Section 8, entries 26–31). I would much rather get a
   clear "I was wrong earlier, here's the correction" than have a bad fact
   silently propagate.

6. **Flag architecturally significant findings loudly.** Do not bury a major
   discovery inside a routine troubleshooting reply. Two findings reshaped
   this entire project — the per-tenant-schema discovery (Section 3) and the
   cross-database write constraint (Section 5). That class of finding gets
   called out clearly, not mentioned in passing.

7. **When I upload a CSV or data export, actually read and analyse it with
   code.** Run real counts, diffs, groupings. Don't ask me to describe what's
   in it. Several key findings came from parsing my exports directly —
   including the `sche_name` discovery in Section 4.

8. **Verify before handing me instructions, where you can.** If you can test
   SQL/dbt/Python logic yourself first (e.g. spinning up a sandbox Postgres
   in your own tool environment) before giving it to me to run for real, do
   that. This caught real bugs in the multi-tenant build before they reached
   my machine.

9. **Produce markdown artifact files for anything worth keeping.** Reference
   docs, checklists, meeting prep, status updates. Don't leave valuable
   synthesis buried in chat text.

10. **Be precise about which shell.** My environment spans Windows
    PowerShell, Windows Command Prompt, WSL2 Ubuntu bash, and pgAdmin's Query
    Tool. Always say which one a command belongs in. They are not
    interchangeable.

11. **Don't suggest Airbyte or extra tooling** unless a genuine non-Postgres
    external source is confirmed. Working stack is **dbt + Airflow only**.

12. **Never lose the multi-tenant frame.** Any single-schema model only
    covers ONE hospital out of 576+. If you write one, say so explicitly.

13. **Ground summaries in real evidence from this project** — actual row
    counts, actual table names, actual error text. Not generic statements.

14. **Verify environment facts against my machine, don't trust these notes
    blindly.** Environments drift. Wrong assumed Postgres version, wrong
    assumed folder paths, and a wrong assumed password all caused confusing
    failures (Section 8, #29–31). A `Get-ChildItem` / directory listing /
    `psql` connection test first is cheaper than debugging the wrong theory.

---

# SECTION 1 — CURRENT STATE (where we actually are right now)

**Last major session: 2026-08-17 — ran the first genuine `dbt run` (not a
script standing in for it) against real production-sourced data, via a new
sandbox-bridge tool, and confirmed the result matches Claimbook and diverges
from Talend on real claims. Also adopted a two-terminal working pattern after
two near-misses where `dbt run` was accidentally pointed at real prod. Full
detail in Section 18. Prior major session: 2026-08-11 — corrected a wrong
root-cause theory from 2026-08-10 (the "duplicate inserts from reruns"
explanation), verified the real explanation against raw source data, and
began real-tenant scale validation. Full detail in Section 16. Prior major
session: 2026-08-10, the four items open after the 2026-08-07 multi-tenant
build — full detail in Section 15.**

## What is DONE and PROVEN (actually executed, not just written)

- ✅ One real Talend job (`preauth manual upload daily`) fully converted to a
  dbt model + Airflow DAG.
- ✅ **Output validated against real Talend production output** — 66/66 rows
  matched for tenant 36, date 2026-07-29, 100% match on every computed field.
  (Full detail in Section 6. This is the project's key credibility result.)
- ✅ Full ClaimbookDB structure mapped (Section 4).
- ✅ **Multi-tenant macro built and tested** (`get_tenant_schemas`) —
  resolves active tenants from `mtdm.mtdm_tenant_tb` via the `is_tenant`
  boolean, with three safety guards. Correctly excludes all three
  deliberately-broken negative-test rows (inactive tenant, NULL schema name,
  schema-listed-but-missing).
- ✅ **`is_tenant` semantics fully confirmed against the real DB** (2026-08-10,
  Section 15). `is_tenant = true` cleanly identifies active real tenants with
  no overlap against group/holding entities — the filter needs no changes.
  Active tenant count is **483**, not 576 (Section 15 explains the
  discrepancy). The `status` column is confirmed entirely blank across all
  821 registry rows — dead weight, not a usable secondary filter.
- ✅ **Model updated with all real target-table fields** — `tenant_id`,
  `functionality` (`'PREAUTH'`), `start_date`, `end_date`, and the
  `claimbook_submission_time` → `claim_submission_time` rename.
- ✅ **Cross-database Python loader** (`load_manual_report.py`) — moves
  staged rows `claimbook` → `cb_reports` idempotently, transactionally,
  with a `--dry-run` mode. Idempotency re-confirmed 2026-08-10 via real
  `manual_report_id` reuse on a delete+insert re-run (Section 15).
- ✅ **Multi-tenant pipeline proven end-to-end**: 5 tenants × 3 rows = 15
  rows staged and loaded. Idempotency confirmed (re-running twice stays at
  15, not 30). Date separation confirmed (distractor rows on other dates
  don't leak). Explicit `tenant_ids` override and inverted
  `tenant_active_flag` both behave correctly.
- ✅ **Re-proven on my real machine**, not just in a sandbox: `dbt debug` →
  `dbt run` → loader dry-run → loader real-run → verification query →
  re-run for idempotency → count confirmed unchanged.
- ✅ **Persistent `airflow scheduler` daemon fully proven** (2026-08-10,
  Section 15) — not just the one-shot `airflow dags test`. Confirmed
  running continuously as a background process, auto-discovering and
  executing a newly-unpaused DAG entirely unattended, surviving a live DAG
  file replacement without restart, and correctly executing all 4 real
  tasks of the restored multi-tenant DAG including the actual
  cross-database write — verified via the loader's own row-level log
  (`deleted 15 row(s)` → `inserted 15 row(s)` → `COMMITTED`), not just an
  Airflow "success" state.
- ✅ **Parallel-run validation done for 5 real tenants beyond tenant 36**
  (2, 30, 42, 44, 52) — see Section 15 for the full walkthrough. Row-count
  mismatches were investigated down to individual claim IDs. One small
  residual (2 claims, tenant 30) remains a genuine open KT question — see
  Section 11.
  ⚠️ **CORRECTED 2026-08-11 (Section 16): the "duplicate inserts from
  repeated job runs, no dedup logic" root cause below is WRONG and was
  retracted by the KT contact, then disproven directly against real data.**
  Checked all 222 duplicate `(tenant_id, preauth_claim_id)` groups
  production-wide for 2026-07-29 (96 tenants) — **zero are byte-identical.**
  Every one is a genuine distinct completion event (SUBMISSION+QUERY pair,
  or multiple real resubmissions/revisions within SUBMISSION) that happens
  to share a claim_id/date. Verified against raw `oltp_preauth_email` on the
  most extreme case (6 events/day on one claim) — our join correctly
  captures exactly the right 6 rows out of 18 total history rows, no under-
  or over-counting. **"UNION naturally deduplicates, this is an improvement
  over Talend" is also wrong** — none of these rows were ever byte-identical,
  so a full-row UNION was never eliminating anything. Full trail: Section 16.
- ✅ **Parallel-run validation extended to 100 real tenants** (2026-08-11,
  Section 16) — 83/100 match exactly. All 17 mismatches are the same
  one-directional pattern (`missing_from_converted` only, never `extra`) —
  see the residual-discrepancy explanation below, now understood, not an
  open mystery.
- ✅ **Residual discrepancy (originally "tenant 30, 2 claims", scaled to 17
  of 100 tenants at n=100) — root cause found, 2026-08-11 (Section 16).**
  `oltp_preauth_status.manual_upload_completed_time` is a mutable
  `character varying` column, not an immutable timestamp — proven directly:
  claim 128364's row `756639` has `transaction_time` = 2026-07-29 but
  `manual_upload_completed_time` = 2026-08-03, five days *after* its own
  row's creation, which is only possible if the field was overwritten after
  the fact. Talend's report is a same-day, point-in-time snapshot; a
  retroactive query days later sees the field's *current* value, which may
  have since moved. Structurally this mainly affects historical/retroactive
  comparison, not live daily runs (Airflow queries `today` same-day, same
  as Talend does — the field has had no time to be overwritten yet). Not
  fully proven with a literal historical snapshot of the exact original
  value, but the mutability itself is proven, and it fully accounts for the
  scattered (no fixed offset, sometimes earlier) pattern seen across all 17
  cases. **KT question closed 2026-08-11** — resolved from our own data,
  no KT input needed.
- ✅ **WSL networking behaviour fully resolved** (2026-08-10, Section 15).
  WSL2 confirmed (not WSL1), mirrored networking confirmed OFF
  (`.wslconfig` doesn't exist), and `localhost` from WSL definitively does
  **not** reach Windows-hosted Postgres (`Connection refused`, tested
  directly). **The gateway-IP workaround (`CLAIMBOOK_HOST=172.29.32.1`) is
  required and load-bearing, not optional legacy caution — do not remove
  it.** Section 9 conflict #2 is closed.

## What is NOT done / still blocked

- ❌ **No real write access to a dev/staging ClaimbookDB.** Still the #1
  blocker. All writes so far have gone to local sandboxes only.
- ❌ Only **1 of ~100+** Talend jobs converted.
- ✅ Scale testing complete at all 483 active tenants (2026-08-11, Section
  16 Part H) — 461/483 match exactly (95.4%), residual pattern fully
  understood and one-directional throughout. Coverage-validation phase
  closed.
- ✅ **Residual discrepancy — root cause found** (2026-08-11, Section 16),
  no longer an open KT question blocking anything, though the KT reply is
  still pending for independent confirmation. See Section 1 and Section 16.
- ❌ **Two real bugs found and fixed on 2026-08-10, worth remembering their
  root causes** (full detail Section 15):
  - `profiles.yml` had drifted to a hardcoded `host: localhost` instead of
    `env_var('CLAIMBOOK_HOST', 'localhost')`, silently breaking every dbt
    invocation from WSL regardless of the correctly-set `CLAIMBOOK_HOST` env
    var. Fixed — file now fully `env_var()`-driven, matching the safer
    version already tracked in git.
  - The live DAG file on disk (`preauth_manual_upload_daily_dag.py`) had
    regressed to a stale **2-task** version (`dbt_run`, `dbt_test` only, no
    loader task, no explicit `>>` dependency chain, referencing a model
    name — `preauth_manual_upload_daily` — that had since been renamed to
    `manual_report_staged`). This directly contradicted the "4-task DAG
    proven end-to-end" claim from the 2026-08-07 session — that claim
    described a version of the file that was no longer what was actually on
    disk. Restored the correct 4-task version from a git-backed copy, with
    paths corrected for the real machine and schedule kept at `0 6 * * *`
    (06:00 daily) per deliberate choice, not `0 2 * * *` as the backup had.
  - **Root-cause lesson from both:** "success" state (whether dbt's exit
    code or Airflow's task state) does not by itself prove the pipeline did
    real work — a `dbt run` that matches zero models, or a task that never
    reaches a live database, can both report success while doing nothing.
    Always verify actual data movement directly (row counts, log detail,
    identity-column behavior) before trusting a green status, especially
    after any environment or config change.
- ⚠️ **Security incident, resolved**: while diffing two backup folders via
  temporarily-public GitHub repos (2026-08-10), one (`ETL-1`) was found to
  contain a plaintext Postgres sandbox password in `profiles.yml`. Password
  was rotated immediately and both repos re-privatized. No real
  ClaimbookDB/cb_reports credentials were exposed — sandbox only. Worth
  remembering: never push `profiles.yml` (or any file with real credentials)
  to a repo, even temporarily, even private-then-public-then-private.
- ✅ **Model materialization vs. documentation mismatch — fixed 2026-08-12**
  (was flagged in Section 15, fixed same session it was picked up as a task,
  but this bullet itself was never updated until now). Docstring previously
  claimed `incremental_strategy = 'delete+insert' with unique_key` — that
  logic was never actually implemented here; it was mis-describing
  `load_manual_report.py`'s own delete+insert against the real `cb_reports`
  target, one layer down. Docstring corrected to describe what the model
  actually does (full-replace every run, by design, since staging is
  transient) and to correctly point at where real idempotency lives. Zero
  behavior change — `config()` itself was never touched, only the comment.

- ✅ **Row-count parity validated for multi-event claims** (2026-08-12,
  Section 17 Part A) — 211/222 (95%) of known multi-event claims (from the
  222-group sample, Section 16 Part C) match exactly on row count using the
  real full-column model logic, not just claim-ID presence. All 11
  mismatches are the same one-directional pattern as the tenant-level
  results (short by exactly 1 event) — same root cause, same direction,
  confirms the theory rather than requiring a new one.
- ✅ **Full row-count test against yesterday's date, all 483 tenants**
  (2026-08-12, Section 17 Part C) — 465/483 (96.3%) exact match using
  `run_date = today - 1` (now the standing convention, computed fresh each
  run, not a fixed historical date). 16 mismatches fit the known
  retroactive-lag pattern (down from 22 at a 13-day-stale date — confirms
  shorter lag → fewer mismatches, as predicted). **2 mismatches are a
  genuinely new, different pattern** — see below.
- 🚩 **Possible live production gap found, unrelated to this migration**
  (2026-08-12, Section 17 Part C): `dhoot_hospital` (811) and
  `orchid_hospital` (818) are the first cases all session where the
  converted query found MORE rows than Talend, not fewer. `dhoot_hospital`
  has 279 real status records going back to 2026-06-22 (7 weeks) with
  **zero Talend reports ever** — looks like a live hospital that's been
  silently missing from reporting the whole time, independent of anything
  to do with this migration. Flagged to the KT contact separately.
  `orchid_hospital`'s history only starts 2026-08-06, so it's more likely
  just a new-tenant lag case, less urgent.
- ✅ **Real dbt tests added — `dbt_test_staging` had been vacuous the whole
  time** (2026-08-12, Section 17 Part D): discovered `manual_report_staged`
  had zero tests defined anywhere (only `sources.yml` existed, no
  `schema.yml`). Every prior "successful" `dbt_test_staging` task, in every
  DAG run treated as proven so far, passed only because `dbt test` had
  nothing to check — not because anything was actually verified. Added
  `models/reports/schema.yml` (not_null + accepted_values on key columns,
  deliberately **no** uniqueness test on `preauth_claim_id` — would
  contradict the proven multi-event finding) plus a singular test checking
  the date-window filter logic actually works. Verified genuinely passing
  (9/9), not vacuous.
- ✅ **Row order fixed and verified against real production data**
  (2026-08-12, Section 17 Part F/G): the model had **no explicit
  `ORDER BY` at all** before this — output order was whatever Postgres's
  query plan happened to produce, not a guarantee. Confirmed against real
  Talend output (tenant 30) that Talend orders by `preauth_claim_id`
  ascending within each tenant, not chronologically. Added
  `ORDER BY tenant_id, preauth_claim_id` to the model. **Proven against
  real production data** (tenant 36/dmh, 2026-08-11): 72 rows on both
  sides, exact sequence match position-for-position, including all 5
  duplicate-claim pairs landing in identical spots. One tenant proven in
  full; the ordering mechanism is deterministic and identical for every
  tenant, so this is good evidence (not yet a full-scale proof) that it
  holds everywhere.
- ✅ **CLAIMS job identified as the next conversion target** (2026-08-12):
  `cb_report.manual_report` has exactly 2 `functionality` values, not the
  4 an earlier session's comment guessed — `PREAUTH` (2.87M rows, already
  converted) and `CLAIMS` (313,736 rows, active through 2026-08-10).
  Request sent to the KT contact for the CLAIMS-side job's query, same way
  the PREAUTH query was originally obtained — no reply yet.
- ⚠️ **Local Airflow scheduler is unreliable for triggering fresh runs**
  (2026-08-12): twice this session (once during a demo, once during an
  end-to-end mechanical test) a manually triggered DAG run got stuck
  indefinitely in `queued`, with the scheduler's own log showing zero
  scheduling activity for it — not an import error, not a blocked/stuck
  prior run (checked and ruled out both). Matches Airflow's own UI warning
  about SQLite + SequentialExecutor concurrency under a webserver +
  scheduler + CLI all hitting one file at once. Workaround used
  successfully: run the DAG's 4 steps by hand, in order, replicating the
  exact commands each `BashOperator` runs (see Section 17 Part E) — proven
  to work, and arguably safer anyway since it avoids relying on a component
  now known to be flaky in this local setup. Not yet root-caused or fixed
  properly (would likely mean LocalExecutor + a real Postgres metadata DB
  instead of SQLite) — not blocking anything since the manual workaround
  works, but worth fixing before relying on Airflow's own scheduling for
  anything that matters.
- ✅ **Live demo done successfully** (2026-08-12) — used an existing proven
  run (2026-08-10, 12:12:31) rather than fighting the scheduler issue above
  live; all 4 tasks shown green with real logs, DAG description visibly
  reads "replaces Talend."
- ✅ **Plain-language explainer document created** (2026-08-12) —
  `ETL_Migration_Explainer.md`, a shareable non-technical writeup of the
  whole project: why it's happening, what was built, where each test ran,
  and the results in plain terms. Given to Keertan as a deliverable, not
  stored in this context file.
- ✅ **First real `dbt run` against real production-sourced data** (2026-08-17,
  Section 18) — via a new bridge tool (`copy_claim_to_sandbox.py`) that
  copies one real claim's current Claimbook data plus its real Talend
  snapshot into the sandbox, read-only on the prod side, so an actual
  `dbt run` (not `full_byte_comparison.py`'s script-derived equivalent) can
  execute against it. Two real claims proven this way (255841, 255920,
  tenant 36, 2026-08-11): real dbt output matches current Claimbook exactly
  on every field; Talend's stored snapshot diverges on the same
  mutability-affected fields already characterized in Section 16/17
  (`al_number`, `insurance_policy_number`, `first_name`).
- ✅ **`manual_upload_completed_actual_tat` null-string quirk characterized**
  (2026-08-17, Section 18) — Talend stores "no value" for this column as
  the literal text string `'null'` (confirmed via `pg_typeof`), not a real
  SQL NULL; our pipeline correctly produces a real NULL. Affected 20/72
  rows (28%) for tenant 36, 2026-08-11. Cosmetic, not a correctness bug.
- ✅ **Mutability pattern confirmed to extend beyond
  `manual_upload_completed_time`** (2026-08-17, Section 18) — `al_number`,
  `insurance_policy_number`, and `first_name` are also editable after
  Talend's original capture. All 8 non-null-quirk mismatches from
  `full_byte_comparison.py`'s first real production run (tenant 36,
  2026-08-11) were individually traced to raw source and found to match
  current Claimbook exactly, with Talend holding the stale value. One
  instance of live mutation was directly observed mid-session (claim
  255920's `al_number` changed between two queries minutes apart).
- ⚠️ **Two-terminal working pattern adopted as standing practice**
  (2026-08-17, Section 18) — after `dbt run --target sandbox` was
  accidentally executed twice against real production (caught both times
  only because prod itself enforces read-only at the connection level, not
  by design safeguard). One terminal is now dedicated to prod only
  (read-only queries), a second to sandbox only (all `dbt run`
  invocations). Always echo host/dbname vars before running anything in
  either.

## Immediate next steps (in order)

1. **Waiting on 2 external replies**, nothing to do until either lands:
   (a) KT contact re: the CLAIMS job query, (b) KT contact re: the
   `dhoot_hospital`/`orchid_hospital` reporting-gap flag. A comprehensive
   results summary (yesterday's 465/483 count results + the row-order
   proof) was also sent to the KT contact — no reply needed on that one,
   informational.
2. Get dev/staging write access (meeting prep doc exists for this) — still
   the #1 real blocker for anything beyond read-only validation and manual
   sandbox testing. The Section 18 real-`dbt run` proof (via the sandbox
   bridge) is new evidence to bring to that conversation, in addition to
   the existing byte-comparison and scale-test results.
3. Once the CLAIMS job query arrives: begin converting it using the now
   twice-proven pattern (macro + model + loader + DAG + real tests +
   explicit row order).
4. Consider properly fixing the local Airflow scheduler reliability issue
   (LocalExecutor + Postgres metadata DB instead of SQLite) — not blocking,
   but the manual-steps workaround shouldn't be the permanent answer.
5. Extend the row-order proof beyond the single tenant it's currently
   confirmed on, if it becomes relevant (e.g. before final cutover
   sign-off) — currently proven correct on real data for one tenant
   (dmh/36), not yet checked at scale like the count-based tests were.
6. Extend `copy_claim_to_sandbox.py` usage to more real claims if further
   real-`dbt run` proof points are useful, or use it to reproduce/root-cause
   any future mismatch found via `full_byte_comparison.py` — it now exists
   as a reusable tool, not a one-off.

---

# SECTION 2 — BUSINESS & TOOLING BACKGROUND

- **ABI Health Pvt Ltd**, Bangalore — insurance claims intermediary between
  hospitals and TPAs.
- **Data residency: all data must stay within India.** Hard requirement.
  Rules out most managed/cloud tiers; everything is self-hosted.
- Current ETL: **Talend Open Studio 8.0.1** (build `20211109_1610`).
- **Talend's free/open-source edition was discontinued January 2024** after
  Qlik's acquisition — no more patches or updates. This is the entire reason
  for the migration.
- Company originally proposed **Airbyte + Airflow + dbt**. Investigation
  showed **Airbyte is not needed** — everything the pipeline touches lives
  inside ClaimbookDB already. Final stack: **dbt Core + Apache Airflow**.
  (Revisit only if an external upstream source is confirmed — Section 9.)
- Licensing (all free to self-host): Airflow = Apache 2.0; dbt Core =
  Apache 2.0; Airbyte = mostly MIT with an ELv2 core (only restricts
  reselling as a hosted service — irrelevant to us, but moot since dropped).
- Alternatives considered and rejected: **Apache Hop** (kept as a mental
  fallback only if `tJava` logic proved too complex for SQL — it didn't);
  **Pentaho CE** (rejected — Hitachi Vantara killed free production use in
  2024, i.e. exactly the trap we're escaping).
- **Everything currently runs manually.** ~20 jobs triggered by hand daily,
  ~90 more at month-end. Someone hand-edits a date in a query and clicks run,
  every single day. **This is the core operational pain Airflow solves** and
  is the strongest argument for the migration.
- **The migration output is consumed downstream:** a separate backend team
  reads `cb_reports` to generate dashboards and reports for hospitals and
  TPAs. Breaking the output format breaks them — hence the emphasis on
  matching the real target table's structure and text-date formats exactly.

---

# SECTION 3 — ⚠️ CENTRAL ARCHITECTURAL FACT #1: ONE SCHEMA PER TENANT

**This is the most important technical fact in the project. Everything else
depends on it.**

ClaimbookDB does **NOT** use a shared table filtered by a `tenant_id` column.
It uses **one separate Postgres schema per tenant (hospital)**, each
containing an identical set of ~280 tables.

- Confirmed by comparing full column structure of `dmh` vs `apollo_nashik` —
  exact match, column for column. **Schema structure therefore only needs
  documenting once**, using any one tenant as representative.
- Example schema names: `dmh`, `apollo_nashik`, `agarwal_vashi`,
  `srikara_ecil`, `winskill_bhagwati`, ... (named after hospitals).
- `dmh` = **Deenanath Mangeshkar Hospital and Research Center**, tenant_id 36.
  Note: `dmh` is NOT a generic/shared schema — it is one real hospital. Early
  in the project this was mistakenly assumed to be shared.
- **This explains Talend's `tLoop_1` component** (seen in a screenshot showing
  "72 execs finished"): it is not looping over rows — it is **switching schema
  context per tenant** and re-running the same query template once per
  hospital. Visible directly in the Talend source as `"+context.schema+"`
  string substitution (Section 10).
- **Consequence:** a single dbt model with a hardcoded `FROM dmh.x` covers
  exactly one hospital. Replicating Talend properly requires a dbt macro that
  resolves the active tenant list and builds a `UNION ALL` across every tenant
  schema. **This has been built and tested** (Section 5) but not yet
  scale-tested beyond 5 tenants.

### Tenant count discrepancy (unresolved)
Stated figure is **576 tenants**, but real `tenant_id` values observed go up
to at least **809**. Either IDs are non-contiguous (deactivated/historical
tenants) or the 576 figure is stale. Open KT question.

### Active-tenant resolution
Use the **`is_tenant` boolean column** in `mtdm.mtdm_tenant_tb`
(`true` = active). An earlier attempt used a `tenant_status: "ACTIVE"` string
var in `dbt_project.yml` — this was **wrong and caused a zero-tenant bug**;
that var was removed. **Do not reintroduce it** (Section 8, #10).

---

# SECTION 4 — CLAIMBOOKDB STRUCTURE (fully mapped)

## Server-level
The Postgres server hosts **26 databases**. Only 2 are understood:

| Database | Role |
|---|---|
| `claimbook` | Source/transactional. Contains 576+ per-tenant schemas + `mtdm` shared master data. |
| `cb_reports` | Reporting/output. Contains one shared schema `cb_report` (70 tables) that Talend writes into. |

**The other 24 databases are unexplored** — open KT question.

⚠️ **`cb_report` is a SCHEMA inside the `cb_reports` DATABASE.** These are
two different things with confusingly similar names. Postgres cannot query
across databases in one session — you must connect to `cb_reports`
specifically to see `cb_report`. This caused a real "zero rows returned"
confusion (Section 8, #15) *and* is the root of the cross-database write
constraint (Section 5).

## `claimbook` database

### Per-tenant schemas (e.g. `dmh`), ~280 tables / 4,305 columns each

- **165 `oltp_*`** — raw transactional data. Key tables:
  - `oltp_pre_authorisation` (102 cols) — core preauth record
  - `oltp_claims` (57 cols) — core claims record
  - `oltp_preauth_status` (50 cols), `oltp_claims_status_tb` (50 cols)
  - `oltp_preauth_email` (61 cols), `oltp_claim_email` (60 cols)
  - `oltp_tpa_organization_tb` (140 cols — largest single table)
  - `oltp_rpa_responds_tb` (52 cols) — the RPA/bot layer, see below
- **26 `vw_*`** — Postgres **views**, pre-joined/aggregated
  (`vw_preauth`, `vw_claim`, `vw_outstanding_collection`,
  `vw_preauth_tat_report`, ...). ⚠️ **Open question worth chasing: do any
  existing Talend jobs read from these views instead of raw tables?** If so,
  those jobs are far simpler to convert than assumed.
- **12 `master_*`** — reference/lookup data
- **77 others** — disputes, international cases, KYC, patient collections,
  quotations, SMS/notification logs

### `mtdm` schema (85 tables, 637 cols) — genuinely shared, non-tenant master data

Confirmed global (appears once, not duplicated per tenant).

**`mtdm.mtdm_tenant_tb` — the tenant registry, and the key to the whole
multi-tenant build.** Discovered by parsing an `information_schema` CSV
export directly:

| column | type | role |
|---|---|---|
| `tenant_id` | integer | goes into every `cb_report` output row |
| `sche_name` | varchar | **the physical Postgres schema name for that tenant** |
| `is_tenant` | boolean | **the active flag — `true` = active, `false` = inactive** |
| `status` | varchar | a separate status string. **Not** the active flag. Available as an optional secondary filter if ever needed. |
| `bidb_sche_name` | varchar | a *second* schema-name-like column, purpose unconfirmed — open question |
| `is_group_tenant`, `group_tenant_id` | — | adjacent columns that raise the possibility `is_tenant` also encodes real-tenant-vs-group-entity. See Section 9. |

**Why `sche_name` matters architecturally:** an earlier plan assumed the
multi-tenant macro would have to scan `information_schema` and *infer* which
schemas were tenants. It doesn't. `mtdm_tenant_tb` is an authoritative
registry, and it's strictly better — `information_schema` gives you schema
names with **no `tenant_id` attached**, and `tenant_id` is a required output
column.

Also: `mtdm.mtdm_tpa_organization_tb` — shared TPA master, joined into most
preauth/claim queries for the TPA name.

## `cb_reports` database → `cb_report` schema (70 tables, 982 cols)

Strong, consistent conventions discovered by analysing all 70 tables:

- **69/70 have `tenant_id`** (only exception: `master_category`)
- **66/70 have `start_date`/`end_date`** — confirmed via real data to be the
  *report run's date window*, not a per-row business date
- **Only 4/70 have a `functionality` discriminator column**, meaning they're
  shared across multiple job types: `inbox_management`, `manual_report`,
  `ops_alert`, `tpa_proxy`. The other 66 are single-purpose.
  → **Implication: `manual_report` (the job converted first) is one of the
  more complex output tables, not a typical one. The remaining ~99 jobs
  should mostly be simpler.**
- `tenant_master` table maps `tenant_id` → `tenant_name` → `tenant_schema`.
  (Note: this is the *reporting-side* equivalent of `mtdm_tenant_tb`. The
  macro uses `mtdm_tenant_tb` because it lives in `claimbook`, the same
  database dbt connects to at compile time — `tenant_master` is in the other
  database and therefore unreachable from a dbt `run_query`.)

### `cb_report.manual_report` — the one target table converted so far (27 columns)

```
manual_report_id                   integer, identity PK  -- never written by us
preauth_claim_id                   integer
mrn                                varchar
first_name                         varchar
tpa_name                           varchar
insurance_policy_number            varchar
tpa_member_id                      varchar
al_number                          varchar
request_type                       varchar
workflow_state                     varchar
claim_submission_time              text   -- 'DD/MM/YYYY HH24:MI:SS' string, NOT a timestamp
automation_received_time           text   -- same text format
automation_tat                     varchar
automation_status                  varchar
manual_upload_completed_time       text   -- same text format
ops_user_name                      varchar
upload_completed_source            varchar
manual_upload_completed_actual_tat text
automation_type                    text   -- 'SUBMISSION' or 'QUERY'
tenant_id                          integer
start_date                         date
end_date                           date
proxy_remarks                      varchar
cl_number                          varchar   -- claims-side jobs only, NULL here
claims_id                          integer   -- claims-side jobs only, NULL here
functionality                      varchar   -- 'PREAUTH' for this job
automation_failure_reason          varchar
```

⚠️ **Critical gotcha:** several columns that look like timestamps are
actually `text` storing `'DD/MM/YYYY HH24:MI:SS'` strings, because that's how
Talend wrote them via `to_char(...)`. A direct `column::date` cast can
misbehave (Section 8, #16). For report-window filtering, use the real `date`
columns `start_date`/`end_date` instead — simpler and correct. If you must
parse the text columns, use
`to_timestamp(col, 'DD/MM/YYYY HH24:MI:SS')::date`.

⚠️ **Any model writing to these tables must write dates back out in the same
text format**, or the downstream backend team's dashboards break.

## The RPA / automation bot layer

`oltp_rpa_responds_tb`, plus real data in `manual_report` (Selenium/
ChromeDriver error logs appearing in `proxy_remarks` and
`automation_failure_reason`), reveal that TPA portal submissions are **first
attempted automatically by a bot**, with manual upload as the human fallback
when automation fails. This is materially bigger business context than the
raw query alone suggested — worth remembering when interpreting any
automation-related column.

---

# SECTION 5 — ⚠️ CENTRAL ARCHITECTURAL FACT #2: dbt CANNOT WRITE CROSS-DATABASE

**Discovered the hard way during the multi-tenant build — hit as a real
`dbt run` failure, not theorised.**

dbt reads source data from the `claimbook` database, but the target table
(`cb_report.manual_report`) lives in the physically separate `cb_reports`
database. **Postgres cannot write across databases in a single connection,
and dbt's Postgres adapter is one connection per target.** Talend gets away
with this because it has independent `tDBConnection` components for read and
write; dbt has no equivalent.

This reshaped the pipeline from "one dbt model" into **two steps**:

```
claimbook.<tenant_schema>.oltp_*   --dbt-->   claimbook.cb_staging.manual_report_staged
                                                        |
                                              load_manual_report.py  (psycopg2)
                                                        |
                                                        v
                                    cb_reports.cb_report.manual_report
```

**Secondary finding from the same investigation:** dbt's `delete+insert`
incremental strategy also fails when the target has an identity PK the model
doesn't produce — dbt derives the insert column list from the *target* table,
so it tries to select `manual_report_id` from a temp table that has no such
column. Staging as a plain table + a separate loader sidesteps this entirely.

**Alternative not yet pursued:** `postgres_fdw` or `dblink` would let one
connection reach both databases, removing the need for the loader script —
but needs a Postgres extension installed server-side, i.e. a DBA/access
request. Worth raising as a future simplification (Section 9, KT item).

**Same constraint applies to validation queries:** you cannot `JOIN` the
converted output against the real Talend output in one query. Run them as two
separate queries in two connections and compare side by side (Section 6).

---

# SECTION 6 — THE VALIDATION RESULT (the project's key credibility asset)

Compared the converted query's output against **Talend's real production
output** already sitting in `cb_report.manual_report`.

- **Tenant:** 36 = Deenanath Mangeshkar Hospital (schema `dmh`)
- **Date:** 2026-07-29
- **Result: 66/66 rows matched.** Same claims, same SUBMISSION/QUERY branch
  assignment, zero missing rows, zero extra rows.
- **100% match on every computed/logic column:** `workflow_state`,
  `automation_received_time`, `automation_tat`, `automation_status`,
  `automation_failure_reason`, `manual_upload_completed_time`,
  `ops_user_name`, `upload_completed_source`,
  `manual_upload_completed_actual_tat`, `proxy_remarks`, `mrn`, `tpa_name`,
  `request_type`, `claim_submission_time`.
- **Only mismatches: 4 raw identity fields** — `first_name`, `al_number`,
  `insurance_policy_number`, `tpa_member_id` (49/66 rows matched on
  *everything*; the rest differed only in these). Cause: **live source data
  was corrected/updated between Talend's original run and our query** — e.g.
  `'-'` placeholder replaced by a real policy number, `'TANAJI KADAM'`
  becoming `'Mr. KADAM TANAJI BALASAHEB'`. **Not a conversion bug.**

**Conclusion: the SQL conversion logic is proven faithful.**

**Methodology note for future comparisons:** pick a date far enough in the
past that source records have settled, or accept identity-field drift and
validate on computed fields only.

## ⚠️ Be precise about what this proves

This validation ran the **converted SQL directly against source tables**
(connected to `claimbook`) and compared against the real Talend output
(connected to `cb_reports`) — as **two separate queries side by side**, not
joined (cross-database join isn't possible, Section 5).

**No `dbt run` had materialised this model as a table in the real database.**
dbt has only ever run against sandboxes. If asked directly in a meeting
whether dbt is already writing to production: **it is not.** The honest
framing is *"the transform logic is proven; wiring dbt into this environment
is the next step, blocked on write access."* That's a stronger position than
it sounds — logic-proven-but-not-yet-deployed is exactly where a good POC
should be before requesting access.

If demoing live, run the two queries in two pgAdmin tabs and lead with the
count. Walk into the identity-field drift yourself with the explanation
ready — a suspiciously perfect demo invites more doubt than a well-explained
imperfection.

---

# SECTION 7 — WHAT HAS BEEN BUILT (the actual assets, with full code)

## The converted job

**"Preauth manual upload daily"** — reads `oltp_pre_authorisation` joined to
patient, person, insurance policy, TPA org, workflow state/request type. Two
branches UNIONed: **SUBMISSION** (status-driven, from `oltp_preauth_status`)
and **QUERY** (email-driven, from `oltp_preauth_email`). Filtered by
`manual_upload_completed_time::date = <date>`. In Talend, that date is
hand-edited daily. In dbt it became `{{ var('run_date') }}`, injected
automatically by Airflow via `{{ ds }}`.

Transform logic for this job is confirmed **pure SQL** — no `tJava` custom
logic needed converting. (Needs re-confirming job-by-job for the other ~99.)

## 7a. `macros/get_tenant_schemas.sql` — replaces Talend's `tLoop_1`

```sql
{#-
=============================================================================
get_tenant_schemas()

Resolves which tenant schemas to loop over, replacing Talend's tLoop_1.

WHERE THE TENANT LIST COMES FROM:
  mtdm.mtdm_tenant_tb  -  the authoritative tenant registry in the claimbook
  database. Key columns:
      tenant_id   integer  -> written into cb_report.manual_report.tenant_id
      sche_name   varchar  -> the PHYSICAL Postgres schema for that tenant
      is_tenant   boolean  -> TRUE = active, FALSE = inactive  (the active flag)
      status      varchar  -> a separate status string; NOT used as the
                              active flag (see tenant_status var below)

THREE SAFETY GUARDS (each one matters in a 576+ tenant database):
  1. is_tenant filter     - skips deactivated tenants
  2. schema-exists join   - skips registry rows whose sche_name has no actual
                            schema (stale/planned tenants)
  3. table-exists check   - skips schemas missing oltp_pre_authorisation
                            (partially provisioned tenants)

VARS:
  tenant_limit         - cap the number of tenants (staged rollout: 5 now,
                         then 25, then all). null/0 = no limit.
  tenant_active_flag   - value of mtdm_tenant_tb.is_tenant treated as active.
                         Default true. Set to false to invert (useful for
                         testing the filter itself).
  tenant_status        - OPTIONAL extra filter on mtdm_tenant_tb.status.
                         Default none = not applied. Only set this if you
                         confirm status carries independent meaning.
  tenant_ids           - optional explicit list, e.g. '{36,112}'. Overrides
                         the active-flag filter and picks exactly those.

RETURNS: a list of dicts -> [{'tenant_id': 36, 'schema': 'dmh'}, ...]
=============================================================================
-#}

{% macro get_tenant_schemas() %}

    {%- set tenant_limit       = var('tenant_limit', 5) -%}
    {%- set tenant_active_flag = var('tenant_active_flag', true) -%}
    {%- set tenant_status      = var('tenant_status', none) -%}
    {%- set tenant_ids         = var('tenant_ids', none) -%}

    {%- set tenant_sql -%}
        select
            t.tenant_id,
            t.sche_name
        from mtdm.mtdm_tenant_tb t
        -- guard 2: the schema must actually exist
        join information_schema.schemata s
          on s.schema_name = t.sche_name
        where t.sche_name is not null
          and btrim(t.sche_name) <> ''
          {%- if tenant_ids %}
          -- explicit tenant list overrides the active-flag filter entirely
          and t.tenant_id in ({{ tenant_ids | join(',') }})
          {%- else %}
          -- is_tenant is the active flag: true = active, false = inactive
          and t.is_tenant is {{ 'true' if tenant_active_flag else 'false' }}
            {%- if tenant_status %}
          and upper(coalesce(t.status, '')) = upper('{{ tenant_status }}')
            {%- endif %}
          {%- endif %}
          -- guard 3: the schema must contain the driving table
          and exists (
              select 1
              from information_schema.tables it
              where it.table_schema = t.sche_name
                and it.table_name   = 'oltp_pre_authorisation'
          )
        order by t.tenant_id
        {%- if not tenant_ids and tenant_limit %}
        limit {{ tenant_limit }}
        {%- endif %}
    {%- endset -%}

    {%- set tenants = [] -%}

    {%- if execute -%}
        {%- set results = run_query(tenant_sql) -%}
        {%- for row in results.rows -%}
            {%- do tenants.append({'tenant_id': row[0], 'schema': row[1]}) -%}
        {%- endfor -%}

        {%- if tenants | length == 0 -%}
            {{ exceptions.raise_compiler_error(
                "get_tenant_schemas() found NO tenants. Check that mtdm.mtdm_tenant_tb "
                ~ "is reachable and that var('tenant_active_flag') matches the real "
                ~ "is_tenant convention (currently: " ~ tenant_active_flag ~ ")."
            ) }}
        {%- endif -%}

        {{ log("get_tenant_schemas(): " ~ (tenants | length) ~ " tenant(s) -> "
               ~ (tenants | map(attribute='schema') | join(', ')), info=True) }}
    {%- endif -%}

    {{ return(tenants) }}

{% endmacro %}
```

⚠️ **Macros must live at `dbt_project/macros/`, not
`dbt_project/models/macros/`.** dbt's default `macro-paths` is `["macros"]`
at the project root; a macro under `models/` is simply never found
(Section 8, #28).

## 7b. `models/reports/manual_report_staged.sql` — the multi-tenant model

Writes to `claimbook.cb_staging.manual_report_staged`. One query block per
tenant, `UNION ALL`'d.

```sql
{{
    config(
        materialized = 'table',
        schema       = none,
        alias        = 'manual_report_staged'
    )
}}

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
```

## 7c. `load_manual_report.py` — the cross-database loader

Idempotent: deletes rows matching the staged data's
`(tenant_id, start_date, end_date, functionality)` keys, then inserts — all
in one transaction, rolls back on any failure. `manual_report_id` is
deliberately excluded (identity PK, generated on insert).

```python
#!/usr/bin/env python3
"""
Moves dbt-staged rows from claimbook -> cb_reports, because Postgres cannot
write across databases in a single connection.

    claimbook.cb_staging.manual_report_staged -> cb_reports.cb_report.manual_report

  !! WRITE OPERATION !! Point CBREPORTS_DBNAME at a sandbox until parallel-run
  validation is signed off. Use --dry-run first.

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
```

## 7d. `preauth_manual_upload_daily_dag.py` — the Airflow DAG (4 tasks)

`dbt_run_staging` → `dbt_test_staging` → `load_dry_run` → `load_to_cb_reports`.
Paths below are the **corrected, verified** ones for my machine.

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# --- paths: quote these in every bash_command; the real Windows path -------
# --- contains a space ("Keertan Kumar") which bash would otherwise split ---
DBT_PROJECT_DIR = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project"
DBT_PROFILES_DIR = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project"
LOADER_PATH = "/mnt/c/Users/Keertan Kumar/Desktop/claimbook_etl/dbt_project/load_manual_report.py"
PYTHON_BIN = "/home/keertan_kumar/airflow_venv/bin/python"
DBT_BIN = "/home/keertan_kumar/airflow_venv/bin/dbt"

TENANT_LIMIT = 5             # <-- raise deliberately as rollout proceeds
TENANT_ACTIVE_FLAG = "true"  # mtdm_tenant_tb.is_tenant: true = active tenant

default_args = {
    "owner": "keertan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="preauth_manual_upload_daily",
    description="Preauth manual upload daily report - multi-tenant (replaces Talend)",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="0 2 * * *",          # 02:00 daily
    catchup=False,
    max_active_runs=1,             # never let two runs write concurrently
    tags=["claimbook", "migration", "preauth"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f'cd "{DBT_PROJECT_DIR}" && '
            f'"{DBT_BIN}" run '
            f'--profiles-dir "{DBT_PROFILES_DIR}" '
            f'--select manual_report_staged '
            f"--vars \"{{run_date: '{{{{ ds }}}}', "
            f"tenant_limit: {TENANT_LIMIT}, "
            f"tenant_active_flag: {TENANT_ACTIVE_FLAG}}}\""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f'cd "{DBT_PROJECT_DIR}" && '
            f'"{DBT_BIN}" test '
            f'--profiles-dir "{DBT_PROFILES_DIR}" '
            f'--select manual_report_staged '
            f"--vars \"{{run_date: '{{{{ ds }}}}', "
            f"tenant_limit: {TENANT_LIMIT}, "
            f"tenant_active_flag: {TENANT_ACTIVE_FLAG}}}\""
        ),
    )

    load_dry_run = BashOperator(
        task_id="load_dry_run",
        bash_command=f'"{PYTHON_BIN}" "{LOADER_PATH}" --dry-run',
    )

    load_to_cb_reports = BashOperator(
        task_id="load_to_cb_reports",
        bash_command=f'"{PYTHON_BIN}" "{LOADER_PATH}"',
    )

    dbt_run >> dbt_test >> load_dry_run >> load_to_cb_reports
```

## 7e. `dbt_project.yml` — current real state

```yaml
name: 'claimbook_etl'
version: '1.0.0'
config-version: 2
profile: 'claimbook'
model-paths: ["models"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  claimbook_etl:
    reports:
      +materialized: table

# ---------------------------------------------------------------------------
# IMPORTANT: do NOT add a `tenant_status` var here. A var set here OVERRIDES
# the macro's own var(..., default) fallback - a stale tenant_status: "ACTIVE"
# line here caused a zero-tenant bug that took a generated-SQL dump to find.
# ---------------------------------------------------------------------------
vars:
  run_date: "2026-07-29"
  tenant_limit: 5
  tenant_active_flag: true
```

## 7f. Sandbox setup scripts

Two scripts, mirroring the real two-database split:

- **`01_setup_source_sandbox.sql`** (run against `claimbook_sandbox`) —
  creates `mtdm.mtdm_tenant_tb` with **8 rows**: 5 active tenants (36-`dmh`,
  112-`apollo_nashik`, 204-`fortis_bg`, 351-`manipal_wf`,
  487-`narayana_hrc`) plus **3 deliberately-broken negative-test rows**:
  `509` (`is_tenant = false`), `601` (`sche_name` NULL), `777` (`sche_name`
  points at a nonexistent schema). All three must be excluded by the macro.
  Also creates the 8 `oltp_*` tables per tenant schema with synthetic data:
  2 rows on the target date + 1 "distractor" row on a different date (proves
  the date filter actually filters). Creates `cb_staging` schema for dbt.
- **`02_setup_target_sandbox.sql`** (run against `cb_reports_sandbox`) —
  creates `cb_report.manual_report` with the real 27-column structure, plus
  1 pre-loaded row simulating existing Talend output, to prove the loader
  replaces rather than duplicates.

Both are safely re-runnable (drop + recreate).

## Full file inventory

| File | Purpose |
|---|---|
| `macros/get_tenant_schemas.sql` | Tenant resolution — replaces `tLoop_1` |
| `models/reports/manual_report_staged.sql` | Multi-tenant `UNION ALL` model |
| `load_manual_report.py` | Cross-DB loader, idempotent |
| `preauth_manual_upload_daily_dag.py` | Airflow DAG (4 tasks) |
| `dbt_project.yml`, `profiles.yml`, `sources.yml` | dbt scaffolding |
| `01_setup_source_sandbox.sql`, `02_setup_target_sandbox.sql` | Sandbox setup |
| `validation_talend_vs_dbt.sql` | Two-query side-by-side comparison for demos |
| `preauth_manual_upload_daily.sql` | ⚠️ The OLD single-tenant model — superseded by `manual_report_staged.sql`, should be deleted |
| `Talend_ETL_Takeover_Checklist.md` | Original KT/takeover checklist |
| `ClaimbookDB_Structural_Reference.md` | DB structure findings |
| `ClaimbookDB_ETL_Status_Update.md` | Plain-language doc for non-technical stakeholders |
| `Talend_Handover_Meeting_Prep.md` | Meeting prep — what to explain, what to ask |
| `README_MULTITENANT.md`, `HOW_TO_RUN.md` | Multi-tenant setup/run guides |
| `SETUP_AND_RUN.md`, `LOCAL_SANDBOX_SETUP_Windows.md` | Earlier setup guides |

---

# SECTION 8 — COMPLETE ERROR LOG (every issue hit, and its fix)

*Read this before debugging anything — many issues recur.*

## Talend / Java setup

1. Downloaded wrong Talend version (7.3.1 vs required 8.0.1) — caught by
   comparing folder name to the production Studio title bar.
2. "No Java virtual machine found" — Talend 8.0.1 (this build) needs **JDK 11
   specifically**, not 8/17/21.
3. Accidentally installed **Zulu 25** — Azul's page defaults to latest LTS.
   Had to explicitly select Java 11 in the version dropdown.
4. Correct JDK: **Zulu 11.90.19** at
   `C:\Program Files\Zulu\zulu-11\bin\javaw.exe`. Requires editing
   `TOS_DI-win-x86_64.ini` to add `-vm` + that path **before** `-vmargs`.
   (Talend launch itself was never fully confirmed — deprioritised once we
   pivoted to building the dbt/Airflow POC instead of running Talend.)

## dbt

5. `--vars` date error: *"Object of type date is not JSON serializable"* —
   YAML auto-converts unquoted dates to date objects. **Fix: quote the date
   inside the string** → `--vars "{run_date: '2026-07-29'}"`.
6. Jinja comment bug #1 — a `--` SQL comment placed *inside* a
   `{{ config(...) }}` block broke compilation. Jinja parses `{{ }}` contents
   as an expression; `--` comments don't work there. Fix: move it out, or use
   `{#- ... -#}`.
7. Jinja comment bug #2 — an explanatory SQL comment contained literal
   `{{ var('tenant_id') }}` as *illustrative text*. **Jinja renders `{{ }}`
   everywhere in the raw file, including inside `--` comments**, so it
   demanded a nonexistent var. Fix: reword, or wrap in
   `{% raw %} ... {% endraw %}`.
8. Trailing semicolon broke `dbt run` — dbt wraps model SQL in its own
   `CREATE TABLE AS (...)`; the `;` broke the wrapper. Remove it.
9. Schema name collision — model-level `schema='cb_report'` combined with the
   target's schema to produce `cb_reports_validate_cb_report`. **Fix: remove
   schema overrides from models and `dbt_project.yml`; let the target own
   it.** Do not undo this.
10. **Stale `tenant_status: "ACTIVE"` var in `dbt_project.yml` caused a
    zero-tenant bug.** After switching the macro to filter on `is_tenant`,
    every run returned zero tenants with no obvious error. Root cause: **a
    var set in `dbt_project.yml` always overrides a macro's `var(...,
    default)` fallback** — normal dbt behaviour, but an easy trap. Found by
    logging the macro's generated SQL and reading it. Removed; do not
    reintroduce.
    → **Rule: if a filter behaves as though a value you never passed is being
    applied, check `dbt_project.yml`'s `vars:` before debugging the macro.**
11. **dbt `delete+insert` incremental fails against a target with an identity
    PK the model doesn't produce** — dbt derives the insert column list from
    the target table, then tries to select `manual_report_id` from a temp
    table that has no such column. Sidestepped by staging as a plain table +
    separate loader.

## Postgres / pgAdmin

12. `CREATE DATABASE cannot run inside a transaction block` — pgAdmin wraps
    multi-statement executions. Fix: run each statement separately, one
    Execute at a time. Check the Auto-Commit toggle if it persists.
13. Ran the sandbox setup script twice → `relation already exists`. Confirmed
    harmless at the time; **now fixed properly** — the current sandbox
    scripts start with `DROP SCHEMA IF EXISTS ... CASCADE` and are safely
    re-runnable.
14. `permission denied for schema dmh` on `dbt run` — the sandbox setup
    script omitted grants. Fix:
    `GRANT USAGE, CREATE ON SCHEMA dmh, mtdm, cb_report, cb_report_validate TO etl_user;`
    plus `GRANT ALL ON ALL TABLES IN SCHEMA dmh TO etl_user;` (and `mtdm`).
15. **Local Postgres runs on port 5433, not 5432** (5432 was already taken).
    Diagnosed via pgAdmin → server Properties → Connection tab. Note
    `netstat | findstr 5432` gave a red herring (matched an unrelated port
    `54328` by substring).
16. `information_schema.tables` returned **zero rows** for `cb_report` —
    because it lives in the separate `cb_reports` *database*, not in
    `claimbook`. Postgres can't see across databases. Fix: connect to
    `cb_reports` first. (This is the same root cause as Section 5.)
17. `date/time field value out of range: "19/06/2020 12:31:59"` — the column
    `manual_upload_completed_time` in `manual_report` is stored as **text**
    in `DD/MM/YYYY HH24:MI:SS` format, not a timestamp, so `::date` used
    month-first parsing and choked on day > 12. Fix:
    `to_timestamp(col, 'DD/MM/YYYY HH24:MI:SS')::date`.
    ⚠️ **Generalises to other jobs:** tables storing dates as pre-formatted
    text need explicit-format parsing, and any dbt model writing to them must
    write dates back out in the same text format or downstream consumers break.
    ⚠️ **But for report-window filtering, prefer the real `date` columns
    `start_date`/`end_date`** — simpler and avoids the parsing entirely.
18. Misleading `tenant_id` pattern — a 20-row sample of `manual_report` showed
    every row as `tenant_id = 36`, suggesting only one tenant that day. It was
    an artifact of insertion order (Talend inserts tenant-by-tenant, so early
    rows cluster). A `GROUP BY tenant_id` showed ~190 distinct tenants.
    **Lesson: check aggregates before concluding anything from a small sample.**
19. **Read-only replica** — `dbt run` failed with
    *"cannot execute CREATE SCHEMA in a read-only transaction"*. This is
    **not a permissions issue**: Postgres replicas are read-only at the
    engine level for everyone. No amount of GRANTs will change it. This is
    why a real dev/staging instance is needed.

## Windows / security

20. **Device Guard blocked `dbt.exe`** — Windows Defender Application Control
    blocked the pip-installed binary. Resolved by escalating to IT with the
    exact path, publisher (dbt Labs), license (Apache 2.0), source
    (PyPI/GitHub), and confirmation it only touched a local sandbox. IT
    allowlisted it.
21. **Stale real credentials left active in a PowerShell session** — after a
    `dbt debug` against real prod credentials (harmless in itself — `dbt
    debug` never reads or writes data), realised a subsequent `dbt run` in
    that same session would have defaulted to a target pointing at the real
    `cb_report`. Resolved: creds were set via `set`/`$env:` (session-scoped,
    not `setx`), so closing the window cleared them.
    → **Habit adopted: always run `dbt debug --profiles-dir .` and visually
    confirm the printed `host:` and `database:` lines before any
    write-capable command.**

## WSL2 / Airflow

22. `venv` creation failed inside `/mnt/c/Users/...` — Windows-mounted paths
    don't support the permissions/symlinks `venv` needs. **Fix: do all
    Python/Airflow work in WSL's native filesystem (`~`)**, reaching into
    `/mnt/c/...` only to copy specific files.
23. Airflow install 404'd on its constraints file — WSL's Ubuntu release
    (codename `resolute`) ships Python 3.14; Airflow 2.10.3 has no
    constraints file for it. **Fix: install Python 3.12 via the deadsnakes
    PPA** (not in that release's default repos), rebuild venv with
    `python3.12 -m venv`.
24. `protobuf` dependency conflict warning when installing dbt alongside
    Airflow — harmless; only affects Airflow's optional OpenTelemetry tracing,
    which we don't use. `airflow version` ran fine after.
25. DAG failed with `cd: too many arguments` — the DAG's `bash_command`
    f-strings didn't quote the path variables, and my Windows path contains a
    space (`Keertan Kumar`), which bash split into multiple args. **Fix: wrap
    every path variable in double quotes inside the f-strings.** Still done
    in the current DAG — do not remove those quotes.
26. **WSL2 → Windows Postgres required fixing THREE separate layers in
    sequence** (this took the longest). ⚠️ **See Section 9, conflict #2 —
    this may no longer be necessary on the current machine setup:**
    - **a. Routing:** `localhost` from WSL doesn't reach Windows-hosted
      services under default NAT networking → *"Connection refused"*.
      Fix used: get the gateway IP via
      `ip route | grep default | awk '{print $3}'` (e.g. `172.29.32.1`) and
      use that as `CLAIMBOOK_HOST`. (A `.wslconfig` with
      `networkingMode=mirrored` + `wsl --shutdown` is the permanent
      alternative.)
    - **b. Windows Firewall:** *"timeout expired"* — the signature of being
      blocked in transit (vs "refused" = nothing listening). Fix: add an
      **Inbound Rule** in Windows Defender Firewall with Advanced Security
      allowing **TCP port 5433**.
    - **c. Postgres auth:** *`no pg_hba.conf entry for host "172.29.x.x"`*.
      Fix: add `host all all 172.29.0.0/16 scram-sha-256` to `pg_hba.conf`
      (broad subnet, since WSL's IP changes between sessions), then **restart
      the Postgres Windows service**. Find the file via `SHOW hba_file;`.
    - After all three: Airflow → dbt → Postgres worked.
27. **Macros in the wrong folder.** `macros/get_tenant_schemas.sql` was
    initially placed at `dbt_project/models/macros/`. dbt's default
    `macro-paths` is `["macros"]` at the **project root** — a macro under
    `models/` is silently never found. Fix: move to `dbt_project/macros/`.

## Assistant errors (for honesty and calibration)

28. **Wrong about table counts.** Early on, "~35 tables per tenant schema"
    was reported — that number came from a query unknowingly **filtered by
    keywords** (`ILIKE '%preauth%' OR '%manual%'...`), not a true total. The
    real figure is **~280 tables per tenant schema**. Corrected once
    discovered. **Treat early filtered-query conclusions with suspicion.**
29. **Initially assumed `dmh` was a generic/shared schema.** It is not — it
    is one specific hospital (Deenanath Mangeshkar, tenant 36).
30. **Wrong assumed Postgres version and folder paths.** Instructions were
    written assuming Postgres 16 at `C:\Program Files\PostgreSQL\16\` and a
    project root at `claimbook_etl\` — the reality is **Postgres 18.4** and
    `claimbook_etl\dbt_project\` under `Desktop\`. Neither was discoverable
    by guessing; both needed `Get-ChildItem` against the real machine.
    **Lesson: verify paths and versions on the actual machine, don't carry
    them forward from notes.**
31. **The "password authentication failed" saga.** `dbt debug` failed
    repeatedly while `psql` succeeded. Theories chased in order: `env_var()`
    fallback defaults, env var not propagating, IPv6 (`::1`) vs IPv4
    (`127.0.0.1`), Unix-socket vs TCP auth differences. **The actual cause
    was simply that the assumed password was wrong.** Once the correct one
    was used, it worked immediately.
    **Lesson: verify the simplest explanation (is the credential itself
    correct?) before the exotic ones — especially when a direct `psql` test
    with the same credential is available as a cheap isolation check.**

---

# SECTION 9 — ⚠️ LIVE CONFLICTS & UNRESOLVED QUESTIONS

## Conflicts between earlier notes and current reality — resolve these

**#1 — Credentials: env vars vs hardcoded `profiles.yml`. RESOLVED 2026-08-10.**
`profiles.yml` had drifted to plain hardcoded YAML (including
`host: localhost`, which turned out to be a real bug — see Section 15) with
the password in plaintext. It has been **restored to the `env_var()`
pattern** for every field (`host`, `port`, `user`, `password`, `dbname`),
matching the safer version already tracked in git. `load_manual_report.py`
already read `CLAIMBOOK_*`/`CBREPORTS_*` env vars correctly the whole time —
only `profiles.yml` had regressed. Separately, a **real credential leak**
happened during this same session: an old backup copy of `profiles.yml` with
a plaintext password was briefly pushed to a temporarily-public GitHub repo
while diffing two backup folders. Password was rotated immediately and the
repo re-privatized. Lesson: never push `profiles.yml`, `.env`, or any file
with real credentials to a repo, even briefly, even if it's re-privatized
right after.

**#2 — WSL networking: does `localhost` work or not? RESOLVED 2026-08-10.**
Fully settled, both original theories ruled out: `wsl.exe -l -v` confirmed
**WSL2** (not WSL1 — rules out shared-network-stack theory), and
`Test-Path "$env:USERPROFILE\.wslconfig"` returned `False` (no file exists —
rules out mirrored networking, since it's off by default with no config).
Directly tested with `CLAIMBOOK_HOST` unset: `psycopg2` connection to
`localhost:5433` from WSL returned **`Connection refused`** — confirmed
`localhost` genuinely does not work. The earlier "successful" `airflow dags
test` run was never actually testing `localhost` at all — `CLAIMBOOK_HOST`
was live in that shell's environment the whole time (exported by
`start_airflow_scheduler.sh`), silently overriding whatever `profiles.yml`
said. **Conclusion: the gateway-IP workaround is required and load-bearing,
not optional legacy caution. Do not remove it.** Separately discovered the
gateway IP being *set* isn't sufficient by itself — `profiles.yml` also has
to actually reference it via `env_var()` rather than a hardcoded value, or
the correct env var is silently ignored (see conflict #1 and Section 15).

**#3 — Sandbox database user: `etl_user` or `postgres`? SETTLED 2026-08-10
(pragmatically, not on least-privilege grounds).**
`start_airflow_scheduler.sh` and `profiles.yml` are both standardized on
`postgres` as a deliberate choice made 2026-08-10, not by accident — worth
remembering if `etl_user` shows up in any other script or note going
forward, so things don't quietly drift apart again. `etl_user` remains the
better long-term habit (least privilege, exercises real GRANT paths) and is
worth switching to before real dev/staging access arrives, but `postgres` is
what's actually live right now.

**#4 — dbt targets: `dev`/`validate` vs `sandbox`.**
Earlier `profiles.yml` had two targets: `dev` → `cb_report` and `validate` →
`cb_report_validate`, with the rule "use `--target validate` for all test
writes." The current file has a **single `sandbox` target** → `cb_staging` in
`claimbook_sandbox`. The safety intent is now carried differently: dbt only
ever writes to a staging schema, and the *loader* is what targets the real
output table, gated by `CBREPORTS_DBNAME` + a `--dry-run` mode. Worth
re-adding a distinct read-only-ish or explicitly-named production target when
real access arrives, so the old "always `--target validate`" habit has an
equivalent.

**#5 — Sandbox database count.**
Earlier: one sandbox DB (`claimbook_sandbox`). Now: **two** —
`claimbook_sandbox` (source) and `cb_reports_sandbox` (target) — deliberately
mirroring the real two-database split so the cross-database load gets
rehearsed rather than skipped. Current setup is correct.

## The `is_tenant` question (highest-priority unknown)

`is_tenant = true` means active — confirmed and built to. But it sits beside
a separate `status` column *and* `is_group_tenant`/`group_tenant_id`, leaving
open whether it also encodes real-tenant-vs-group-entity. One query settles
it, against the **real** database:

```sql
SELECT is_tenant, status, is_group_tenant, count(*)
FROM mtdm.mtdm_tenant_tb
GROUP BY is_tenant, status, is_group_tenant
ORDER BY is_tenant, status;
```

- If `is_tenant = false` rows read as **decommissioned hospitals** → current
  filter is correct as-is.
- If they read as **group/holding entities** → the filter needs an added
  `status` condition (the macro already supports this via the optional
  `tenant_status` var).

## Scale risks not yet stress-tested

- **Shared-fate on the date cast.** Source `manual_upload_completed_time` is
  `character varying`, not a timestamp, so `::date` is a text cast. Talend
  ran each tenant as a **separate query**, so one tenant with a malformed
  date string failed only that tenant. The current model puts all tenants in
  one `UNION ALL` — **one bad row anywhere fails the entire run.** Fine at 5
  tenants; a real risk at 576.
- **Query size.** At ~576 tenants the generated SQL is ~576 stacked query
  blocks. It will compile, but watch runtime. **If either becomes a problem,
  move the loop into Airflow** (one dbt invocation per tenant batch) rather
  than one giant model — this also restores Talend's per-tenant failure
  isolation, solving both issues at once.

## Rollout plan

`tenant_limit` exists so this scales deliberately:

| Stage | `tenant_limit` | Gate before proceeding |
|---|---|---|
| 1 | 5 | row counts match Talend for all 5 |
| 2 | 25 | no date-cast failures; runtime acceptable |
| 3 | 100 | check query plan / consider batching |
| 4 | 0 (all) | needs real write access + sign-off |

---

# SECTION 10 — MY ENVIRONMENT (exact, current, verified)

| Thing | Value |
|---|---|
| OS | Windows 11 |
| dbt project root | `C:\Users\Keertan Kumar\Desktop\claimbook_etl\dbt_project` |
| dbt version | 1.12.0, postgres adapter 1.11.0 |
| Python (Windows) | 3.13, at `C:\Users\Keertan Kumar\AppData\Local\Programs\Python\Python313` |
| **Local Postgres** | **18.4**, binary at `C:\Program Files\PostgreSQL\18\bin\psql.exe`, **port 5433** |
| Sandbox DBs | `claimbook_sandbox` (source) + `cb_reports_sandbox` (target) |
| Sandbox user | `postgres` in the latest session; `etl_user`/`sandbox_pw` per earlier notes — see Section 9, conflict #3 |
| WSL | Ubuntu (codename `resolute`), Python 3.12 via deadsnakes; **username `keertan_kumar`** |
| Airflow | 2.10.3, venv at `/home/keertan_kumar/airflow_venv`, `AIRFLOW_HOME=~/airflow_home` |
| Airflow DAGs dir | `/home/keertan_kumar/airflow_home/dags/` |
| WSL → Windows host | gateway IP `172.29.32.1` (changes between sessions) — but see Section 9, conflict #2 |
| Scheduler start script | `/home/keertan_kumar/start_airflow_scheduler.sh` (exports creds, then `airflow scheduler`) |
| Talend | Open Studio 8.0.1 + Zulu JDK 11, installed locally for reference |
| Real ClaimbookDB | user `claimbook_ranjithad`; hosts seen: `4.213.181.70:5433` and `192.168.7.93:5432` (the latter timed out — likely needs VPN/office network) |

## Actual folder layout

```
C:\Users\Keertan Kumar\Desktop\claimbook_etl\
├── airflow\dags\preauth_manual_upload_daily_dag.py   <- a COPY; not what Airflow runs
├── dbt_project\                                       <- the real dbt project root
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── load_manual_report.py
│   ├── macros\
│   │   └── get_tenant_schemas.sql   <- MUST be here, not under models\
│   ├── models\
│   │   ├── reports\manual_report_staged.sql
│   │   └── staging\sources.yml
│   ├── logs\dbt.log
│   └── target\                       <- dbt-generated, ignore
├── 01_setup_source_sandbox.sql
├── 02_setup_target_sandbox.sql
└── logs\dbt.log                      <- separate top-level log, not the operative one
```

**The DAG Airflow actually runs is the WSL copy** at
`/home/keertan_kumar/airflow_home/dags/preauth_manual_upload_daily_dag.py` —
editing the Windows copy alone changes nothing.

⚠️ `sources.yml` still hardcodes `schema: dmh`. Harmless (the multi-tenant
model doesn't use `source()`) but now misleading — worth deleting or updating.

## Which shell for what

- **Windows PowerShell:** all `dbt`, `psql`, and manual `python` commands;
  editing project files
- **WSL2 Ubuntu bash:** all `airflow` commands, exclusively
- **pgAdmin Query Tool:** exploratory SQL against real ClaimbookDB

## Standard command patterns

```powershell
# Windows PowerShell — sandbox run
$PSQL = "C:\Program Files\PostgreSQL\18\bin\psql.exe"

$env:CLAIMBOOK_HOST="localhost"; $env:CLAIMBOOK_PORT="5433"
$env:CLAIMBOOK_USER="postgres";  $env:CLAIMBOOK_PASSWORD="<password>"
$env:CLAIMBOOK_DBNAME="claimbook_sandbox"
$env:CBREPORTS_DBNAME="cb_reports_sandbox"

cd "C:\Users\Keertan Kumar\Desktop\claimbook_etl\dbt_project"
dbt debug --profiles-dir .          # ALWAYS first; check host:/database:
dbt run --profiles-dir . --vars "{run_date: '2026-07-29'}"
python load_manual_report.py --dry-run
python load_manual_report.py
```

```powershell
# Useful variations
dbt run --profiles-dir . --vars "{run_date: '2026-07-29', tenant_ids: [36, 487]}"
dbt run --profiles-dir . --vars "{run_date: '2026-07-29', tenant_limit: 0}"
# prove is_tenant drives the filter — returns ONLY the inactive tenant:
dbt run --profiles-dir . --vars "{run_date: '2026-07-29', tenant_active_flag: false, tenant_limit: 99}"
```

```bash
# WSL2 — Airflow
cd ~ && source airflow_venv/bin/activate
export AIRFLOW_HOME=~/airflow_home
airflow dags list-import-errors      # "No data found" = success
airflow dags test preauth_manual_upload_daily 2026-07-29
```

⚠️ Environment variables set with `set`/`$env:`/`export` are **session-scoped
only** — they vanish when the window closes. Deliberate (safer for
credentials) but means re-setting them in every new terminal. Airflow's
`BashOperator` subprocesses inherit whatever the **scheduler process** had
exported — so scheduler-side vars must be set in the shell that starts it.

---

# SECTION 11 — OPEN QUESTIONS FOR KT

A KT session with the person currently managing Talend jobs is planned.
Priority order:

1. **Dev/staging ClaimbookDB write access** — the #1 blocker. The replica is
   read-only at the engine level; no workaround exists.
2. **[CLOSED, 2026-08-11 — resolved without KT input, see Section 16]**
   Residual discrepancy (originally tenant 30, claims `128364`/`128476`;
   scaled to 17 of 100 tenants sampled at n=100). **Root cause found
   directly from our own data**: `oltp_preauth_status
   .manual_upload_completed_time` is a mutable `character varying` column,
   not an immutable timestamp. Proof: claim 128364's row `756639` has
   `transaction_time` = 2026-07-29 but `manual_upload_completed_time` =
   2026-08-03 — five days *after* the row's own creation, only possible if
   the field was overwritten in place after the fact. Talend's report is a
   same-day, point-in-time snapshot of that field's value; our comparison
   queries run retroactively, days later, and see whatever the field
   currently holds — which may have moved since. Doesn't affect live daily
   runs (same-day query, same as Talend, no time for the field to have
   moved yet) — only retroactive/historical comparison testing. Not
   proven with a literal historical snapshot of the exact original value,
   but the mutability itself is directly proven and fully accounts for the
   scattered, no-fixed-offset pattern seen across all cases. Confirmed this
   is NOT explained by manual re-runs with hand-typed dates** — Keertan
   confirmed report jobs are not manually re-run/backfilled that way. (Full
   investigation trail in Section 15 and Section 16.)
3. What is **`bidb_sche_name`** on `mtdm_tenant_tb` for? Does any job use it?
4. Is `oltp_claims` / are the `oltp_*` tables populated by anything **upstream
   of Talend**? (Final confirmation that Airbyte is unnecessary.)
5. Full **job inventory**: all ~100+ jobs — name, business purpose, consumer,
   frequency, still-active or legacy.
6. **Which of the 70 `cb_report` tables** does each job write to? (Only one
   mapping confirmed: manual-upload job → `manual_report`.)
7. Actual **`tJava` logic** per job. (Confirmed for this job that transforms
   are pure SQL and Java is only "additional customization" — needs
   confirming job by job.)
8. Do any jobs read from the **`vw_*` views** rather than raw `oltp_*`?
9. **Scheduling mechanism** — TAC vs cron vs fully manual.
10. Is the **Talend project under version control**? (If local-only, that's a
    serious risk worth flagging plainly.)
11. What are the **other 24 databases** on the server for?
12. **Tenant count: RESOLVED 2026-08-10 from real data**, no longer needs
    asking as an open question — confirmed **483 active tenants**
    (`is_tenant = true`), out of 821 total registry rows (288 inactive real
    tenants + 50 group/holding entities + the 483 active). The 576 figure
    appears to be stale; worth confirming with the team whether anyone still
    relies on it for planning.
13. For the other 3 `functionality`-pattern tables (`inbox_management`,
    `ops_alert`, `tpa_proxy`) — which jobs write to them, and what
    `functionality` values do they use?
14. Current **failure/retry/alerting** behaviour for the manual runs, and
    where exported job logs live.
15. Is **`postgres_fdw` / `dblink`** available or installable on the
    production server? Would collapse the two-step architecture into one
    cross-database query (Section 5).
16. **Who originally wrote/maintains the jobs** — ongoing point of contact
    for logic questions after KT ends. Plus business stakeholders per report,
    for validating outputs against real expectations.
17. **RESOLVED 2026-08-11, no longer a KT question — see Section 16.**
    2026-08-10 wrongly attributed repeated `preauth_claim_id` appearances in
    `manual_report` to duplicate row inserts from reruns with no dedup logic.
    The KT contact pushed back ("I don't do reruns... unless query is
    returning the data like that") and was right. Checked all 222 repeat
    groups (96 tenants, 2026-07-29): zero byte-identical, all genuine
    distinct completion events (SUBMISSION+QUERY pairs, or real
    resubmissions/revisions). Verified against raw source data on the most
    extreme case — claim 59961, tenant 68 (`nh_rtagorecardiac`), 18 raw
    `oltp_preauth_email` rows, our join correctly isolates exactly the 6 with
    `manual_upload_completed_time` on the target date. No dedup question
    remains: there is nothing to dedup, and the converted model's full-row
    `UNION` was never doing so (nothing was ever byte-identical to begin
    with). Not a KT ask anymore.
18. **[SENT 2026-08-12, awaiting reply — see Section 17 Part B]** Requested
    the Talend job that writes `functionality = 'CLAIMS'` into
    `manual_report` (313,736 rows, active through 2026-08-10) — the same way
    the original PREAUTH query was obtained. Only 2 `functionality` values
    exist on this table (PREAUTH, CLAIMS), not 4 as an earlier session's
    comment guessed — that comment was wrong, corrected.
19. **[SENT 2026-08-12, awaiting reply — see Section 17 Part C]** Flagged
    `dhoot_hospital` (811) and `orchid_hospital` (818) as possibly missing
    from Talend's reporting entirely despite having real, active claim data
    — `dhoot_hospital` specifically has 7 weeks of real history
    (since 2026-06-22) and zero reports ever. Asked whether these tenants'
    jobs are actually configured/running. This is a live-system question,
    not something about our migration.

## Meeting framing (from the prep doc)

Lead with the 66/66 proof-of-concept result — concrete evidence, not a
proposal; it earns credibility fast. Then the architectural discovery
(per-tenant schemas), since it directly affects scope and timeline. Close
with the access blocker, framed as *"this is the one thing slowing everything
else down"* — not a complaint. Be precise that dbt has not written to
production (Section 6).

---

# SECTION 12 — REFERENCE: THE ORIGINAL TALEND QUERY

The real production query, verbatim, as given to me. **Source of truth for
the converted model.** Note the `"+context.schema+"` and `"+context.date+"`
Java string substitution — this is `tLoop_1` switching tenant schema per
iteration.

```sql
select * from(SELECT pre.preauth_claim_id,patient.mrn,person.first_name,mto.name AS TPA_Name,
ip.insurance_policy_number,ip.tpa_member_id,pre.al_number,wrt.code as request_type,st.name
as workflow_state,to_char(ps.status_update_date_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text) as claimbook_submission_time,
to_char(ps.automation_received_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text) as automation_received_time,
ps.automation_tat,ps.automation_status, ps.automation_failure_reason,
to_char(ps.manual_upload_completed_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text) as manual_upload_completed_time,
ps.manual_upload_created_by as ops_user_name,ps.upload_completed_source,
age(ps.manual_upload_completed_time::timestamp,ps.automation_received_time::timestamp)*(3600/60)/60 as manual_upload_completed_actual_tat,
'SUBMISSION' as automation_type,ps.proxy_remarks
from  "+context.schema+".oltp_pre_authorisation pre
left join  "+context.schema+".oltp_preauth_status ps on pre.preauth_claim_id=ps.preauth_claim_id
left join  "+context.schema+".oltp_patient_tb patient on patient.patient_id=pre.patient_id
left join  "+context.schema+".oltp_person person on person.person_id=patient.person_id
left join  "+context.schema+".oltp_insurance_policy ip on ip.insurance_policy_id=pre.insurance_policy_id
left join mtdm.mtdm_tpa_organization_tb mto on mto.tpa_organization_id=ip.tpa_organization_id
left JOIN  "+context.schema+".oltp_workflow_state st ON st.workflow_state_id = ps.workflow_state_id
left JOIN   "+context.schema+".oltp_workflow_request_type wrt ON wrt.request_type_id = ps.request_type_id
where  ps.manual_upload_completed_time::date=\'"+context.date+"\'
union
(SELECT pre.preauth_claim_id,patient.mrn,person.first_name,mto.name AS TPA_Name,
ip.insurance_policy_number,ip.tpa_member_id,pre.al_number,pe.request_type as request_type,pe.state as workflow_state,
to_char(pe.received_date_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text),
to_char(pe.automation_received_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text),pe.automation_tat,pe.automation_status,  NULL AS automation_failure_reason,
to_char(pe.manual_upload_completed_time::timestamp without time zone, 'DD/MM/YYYY HH24:MI:SS'::text),
pe.manual_upload_created_by,pe.source,
age(pe.manual_upload_completed_time::timestamp,pe.automation_received_time::timestamp)*(3600/60)/60 as manual_upload_completed_actual_tat,
'QUERY' as automation_type,pe.proxy_remarks
from "+context.schema+".oltp_pre_authorisation pre
left join   "+context.schema+".oltp_preauth_email pe on pre.preauth_claim_id=pe.preauth_id
left join   "+context.schema+".oltp_patient_tb patient on patient.patient_id=pre.patient_id
left join   "+context.schema+".oltp_person person on person.person_id=patient.person_id
left join   "+context.schema+".oltp_insurance_policy ip on ip.insurance_policy_id=pre.insurance_policy_id
left join mtdm.mtdm_tpa_organization_tb mto on mto.tpa_organization_id=ip.tpa_organization_id
where pe.manual_upload_completed_time::date=\'"+context.date+"\')) as foo order by preauth_claim_id;
```

## Column mapping to `cb_report.manual_report`

Produced by the query: `preauth_claim_id`, `mrn`, `first_name`, `tpa_name`,
`insurance_policy_number`, `tpa_member_id`, `al_number`, `request_type`,
`workflow_state`, **`claim_submission_time`** (⚠️ renamed from
`claimbook_submission_time`), `automation_received_time`, `automation_tat`,
`automation_status`, `automation_failure_reason`,
`manual_upload_completed_time`, `ops_user_name`, `upload_completed_source`,
`manual_upload_completed_actual_tat`, `automation_type`, `proxy_remarks`.

Added outside the SELECT: **`tenant_id`**, **`functionality` = `'PREAUTH'`**,
**`start_date`**, **`end_date`** (both = run date).

Not used by this job: `manual_report_id` (auto identity PK), `cl_number`,
`claims_id`.

## The two validated comparison queries (for demos / regression checks)

Run **separately**, in two connections — cannot be joined (Section 5).

```sql
-- QUERY 1: converted logic. CONNECT TO "claimbook".
-- (the query above, with "+context.schema+" replaced by dmh and
--  "+context.date+" replaced by 2026-07-29)

-- QUERY 2: Talend's real output. CONNECT TO "cb_reports".
SELECT preauth_claim_id, mrn, first_name, tpa_name, insurance_policy_number,
       tpa_member_id, al_number, request_type, workflow_state,
       claim_submission_time, automation_received_time, automation_tat,
       automation_status, automation_failure_reason,
       manual_upload_completed_time, ops_user_name, upload_completed_source,
       manual_upload_completed_actual_tat, automation_type, proxy_remarks
FROM cb_report.manual_report
WHERE tenant_id = 36
  AND functionality = 'PREAUTH'
  AND start_date = '2026-07-29'
ORDER BY preauth_claim_id;
```

⚠️ The `functionality = 'PREAUTH'` filter is **required** — `manual_report`
is shared across multiple job types; without it you pull in other jobs' rows.

---

# SECTION 13 — TALEND COMPONENT REFERENCE

For reading Talend job canvases during KT:

| Component | What it does | Maps to |
|---|---|---|
| `tDBConnection` | Opens a reusable DB connection | dbt `profiles.yml` |
| `tDBInput` | Runs a SQL query, streams rows in | dbt source / model `FROM` |
| `tLoop` | Repeats a subjob N times — **here: once per tenant schema** | `get_tenant_schemas()` macro |
| `tJava` | Raw custom Java. **Opaque — must be opened and read per job** | dbt SQL (confirmed for this job: logic is SQL, Java is cosmetic) |
| `tMap` | Visual field mapping/filter/join | dbt SQL `SELECT` |
| `tDBOutput` | Writes rows to a target table | `load_manual_report.py` (not dbt — see Section 5) |
| `tLogRow` | Debug output only | n/a |
| `tPrejob` / `tPostjob` | Run once before/after the job | Airflow task ordering |

**Two kinds of line on a Talend canvas — don't confuse them:**
- **Green `row/Main`** = actual data flow (rows physically move)
- **`OnComponentOk` / `OnSubjobOk`** = trigger/control flow only (no data)

Talend Studio has **no built-in scheduler** — jobs are exported as
scripts/jars and triggered externally (TAC, cron, or, here, by hand).
Editing a job in Studio changes nothing in production until it is re-exported
and redeployed.

---

# SECTION 15 — 2026-08-10 SESSION: THE FOUR OPEN ITEMS, RESOLVED

This session picked up the 4 items left open at the end of Section 1's
"immediate next steps" from 2026-08-07. All four are now resolved. Full
trail below for anyone (including a future Claude instance) who needs the
reasoning, not just the conclusion.

## Item 1 — `is_tenant` semantics, confirmed

Ran the exact confirming query from Section 9 against the **real**
`claimbook` database:

```sql
SELECT is_tenant, status, is_group_tenant, count(*)
FROM mtdm.mtdm_tenant_tb
GROUP BY is_tenant, status, is_group_tenant
ORDER BY is_tenant, status;
```

Result:

| is_tenant | is_group_tenant | count | meaning |
|---|---|---|---|
| f | f | 288 | inactive real tenants |
| f | t | 50 | group/holding entities |
| t | f | 483 | active real tenants |

No row exists for `is_tenant = true AND is_group_tenant = true` — the two
conditions never conflict. **The macro's existing `is_tenant = true` filter
is already correct and needs no `is_group_tenant` addition.** Two side
findings: `status` is entirely blank across all 821 rows (not a usable
secondary filter, contrary to what Section 4 speculated); and the real
active-tenant count is **483**, which resolves the "576 vs 809" confusion —
576 looks stale, 821 is the real total registry size.

## Item 2 — Parallel-run validation across 5 real tenants

Selected 5 active tenants with real volume on 2026-07-29 (tenant IDs 2, 30,
42, 44, 52 — schemas `vikram`, `srmc`, `kdh`, `nh_mazumdar`, `hiranandani`).
Compared converted-query row counts against real `cb_report.manual_report`
counts:

| tenant | converted | Talend | diff |
|---|---|---|---|
| 2 | 12 | 14 | +2 |
| 30 | 35 | 46 | +11 |
| 42 | 10 | 12 | +2 |
| 44 | 38 | 49 | +11 |
| 52 | 23 | 29 | +6 |

**Root cause 1 (explains most of the gap) — ⚠️ RETRACTED 2026-08-11, see
Section 16 for the correction.** This section originally claimed Talend's
real output table contains genuine duplicate row inserts from
repeated/retried job runs with no dedup logic in `tDBOutput`, and that the
converted query was "naturally duplicate-free" and an improvement. **Both
claims are wrong.** The KT contact confirmed there are no reruns, and a
column-by-column check of all 222 same-day repeat groups (96 tenants)
production-wide found zero byte-identical rows — every repeat is a genuine
distinct completion event (SUBMISSION+QUERY pair, or a real
resubmission/revision), which a full-row `UNION` was never deduplicating in
the first place since nothing was ever byte-identical. The original
`GROUP BY preauth_claim_id HAVING count(*) > 1` count (21 groups across the
5 tenants here) is still numerically fine as a count of repeats — it's the
*explanation* for those repeats that was wrong. Full trail: Section 16.

**Root cause 2 (small residual, 1-2 claims per tenant) — ⚠️ UPDATED
2026-08-11, see Section 16 for the resolution.** After removing
duplicates, tenant 30 still had 2 claims (`128364`, `128476`) present in
Talend's output but absent from the converted query. Investigated their
full `oltp_preauth_status` history directly — claim 128364 has 9 history
rows across dates 2026-08-03 and 2026-08-06; claim 128476 has 5 across
2026-07-28 and 2026-07-30. **Neither claim has any
`manual_upload_completed_time` timestamp actually falling on 2026-07-29**,
the date they're tagged with in Talend's output. Initially hypothesized
this was a manual backfill/re-run with a hand-typed historical date —
**Keertan corrected this: report jobs are not manually re-run that way.**
That theory was retracted. This was left open (KT question, Section 11
#2) at the time this section was written, then resolved 2026-08-11 without
needing KT input: `manual_upload_completed_time` is a mutable field, and
retroactive queries see its current value, not what it said when Talend's
job actually ran. Full explanation and evidence: Section 16. Also
discovered along the way: `oltp_preauth_status`
is a **full history/audit table**, not one-row-per-claim (one claim in this
investigation had 57 history rows) — `is_active` is not a reliable
current-row marker (it was `false` on every single row checked, including
the one the parent table's FK pointed to as "current"). General rule worth
keeping: never filter/join a history-style table directly by a raw date
column without first reducing to one row per entity via whatever actually
marks "current."

## Item 3 — WSL networking, resolved

Two-step confirmation, both original theories in Section 9 conflict #2 ruled
out:
1. `wsl.exe -l -v` → confirmed **WSL2** (not WSL1 — rules out the
   shared-network-stack theory).
2. `Test-Path "$env:USERPROFILE\.wslconfig"` → `False`. No `.wslconfig`
   exists at all, so mirrored networking is off by default (rules out that
   theory too).

With both theories dead, directly tested the actual behavior: unset
`CLAIMBOOK_HOST` in a WSL shell and attempted a raw `psycopg2` connection to
`localhost:5433` — got **`Connection refused`**, not a timeout (meaning WSL
resolved `localhost` to something with nothing listening, consistent with
WSL's own loopback rather than the Windows host). **Conclusion: `localhost`
genuinely does not work from WSL to reach Windows-hosted Postgres.** The
earlier "successful" `airflow dags test` run (Section 1, 2026-08-07) never
actually tested this — `CLAIMBOOK_HOST=172.29.32.1` was live in that
session's shell environment the entire time (set by
`start_airflow_scheduler.sh`), silently overriding whatever `profiles.yml`
said. **The gateway-IP workaround is required and load-bearing. Do not
remove it or attempt to "simplify it away."**

## Item 4 — Persistent Airflow scheduler, fully proven

Started `airflow scheduler` in a dedicated foreground terminal via
`start_airflow_scheduler.sh`. Along the way, hit and fixed a real syntax
error in that script — line 5 had a literal, never-filled-in placeholder
(`export CLAIMBOOK_PASSWORD=<******>`), not a real password. Fixed by
removing the placeholder brackets and setting a real value; also confirmed
`CLAIMBOOK_USER=postgres` as the deliberate standard going forward (Section
9, conflict #3).

Scheduler ran continuously and healthily — confirmed via 5-minute heartbeat
log lines over 15+ minutes with no crash. Found and fixed a second
environment issue in a separate terminal: `airflow dags list` initially
failed with `no such table: dag` because `AIRFLOW_HOME` wasn't set in that
shell, causing Airflow to silently initialize a **separate, disconnected**
metadata database at the wrong default path (`~/airflow` instead of
`~/airflow_home`). Fixed by exporting `AIRFLOW_HOME` correctly in that
terminal.

**Discovered the live DAG's real task count didn't match documentation.**
`airflow dags list` showed the DAG registered but paused
(`dags_are_paused_at_creation` default). After unpausing, the scheduler
picked it up entirely on its own and ran it — but it only executed **2
tasks** (`dbt_run_preauth_manual_upload`, `dbt_test_preauth_manual_upload`),
not the 4 tasks Section 1 (2026-08-07 version) claimed were "proven
end-to-end." Confirmed by reading the actual file on disk (94 lines, ends
right after the `dbt_test` task definition, no loader task, no explicit `>>`
dependency chain) — **this was a real regression, not a display artifact.**
That earlier "proven" claim described a version of the file that was no
longer what was actually on disk.

Also discovered this 2-task version's `dbt_run` was a **false-positive
success**: its log showed `[WARNING]: The selection criterion
'preauth_manual_upload_daily' does not match any enabled nodes` /
`Nothing to do` — the model had been renamed to `manual_report_staged`
(with an alias) since that DAG file was last touched, so the old file's
`--select preauth_manual_upload_daily` matched zero models and exited
`0` without ever attempting a database connection. **A "success" exit code
does not by itself prove real work happened — this pattern recurred twice
more later in the session and is worth remembering generally.**

**Fix: restored the correct 4-task DAG file** from a git-backed backup
(`ETL-1` repo — see the git-diffing sub-section below), with paths corrected
for the real machine (`keertan_kumar`, `airflow_venv`,
`Desktop\claimbook_etl\dbt_project`) and the schedule kept at `0 6 * * *`
(06:00) per deliberate choice — the backup itself had `0 2 * * *`, which
would have silently reverted an intentional later change if restored
blind.

**Hit and fixed the `profiles.yml` hardcoded-`localhost` bug** (see Item 3
above and Section 9 conflict #1) as part of getting the restored 4-task DAG
to actually run. After the fix, triggered the DAG manually against a date
with real sandbox source data (`2026-07-29`) and got a **fully clean,
verified end-to-end run**: `dbt_run_staging` staged 15 rows
(`get_tenant_schemas(): 5 tenant(s) -> dmh, apollo_nashik, fortis_bg,
manipal_wf, narayana_hrc`), `dbt_test_staging` passed, `load_dry_run`
succeeded, and `load_to_cb_reports`'s own log confirmed real row-level
action: `[source] 15 staged row(s)` → `[target] 15 existing row(s) match` →
`deleted 15 row(s)` → `inserted 15 row(s)` → `post-load count: 15` →
`COMMITTED`. This is genuine proof of idempotent delete+insert behavior,
verified from the loader's own detailed log rather than inferred — an
initial suspicion that the same `manual_report_id` values being reused
meant "nothing happened" turned out to be wrong; the loader evidently
preserves original IDs on a matched delete+insert by design, and the log
settles it unambiguously.

### Git-based diffing of two DAG backup folders (side investigation)

To resolve which of two backup DAG files was correct, both were pushed to
temporarily-public GitHub repos (`ETL-1`, `ETL-2`) for direct diffing.
**Real finding: `ETL-1/dbt_project/profiles.yml` had a plaintext Postgres
sandbox password committed in it.** Password rotated immediately, both repos
re-privatized. No real ClaimbookDB/`cb_reports` credentials were exposed —
sandbox only — but treat this as a live lesson: **never push `profiles.yml`
or any credential-bearing file to a repo, even temporarily, even if it's
made private again right after.** Diffing itself showed `load_manual_report.py`,
`dbt_project.yml`, `sources.yml`, and the model file were byte-identical
between the two repos — only the DAG file's hardcoded paths differed, and
neither repo's DAG matched what was actually live on Keertan's machine (both
had the correct 4-task structure; the live file had regressed to the stale
2-task version described above).

## New issue found, not yet fixed: staging model materialization vs docstring

While investigating why `cb_staging.manual_report_staged` appeared empty
after a later run, discovered `manual_report_staged.sql`'s `config()` block
sets `materialized = 'table'`, but its own docstring describes
`incremental_strategy = 'delete+insert' with unique_key`.
**`incremental_strategy` only takes effect when `materialized =
'incremental'` — with `materialized = 'table'`, dbt silently ignores it and
does a full drop-and-rebuild every run**, not a targeted delete+insert by
key. Not currently a functional bug (staging is transient — loaded
immediately within the same DAG run before another run can touch it), but
the docstring is inaccurate, and the staging table cannot be safely
inspected after a later run has occurred (a later run with zero matching
source rows will silently wipe out an earlier run's staged rows). Worth
fixing before concurrent/overlapping runs become a real scenario.

---

---

# SECTION 16 — 2026-08-11 SESSION: DUPLICATE THEORY CORRECTED, SCALE-TEST STARTED

## Background: what this session was supposed to be

Picked up "immediate next step" #1 from Section 1/15 — scale-testing the
multi-tenant pipeline beyond the 5 already-tested tenants. Real
dev/staging write access to `claimbook` is still blocked (Section 11 KT
#1), so the plan was **read-only** validation: extend the row-count /
claim-ID comparison already done for 5 tenants to a larger set of real
tenants, without writing anything.

## Part A — why dbt can't be used for this, and what was built instead

`dbt run` against real `claimbook` would `CREATE TABLE
claimbook.cb_staging.manual_report_staged` — a write. Since real-claimbook
writes are still blocked, dbt is unusable for this task regardless of
target. Built a standalone script instead:
**`scale_test_readonly.py`** — plain `psycopg2`, no dbt involved at all.

- Selects tenants with the exact same 3-guard query as
  `get_tenant_schemas()` (`is_tenant = true`, schema-exists join,
  `oltp_pre_authorisation`-exists check), ordered by `tenant_id`, `LIMIT N`
  — so it picks precisely the tenants the real pipeline would pick at that
  `tenant_limit`.
- For each tenant, runs the same SUBMISSION+QUERY union logic as
  `manual_report_staged.sql`, trimmed to just `preauth_claim_id` (identity
  only — field-level correctness is already proven, 66/66, Section 6; this
  test is about tenant/claim coverage at scale, not re-proving field
  mapping).
- Compares against real `cb_report.manual_report` (`tenant_id` +
  `functionality = 'PREAUTH'` + `start_date` filter, same as the proven
  Section 13 validation query), reporting `converted` / `talend_raw` (with
  dupes) / `talend_dedup` (distinct) counts per tenant, plus which specific
  claim IDs are missing or extra.
- **Both connections opened with `conn.set_session(readonly=True)`.** This
  is enforced by Postgres itself, not script discipline — confirmed
  directly: an `INSERT` attempt on such a session raises
  `ReadOnlySqlTransaction: cannot execute INSERT in a read-only
  transaction`. Given this touches real prod, wanted a guarantee stronger
  than "the script just doesn't happen to write anything."
- Verified against a throwaway local Postgres fixture (2 valid tenants + 3
  deliberately-broken negative-test tenants: inactive, ghost schema,
  schema-without-the-driving-table) before handing it over. All 3 negative
  tests correctly excluded; a genuine duplicate-insert case and a
  genuine-residual case were both fixture-simulated and correctly
  classified (`match = NO`, correct missing-claim-ID reported).

**Status: built and verified, not yet run against real data.** Blocked on
confirming the WSL shell actually points at real prod before the first
real invocation (see below).

## Part B — the gateway-IP mixup caught before it caused a false result

Asked Keertan to confirm the shell's `CLAIMBOOK_*` env vars before running
anything real. Result: `CLAIMBOOK_HOST=172.29.32.1`, everything else blank.
**`172.29.32.1` is the WSL→Windows gateway address — it points at the
local sandbox Postgres (port 5433, only 5 tenant schemas seeded), not real
production**, per the Item 3 investigation in Section 15. Real ClaimbookDB
is a separate real server: `4.213.181.70:5433`, user
`claimbook_ranjithad` (Section 12 reference table). Caught before any
query ran — re-issued the export commands pointed at the real host and
asked for re-confirmation. **As of end of session, still waiting on that
re-confirmation** — the scale-test has not yet been run against real data.

## Part C — the duplicate-insert root cause was wrong (the main finding)

The person managing the Talend jobs replied to the KT question logged as
Section 11 #17 with: *"I don't do reruns... there should not be duplicate
entries unless query is returning the data like that."** This directly
contradicted the 2026-08-10 theory ("genuine duplicate row inserts from
repeated job runs with no dedup logic" — Section 15 Item 2, Section 1,
Section 11 #17, all now corrected in place with pointers to this section).

**Investigation, in order:**

1. Ran a real, read-only query against `cb_report.manual_report` pulling
   every full row belonging to a duplicated `(tenant_id, preauth_claim_id)`
   group, for `functionality = 'PREAUTH'`, `start_date = '2026-07-29'` —
   no tenant filter, so this covered the entire real table for that date,
   not just the 5 previously-sampled tenants.
2. **499 rows, 222 duplicate groups, 96 distinct tenants involved** — far
   broader than the original 5-tenant sample suggested.
3. Compared every group column-by-column (excluding the identity PK
   `manual_report_id`). **Zero of the 222 groups are byte-identical.**
   Every single one differs in at least one real column.
4. Classified the 222: **148 are a SUBMISSION-branch event paired with a
   QUERY-branch event** for the same claim, same day (e.g. tenant 2, claim
   61806 — an automated completion at 16:31:46 and a separate email-driven
   completion at 16:32:04, 18 seconds apart). **74 are multiple genuine
   events within the SUBMISSION branch itself** — e.g. tenant 17, claim
   49174: a `DISCHARGE` request completed 13:00:43 by one ops user, then a
   `REVISION` completed 17:02:09 by a different ops user, same claim, same
   day.
5. Most extreme case: **tenant 68 (`nh_rtagorecardiac`), claim 59961 — 6
   separate completions in one day**, all `QUERY`/portal, by 3 different
   agents (MeghaK_tpa, SimonB_tpa, AmruthaM_tpa) at different times.
6. **Verified this against raw source data**, not just the reporting
   table: queried `nh_rtagorecardiac.oltp_preauth_email` directly for
   `preauth_id = 59961`. Found **18 total raw history rows** for that
   claim — most `SUBMITTED`-state emails that never got a manual upload
   completed at all (`manual_upload_completed_time IS NULL`), plus a few
   that completed on other dates (7/28, 7/31, 8/1). **Exactly 6 have
   `manual_upload_completed_time` on 2026-07-29**, and those 6 match
   timestamp-for-timestamp and agent-for-agent against the 6 rows already
   seen in `cb_reports`. This confirms the converted model's
   `WHERE pe.manual_upload_completed_time::date = run_date` filter is
   neither dropping real events nor fabricating extra ones — it's pulling
   exactly the right subset out of a genuinely busy history.

**Business confirmation from Keertan** (matches the data exactly): every
step of a claim's lifecycle — submission, query, revision, enhancement —
is logged in `claimbookdb`/`cb_reports` with its own timestamp and user.
Repeated appearances of one `preauth_claim_id` on one day are normal,
expected, and already correctly captured by the converted model, not a
gap that needs dedup logic.

**Corrections made to the rest of this file as a result** (all done, not
just noted): Section 1 DONE/PROVEN bullet, Section 11 KT question #17
(now marked resolved, no longer a KT ask), Section 15 Item 2's Root cause
1 (marked retracted in place, pointing here). **"UNION naturally
deduplicates, this is an improvement over Talend" was also retracted** —
none of the 222 groups were ever byte-identical, so a full-row `UNION`
was never eliminating anything; the earlier framing was reasoning from a
false premise, not a real behavioral advantage of the converted pipeline.

**What this does NOT change:** the converted model's logic itself is
unchanged — no code was modified. This was a validation and
documentation correction, not a pipeline fix, because the pipeline was
already doing the right thing.

**Reply sent to the KT contact:** confirmed they were right, no reruns
involved, gave the concrete evidence above (222 groups, 0 identical, the
two patterns, the claim-59961 example) and thanked them for the pushback.

## Part D — real-prod connection got sorted out (took a few tries)

First attempt: `CLAIMBOOK_HOST` alone was set, still to the sandbox gateway
IP (`172.29.32.1`), in one terminal; a second terminal had different
sandbox values entirely (`claimbook_sandbox`/`etl_user`) with no host —
confirmed env vars are genuinely per-terminal-session here, same class of
issue as the PowerShell `$env:` scoping already documented in Section 10.
Consolidated to one terminal (the WSL bash session already `cd`'d into
`dbt_project`, `airflow_venv` active) and exported the real values
explicitly. Second snag: guessed `CLAIMBOOK_DBNAME=claimbookdb` — wrong,
got `FATAL: database "claimbookdb" does not exist` (which did at least
confirm host/port/user were all correct — Postgres itself rejected the
request, not the network). The project's own `load_manual_report.py`
(Section 7) already defaults to `CLAIMBOOK_DBNAME=claimbook` — used that
instead, worked immediately. **Confirmed real-prod values**: host
`4.213.181.70`, port `5433`, user `claimbook_ranjithad`, `CLAIMBOOK_DBNAME
=claimbook`, `CBREPORTS_DBNAME=cb_reports`.

## Part E — script crashed at 100 tenants, fixed and re-verified

`scale_test_readonly.py`'s original 30s `statement_timeout` was too tight
for one tenant (`nh_mazumdar`) under real production load — the whole
100-tenant run died on an unhandled `QueryCanceled` after only 10 tenants
had been checked. **Bug was mine**: no per-tenant error handling, so one
slow query took down the entire batch. Fixed: wrapped the per-tenant work
in `try/except psycopg2.Error`, logs the tenant as failed and continues
rather than crashing; added a `--timeout` flag (used 60s on the actual
100-tenant run). Verified the fix actually works — not just assumed —
against a throwaway fixture with a deliberately pathological cross-join
tenant forced to time out at 500ms: confirmed (a) the failing tenant gets
cleanly logged and skipped, and (b) — the part that actually mattered —
the connection survives and a tenant queried *after* the failure still
returns correct results. This relies on both connections being opened
with `autocommit=True`: in autocommit mode a canceled statement only
aborts its own implicit transaction, no explicit `.rollback()` needed to
keep going. Confirmed this directly too, not just from documentation.

## Part F — 25-tenant then 100-tenant real results

25 tenants: 19/25 matched exactly. 100 tenants (after the Part E fix,
`--timeout 60s`): **83/100 matched exactly.** All 17 mismatches, at both
sample sizes, share the identical shape: `missing_from_converted` only,
**zero** `extra_in_converted` — the converted query never invents a claim
Talend doesn't have, it only ever comes up short, always by 1-3 claims.
That consistency across 100 real tenants is what elevated this from "one
odd tenant" to a pattern worth actually explaining (see Part G). Rate
held roughly steady going from n=25 (24%) to n=100 (17%) — not blowing up
with scale, which is reassuring for eventual cutover risk sizing. One
notable single-tenant case: `apollo_gleneagles` (166) had 3 missing claims
in one group, the largest single-tenant residual seen so far.

## Part G — the residual discrepancy's actual root cause, found without KT

Keertan's own working theory going in: every real workflow step
(submission, query, revision, enhancement) gets logged in
`claimbookdb`/`cb_reports`, so daily reports should genuinely capture
everything that happened that day, closed or not. **Checked this against
the actual original Talend query (Section 12, the literal source of
truth)** — it only ever joins two event tables, `oltp_preauth_status` and
`oltp_preauth_email`, exactly what the converted model already
replicates. None of the other 16 `oltp_preauth_*` tables on a tenant
schema (discharge polling, automation log, documents, etc.) are touched
by the real query at all. So there's no missing join, no uncaptured
"enhancement" table — coverage-wise, the converted model already matches
Talend's real logic exactly.

That ruled out a coverage gap and pointed at something else: **inspected
`oltp_preauth_status`'s actual column types** and found
`manual_upload_completed_time` is stored as `character varying`, not an
immutable timestamp type — nothing stops it from being overwritten in
place. Direct proof this actually happens: claim 128364's row
`preauth_status_id = 756639` has `transaction_time` = **2026-07-29**
15:58:55 (when the row was created) but `manual_upload_completed_time` =
**2026-08-03** 13:51:31 — five days *after* its own row's creation.
That's only possible if the field was updated after the fact, on the same
row (that row also shows `automation_status = 'Failure'` — consistent
with an initial attempt that failed and was later actually completed).
Claim 128476 shows the identical fingerprint: row `756659`, `transaction_
time` = 7/29 16:11:15, `manual_upload_completed_time` = **7/30** 10:45:22.

**Conclusion**: Talend's report is generated live, same-day — it captured
whatever `manual_upload_completed_time` said *at that moment* on
2026-07-29. Our comparison queries run retroactively, 13+ days later, and
see the field's *current* value, which has since been overwritten by real
subsequent activity on the same claim. This is not a pipeline bug on
either side — it's an artifact of comparing a live-captured snapshot
against a retroactive query of mutable data. **Practically important**:
this should barely affect the live pipeline once in production, since
Airflow will query `manual_upload_completed_time::date = today` on the
same day, the same way Talend does — there's essentially no window for
the field to have been overwritten yet at query time. It only shows up
in retroactive validation testing like this scale-test, which is
deliberately re-checking old dates long after the fact.

**Calibration**: the *mutability* of the field is directly proven (the
transaction_time-after-completed_time inconsistency is conclusive on its
own). The *exact original value* Talend's job saw on 2026-07-29 is not
directly proven — no separate audit/version column exists to confirm it
literally read `2026-07-29` at that moment rather than something else
that also wouldn't match. But the mutability alone is sufficient to fully
explain the observed pattern (no fixed offset, occasionally the nearest
activity is even *before* the report date) without requiring any bug in
either system. **Question closed 2026-08-11** — resolved from our own
data; sent a close-out to the KT contact so they're not left digging for
an answer that's already been found.

---

*Between Part C and Part G, sent a simplified, jargon-free follow-up to
the KT contact narrowing the original (schema-heavy, apparently unclear)
question down to one plain question: does the report's date mean "claim
completed that day" or "row loaded into the DB that day"? Superseded by
Part G's finding before a reply came back — close-out sent, question
closed.*

## Part H — full 483-tenant scale-test, final number

Ran clean, no crashes — the Part E timeout-resilience fix held for the
entire run. **461/483 tenants match exactly (95.4%).** All 22 mismatches
are still the identical one-directional pattern seen at every smaller
sample size: `missing_from_converted` only, **zero** `extra_in_converted`
across all 483 real active tenants, no exceptions. 28 residual claims
total, spread across the 22 tenants, largest single-tenant residual still
`apollo_gleneagles` (166) at 3 claims. Mismatch rate dropped from 17%
(n=100) to 4.6% (n=483) — not a weakening of the pattern, just tenant mix:
tenants are selected in `tenant_id` order, and the tail (400s-800s) is
dominated by low/zero-volume tenants, so there's less exposure to the
mutable-timestamp scenario (Part G) to begin with.

**This closes out the coverage-validation phase of the migration.**
Claim-ID coverage has now been checked against real production data at
5 → 25 → 100 → all 483 active tenants, the residual pattern is fully
understood and consistently one-directional throughout, and the root
cause (Part G) is a property of retroactive testing, not a live-pipeline
risk. Full CSV: `scale_test_483_20260729.csv` (Keertan's machine).

## Open thread carried forward (from Section 16 — now resolved, see Section 17 Part A)

Item 6 in the "Immediate next steps" list above: this session verified
row-count/content correctness for **one** extreme multi-event claim by
hand. That's strong evidence the join logic is sound, but it's an n=1
manual spot check, not a systematic test across many multi-event claims.
Worth folding into the scale-test work once real-prod access is confirmed
(Part B above).

---

# SECTION 17 — 2026-08-12 SESSION: PARITY AT SCALE, MISSING TESTS FOUND,
# ROW ORDER FIXED AND PROVEN, NEW LIVE-SYSTEM GAP FOUND

## Part A — row-count parity, systematically (closes Section 16's open thread)

Built `parity_check.py`: for each of the 222 known multi-event claims
(Section 16 Part C's sample — reused rather than re-gathered, since
Talend's counts for them were already known), runs the full ~20-column
model logic scoped to that one claim and compares row count against
Talend's. Verified against a fixture first (a genuine 2-event claim, a
1-event claim, and a deliberately-wrong count to prove real mismatches
get caught, not just matches rubber-stamped).

**Result: 211/222 (95%) match exactly**, including the extreme case
(claim 59961, 6/6) and several 3/3 and 4/4 cases. All 11 mismatches are
`converted = talend - 1` — never more than one event short, never the
other direction. This isn't a new phenomenon — it's the Part G mechanism
(mutable `manual_upload_completed_time`) showing up at the individual-event
level: for a multi-event claim, sometimes only *one* of its events has
been reworked since Talend ran, so that one event silently ages off while
its siblings still match. Same root cause, same direction, genuinely
completes rather than complicates the existing explanation.

## Part B — CLAIMS job identified, request sent

`cb_report.manual_report` has exactly 2 `functionality` values — PREAUTH
(2.87M rows, already converted) and CLAIMS (313,736 rows, active through
2026-08-10). An earlier session's comment guessing "4 job types" for this
table was wrong; corrected. CLAIMS is a strong next-conversion candidate:
same target table, same architecture already proven, just needs its
source job's query from the KT contact — request sent 2026-08-12, no
reply yet (see Section 11 #18).

Also clarified for the record: Keertan does not have a working Talend
Studio project of his own — the actual Talend project with all its jobs
is run by someone else in a different city. The original PREAUTH query
was hand-delivered by that person (single-tenant, then generalized to a
variable by us) — the same process is needed for every future job,
there's no way to browse a job inventory locally.

## Part C — full-scale test against yesterday's date; two real findings

Built `full_count_test.py`: defaults `run_date` to **yesterday, computed
fresh each run** (`date.today() - timedelta(days=1)`) rather than a fixed
historical date — this is now the standing convention going forward, per
Keertan's explicit request. Compares raw row **count** per tenant (not
claim-ID sets) using the real full-column query, matching
`load_manual_report.py`'s actual key: `tenant_id, start_date, end_date,
functionality`. Verified against a fixture (genuine match case + a
deliberately-planted fake-Talend-row mismatch case) before running for
real.

Before running at scale, checked that yesterday's Talend data actually
existed yet (`count(*) ... WHERE start_date = '2026-08-11'` → 1707 rows,
confirmed present) — avoided wasting a 483-tenant run on a date that
hadn't been generated yet.

**Result: 465/483 (96.3%) exact match.** Two distinct findings in the 18
mismatches:

1. **16 tenants, the known pattern** — `talend = converted + 1`, every
   time. Confirms the lag theory directly: shorter gap between Talend's
   run and our check (same-day vs. the earlier 13-day-stale test) means
   fewer mismatches (16 vs. 22), but doesn't reach zero — reworking can
   happen within hours, not just weeks.
2. **🚩 2 tenants, a genuinely new and different shape** —
   `dhoot_hospital` (811) and `orchid_hospital` (818), both showing
   `converted > talend` (talend=0 for both) — the **first and only** cases
   all session where the converted query found more than Talend, not
   less. Checked directly: both are real, active tenants
   (`is_tenant = true`), and `dhoot_hospital` specifically has **279 real
   `oltp_preauth_status` rows going back to 2026-06-22** — 7 weeks of
   genuine claim activity — with **zero rows ever** in
   `cb_report.manual_report`. `orchid_hospital`'s history only starts
   2026-08-06, so it's more likely just new-tenant lag (Talend hasn't
   caught up yet) — less conclusive, less urgent. `dhoot_hospital` looks
   like a real, live gap in the *current production reporting system*,
   unrelated to this migration — flagged to the KT contact separately
   (Section 11 #19).

## Part D — `dbt_test_staging` had never actually tested anything

While attempting a full manual end-to-end run (see Part E), `dbt test`
returned "Nothing to do" — **zero tests were defined** for
`manual_report_staged` anywhere. Only `sources.yml` existed under
`models/`; no `schema.yml`, no test definitions at all. This means every
prior "successful" `dbt_test_staging` task — in every DAG run treated as
proven so far, including the 2026-08-10 demo run — passed only because
there was nothing to fail, not because anything was verified. `dbt test`
exiting 0 with no tests defined looks identical to a real pass from
Airflow's `BashOperator` perspective.

Fixed: added `models/reports/schema.yml` — `not_null` on
`preauth_claim_id`, `tenant_id`, `start_date`, `end_date`, `functionality`;
`accepted_values` on `functionality` (`PREAUTH`) and `automation_type`
(`SUBMISSION`, `QUERY`). **Deliberately no uniqueness test on
`preauth_claim_id`** — would directly contradict the proven multi-event
finding (Section 16 Part C) and fail on exactly the claims proven correct
today (Part A above). Also added `tests/test_date_window_sanity.sql`, a
singular test checking `manual_upload_completed_time` actually falls
within `start_date`/`end_date` — a real behavioral check, not just shape.
Verified genuinely passing (9/9), confirmed non-vacuous by checking the
test count and names in the run output, not just the exit code.

## Part E — local Airflow scheduler proved unreliable; worked around, not fixed

Attempted two things this session that both hit the same failure: a live
demo trigger, and later a full manual "run the real DAG for yesterday's
date" end-to-end test. Both times, a manually-triggered run got stuck in
`queued` indefinitely — task states all `None`, scheduler log showing
only its routine 5-minute heartbeat with zero scheduling activity for the
run. Ruled out the obvious causes directly rather than guessing: no DAG
import errors (`airflow dags list-import-errors` → "No data found"), not
a stale blocked prior run (checked and cleared one, the new run stayed
stuck regardless). This matches Airflow's own UI warning about SQLite +
`SequentialExecutor` concurrency issues under a webserver + scheduler +
CLI all hitting one database file — a real limitation of this local
sandbox setup, not something wrong with the pipeline.

**Demo workaround**: showed an existing proven successful run
(2026-08-10, 12:12:31) instead of fighting for a fresh live one — all 4
tasks green, real logs, worked well as a demo.

**End-to-end test workaround**: ran the DAG's exact 4 steps by hand, in
the same order, replicating the literal `bash_command` each
`BashOperator` runs (`dbt run` → `dbt test` → loader `--dry-run` → loader
real run) rather than depending on the scheduler to execute them. This
worked cleanly and is arguably more informative than watching boxes turn
green anyway, since it surfaces real command output at each step (this is
exactly how Part D's missing-tests gap was found — via the actual `dbt
test` output, not the DAG UI).

**Not fixed, only worked around.** A real fix would likely mean switching
from SQLite + `SequentialExecutor` to a real Postgres metadata database +
`LocalExecutor` — bigger, more invasive change, not attempted this
session. Doesn't block anything right now since the manual-steps
workaround is proven to work, but shouldn't be treated as a permanent
answer if this pipeline needs to run unattended for real.

## Part F — row order: a real gap, found, fixed, and proven

Prompted by a direct question: does Talend's row order matter, and does
ours match it? This had never been tested all session — every prior
comparison was set-based (claim-ID coverage) or count-based (row counts),
never order.

**Investigated real Talend output** (tenant 30, 2026-07-29, ordered by
Talend's own `manual_report_id`): `preauth_claim_id` climbs steadily
upward across all 44 rows — Talend orders by **claim ID ascending**, not
chronologically (`manual_upload_completed_time` jumps around freely in
the same rows). One extra data point: a duplicate-claim pair
(`128566`) has its *second* row's timestamp earlier than its first —
order doesn't even hold within a repeated claim — which is itself a
small, independent confirmation of the mutable-timestamp finding (Part G,
Section 16): a value that changed after insertion wouldn't necessarily
still reflect insertion order.

**Checked our model — found a separate, real gap**: `manual_report_staged.sql`
had **no `ORDER BY` clause at all**. Postgres doesn't guarantee output
order without one — whatever consistent-looking order existed before was
incidental to the query plan, not a real guarantee, and could have
silently changed after something as routine as a `VACUUM` or index
rebuild. This was true regardless of whether it happened to match
Talend's order or not.

**Fixed**: added `order by tenant_id, preauth_claim_id` after the
per-tenant `UNION ALL` chain (applies to the whole combined result set,
no restructuring needed). Explicitly does **not** attempt to replicate
secondary order among multiple rows sharing one claim_id — the real data
showed that doesn't follow a clean, reproducible rule (see the `128566`
example above), so no secondary sort key is claimed that isn't actually
guaranteed.

**Verified against a synthetic fixture first** (deliberately fed the
tenant list out of order — 200 before 100 — confirmed the `ORDER BY`,
not tenant list order, controlled the actual output).

**Then verified against real production data — this is the important
part.** Pulled real rows from both sides for tenant `dmh` (36),
2026-08-11: initial comparison query had a bug (used the claim-ID-only
simplified query, which silently collapsed the tenant's 5 genuine
duplicate-claim pairs — 67 rows instead of the true 72). Caught and
corrected immediately, re-ran with the real full-column model logic:
**72 rows on both sides, exact sequence match position-for-position**,
including all 5 duplicate-claim pairs landing in identical spots on both
sides. This is a real, complete, verified match on real data — not a
claim resting on the fixture alone.

**Scope honestly stated**: proven in full for one tenant so far, not yet
re-run across all 483 like the count-based tests were. The ordering
mechanism itself is deterministic and identical for every tenant (same
`ORDER BY` clause, no tenant-specific logic), so this is good evidence —
not yet full-scale proof — that it holds everywhere. Worth extending to
scale before final cutover sign-off (Section 1's Immediate Next Steps).

## Part G — restart recovery; two file-placement lessons worth remembering

Mid-session, Keertan's machine restarted, killing every terminal
(scheduler, webserver, all shell state). Recovered cleanly: Postgres
itself survived (a service, not a terminal process); re-activated the
venv, re-placed the two new test files, re-exported sandbox env vars,
re-confirmed via echo before touching anything.

Two real snags surfaced while getting back to a fully re-tested state,
both worth remembering for next time rather than re-diagnosing from
scratch:

1. **Browser duplicate-download naming.** Presenting a file with the same
   name twice in one conversation (e.g. `manual_report_staged.sql`,
   presented once after the docstring fix and again after the `ORDER BY`
   fix) causes the browser to save the second one as
   `filename (1).ext` instead of overwriting. A plain `mv "filename.ext"
   ...` after that silently grabs the **stale first copy**, with no error
   — `mv` succeeds fine, since the stale file genuinely exists at that
   path. Hit this twice this session (the model file, then `schema.yml`).
   Fix each time: check `ls` in Downloads for a `(1)` suffix, move that
   one instead. Diagnosis method that worked: `wc -l` / `grep` the
   in-place file for an expected marker (a line count, a specific string)
   rather than trusting that a move without an error means the right
   content landed.
2. **dbt's `target/partial_parse.msgpack` cache survives a restart.**
   After re-placing the corrected `schema.yml` a second time, the
   deprecation warning it was supposed to fix came back — the file on
   disk was confirmed correct (`grep` showed the right content), but dbt
   still behaved as if it weren't. `rm -rf target/` before the next run
   forced a full re-parse and resolved it. The restart doesn't clear this
   cache (it's just a file), so a stale parse from *before* the restart
   can persist and mask a genuinely-updated file after it.

---

# SECTION 18 — REAL `dbt run` PROOF AGAINST REAL PRODUCTION-SOURCED DATA (2026-08-17)

## Context

Section 17 validated the pipeline using `full_byte_comparison.py`, which
independently re-implements the model's join logic in Python rather than
running dbt itself — necessary because real dev/staging write access to
Claimbook is still not granted (Section 1, standing blocker). This left an
open question: does a *real* `dbt run` — not a script standing in for it —
actually produce the same result?

This session closes that gap for two specific real claims, without
requiring real prod write access, by building a bridge tool that copies
real (read-only) prod data into the sandbox, where dbt already has full
write access.

## New tool: `validation/copy_claim_to_sandbox.py`

Standalone script, not part of dbt. Copies ONE real claim's data at a time
from real production (`claimbook` + `cb_reports`, read-only) into the
sandbox (`claimbook_sandbox`, writable) — both the real current Claimbook
row (`oltp_person` / `oltp_patient_tb` / `oltp_insurance_policy` /
`oltp_pre_authorisation` / `oltp_preauth_status`) and the real historical
Talend snapshot (`cb_report.manual_report` on prod →
`cb_report.preauth_manual_upload_daily` on sandbox — **note the table name
differs between environments**, confirmed via `\dt cb_report.*` on prod).

Safety model:
- Source connections are opened `readonly=True` at the Postgres session
  level — structurally cannot write to prod, same pattern as
  `full_byte_comparison.py`.
- Target (sandbox) uses a **deliberately separate env var prefix**
  (`SANDBOX_*`, not `CLAIMBOOK_*`) so it can never be confused with prod
  vars — this was a direct response to a near-miss during this session
  (see "Two-terminal incident" below).
- Defaults to `--dry-run` (prints the exact plan, no writes). Requires
  explicit `--execute` to write anything.
- One claim per run only. No bulk/loop mode — this is a demonstration/
  debugging tool, not a migration tool.

Located at `validation/copy_claim_to_sandbox.py` in the repo.

## Permissions required in sandbox (new)

`etl_user` needed explicit grants that weren't previously in place:
```sql
GRANT INSERT, UPDATE ON dmh.oltp_person, dmh.oltp_patient_tb,
  dmh.oltp_insurance_policy, dmh.oltp_pre_authorisation,
  dmh.oltp_preauth_status TO etl_user;
```
Note: **both INSERT and UPDATE are required**, even for insert-only
workflows, because the script uses `ON CONFLICT ... DO UPDATE` — Postgres
checks UPDATE privilege on that clause even when no conflict occurs. This
cost one failed run before being caught.

Also confirmed: raw `dmh`/tenant-schema tables in sandbox are **read-only
by default** even to `etl_user` — only `cb_staging.manual_report_staged`
and `cb_report.preauth_manual_upload_daily` (the dbt/Talend output-side
tables) had default write access. This wasn't previously documented and
is a deliberate-looking safeguard (prevents accidental corruption of
fixture data dbt tests run against) rather than an oversight — worth
knowing if this comes up again.

## Two-terminal working pattern (new standing practice)

This session repeatedly hit a real risk: a single terminal's exported
`CLAIMBOOK_*` env vars got reused across both prod and sandbox work,
because both environments use the *same* variable names
(`CLAIMBOOK_HOST` etc.), just with different values.

**Real incident, twice in this session:** `dbt run --target sandbox` was
executed in a terminal that still had prod's `CLAIMBOOK_HOST=4.213.181.70`
exported. dbt attempted `CREATE SCHEMA` against real production. Both
times, the only thing that prevented an actual write to prod was that
prod itself is enforcing read-only at the connection level — not
something to rely on as a safeguard going forward.

**Fix adopted:** two separate, dedicated WSL terminals for the rest of
this project:
- **Terminal 1 — PROD only.** `CLAIMBOOK_HOST=4.213.181.70`,
  `CLAIMBOOK_DBNAME=claimbook`, `CBREPORTS_*` set to real prod. Used for
  all read-only queries and validation scripts against real data. Never
  run `dbt run` here.
- **Terminal 2 — SANDBOX only.** `CLAIMBOOK_HOST=172.29.32.1` (gateway
  IP), `CLAIMBOOK_DBNAME=claimbook_sandbox`. Used for all `dbt run`
  invocations and any sandbox writes.

Before running anything in either terminal, echo the relevant host/dbname
vars first and visually confirm before proceeding — this is now a hard
rule, not a courtesy check, given the two near-misses above.

`copy_claim_to_sandbox.py` is the one exception that legitimately needs
both prod and sandbox vars in the same terminal (source = prod read-only,
target = sandbox writable) — it uses the separate `SANDBOX_*` prefix
specifically so this dual-context terminal can't accidentally point dbt
itself at prod.

## Other findings from this session

- **`manual_upload_completed_actual_tat` null-string quirk (new,
  confirmed systematic):** Talend's real stored output represents "no
  value" for this column as the literal 4-character text string `'null'`
  (confirmed via `pg_typeof()` — column is `text`), not a real SQL NULL.
  Our pipeline correctly produces a real NULL. Affected 20/72 rows for
  tenant 36 on 2026-08-11 (28%). Cosmetic, not a correctness bug — flagged
  for a product decision on whether to replicate Talend's representation,
  not urgent.

- **Mutability pattern confirmed to extend beyond
  `manual_upload_completed_time`:** `al_number`,
  `insurance_policy_number`, and `first_name` are also editable after
  Talend's original capture. All 8 non-null-quirk mismatches found via
  `full_byte_comparison.py` for tenant 36 / 2026-08-11 were independently
  verified: in every case, our converted output matched **current** raw
  Claimbook exactly, and Talend's stored value was the stale one.
  Verified via direct `psql` queries tracing the real join chain
  (`oltp_pre_authorisation` → `oltp_patient_tb` → `oltp_person`, and →
  `oltp_insurance_policy`).

- **Live mutation caught mid-session:** claim 255920's `al_number`
  changed from `110101043006` to `110101043006-1` between two queries run
  minutes apart in this same session — a real-time, directly-observed
  instance of the mutability pattern, not just an inference from
  Talend-vs-now comparison.

- **`full_byte_comparison.py` first real production run (tenant 36,
  2026-08-11, 72 rows):** 53/72 exact match, 11/72 the null-string quirk,
  8/72 genuine mutability-explained differences, 0/72 unexplained
  pipeline errors.

- **dbt `run_date` var defaults to yesterday, computed fresh** (see
  `dbt_project.yml`) — for any historical/backfill-style run, it must be
  explicitly overridden: `dbt run --select manual_report_staged --target
  sandbox --vars '{"run_date": "YYYY-MM-DD"}'`. Running without this
  against an old date silently returns 0 rows (the model's `WHERE
  ps.manual_upload_completed_time::date = run_date` filter excludes
  everything), not an error — worth remembering, this cost one confusing
  zero-row result before being traced.

## Real dbt run results (genuine `dbt run`, not script-derived)

Two real production claims (tenant 36, run_date 2026-08-11) copied via
`copy_claim_to_sandbox.py --execute`, then processed by an actual
`dbt run --select manual_report_staged --target sandbox --vars
'{"run_date": "2026-08-11"}'` execution (`SELECT 2` — both rows
materialized):

| Claim ID | Field | dbt output (real `dbt run`) | Talend (real snapshot) |
|---|---|---|---|
| 255841 | first_name | `Mr. SALUNKHE PRAKASH MARUTI` | `PRAKASH SALUNKHE` |
| 255841 | al_number | `110202560518-1` | `110202560518` |
| 255841 | insurance_policy_number | `4016/X/157974480/07/000` | `4016/X/157974480/07/000` (matches) |
| 255920 | first_name | `Miss. THOSAR KETAKI MILIND` | `Miss. THOSAR KETAKI MILIND` (matches) |
| 255920 | al_number | `110101043006-1` | `110101042925` |
| 255920 | insurance_policy_number | `4128I/HSNR/403817432/00/000` | `4128i/HSNR/403817432/01/000` |

This is the first session-confirmed instance of a genuine `dbt run`
(not a script re-implementation) processing real production-sourced data
end to end, with output independently verified against both the real raw
source and the real Talend snapshot.

## Status update

- Real dev/staging write access to `claimbook`/`cb_reports` — still not
  granted, still the #1 blocker for running dbt directly against prod.
  This session's sandbox-bridge approach is a workaround, not a
  replacement for that access.
- `copy_claim_to_sandbox.py` is reusable for any future claim needing
  this same real-dbt-run proof, without needing prod write access.

---

*End of context file. If anything here conflicts with something I tell you
live in chat, trust me over this file and tell me the file needs updating.*
