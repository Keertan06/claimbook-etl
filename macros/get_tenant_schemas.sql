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
  This is better than scanning information_schema for schema names, because
  information_schema gives you schema names with no tenant_id attached - and
  tenant_id is a required output column.

THREE SAFETY GUARDS (each one matters in a 821-row registry, 483 active):
  1. is_tenant filter     - skips deactivated tenants
  2. schema-exists join   - skips registry rows whose sche_name has no actual
                            schema (stale/planned tenants). Without this the
                            model fails compilation on one bad row.
  3. table-exists check   - skips schemas missing oltp_pre_authorisation
                            (partially provisioned tenants).

VARS:
  tenant_limit         - cap the number of tenants (used for staged rollout:
                         5 now, then 25, then 100, then all 483 - see
                         docs/PROJECT_CONTEXT_ABI_Health_MASTER.md Section 16
                         for the validation results at each stage).
                         null/0 = no limit.
  tenant_active_flag   - value of mtdm_tenant_tb.is_tenant treated as active.
                         Default true. Set to false to invert (useful for
                         testing the filter itself).
  tenant_status        - OPTIONAL extra filter on mtdm_tenant_tb.status.
                         Default none = not applied. Only set this if you
                         confirm status carries independent meaning (as of
                         2026-08, this column is blank on every row - not
                         currently usable).
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

    {#- run_query only returns real rows during execute; guard against parse -#}
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
