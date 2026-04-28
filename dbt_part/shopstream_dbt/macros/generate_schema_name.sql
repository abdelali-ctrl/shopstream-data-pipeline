{# 
   Override the default schema-name behavior so models land in the schemas
   declared in dbt_project.yml (staging / core / marts) rather than the
   default <target_schema>_<custom_schema> concatenation.

   Rationale: prod and dev both target the same Snowflake database
   (SHOPSTREAM_DWH) and we want stable, predictable schema names that
   match the docs, the verify scripts, and the Power BI semantic model.
   For multi-tenant or per-developer schemas, prefix the target name
   in the dbt profile instead.

   Reference:
   https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
