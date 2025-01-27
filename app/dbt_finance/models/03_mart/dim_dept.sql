
{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude model_name\".") }}
  {% endif %}
{% endif %}


{% if target.schema != 'fpa' %}
  {% set query %}
    DROP TABLE IF EXISTS {{ target.schema }}.dim_dept;
    CREATE TABLE {{ target.schema }}.dim_dept AS
    SELECT *
    FROM fpa.dim_dept;
  {% endset %}
  {% do run_query(query) %}
{% endif %}


{{
  config(
    materialized = 'incremental',
    unique_key = 'Dept',
    on_schema_change='append_new_columns',
    )
}}


with account_list as (
SELECT cast(right(n.Account, 2) as int) as Dept
    , MIN(n.Account_Name) as Dept_Name
from {{ source('fpa', 'stg_netsuite') }} n
where n.Account like '%-%'
    and n.Account_Name like '%(%'
group by 1
)

select n.Dept
    , coalesce(pt.Dept_Name, n.Dept_Name) as Dept_Name
from account_list n
  left join {{ this }} pt
    on n.Dept = pt.Dept