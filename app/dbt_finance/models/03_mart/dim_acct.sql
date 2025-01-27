
{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude model_name\".") }}
  {% endif %}
{% endif %}


{% if target.schema != 'fpa' %}
  {% set query %}
    DROP TABLE IF EXISTS {{ target.schema }}.dim_acct;
    CREATE TABLE {{ target.schema }}.dim_acct AS
    SELECT *
    FROM fpa.dim_acct;
  {% endset %}
  {% do run_query(query) %}
{% endif %}


{{
  config(
    materialized = 'incremental',
    unique_key = 'Account',
    on_schema_change='append_new_columns',
    )
}}

with base as (
  SELECT cast(left(n.Account, 6) as int) as Account
    , n.Account_Name as Account_Name
    , cast(left(n.Account, 1) as int) as Category
    , case when cast(left(n.Account, 1) as int) = 1 then '1 - Asset'
        when cast(left(n.Account, 1) as int) = 2 then '2 - Liability'
        when cast(left(n.Account, 1) as int) = 3 then '3 - Equity'
        when cast(left(n.Account, 1) as int) = 4 then '4 - Revenue'
        when cast(left(n.Account, 1) as int) >= 5 then '5 - Expense'
        end as Category_Name
  from {{ source('fpa', 'stg_netsuite') }} n
  where n.Account is not null

  union all

  select cast(n.acct as int) as Account
    , cast(null as string) as Account_Name
    , cast(left(cast(n.acct as string), 1) as int) as Category
    , case when cast(left(cast(n.acct as string), 1) as int) = 1 then '1 - Asset'
        when cast(left(cast(n.acct as string), 1) as int) = 2 then '2 - Liability'
        when cast(left(cast(n.acct as string), 1) as int) = 3 then '3 - Equity'
        when cast(left(cast(n.acct as string), 1) as int) = 4 then '4 - Revenue'
        when cast(left(cast(n.acct as string), 1) as int) >= 5 then '5 - Expense'
        end as Category_Name
  from {{ source('fpa', 'stg_netsuite_bs_202212') }} n
  where n.acct is not null
)

, account_list as (
  SELECT b.Account
    , max(b.Account_Name) as Account_Name
    , b.Category
    , b.Category_Name
    , cast(null as string) as SubCategory1
    , cast(null as string) as SubCategory2
    , cast(null as string) as Cash_Type
  from base b
  where b.Account is not null
  group by all
)

select n.Account
    , n.Account_Name
    , n.Category
    , n.Category_Name
    , coalesce(pt.SubCategory1, n.SubCategory1) as SubCategory1
    , coalesce(pt.SubCategory2, n.SubCategory2) as SubCategory2
from account_list n
  left join {{ this }} pt
    on n.Account = pt.Account