
{{
  config(
    warn_if = '>0',
    error_if = '<0'
    )
}}

select p.Account
from {{ ref('dim_acct') }} p
where p.SubCategory1 is null
  and p.Account_Name is not null