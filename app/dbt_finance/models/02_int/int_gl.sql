
with base as (
    SELECT b.pk
        , b.Entry_ID
        , b.Effective_Date
        , b.PostedEOM
        , b.Company
        , b.Account
        , b.Account_Name
        , c.Type as Account_Type
        , case when a.Category = 4 then '00 - Revenue'
            when a.Category >= 5 then '01 - Expense'
            else c.Cash_Type end as Cash_Type
        , a.Category
        , a.Category_Name
        , a.SubCategory1
        , a.SubCategory2
        , b.Dept
        , d.Dept_Name
        , case when a.Category in (2, 3, 4) then -b.Amount else b.Amount end as Amount
        , case when c.Cash_Type is not null then -b.Amount
            when a.Category >= 4 then -b.Amount
            else null end as AmountCF
        , extract(year from b.Effective_Date) as yr
        , extract(month from b.Effective_Date) as mnth
        , case when a.Category >= 5 then '2 - Cash Expense'
            when a.Category = 4 then '1 - Cash Revenue'
            else c.Category_CashIS end as Category_CashIS
        , b.Class
        , b.Description
        , b.Memo
    FROM {{ ref('stg_netsuite_mod') }} b
        join {{ ref('dim_acct') }} a
            on b.Account = a.Account
        left join {{ ref('dim_dept') }} d
            on b.Dept = d.Dept
        left join {{ source('fpa', 'stg_netsuite__chart_of_accounts') }} c
            on b.Account = c.Account
    where round(b.Amount, 2) != 0
)

select b.*
from base b