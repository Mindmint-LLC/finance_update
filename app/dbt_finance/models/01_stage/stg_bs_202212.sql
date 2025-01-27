

select b.acct as Account
    , cast('2022-12-31' as date) as Effective_Date
    , b.Company
    , a.Account_Name
    , c.Type as Account_Type
    , c.Cash_Type
    , a.Category
    , a.Category_Name
    , a.SubCategory1
    , a.SubCategory2
    , round(b.Amount, 2) as Amount
    , 2022 as yr
    , 12 as mnth
from {{ source('fpa', 'stg_netsuite_bs_202212') }} b
    join {{ ref('dim_acct') }} a
        on b.acct = a.Account
    left join {{ source('fpa', 'stg_netsuite__chart_of_accounts') }} c
        on b.acct = c.Account