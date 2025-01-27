
{{
  config(
    enabled = false,
    )
}}

with entries as (
    select g.Effective_Date
        , g.Entry_ID
        , g.yr
        , g.mnth
        , g.Company
        , sum(g.AmountCashOp) as operating_cash
    from {{ ref('int_gl') }} g
    where g.Cash_Type = '02 - Operating Activities'
    group by all
)

, amounts as (
    select e.Entry_ID
        , e.Company
        , g.Account
        , g.Account_Name
        , g.Category
        , g.SubCategory1
        , g.SubCategory2
        , g.Dept
        , g.Dept_Name
        , sum(g.AmountCashOp) as Amount
    from entries e
        join {{ ref('int_gl') }} as g
            on e.Entry_ID = g.Entry_ID
            and e.Company = g.Company
            and g.Category >= 4
    group by all
    having sum(g.Amount) != 0
)

, amounts_agg as (
    select a.Entry_ID
        , a.Company
        , sum(a.Amount) as Amount_Total
    from amounts a
    group by all
    having sum(a.Amount) != 0
)

, amt_calc as (
    select a.*
        , a.Amount / g.Amount_Total as perc_applied
    from amounts a
        join amounts_agg g
            on a.Entry_ID = g.Entry_ID
            and a.Company = g.Company
)

, final as (
    select e.Effective_Date
        , e.Company
        , a.Account
        , a.Account_Name
        , -1 as Category
        , case when a.Category = 4 then '7 - Cash Revenue'
            when a.Category > 4 then '8 - Cash Expense'
            else '9 - Cash Other'
            end as Category_Name
        , a.SubCategory1
        , a.SubCategory2
        , a.Dept
        , a.Dept_Name
        , e.operating_cash * coalesce(a.perc_applied, 1.0) as Amount
        , e.yr
        , e.mnth
        , e.Entry_ID
    from entries e
        left join amt_calc a
            on e.Entry_ID = a.Entry_ID
            and e.Company = a.Company
)

select f.Effective_Date
    , f.Company
    , f.Account
    , f.Account_Name
    , f.Category
    , f.Category_Name
    , f.SubCategory1
    , f.SubCategory2
    , f.Dept
    , f.Dept_Name
    , sum(f.Amount) as Amount
    , f.yr
    , f.mnth
    , f.Entry_ID
from final f
where coalesce(f.Account, -1) not in (480005)
group by all