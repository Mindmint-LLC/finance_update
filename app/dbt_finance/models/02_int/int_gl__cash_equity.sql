
select f.Effective_Date
    , f.PostedEOM
    , f.Company
    , -1 as Category
    , '3 - Equity_cash' as Category_Name
    , sum(f.Amount) as Amount
    , sum(f.Amount) as AmountCash
    , f.yr
    , f.mnth
    , '04 - Financing Activities' as Cash_Type
from {{ ref('int_gl__summary') }} f
where f.Category = 3
group by all