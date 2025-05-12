    select f.Effective_Date
        , f.PostedEOM
        , f.Company
        , -1 as Category
        , '6 - Income' as Category_Name
        , sum(case when f.Category >= 5 then -f.Amount else f.Amount end) as Amount
        , sum(case when f.Category >= 5 then -f.Amount else f.Amount end) as AmountCF
        , sum(case when f.Category >= 5 then -f.Amount else f.Amount end) as AmountCashIS
        , f.yr
        , f.mnth
        , '01 - Income' as Cash_Type
        , '0 - Income' as Category_CashIS
    from {{ ref('int_gl__summary') }} f
    where f.Category >= 4
    group by all