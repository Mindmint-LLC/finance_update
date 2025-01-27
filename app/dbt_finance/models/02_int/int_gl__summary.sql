    SELECT g.Effective_Date
        , g.PostedEOM
        , g.Company
        , g.Account
        , g.Account_Name
        , g.Account_Type
        , g.Cash_Type
        , g.Category
        , g.Category_Name
        , g.SubCategory1
        , g.SubCategory2
        , g.Dept
        , g.Dept_Name
        , sum(g.Amount) as Amount
        , sum(g.AmountCF) as AmountCF
        , g.yr
        , g.mnth
        , g.Category_CashIS
    from {{ ref('int_gl') }} g
    where g.Account not in (480005)
    group by all