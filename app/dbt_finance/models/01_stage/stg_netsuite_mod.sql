
SELECT row_number() over () as pk
    , n.Entry_ID
    , cast(left(n.Account, 6) as int) as Account
    , cast(case when n.Account like '%-%' then right(n.Account, 2) end as int) as Dept
    , n.Account_Name
    , n.Class
    , n.Description
    , n.Memo
    , cast(n.Effective_Date as date) as Effective_Date
    , last_day(cast(n.Posted_Date as date)) as PostedEOM
    , n.Company
    , round(n.Amount, 2) as Amount
FROM {{ source('fpa', 'stg_netsuite') }} n
WHERE n.Effective_Date is not null
    AND round(n.Amount, 2) != 0