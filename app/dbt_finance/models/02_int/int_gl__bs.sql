with base as (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_bs_202212'),
            ref('int_gl')
        ]
    ) }}
)

, summarized as (
    select b.Effective_Date
        , b.Company
        , b.Account
        , b.Account_Name
        , b.Account_Type
        , b.Cash_Type
        , -1 as Category
        , concat(b.Category_Name, '_bal') as Category_Name
        , b.SubCategory1
        , b.SubCategory2
        , b.yr
        , b.mnth
        , sum(b.Amount) as Amount
    from base b
    where b.Category in (1, 2, 3)
    group by all
)

, eom as (
    select distinct Effective_Date, yr, mnth
    from summarized b
)

, accts as (
    select distinct b.Company
        , b.Account
        , b.Account_Name
        , b.Account_Type
        , b.Cash_Type
        , b.Category
        , b.Category_Name
        , b.SubCategory1
        , b.SubCategory2
    from summarized b
)

, combined as (
    select *
    from eom e
        cross join accts a
)

, final as (
    select c.*
        , s.Amount
    from combined c
        left join summarized s
            on c.Effective_Date = s.Effective_Date
            and c.Account = s.Account
            and c.Company = s.Company
)

select f.* except(Amount)
    , sum(f.Amount) over (partition by f.Company, f.Account order by f.Effective_Date) as Amount
from final f