
with base as (
    {{ dbt_utils.union_relations(
        relations=[
            ref('int_gl__summary'),
            ref('int_gl__bs'),
            ref('int_gl__cash_equity'),
            ref('int_gl__income')
        ]
    ) }}
)

select b.*
from base b