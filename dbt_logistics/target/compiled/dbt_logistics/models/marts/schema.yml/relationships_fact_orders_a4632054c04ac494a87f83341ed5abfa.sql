
    
    

with child as (
    select geography_key as from_field
    from "smart_logistics_dwh"."gold"."fact_orders"
    where geography_key is not null
),

parent as (
    select geography_key as to_field
    from "smart_logistics_dwh"."gold"."dim_geography"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


