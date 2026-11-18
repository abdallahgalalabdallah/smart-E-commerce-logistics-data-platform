
    
    

with child as (
    select order_key as from_field
    from "smart_logistics_dwh"."gold"."fact_order_items"
    where order_key is not null
),

parent as (
    select order_key as to_field
    from "smart_logistics_dwh"."gold"."fact_orders"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


