
    
    

select
    order_key as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."fact_orders"
where order_key is not null
group by order_key
having count(*) > 1


