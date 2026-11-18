
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."fact_orders"
where order_id is not null
group by order_id
having count(*) > 1


