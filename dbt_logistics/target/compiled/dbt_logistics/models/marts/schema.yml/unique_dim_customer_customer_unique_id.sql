
    
    

select
    customer_unique_id as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_customer"
where customer_unique_id is not null
group by customer_unique_id
having count(*) > 1


