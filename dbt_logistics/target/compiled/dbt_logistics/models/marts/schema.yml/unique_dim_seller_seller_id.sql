
    
    

select
    seller_id as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_seller"
where seller_id is not null
group by seller_id
having count(*) > 1


