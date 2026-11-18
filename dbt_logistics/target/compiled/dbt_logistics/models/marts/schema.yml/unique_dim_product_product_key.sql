
    
    

select
    product_key as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_product"
where product_key is not null
group by product_key
having count(*) > 1


