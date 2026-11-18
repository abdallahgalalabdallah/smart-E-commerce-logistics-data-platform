
    
    

select
    zip_code_prefix as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_geography"
where zip_code_prefix is not null
group by zip_code_prefix
having count(*) > 1


