
    
    

select
    geography_key as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_geography"
where geography_key is not null
group by geography_key
having count(*) > 1


