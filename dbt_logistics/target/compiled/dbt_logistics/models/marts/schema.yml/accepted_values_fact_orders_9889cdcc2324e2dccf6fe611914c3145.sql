
    
    

with all_values as (

    select
        main_payment_type as value_field,
        count(*) as n_records

    from "smart_logistics_dwh"."gold"."fact_orders"
    group by main_payment_type

)

select *
from all_values
where value_field not in (
    'credit_card','boleto','voucher','debit_card','not_defined'
)


