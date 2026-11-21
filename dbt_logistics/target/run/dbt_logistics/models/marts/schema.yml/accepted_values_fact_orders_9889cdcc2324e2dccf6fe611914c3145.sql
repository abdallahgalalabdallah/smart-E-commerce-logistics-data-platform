
    
    -- Create target schema if it does not exist
  USE [smart_logistics_dwh];
  DECLARE @dbt_schema_lock int;
  EXEC @dbt_schema_lock = sp_getapplock
    @Resource = 'dbt_create_schema_dbo',
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 30000;
  BEGIN TRY
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
    BEGIN
      EXEC('CREATE SCHEMA "dbo"')
    END
  END TRY
  BEGIN CATCH
    IF @dbt_schema_lock >= 0
      EXEC sp_releaseapplock @Resource = 'dbt_create_schema_dbo', @LockOwner = 'Session';
    THROW;
  END CATCH
  IF @dbt_schema_lock >= 0
    EXEC sp_releaseapplock @Resource = 'dbt_create_schema_dbo', @LockOwner = 'Session';

  

  
  EXEC('create view 
    [dbo].[testview_6cb1590a2b1ecfe8cda2f079d88a9e57_16890]
   as 
    
    
    

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
    ''credit_card'',''boleto'',''voucher'',''debit_card'',''not_defined''
)



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbo].[testview_6cb1590a2b1ecfe8cda2f079d88a9e57_16890]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbo].[testview_6cb1590a2b1ecfe8cda2f079d88a9e57_16890]
  ;')