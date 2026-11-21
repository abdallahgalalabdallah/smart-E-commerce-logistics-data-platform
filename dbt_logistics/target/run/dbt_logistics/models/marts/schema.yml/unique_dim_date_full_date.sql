
    
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
    [dbo].[testview_5df9475f146f0d30820b6b24aa56bec8_15335]
   as 
    
    
    

select
    full_date as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_date"
where full_date is not null
group by full_date
having count(*) > 1



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbo].[testview_5df9475f146f0d30820b6b24aa56bec8_15335]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbo].[testview_5df9475f146f0d30820b6b24aa56bec8_15335]
  ;')