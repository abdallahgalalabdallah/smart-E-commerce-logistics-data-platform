
    
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
    [dbo].[testview_c5d4f7d1d4ae80e2333a426bf519f484_8308]
   as 
    
    
    

select
    seller_id as unique_field,
    count(*) as n_records

from "smart_logistics_dwh"."gold"."dim_seller"
where seller_id is not null
group by seller_id
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
    [dbo].[testview_c5d4f7d1d4ae80e2333a426bf519f484_8308]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbo].[testview_c5d4f7d1d4ae80e2333a426bf519f484_8308]
  ;')