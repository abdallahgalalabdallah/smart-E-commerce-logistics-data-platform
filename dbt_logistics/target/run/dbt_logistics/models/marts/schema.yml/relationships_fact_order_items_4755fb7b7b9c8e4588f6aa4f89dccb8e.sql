
    
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
    [dbo].[testview_764b6ba850d659a35e6fcc965dc1aa74_7925]
   as 
    
    
    

with child as (
    select product_key as from_field
    from "smart_logistics_dwh"."gold"."fact_order_items"
    where product_key is not null
),

parent as (
    select product_key as to_field
    from "smart_logistics_dwh"."gold"."dim_product"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbo].[testview_764b6ba850d659a35e6fcc965dc1aa74_7925]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbo].[testview_764b6ba850d659a35e6fcc965dc1aa74_7925]
  ;')