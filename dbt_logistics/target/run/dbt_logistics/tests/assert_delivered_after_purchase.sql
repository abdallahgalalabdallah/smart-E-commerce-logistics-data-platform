
    
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
    [dbo].[testview_1d95ace7323ce19b99b7742792f3c1cb_11790]
   as 
    -- Fails if any delivered order has a missing delivery timestamp

-- or was delivered earlier than its purchase timestamp.

SELECT
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
FROM "smart_logistics_dwh"."staging"."stg_olist__orders"
WHERE order_status = ''delivered''
  AND (
      order_delivered_customer_date IS NULL
      OR order_delivered_customer_date < order_purchase_timestamp
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
    [dbo].[testview_1d95ace7323ce19b99b7742792f3c1cb_11790]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbo].[testview_1d95ace7323ce19b99b7742792f3c1cb_11790]
  ;')