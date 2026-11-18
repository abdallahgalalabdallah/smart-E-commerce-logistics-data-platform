
  
    USE [smart_logistics_dwh];
    USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "gold"."dim_date__dbt_tmp__dbt_tmp_vw" AS WITH all_order_dates AS (
    SELECT CAST(order_purchase_timestamp AS DATE) AS full_date
    FROM "smart_logistics_dwh"."staging"."stg_olist__orders"
    WHERE order_purchase_timestamp IS NOT NULL

    UNION

    SELECT CAST(order_delivered_customer_date AS DATE) AS full_date
    FROM "smart_logistics_dwh"."staging"."stg_olist__orders"
    WHERE order_delivered_customer_date IS NOT NULL
)
SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), coalesce(cast(full_date as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_'')), '''')), 2))
 AS date_key,
    full_date,
    YEAR(full_date) AS year_number,
    DATEPART(QUARTER, full_date) AS quarter_number,
    MONTH(full_date) AS month_number,
    DATENAME(MONTH, full_date) AS month_name,
    DAY(full_date) AS day_of_month,
    DATEPART(WEEKDAY, full_date) AS day_of_week,
    DATENAME(WEEKDAY, full_date) AS day_name,
    CASE
        WHEN DATENAME(WEEKDAY, full_date) IN (''Saturday'', ''Sunday'') THEN 1
        ELSE 0
    END AS is_weekend
FROM all_order_dates;
    ')

EXEC('IF OBJECT_ID(''gold.dim_date__dbt_tmp'', ''U'') IS NOT NULL
                EXEC(''DROP TABLE "smart_logistics_dwh"."gold"."dim_date__dbt_tmp"'');SELECT * INTO "smart_logistics_dwh"."gold"."dim_date__dbt_tmp" FROM "smart_logistics_dwh"."gold"."dim_date__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS "gold"."dim_date__dbt_tmp__dbt_tmp_vw"')



    
    use [smart_logistics_dwh];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_dim_date__dbt_tmp_cci'
        AND object_id=object_id('"gold"."dim_date__dbt_tmp"')
    )
    DROP index "gold"."dim_date__dbt_tmp".gold_dim_date__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_dim_date__dbt_tmp_cci
    ON "gold"."dim_date__dbt_tmp"

   


  