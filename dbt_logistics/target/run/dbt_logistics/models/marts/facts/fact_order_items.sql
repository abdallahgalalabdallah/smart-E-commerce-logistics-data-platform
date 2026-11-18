
  
    USE [smart_logistics_dwh];
    USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "gold"."fact_order_items__dbt_tmp__dbt_tmp_vw" AS SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), concat(coalesce(cast(i.order_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(i.order_item_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''))), '''')), 2))
 AS order_item_key,
    i.order_id,
    i.order_item_id,
    fo.order_key,
    dp.product_key,
    ds.seller_key,
    i.shipping_limit_date,
    i.price,
    i.freight_value
FROM "smart_logistics_dwh"."intermediate"."int_order_item_enriched" i
INNER JOIN "smart_logistics_dwh"."gold"."fact_orders" fo
    ON i.order_id = fo.order_id
LEFT JOIN "smart_logistics_dwh"."gold"."dim_product" dp
    ON i.product_id = dp.product_id
LEFT JOIN "smart_logistics_dwh"."gold"."dim_seller" ds
    ON i.seller_id = ds.seller_id;
    ')

EXEC('IF OBJECT_ID(''gold.fact_order_items__dbt_tmp'', ''U'') IS NOT NULL
                EXEC(''DROP TABLE "smart_logistics_dwh"."gold"."fact_order_items__dbt_tmp"'');SELECT * INTO "smart_logistics_dwh"."gold"."fact_order_items__dbt_tmp" FROM "smart_logistics_dwh"."gold"."fact_order_items__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS "gold"."fact_order_items__dbt_tmp__dbt_tmp_vw"')



    
    use [smart_logistics_dwh];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_fact_order_items__dbt_tmp_cci'
        AND object_id=object_id('"gold"."fact_order_items__dbt_tmp"')
    )
    DROP index "gold"."fact_order_items__dbt_tmp".gold_fact_order_items__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_fact_order_items__dbt_tmp_cci
    ON "gold"."fact_order_items__dbt_tmp"

   


  