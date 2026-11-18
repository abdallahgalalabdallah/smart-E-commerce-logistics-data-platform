
  
    USE [smart_logistics_dwh];
    USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "gold"."agg_daily_logistics_summary__dbt_tmp__dbt_tmp_vw" AS SELECT
    dd.full_date AS purchase_date,
    dg.state AS customer_state,
    COUNT(fo.order_id) AS total_orders,
    SUM(fo.item_count) AS total_items,
    SUM(fo.payment_total) AS total_revenue,
    SUM(fo.freight_total) AS total_freight,
    ROUND(AVG(CAST(fo.delivery_days AS FLOAT)), 2) AS avg_delivery_days,
    ROUND(AVG(CAST(fo.delay_days AS FLOAT)), 2) AS avg_delay_days,
    ROUND(AVG(fo.avg_review_score), 2) AS avg_review_score
FROM "smart_logistics_dwh"."gold"."fact_orders" fo
LEFT JOIN "smart_logistics_dwh"."gold"."dim_date" dd
    ON fo.purchase_date_key = dd.date_key
LEFT JOIN "smart_logistics_dwh"."gold"."dim_geography" dg
    ON fo.geography_key = dg.geography_key
GROUP BY
    dd.full_date,
    dg.state;
    ')

EXEC('IF OBJECT_ID(''gold.agg_daily_logistics_summary__dbt_tmp'', ''U'') IS NOT NULL
                EXEC(''DROP TABLE "smart_logistics_dwh"."gold"."agg_daily_logistics_summary__dbt_tmp"'');SELECT * INTO "smart_logistics_dwh"."gold"."agg_daily_logistics_summary__dbt_tmp" FROM "smart_logistics_dwh"."gold"."agg_daily_logistics_summary__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS "gold"."agg_daily_logistics_summary__dbt_tmp__dbt_tmp_vw"')



    
    use [smart_logistics_dwh];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_agg_daily_logistics_summary__dbt_tmp_cci'
        AND object_id=object_id('"gold"."agg_daily_logistics_summary__dbt_tmp"')
    )
    DROP index "gold"."agg_daily_logistics_summary__dbt_tmp".gold_agg_daily_logistics_summary__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_agg_daily_logistics_summary__dbt_tmp_cci
    ON "gold"."agg_daily_logistics_summary__dbt_tmp"

   


  