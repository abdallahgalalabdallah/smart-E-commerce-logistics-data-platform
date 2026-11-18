
  
    USE [smart_logistics_dwh];
    USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "gold"."dim_product__dbt_tmp__dbt_tmp_vw" AS SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), coalesce(cast(p.product_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_'')), '''')), 2))
 AS product_key,
    p.product_id,
    COALESCE(ct.product_category_name_english, p.product_category_name, ''unknown'') AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_photos_qty
FROM "smart_logistics_dwh"."staging"."stg_olist__products" p
LEFT JOIN "smart_logistics_dwh"."staging"."stg_olist__category_translation" ct
    ON p.product_category_name = ct.product_category_name;
    ')

EXEC('IF OBJECT_ID(''gold.dim_product__dbt_tmp'', ''U'') IS NOT NULL
                EXEC(''DROP TABLE "smart_logistics_dwh"."gold"."dim_product__dbt_tmp"'');SELECT * INTO "smart_logistics_dwh"."gold"."dim_product__dbt_tmp" FROM "smart_logistics_dwh"."gold"."dim_product__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS "gold"."dim_product__dbt_tmp__dbt_tmp_vw"')



    
    use [smart_logistics_dwh];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_dim_product__dbt_tmp_cci'
        AND object_id=object_id('"gold"."dim_product__dbt_tmp"')
    )
    DROP index "gold"."dim_product__dbt_tmp".gold_dim_product__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_dim_product__dbt_tmp_cci
    ON "gold"."dim_product__dbt_tmp"

   


  