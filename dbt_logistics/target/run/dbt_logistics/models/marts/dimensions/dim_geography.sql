
  
    USE [smart_logistics_dwh];
    USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "gold"."dim_geography__dbt_tmp__dbt_tmp_vw" AS SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), coalesce(cast(geolocation_zip_code_prefix as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_'')), '''')), 2))
 AS geography_key,
    geolocation_zip_code_prefix AS zip_code_prefix,
    geolocation_lat AS latitude,
    geolocation_lng AS longitude,
    geolocation_city AS city,
    geolocation_state AS state
FROM "smart_logistics_dwh"."staging"."stg_olist__geolocation";
    ')

EXEC('IF OBJECT_ID(''gold.dim_geography__dbt_tmp'', ''U'') IS NOT NULL
                EXEC(''DROP TABLE "smart_logistics_dwh"."gold"."dim_geography__dbt_tmp"'');SELECT * INTO "smart_logistics_dwh"."gold"."dim_geography__dbt_tmp" FROM "smart_logistics_dwh"."gold"."dim_geography__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS "gold"."dim_geography__dbt_tmp__dbt_tmp_vw"')



    
    use [smart_logistics_dwh];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'gold_dim_geography__dbt_tmp_cci'
        AND object_id=object_id('"gold"."dim_geography__dbt_tmp"')
    )
    DROP index "gold"."dim_geography__dbt_tmp".gold_dim_geography__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX gold_dim_geography__dbt_tmp_cci
    ON "gold"."dim_geography__dbt_tmp"

   


  