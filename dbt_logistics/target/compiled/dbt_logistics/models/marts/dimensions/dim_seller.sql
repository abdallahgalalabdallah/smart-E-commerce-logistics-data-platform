SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(seller_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM "smart_logistics_dwh"."staging"."stg_olist__sellers"