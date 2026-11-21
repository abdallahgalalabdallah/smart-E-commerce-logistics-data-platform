SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(geolocation_zip_code_prefix as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS geography_key,
    geolocation_zip_code_prefix AS zip_code_prefix,
    geolocation_lat AS latitude,
    geolocation_lng AS longitude,
    geolocation_city AS city,
    geolocation_state AS state
FROM "smart_logistics_dwh"."staging"."stg_olist__geolocation"