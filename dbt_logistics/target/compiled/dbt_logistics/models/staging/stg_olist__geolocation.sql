SELECT
    CAST(geolocation_zip_code_prefix AS VARCHAR(10)) AS geolocation_zip_code_prefix,
    CAST(geolocation_lat AS FLOAT) AS geolocation_lat,
    CAST(geolocation_lng AS FLOAT) AS geolocation_lng,
    TRIM(CAST(geolocation_city AS VARCHAR(100))) AS geolocation_city,
    UPPER(TRIM(CAST(geolocation_state AS VARCHAR(10)))) AS geolocation_state
FROM "smart_logistics_dwh"."silver"."geolocation"