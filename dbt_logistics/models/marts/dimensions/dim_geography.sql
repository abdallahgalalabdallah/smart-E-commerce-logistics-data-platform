SELECT
    {{ dbt_utils.generate_surrogate_key(['geolocation_zip_code_prefix']) }} AS geography_key,
    geolocation_zip_code_prefix AS zip_code_prefix,
    geolocation_lat AS latitude,
    geolocation_lng AS longitude,
    geolocation_city AS city,
    geolocation_state AS state
FROM {{ ref('stg_olist__geolocation') }}