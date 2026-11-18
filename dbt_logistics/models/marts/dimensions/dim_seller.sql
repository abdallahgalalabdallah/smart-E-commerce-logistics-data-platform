SELECT
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ ref('stg_olist__sellers') }}