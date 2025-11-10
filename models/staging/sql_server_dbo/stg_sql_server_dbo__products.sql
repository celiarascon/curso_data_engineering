{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
        trim(product_id) as product_name_key,
        md5(trim(product_id)) as product_id,
        -- hay que HIstorificar el precio con snapshot asiq crear tabla aparte con precio, nombre y fecha del precio
        --price::decimal(18,2) as price_dollar,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_products
)    
SELECT * FROM renamed_casted


