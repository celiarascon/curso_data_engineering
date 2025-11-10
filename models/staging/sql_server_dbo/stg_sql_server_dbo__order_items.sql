{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_id,
        product_id,
        quantity,
        _fivetran_deleted AS _fivetran_deleted,
        CONVERT_TIMEZONE('UTC',_fivetran_synced) as utc_time
    FROM src_addresses
)    
SELECT * FROM renamed_casted