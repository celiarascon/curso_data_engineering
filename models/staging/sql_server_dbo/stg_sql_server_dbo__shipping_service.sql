{{
  config(
    materialized='view'
  )
}}

WITH src_shipping_service AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT DISTINCT
        md5(shipping_service) as shipping_service_id,
        shipping_service as shipping_service,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_shipping_service
)    
SELECT * FROM renamed_casted