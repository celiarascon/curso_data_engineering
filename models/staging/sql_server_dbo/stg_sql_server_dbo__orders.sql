{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
    nullif(trim(order_id), '') as order_id,
    nullif(trim(address_id), '') as address_id,
    nullif(trim(user_id), '') as user_id,
    shipping_cost as shipping_cost,
    nullif(trim(promo_id), '') as promo_name,
    md5(trim(shipping_service)) as shipping_service_id,
    md5(trim(status)) as status_id,
    order_cost as order_cost,
    nullif(trim(tracking_id), '') as tracking_id,
    convert_timezone('UTC', delivered_at) as delivered_at_utc,
    convert_timezone('UTC', created_at) as created_at_utc,
    convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_at_utc,
    _fivetran_deleted AS _fivetran_deleted,
    convert_timezone('UTC', _fivetran_synced) as utc_time
    FROM src_orders
)    
SELECT * FROM renamed_casted


