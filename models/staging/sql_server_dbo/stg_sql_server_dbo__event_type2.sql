{{
  config(
    materialized='view'
  )
}}

WITH src_event_type2 AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        md5(trim(event_type)) as events_id,
        event_type as event_name,
        md5(trim(product_id)) as product_id,
        product_id as product_name_key,
    FROM src_event_type2
    WHERE event_type IN ('add_to_cart', 'page_view')
)    
SELECT * FROM renamed_casted