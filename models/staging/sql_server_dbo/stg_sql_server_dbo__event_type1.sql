{{
  config(
    materialized='view'
  )
}}

WITH src_event_type1 AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        md5(trim(event_type)) as events_id,
        event_type as event_name,
        md5(trim(order_id)) as order_id,
        order_id as order_name_key,
    FROM src_event_type1
    WHERE event_type IN ('checkout', 'package_shipped')
)    
SELECT * FROM renamed_casted