{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        md5(event_id) as event_id,
        event_id as event_name_key,
        page_url as page_url,
        md5(trim(event_type)) as events_id,
        nullif(trim(user_id),'') as user_id,
        nullif(trim(session_id),'') as session_id,
        nullif(trim(product_id),'') as product_id,
        nullif(trim(order_id),'') as order_id,
        convert_timezone('UTC', created_at) as created_at_utc,
        _fivetran_deleted AS _fivetran_deleted,
        CONVERT_TIMEZONE('UTC',_fivetran_synced) as utc_time
    FROM src_events
)    
SELECT * FROM renamed_casted