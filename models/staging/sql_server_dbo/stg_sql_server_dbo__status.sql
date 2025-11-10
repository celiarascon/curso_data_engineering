{{
  config(
    materialized='view'
  )
}}

WITH src_status AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT DISTINCT
        md5(status) as status_id,
        status as status_name,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_status
)    
SELECT * FROM renamed_casted