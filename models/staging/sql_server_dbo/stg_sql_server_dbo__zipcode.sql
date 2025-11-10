{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
        md5(zipcode) as zipcode_id,
        zipcode as zipcode_name,
        md5(state) as state_id,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_addresses
)    
SELECT * FROM renamed_casted
