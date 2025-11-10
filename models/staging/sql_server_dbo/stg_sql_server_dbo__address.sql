{{
  config(
    materialized='view'
  )
}}

WITH src_address AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
        address_id as address_name,
        md5(address) as addresses_id,
        md5(zipcode) as zipcode_id,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_address
)    
SELECT * FROM renamed_casted