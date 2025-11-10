{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        trim(user_id) as user_name_key,
        md5(trim(user_id)) as user_id,
        convert_timezone('UTC', updated_at) as updated_at_utc,
        convert_timezone('UTC', created_at) as created_at_utc,
        nullif(trim(address_id),'') as address_name,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_users
)    
SELECT * FROM renamed_casted