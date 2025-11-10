{{
  config(
    materialized='view'
  )
}}

WITH src_user AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        user_id as user_name_key,
        md5(user_id) as users_id,
        nullif(trim(first_name),'') as first_name,
        nullif(trim(last_name),'')  as last_name,
        nullif(trim(email),'')      as email,
        nullif(trim(phone_number),'') as phone_number,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_user
)    
SELECT * FROM renamed_casted
