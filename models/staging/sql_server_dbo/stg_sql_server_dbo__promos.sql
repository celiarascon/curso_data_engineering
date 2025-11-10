
{{
  config(
    materialized='view'
  )
}}

WITH src_promo AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
        md5(promo_id) as promo_id,
        promo_id as promo_name,
        discount as discount_dollar,
       CASE 
            WHEN lower(trim(status)) = 'active' THEN true
            WHEN lower(trim(status)) = 'inactive' THEN false
            ELSE FALSE
        END AS status_bool,
        convert_timezone('UTC',_fivetran_synced) as utc_time,
        _fivetran_deleted
    FROM src_promo
    UNION ALL
    SELECT 
        md5('Sin promo') as promo_id,
        'Sin promo' as promo_name,
        0.0 as discount_dollar,
        false as status_bool,
        convert_timezone('UTC',CURRENT_DATE) as utc_time,
        NULL as _fivetran_deleted
    )

SELECT * FROM renamed_casted
