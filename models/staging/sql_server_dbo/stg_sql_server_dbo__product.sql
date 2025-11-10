{{
  config(
    materialized='view'
  )
}}

WITH src_product AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
        trim(product_id) as product_name_key,
        md5(trim(product_id)) as products_id,
        nullif(trim(name),'') as name,
        -- snapshot para historificarlo 
        -- inventory,
        _fivetran_deleted AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) as utc_time
    FROM src_product
)    
SELECT * FROM renamed_casted