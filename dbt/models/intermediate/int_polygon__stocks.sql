{{
    config(
        materialized='table',
        unique_key='stock_bar_id'
    )
}}

with source as ( select * from {{ ref('stg_polygon__stocks') }} )

select * from source
