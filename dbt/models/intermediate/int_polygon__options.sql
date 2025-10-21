{{
    config(
        materialized='table',
        unique_key='option_bar_id'
    )
}}

with source as ( select * from {{ ref('stg_polygon__options') }} )

select * from source
