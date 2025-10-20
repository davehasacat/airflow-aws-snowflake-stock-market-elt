{{
    config(
        materialized='table',
        unique_key='option_bar_id'
    )
}}

with source as (
    select * from {{ ref('snapshot_polygon_options') }} where dbt_valid_to is null
)

select * from source
