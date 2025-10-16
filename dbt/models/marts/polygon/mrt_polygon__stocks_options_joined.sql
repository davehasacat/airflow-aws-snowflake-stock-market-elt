{{
    config(
        materialized='incremental',
        unique_key='option_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('int_polygon__stocks_options_joined') }}
)

select * from source
