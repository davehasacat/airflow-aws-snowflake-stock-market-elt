{{
    config(
        materialized='table',
        unique_key='option_bar_id'
    )
}}

with source as (
    select * from {{ ref('snapshot_polygon_options_bars') }} where dbt_valid_to is null
),

renamed_and_casted as (
select
  option_symbol || '_' || trade_date as option_bar_id,    -- use as primary key
  option_symbol,
  trade_date,
  strike_price,
  option_type,
  open as open_price,
  high as high_price,
  low as low_price,
  close as close_price,
  volume,
  vwap as volume_weighted_average_price,
  transactions,
  inserted_at as loaded_at
  raw_rec as raw_json_record
from source
)

select * from renamed_and_casted
