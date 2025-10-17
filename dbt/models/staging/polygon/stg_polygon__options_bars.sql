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
  open as open_price,
  high as high_price,
  low as low_price,
  close as close_price,
  volume,
  vwap as volume_weighted_average_price,
  transactions,
  inserted_at as loaded_at,
  raw_rec as raw_json_record
from source
),

parse_option_symbol as (
select 
  t1.*,

  -- underlying ticker: 1–6 uppercase letters
  regexp_substr(
    option_symbol,
    '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$',
    1, 1, 'c', 1
  ) as underlying_ticker,

  -- expiration date: yy mm dd → full date
  date_from_parts(
    2000 + to_number(regexp_substr(option_symbol, '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 2)),
            to_number(regexp_substr(option_symbol, '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 3)),
            to_number(regexp_substr(option_symbol, '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 4))
  ) as expiration_date,

  -- option type
  case regexp_substr(option_symbol, '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 5)
    when 'C' then 'call'
    when 'P' then 'put'
  end as option_type,

  -- strike price: 8 digits with 3 implied decimals
  to_number(
    regexp_substr(option_symbol, '^O:([A-Z]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 6)
  ) / 1000 as strike_price

from renamed_and_casted t1
)

select * from parse_option_symbol
