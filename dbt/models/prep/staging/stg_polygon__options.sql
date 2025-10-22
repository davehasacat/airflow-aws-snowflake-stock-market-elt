{{
    config(
        materialized='table',
        unique_key='option_bar_id'
    )
}}

with source as (
    select * from {{ ref('snapshot_polygon_options') }} where dbt_valid_to is null
),

renamed_and_casted as (
select
  option_symbol || '_' || polygon_trade_date as option_bar_id,    -- use as primary key
  option_symbol,
  polygon_trade_date,
  "open" as open_price,
  high as high_price,
  low as low_price,
  "close" as close_price,
  volume,
  vwap as volume_weighted_average_price,
  transactions,
  inserted_at as loaded_at,
from source
),

parse_option_symbol as (
select 
  t1.*,

  -- quick flag: does it match the OCC pattern (1–6 alnum root)?
  regexp_like(option_symbol, '^O:[A-Z0-9]{1,6}\\d{6}[CP]\\d{8}$') as is_parsable,

  -- underlying ticker: 1–6 uppercase letters or digits
  regexp_substr(
    option_symbol,
    '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 1
  ) as underlying_ticker,

  -- expiration date: yy mm dd → full date
  date_from_parts(
    2000 + to_number(regexp_substr(option_symbol, '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 2)),
            to_number(regexp_substr(option_symbol, '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 3)),
            to_number(regexp_substr(option_symbol, '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 4))
  ) as expiration_date,

  -- option type
  case regexp_substr(option_symbol, '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 5)
    when 'C' then 'call'
    when 'P' then 'put'
  end as option_type,

  -- strike price: 8 digits with 3 implied decimals
  to_number(
    regexp_substr(option_symbol, '^O:([A-Z0-9]{1,6})(\\d{2})(\\d{2})(\\d{2})([CP])(\\d{8})$', 1, 1, 'c', 6)
  ) / 1000 as strike_price

from renamed_and_casted t1
)

select * from parse_option_symbol
