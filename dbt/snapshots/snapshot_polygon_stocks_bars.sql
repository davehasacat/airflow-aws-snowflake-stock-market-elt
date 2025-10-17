{% snapshot snapshot_polygon_stocks_bars %}

{{
    config(
      target_schema='public',
      unique_key="ticker || '_' || trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select * from {{ source('public', 'source_polygon_stocks_raw') }}

{% endsnapshot %}
