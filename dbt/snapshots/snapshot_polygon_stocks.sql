{% snapshot snapshot_polygon_stocks %}

{{
    config(
      target_schema='snapshots',
      unique_key="ticker || '_' || polygon_trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select * from {{ source('raw', 'source_polygon_stocks_raw') }}

{% endsnapshot %}
