{% snapshot snapshot_polygon_options %}

{{
    config(
      target_schema='snapshots',
      unique_key="option_symbol || '_' || polygon_trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select * from {{ source('raw', 'source_polygon_options_raw') }}

{% endsnapshot %}
