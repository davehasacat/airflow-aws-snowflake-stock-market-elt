{% snapshot snapshot_polygon_options_bars %}

{{
    config(
      target_schema='public',
      unique_key="option_symbol || '_' || trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select * from {{ source('public', 'source_polygon_options_raw') }}

{% endsnapshot %}
