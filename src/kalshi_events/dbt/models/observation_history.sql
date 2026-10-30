{% if var('historical_compact', false) %}
{{ config(materialized='view') }}
{% elif var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
{{ config(materialized='incremental', incremental_strategy='append') }}
{% endif %}
select observation_id, capture_id, entity_kind, entity_ticker, received_at,
       provider_updated_at, source_endpoint, source_kind, event_ticker, series_ticker,
       cast(payload_json as json) as payload_json
from {{ source('landing', 'observations') }}
{% if var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
where capture_id = '{{ var("delta_capture_id") }}'
{% endif %}
