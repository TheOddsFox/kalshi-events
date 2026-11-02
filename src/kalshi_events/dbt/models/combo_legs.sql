{% if var('historical_combo_prepared', false) %}
{{ config(materialized='view') }}
select combo_market_ticker, leg_index, event_ticker, market_ticker, side,
       yes_settlement_value_dollars, resolved, payload_json
from {{ source('landing', 'combo_legs_canonical') }}
{% else %}
{% if var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
{{ config(materialized='incremental', incremental_strategy='append') }}
{% endif %}
{% if var('historical_compact', false) %}
with combo_sources as (
    select market_ticker,
           json_extract(payload_json, '$.mve_selected_legs') as selected_legs
    from {{ ref('current_markets') }}
    where json_extract(payload_json, '$.mve_selected_legs') is not null
)
{% endif %}
select m.market_ticker as combo_market_ticker,
       cast(leg.key as integer) as leg_index,
       json_extract_string(leg.value, '$.event_ticker') as event_ticker,
       json_extract_string(leg.value, '$.market_ticker') as market_ticker,
       json_extract_string(leg.value, '$.side') as side,
       cast(json_extract_string(leg.value, '$.yes_settlement_value_dollars') as decimal(38,18)) as yes_settlement_value_dollars,
       (target.market_ticker is not null and target.event_ticker = json_extract_string(leg.value, '$.event_ticker') and native_event.event_ticker is not null) as resolved,
       leg.value as payload_json
from {% if var('historical_compact', false) %}combo_sources{% else %}{{ ref('current_markets') }}{% endif %} m,
     lateral json_each({% if var('historical_compact', false) %}m.selected_legs{% else %}json_extract(m.payload_json, '$.mve_selected_legs'){% endif %}) leg
left join {{ ref('_historical_current_markets' if var('historical_compact', false) else 'current_markets') }} target
  on json_extract_string(leg.value, '$.market_ticker') = target.market_ticker
left join {{ ref('_historical_current_events' if var('historical_compact', false) else 'current_events') }} native_event
  on json_extract_string(leg.value, '$.event_ticker') = native_event.event_ticker
{% if var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
where m.market_ticker in (select market_ticker from {{ source('landing', 'affected_markets') }})
{% endif %}
{% endif %}
