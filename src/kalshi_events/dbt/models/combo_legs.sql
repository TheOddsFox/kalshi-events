select m.market_ticker as combo_market_ticker,
       cast(leg.key as integer) as leg_index,
       json_extract_string(leg.value, '$.event_ticker') as event_ticker,
       json_extract_string(leg.value, '$.market_ticker') as market_ticker,
       json_extract_string(leg.value, '$.side') as side,
       cast(json_extract_string(leg.value, '$.yes_settlement_value_dollars') as decimal(38,18)) as yes_settlement_value_dollars,
       (target.market_ticker is not null and target.event_ticker = json_extract_string(leg.value, '$.event_ticker') and native_event.event_ticker is not null) as resolved,
       leg.value as payload_json
from {{ ref('current_markets') }} m,
     lateral json_each(json_extract(m.payload_json, '$.mve_selected_legs')) leg
left join {{ ref('current_markets') }} target on json_extract_string(leg.value, '$.market_ticker') = target.market_ticker
left join {{ ref('current_events') }} native_event on json_extract_string(leg.value, '$.event_ticker') = native_event.event_ticker
