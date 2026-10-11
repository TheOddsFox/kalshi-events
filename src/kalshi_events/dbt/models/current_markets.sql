with latest as (
    select * from {{ ref('observation_history') }}
    where entity_kind = 'markets' and source_kind = 'direct'
    qualify row_number() over (partition by entity_ticker order by received_at desc, observation_id desc) = 1
)
select m.entity_ticker as market_ticker, m.event_ticker, e.series_ticker,
       (e.event_ticker is not null and s.series_ticker is not null) as usable,
       m.observation_id, m.capture_id, m.received_at, m.provider_updated_at, m.source_endpoint,
       json_extract_string(m.payload_json, '$.status') as status,
       json_extract_string(m.payload_json, '$.result') as result,
       json_extract_string(m.payload_json, '$.settlement_ts') as settlement_ts,
       json_extract_string(m.payload_json, '$.rules_primary') as rules_primary,
       json_extract_string(m.payload_json, '$.rules_secondary') as rules_secondary,
       json_extract(m.payload_json, '$.price_ranges') as price_ranges,
       json_extract_string(m.payload_json, '$.price_level_structure') as price_level_structure,
       cast(json_extract_string(m.payload_json, '$.last_price_dollars') as decimal(38,18)) as last_price_dollars,
       cast(json_extract_string(m.payload_json, '$.volume_fp') as decimal(38,18)) as volume_fp,
       cast(json_extract_string(m.payload_json, '$.open_interest_fp') as decimal(38,18)) as open_interest_fp,
       cast(json_extract_string(m.payload_json, '$.settlement_value_dollars') as decimal(38,18)) as settlement_value_dollars,
       m.payload_json
from latest m
left join {{ ref('current_events') }} e on m.event_ticker = e.event_ticker
left join {{ ref('current_series') }} s on e.series_ticker = s.series_ticker
