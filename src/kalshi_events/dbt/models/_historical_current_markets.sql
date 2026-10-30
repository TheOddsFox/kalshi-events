{{ config(enabled=var('historical_compact', false), materialized='table') }}

with eligible as (
    select distinct entity_ticker as market_ticker
    from {{ source('landing', 'membership') }}
    where entity_kind = 'markets'
      and scope in ('markets_live', 'markets_combo', 'markets_history')
),
latest as (
    select h.entity_ticker, h.event_ticker, h.observation_id, h.capture_id,
           h.received_at, h.provider_updated_at, h.source_endpoint
    from {{ ref('observation_history') }} h
    join eligible e on e.market_ticker = h.entity_ticker
    where h.entity_kind = 'markets' and h.source_kind = 'direct'
    qualify row_number() over (
        partition by h.entity_ticker order by h.received_at desc, h.observation_id desc
    ) = 1
)
select m.entity_ticker as market_ticker, m.event_ticker, e.series_ticker,
       (e.event_ticker is not null and s.series_ticker is not null) as usable,
       m.observation_id, m.capture_id, m.received_at, m.provider_updated_at,
       m.source_endpoint
from latest m
left join {{ ref('current_events') }} e on m.event_ticker = e.event_ticker
left join {{ ref('current_series') }} s on e.series_ticker = s.series_ticker
