{% if var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
{{ config(materialized='incremental', incremental_strategy='append') }}
{% endif %}
{% if var('catalogue_version', 1) == 2 %}
with listed as (
    select distinct entity_ticker as event_ticker
    from {{ source('landing', 'membership') }}
    where entity_kind = 'events' and scope in ('events_live', 'events_combo')
),
latest_market_parents as (
    select entity_ticker as market_ticker, event_ticker
    from {{ ref('observation_history') }}
    where entity_kind = 'markets' and source_kind = 'direct'
    qualify row_number() over (
        partition by entity_ticker order by received_at desc, observation_id desc
    ) = 1
),
required as (
    select distinct m.event_ticker
    from {{ source('landing', 'membership') }} mm
    join latest_market_parents m on m.market_ticker = mm.entity_ticker
    where mm.entity_kind = 'markets'
      and mm.scope in ('markets_live', 'markets_combo', 'markets_history')
      and not exists (
          select 1 from listed l where l.event_ticker = m.event_ticker
      )
      and exists (
          select 1 from {{ source('landing', 'parent_eligibility') }} p
          where p.capture_id = '{{ var("current_capture_id") }}'
            and p.entity_kind = 'events'
            and p.entity_ticker = m.event_ticker
            and p.status = 200
      )
),
eligible as (
    select event_ticker from listed
    union
    select event_ticker from required
),
latest as (
    select h.entity_ticker as event_ticker, h.event_ticker as native_event_ticker,
           h.series_ticker, h.observation_id, h.capture_id, h.received_at,
           h.provider_updated_at, h.source_endpoint, h.payload_json
    from {{ ref('observation_history') }} h
    join eligible e on e.event_ticker = h.entity_ticker
    where h.entity_kind = 'events'
    {% if var('incremental_update', false) %}
      and h.entity_ticker in (select event_ticker from {{ source('landing', 'affected_events') }})
    {% endif %}
    qualify row_number() over (
        partition by h.entity_ticker order by h.received_at desc, h.observation_id desc
    ) = 1
)
select event_ticker, series_ticker, observation_id, capture_id, received_at,
       provider_updated_at, source_endpoint, payload_json
from latest
{% else %}
select entity_ticker as event_ticker, series_ticker, observation_id, capture_id, received_at,
       provider_updated_at, source_endpoint, payload_json
from {{ ref('observation_history') }}
where entity_kind = 'events'
qualify row_number() over (partition by entity_ticker order by received_at desc, observation_id desc) = 1
{% endif %}
