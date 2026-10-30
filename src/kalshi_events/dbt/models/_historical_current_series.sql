{{ config(enabled=var('historical_compact', false), materialized='table') }}

with listed as (
    select distinct entity_ticker as series_ticker
    from {{ source('landing', 'membership') }}
    where entity_kind = 'series' and scope = 'series'
),
required as (
    select distinct e.series_ticker
    from {{ ref('current_events') }} e
    where e.series_ticker is not null
      and not exists (
          select 1 from listed l where l.series_ticker = e.series_ticker
      )
      and exists (
          select 1 from {{ source('landing', 'parent_eligibility') }} p
          where p.capture_id = '{{ var("current_capture_id") }}'
            and p.entity_kind = 'series'
            and p.entity_ticker = e.series_ticker
            and p.status = 200
      )
),
eligible as (
    select series_ticker from listed
    union
    select series_ticker from required
),
latest as (
    select h.entity_ticker as series_ticker, h.observation_id, h.capture_id,
           h.received_at, h.provider_updated_at, h.source_endpoint
    from {{ ref('observation_history') }} h
    join eligible e on e.series_ticker = h.entity_ticker
    where h.entity_kind = 'series'
    qualify row_number() over (
        partition by h.entity_ticker order by h.received_at desc, h.observation_id desc
    ) = 1
)
select * from latest
