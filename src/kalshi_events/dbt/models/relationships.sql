{% if var('catalogue_version', 1) == 2 and var('incremental_update', false) %}
{{ config(materialized='incremental', incremental_strategy='append') }}
{% endif %}
select 'series' as parent_kind, e.series_ticker as parent_ticker,
       'events' as child_kind, e.event_ticker as child_ticker,
       s.series_ticker is not null as resolved
from {{ ref('current_events') }} e
left join {{ ref('current_series') }} s on e.series_ticker = s.series_ticker
{% if var('incremental_update', false) %}
where e.event_ticker in (select event_ticker from {{ source('landing', 'affected_events') }})
{% endif %}
union all
select 'events', event_ticker, 'markets', market_ticker, usable
from {{ ref('current_markets') }}
{% if var('incremental_update', false) %}
where market_ticker in (select market_ticker from {{ source('landing', 'affected_markets') }})
{% endif %}
