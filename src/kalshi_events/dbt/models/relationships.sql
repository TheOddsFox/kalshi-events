select 'series' as parent_kind, e.series_ticker as parent_ticker,
       'events' as child_kind, e.event_ticker as child_ticker,
       s.series_ticker is not null as resolved
from {{ ref('current_events') }} e
left join {{ ref('current_series') }} s on e.series_ticker = s.series_ticker
union all
select 'events', event_ticker, 'markets', market_ticker, usable
from {{ ref('current_markets') }}
