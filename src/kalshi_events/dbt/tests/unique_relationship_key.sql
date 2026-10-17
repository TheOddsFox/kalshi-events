select parent_kind, parent_ticker, child_kind, child_ticker
from {{ ref('relationships') }}
group by parent_kind, parent_ticker, child_kind, child_ticker
having count(*) > 1
