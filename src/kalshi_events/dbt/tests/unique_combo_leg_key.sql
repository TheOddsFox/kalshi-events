select combo_market_ticker, leg_index
from {{ ref('combo_legs') }}
group by combo_market_ticker, leg_index
having count(*) > 1
