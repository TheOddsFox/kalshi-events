select entity_ticker as series_ticker, observation_id, capture_id, received_at,
       provider_updated_at, source_endpoint, payload_json
from {{ ref('observation_history') }}
where entity_kind = 'series'
qualify row_number() over (partition by entity_ticker order by received_at desc, observation_id desc) = 1
