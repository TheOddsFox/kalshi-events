select observation_id, capture_id, entity_kind, entity_ticker, received_at,
       provider_updated_at, source_endpoint, source_kind, event_ticker, series_ticker,
       cast(payload_json as json) as payload_json
from {{ source('landing', 'observations') }}
