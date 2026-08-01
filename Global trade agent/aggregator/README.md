# Aggregator Agent

Single source of truth for external tariff reference data — MFN rates,
FTA preferential rates, rules of origin, and active trade remedies.

## Architecture

```
config.yaml (aggregator: section)
       │
       ▼
AggregatorAgent.query(hs, origin, dest, date)
       │
       ├─► USHTSConnector.fetch()  ──┐
       ├─► WITSConnector.fetch()    │  RawRate[]  +  ConnectorError
       └─► [stub connectors]  ──────┘
                                     │
                                     ▼
                              Reconciler.reconcile()
                                     │
                                     ▼
                              CanonicalRate  ──► RateStore.upsert()
                                     │
                                     ▼
                              feed_adapter.to_feed_entry()
                                     │
                                     ▼
                         {id, timestamp, headline, detail, status, source}
```

The scheduler wraps `AggregatorAgent` and calls `query()` on each configured
lane at the configured interval, diffing results and notifying subscribers.

## Public interface

### `AggregatorAgent`

```python
from aggregator.aggregator_agent import AggregatorAgent
from datetime import date

agent = AggregatorAgent(db_path="aggregator/data/rates.db")
rate  = agent.query("8471.30", "VN", "US", date.today())
# rate: CanonicalRate | None
```

**`query(hs_code, origin, destination, effective_date)`**
- Runs all enabled connectors.  `ConnectorError` from any single connector is
  caught and recorded — it never aborts the whole query.
- Passes results + errors to the `Reconciler`.
- Upserts the `CanonicalRate` to the SQLite store.
- Returns `None` only when every connector produced no data for this lane.

### `RateScheduler`

```python
from aggregator.scheduler import RateScheduler

def on_change(new: CanonicalRate, old: CanonicalRate | None) -> None:
    print(f"Rate changed: {new.summary}")

scheduler = RateScheduler(agent)
scheduler.subscribe(on_change)
scheduler.start()          # non-blocking background thread
scheduler.refresh_now()    # force immediate refresh (optional)
scheduler.stop()
```

### Feed adapter

```python
from aggregator.feed_adapter import to_feed_entry

entry = to_feed_entry(rate)
# entry: {id, timestamp, time_short, headline, detail, status, source}
# Drop-in compatible with data_simulator.get_tariff_feed() item shape.
```

## Configuration (`config.yaml`, `aggregator:` section)

| Key | Description |
|---|---|
| `sources.<name>.enabled` | Whether to instantiate and query this connector |
| `sources.<name>.authoritative_for` | ISO alpha-2 destinations this source owns |
| `precedence.<name>` | Integer; higher = chosen first on disagreement |
| `confidence.*` | Named bands read by the reconciler (0.0–1.0) |
| `store.db_path` | SQLite file path for the rate store |
| `refresh.interval_hours` | How often the scheduler re-fetches configured lanes |
| `refresh.staleness_threshold_days` | Days before a failed lane is marked `is_stale` |
| `refresh.lanes` | List of `{hs_code, origin, destination}` dicts to refresh |

## Confidence bands

| Band | Typical value | Condition |
|---|---|---|
| `authoritative_present` | 0.90 | Auth source answered (alone or corroborated) |
| `non_authoritative_agree` | 0.75 | Multiple non-auth agree; auth unreachable |
| `fallback_only` | 0.50 | Single non-auth source; auth unavailable or silent |
| `sources_disagree` | 0.45 | Concrete values from multiple sources conflict |

## Data flow notes

- **Silence vs disagreement**: a source returning `mfn_rate=None` is not in
  conflict with a source that has a value.  Only two concrete, comparable
  values that differ constitute a disagreement.
- **MFN backfill**: if the authoritative winner has no MFN data but a
  non-auth source does, the MFN is backfilled and confidence is clamped to
  `fallback_only` (the auth source contributed no rate value).
- **Preferential backfill**: if the winner has `preferential_rate=None` but
  another source has an FTA rate, it is merged in without lowering confidence.
- **US HTS cross-reference caveat**: `special=""` may mean "no FTA" OR "FTA
  exists but rate is in a See 99xx.xx quota-schedule cross-reference".  A
  note is added to `disagreement_details` so consumers do not assume certainty.

## Connector status (Phase 1)

| Connector | Status | Notes |
|---|---|---|
| US HTS | Live | Verified against USITC public API 2025-07-31 |
| WITS | Blocked | 403 from corporate network; field names unverified |
| EU TARIC | Stub | Endpoint TBD |
| MacMap | Stub | Credentials required |
| SAP GTS | Stub | Client-licensed; production only |

## Running tests

```bash
python -m pytest aggregator/tests/ -v
```

All tests are offline.  US HTS HTTP calls are mocked via `unittest.mock.patch`.
WITS mocks are labeled **unverified** — the response shape is documentation-
based, not verified against a live response.

## Store threading

The `RateStore` is thread-safe for the production file-backed case: each
`_connect()` call opens its own `sqlite3.Connection`, so the scheduler
thread and query threads never share a connection.  SQLite's WAL serialises
concurrent writers.

For `:memory:` (tests only), a single persistent connection is shared with
`check_same_thread=False` and a `threading.Lock` protecting each operation.
