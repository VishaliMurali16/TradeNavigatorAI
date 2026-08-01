"""
Feed adapter — maps a CanonicalRate to the get_tariff_feed() dict shape.

The UI feed (data_simulator.get_tariff_feed) expects dicts with keys:
    {id, timestamp, time_short, headline, detail, status, source}

This module provides a thin, pure-function adapter so a later wiring step
can inject live CanonicalRate records into the existing feed without
modifying app.py or data_simulator.py.

Field mapping
-------------
    id         <- f"{rate.hs6}_{rate.origin}_{rate.destination}_{rate.effective_date}"
    timestamp  <- rate.fetched_at, formatted "%H:%M UTC"
    time_short <- rate.fetched_at, formatted "%H:%M"
    headline   <- rate.summary          (one-line human description)
    detail     <- rate.detail_summary   (RoO, remedies, disagreement note)
    status     <- rate.feed_status      ('cleared' | 'issued')
    source     <- rate.best_source      (authoritative connector name)

Note on the UI
--------------
The current frontend (app.py) completely replaces feedContainer.innerHTML on
every poll — it does not de-duplicate by id or key on it in the DOM at all.
The id is therefore inert for the existing UI, but it is the right stable
unique key for any external consumer (JSON API clients, future SSE streams,
analytics) that needs to correlate or de-duplicate rate entries across polls.
Including effective_date prevents two rates for the same lane on different
dates from appearing identical to those consumers.
"""

from __future__ import annotations

from aggregator.models import CanonicalRate


def to_feed_entry(rate: CanonicalRate) -> dict:
    """Convert a CanonicalRate into a feed dict compatible with get_tariff_feed().

    The returned dict is a plain Python dict and can be appended to the feed
    list returned by get_tariff_feed() or used as a drop-in replacement.
    """
    ts = rate.fetched_at
    return {
        "id": f"{rate.hs6}_{rate.origin}_{rate.destination}_{rate.effective_date}",
        "timestamp": ts.strftime("%H:%M UTC"),
        "time_short": ts.strftime("%H:%M"),
        "headline": rate.summary,
        "detail": rate.detail_summary,
        "status": rate.feed_status,
        "source": rate.best_source,
    }
