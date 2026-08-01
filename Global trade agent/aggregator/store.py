"""
SQLite-backed rate store for CanonicalRate records.

Key: (hs6, origin, destination, effective_date) — all in canonical form.
    hs6          : first 6 significant digits of any HS code (dots stripped)
    origin       : ISO 3166-1 alpha-2 (stored and queried case-insensitively)
    destination  : ISO 3166-1 alpha-2
    effective_date: ISO date string YYYY-MM-DD

The full CanonicalRate is stored as a Pydantic JSON payload, so the schema
is forward-compatible as new fields are added to the model.  A separate
fetched_at column is kept for staleness queries without decoding the payload.

Threading model
---------------
File-backed (production):
    Each _connect() call opens its own sqlite3.Connection and closes it on
    exit.  Multiple threads each hold a private connection; no Python-level
    lock is needed.  SQLite's WAL (or journal) serialises concurrent writers
    at the file level; timeout=10 retries for up to 10 s before raising
    OperationalError on a write collision.

In-memory (':memory:', tests only):
    sqlite3.connect(':memory:') creates a brand-new empty database on every
    call, so a single persistent connection is kept instead.  Two risks arise:
      1. check_same_thread=True (default) would block cross-thread use.
      2. Interleaved commits from two threads would corrupt the shared state.
    Both are addressed: check_same_thread=False is passed at creation, and a
    threading.Lock serialises every _connect() call so only one thread
    operates on the connection at a time.  Tests are single-threaded today,
    so the lock is never contested; it is defensive for future use.

Usage
-----
    store = RateStore("aggregator/data/rates.db")
    store.upsert(canonical_rate)
    rate = store.get("8471.30", "VN", "US", date(2025, 1, 15))
    rate = store.get("847130",  "VN", "US", date(2025, 1, 15))  # same row — hs6 key
"""

from __future__ import annotations

import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date
from typing import Generator

from aggregator.models import CanonicalRate

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS canonical_rates (
    hs6            TEXT NOT NULL,
    origin         TEXT NOT NULL,
    destination    TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    payload        TEXT NOT NULL,
    fetched_at     TEXT NOT NULL,
    PRIMARY KEY (hs6, origin, destination, effective_date)
)
"""

_UPSERT = """
INSERT OR REPLACE INTO canonical_rates
    (hs6, origin, destination, effective_date, payload, fetched_at)
VALUES (?, ?, ?, ?, ?, ?)
"""

_SELECT = """
SELECT payload FROM canonical_rates
WHERE hs6 = ? AND origin = ? AND destination = ? AND effective_date = ?
"""

_LIST_RECENT = """
SELECT payload FROM canonical_rates
ORDER BY fetched_at DESC
LIMIT ?
"""


def _to_hs6(hs_code: str) -> str:
    """Return the first 6 significant digits of any HS code format (dots stripped)."""
    return re.sub(r"[^\d]", "", hs_code)[:6]


class RateStore:
    """Persistent cache of CanonicalRate records backed by SQLite."""

    def __init__(self, db_path: str) -> None:
        """
        Parameters
        ----------
        db_path : filesystem path to the SQLite database file,
                  or ':memory:' for an in-process in-memory database.
        """
        self._db_path = db_path
        if db_path == ":memory:":
            # Persistent connection so schema and data survive across calls.
            # check_same_thread=False allows cross-thread use; the lock below
            # ensures only one thread operates on it at a time.
            self._mem_conn: sqlite3.Connection | None = sqlite3.connect(
                ":memory:", check_same_thread=False
            )
            self._mem_lock: threading.Lock | None = threading.Lock()
        else:
            self._mem_conn = None
            self._mem_lock = None
        self._init_db()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> CanonicalRate | None:
        """Retrieve the stored CanonicalRate for a lane, or None if absent.

        hs_code may be in any valid format; the lookup always uses hs6.
        """
        key = self._key(hs_code, origin, destination, effective_date)
        with self._connect() as conn:
            row = conn.execute(_SELECT, key).fetchone()
        if row is None:
            return None
        return CanonicalRate.model_validate_json(row[0])

    def list_recent(self, limit: int) -> list[CanonicalRate]:
        """Return the most recently fetched rates, newest first, up to limit.

        Used by the feed adapter path in app.py — reads from the on-disk store
        so the Flask route thread sees rates written by the scheduler thread.
        """
        with self._connect() as conn:
            rows = conn.execute(_LIST_RECENT, (limit,)).fetchall()
        return [CanonicalRate.model_validate_json(row[0]) for row in rows]

    def upsert(self, rate: CanonicalRate) -> None:
        """Insert or replace the CanonicalRate for its lane.

        The primary key is (hs6, origin, destination, effective_date).
        Subsequent upserts overwrite the previous record completely.
        """
        with self._connect() as conn:
            conn.execute(
                _UPSERT,
                (
                    rate.hs6,
                    rate.origin.upper(),
                    rate.destination.upper(),
                    str(rate.effective_date),
                    rate.model_dump_json(),
                    str(rate.fetched_at),
                ),
            )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(_CREATE_TABLE)

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        if self._mem_conn is not None:
            # In-memory: serialise all access on the single shared connection.
            assert self._mem_lock is not None
            with self._mem_lock:
                yield self._mem_conn
                self._mem_conn.commit()
        else:
            # File-backed: private connection per call; threads never share
            # a connection object, so no Python-level lock is needed.
            #
            # WAL (Write-Ahead Logging): readers and one writer proceed
            # concurrently — readers never block writers and vice versa.
            # The default journal_mode=DELETE takes an exclusive lock that
            # blocks all readers while a writer holds it, which is the
            # "database is locked" failure in the scheduler-writes/API-reads
            # scenario.  WAL eliminates that.
            #
            # busy_timeout via PRAGMA (not the connect `timeout` kwarg): when
            # a second writer races with an active writer, SQLite retries for
            # up to 10 000 ms before raising OperationalError.  The connect-
            # kwarg `timeout` only applies during the open handshake; the
            # PRAGMA is the correct SQLite-level knob for post-open contention.
            conn = sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _key(
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> tuple[str, str, str, str]:
        return (
            _to_hs6(hs_code),
            origin.upper(),
            destination.upper(),
            str(effective_date),
        )
