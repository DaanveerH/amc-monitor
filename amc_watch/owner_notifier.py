"""Durable owner incident notifier that remains useful during database outages."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import signal
import socket
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit, urlunsplit

from .owner_delivery import (
    DeliveryDisposition,
    DeliveryResult,
    DiscordOwnerWebhook,
    sanitize_owner_text,
)


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
_SEVERITIES = {"info", "warning", "critical"}
DEFAULT_SERVICES = ("amc-worker", "amc-discord-bot")
_DEPLOYMENT_PHASES = {
    "migration": ("deployment.migration", "Database migration"),
    "cutover": ("deployment.cutover", "Production cutover"),
}


def _unix_now() -> float:
    return time.time()


def _utc_display(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _validate_identifier(value: str, label: str) -> str:
    value = value.strip()
    if not _SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} contains unsupported characters")
    return value


def psycopg_connection_url(database_url: str) -> str:
    """Remove SQLAlchemy's driver suffix for direct psycopg connections."""

    parsed = urlsplit(database_url)
    if parsed.scheme == "postgresql+psycopg":
        return urlunsplit(
            ("postgresql", parsed.netloc, parsed.path, parsed.query, parsed.fragment)
        )
    if parsed.scheme == "postgres+psycopg":
        return urlunsplit(
            ("postgres", parsed.netloc, parsed.path, parsed.query, parsed.fragment)
        )
    return database_url


@dataclass(frozen=True)
class Heartbeat:
    service_name: str
    observed_at: float
    status: str = "healthy"


class HeartbeatSource(Protocol):
    def fetch(self, service_names: Sequence[str]) -> Sequence[Heartbeat]: ...


class HeartbeatSink(Protocol):
    def touch(self, service_name: str, *, now: float) -> None: ...


class IncidentBridge(Protocol):
    def sync(self, ledger: "IncidentLedger", *, now: float) -> int: ...


class PsycopgHeartbeatSource:
    """Read the deliberately narrow service-heartbeat view from PostgreSQL."""

    def __init__(self, database_url: str) -> None:
        self._database_url = psycopg_connection_url(database_url)
        self._instance_id = f"{socket.gethostname()}:{os.getpid()}"

    def fetch(self, service_names: Sequence[str]) -> Sequence[Heartbeat]:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment preflight catches it
            raise RuntimeError("psycopg is unavailable") from exc

        query = """
            SELECT service_name, last_seen_at, status
              FROM service_heartbeats
             WHERE service_name = ANY(%s)
        """
        with psycopg.connect(self._database_url, connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (list(service_names),))
                rows = cursor.fetchall()
        heartbeats: list[Heartbeat] = []
        for name, observed_at, status in rows:
            if isinstance(observed_at, datetime):
                if observed_at.tzinfo is None:
                    observed_at = observed_at.replace(tzinfo=timezone.utc)
                timestamp = observed_at.timestamp()
            else:
                timestamp = float(observed_at)
            heartbeats.append(Heartbeat(str(name), timestamp, str(status or "unknown")))
        return heartbeats

    def touch(self, service_name: str, *, now: float) -> None:
        observed_at = datetime.fromtimestamp(now, timezone.utc)
        expires_at = datetime.fromtimestamp(now + 120, timezone.utc)
        query = """
            INSERT INTO service_heartbeats (
                service_name, instance_id, status, details,
                started_at, last_seen_at, expires_at
            ) VALUES (%s, %s, 'healthy', '{}'::jsonb, %s, %s, %s)
            ON CONFLICT (service_name) DO UPDATE SET
                instance_id = EXCLUDED.instance_id,
                status = EXCLUDED.status,
                details = EXCLUDED.details,
                last_seen_at = EXCLUDED.last_seen_at,
                expires_at = EXCLUDED.expires_at
        """
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment preflight catches it
            raise RuntimeError("psycopg is unavailable") from exc
        with psycopg.connect(self._database_url, connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        service_name,
                        self._instance_id,
                        observed_at,
                        observed_at,
                        expires_at,
                    ),
                )


class PsycopgIncidentBridge:
    """Move shared owner incidents into the DB-independent local outbox.

    PostgreSQL rows are acknowledged only after SQLite commits. If the process
    dies before the acknowledgement, the lease expires and the deterministic
    local incident key makes the replay a no-op.
    """

    def __init__(
        self,
        database_url: str,
        *,
        claimant: str | None = None,
        batch_size: int = 50,
        lease_seconds: int = 120,
    ) -> None:
        self._database_url = psycopg_connection_url(database_url)
        default_claimant = f"{socket.gethostname()}:{os.getpid()}"
        self._claimant = sanitize_owner_text(claimant or default_claimant, limit=100)
        self._batch_size = max(1, min(int(batch_size), 100))
        self._lease_seconds = max(30, int(lease_seconds))

    @staticmethod
    def _local_key(value: object) -> str:
        raw = str(value or "unknown")
        if _SAFE_IDENTIFIER.fullmatch(raw):
            return raw
        digest = hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:32]
        return f"database-incident:{digest}"

    @staticmethod
    def _local_source(value: object) -> str:
        source = re.sub(r"[^A-Za-z0-9_.:-]+", "-", str(value or "amc-worker"))[:120]
        return source.strip("-") or "amc-worker"

    def _claim(self, connection: Any, now: datetime) -> list[tuple[Any, ...]]:
        query = """
            SELECT o.id, o.event_type, i.incident_key, i.incident_type,
                   o.payload, o.created_at
              FROM owner_outbox AS o
              JOIN owner_incidents AS i ON i.id = o.incident_id
             WHERE (
                       (o.status = 'pending' AND o.available_at <= %s)
                    OR (o.status = 'sending' AND o.lease_expires_at <= %s)
                   )
             ORDER BY o.available_at, o.created_at
             FOR UPDATE OF o SKIP LOCKED
             LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(query, (now, now, self._batch_size))
            rows = list(cursor.fetchall())
            if rows:
                cursor.executemany(
                    """
                    UPDATE owner_outbox
                       SET status = 'sending', claimed_by = %s,
                           lease_expires_at = %s, attempts = attempts + 1
                     WHERE id = %s
                    """,
                    [
                        (
                            self._claimant,
                            datetime.fromtimestamp(
                                now.timestamp() + self._lease_seconds, timezone.utc
                            ),
                            row[0],
                        )
                        for row in rows
                    ],
                )
        connection.commit()
        return rows

    def _apply_event(
        self,
        ledger: "IncidentLedger",
        *,
        event_type: object,
        incident_key: object,
        incident_type: object,
        payload: object,
        event_at: float,
    ) -> None:
        """Apply one immutable PostgreSQL outbox event to the local ledger.

        Incident rows are mutable and may already describe a later recovery or
        reopen by the time the notifier leases an older outbox row. Delivery
        semantics therefore come exclusively from the leased row's event type
        and payload snapshot.
        """

        if not isinstance(payload, Mapping):
            raise ValueError("owner incident payload must be an object")
        event = str(event_type)
        key = self._local_key(incident_key)
        summary = sanitize_owner_text(payload.get("summary"), limit=300)
        if event == "recovery":
            ledger.resolve(
                key,
                now=event_at,
                summary=summary or "Service incident recovered",
                preserve_pending_opening=True,
            )
            return
        if event not in {"opened", "reminder"}:
            raise ValueError("unsupported event type")

        normalized_severity = str(payload.get("severity") or "warning").lower()
        if normalized_severity not in _SEVERITIES:
            normalized_severity = "warning"
        ledger.report(
            key,
            severity=normalized_severity,
            source=self._local_source(payload.get("resource")),
            summary=summary or "Service incident reported",
            details=(
                "Incident type: "
                + sanitize_owner_text(incident_type, limit=80)
            ),
            now=event_at,
        )

    def _acknowledge(self, connection: Any, outbox_id: Any, now: datetime) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE owner_outbox
                   SET status = 'delivered', delivered_at = %s,
                       lease_expires_at = NULL, last_error_code = NULL
                 WHERE id = %s AND status = 'sending' AND claimed_by = %s
                """,
                (now, outbox_id, self._claimant),
            )
        connection.commit()

    def _reject(self, connection: Any, outbox_id: Any, error_code: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE owner_outbox
                   SET status = 'dead', lease_expires_at = NULL,
                       last_error_code = %s
                 WHERE id = %s AND status = 'sending' AND claimed_by = %s
                """,
                (error_code[:100], outbox_id, self._claimant),
            )
        connection.commit()

    def sync(self, ledger: "IncidentLedger", *, now: float) -> int:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment preflight catches it
            raise RuntimeError("psycopg is unavailable") from exc

        observed_at = datetime.fromtimestamp(now, timezone.utc)
        imported = 0
        with psycopg.connect(self._database_url, connect_timeout=10) as connection:
            rows = self._claim(connection, observed_at)
            for (
                outbox_id,
                event_type,
                incident_key,
                incident_type,
                payload,
                created_at,
            ) in rows:
                try:
                    if isinstance(created_at, datetime):
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                        event_at = created_at.timestamp()
                    else:
                        event_at = float(created_at)
                    self._apply_event(
                        ledger,
                        event_type=event_type,
                        incident_key=incident_key,
                        incident_type=incident_type,
                        payload=payload,
                        event_at=event_at,
                    )
                except (TypeError, ValueError, sqlite3.Error):
                    self._reject(connection, outbox_id, "invalid_incident")
                    continue
                self._acknowledge(connection, outbox_id, observed_at)
                imported += 1
        return imported


@dataclass(frozen=True)
class DrainReport:
    delivered: int = 0
    retry_scheduled: int = 0
    permanently_failed: int = 0
    hourly_limited: bool = False


@dataclass(frozen=True)
class _ClaimedMessage:
    message_id: int
    incident_key: str | None
    cycle: int | None
    event_kind: str
    content: str
    attempts: int


class IncidentLedger:
    """SQLite incident state and outbox with crash-safe edge semantics."""

    def __init__(
        self,
        path: str | Path,
        *,
        reminder_seconds: float = 6 * 60 * 60,
        hourly_delivery_cap: int = 10,
    ) -> None:
        self.path = Path(path)
        self.reminder_seconds = max(60.0, float(reminder_seconds))
        self.hourly_delivery_cap = max(1, int(hourly_delivery_cap))
        self._prepare_file()
        self._initialize()

    def _prepare_file(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise RuntimeError("owner incident ledger cannot be a symlink")
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.path, flags, 0o600)
        os.close(descriptor)
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_key TEXT PRIMARY KEY,
                    cycle INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('open', 'resolved')),
                    severity TEXT NOT NULL,
                    source TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    details TEXT NOT NULL,
                    opened_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    resolved_at REAL,
                    occurrence_count INTEGER NOT NULL DEFAULT 1,
                    opening_delivered_at REAL,
                    last_delivered_at REAL,
                    recovery_delivered_at REAL
                );
                CREATE TABLE IF NOT EXISTS owner_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_key TEXT,
                    cycle INTEGER,
                    event_kind TEXT NOT NULL,
                    sequence INTEGER NOT NULL DEFAULT 0,
                    content TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    lease_until REAL,
                    delivered_at REAL,
                    cancelled_at REAL,
                    last_error TEXT,
                    UNIQUE (incident_key, cycle, event_kind, sequence)
                );
                CREATE INDEX IF NOT EXISTS owner_outbox_due
                    ON owner_outbox (available_at, id)
                    WHERE delivered_at IS NULL AND cancelled_at IS NULL;
                CREATE TABLE IF NOT EXISTS owner_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    delivered_at REAL NOT NULL,
                    event_kind TEXT NOT NULL,
                    incident_key TEXT
                );
                CREATE INDEX IF NOT EXISTS owner_deliveries_time
                    ON owner_deliveries (delivered_at);
                """
            )

    @staticmethod
    def _opening_content(
        key: str,
        severity: str,
        source: str,
        summary: str,
        details: str,
        now: float,
    ) -> str:
        lines = [
            "🚨 AMC Seat Watch incident",
            f"Severity: {severity.upper()}",
            f"Service: {source}",
            f"Issue: {summary}",
        ]
        if details:
            lines.append(f"Details: {details}")
        lines.extend((f"Incident: {key}", f"Opened: {_utc_display(now)}"))
        return sanitize_owner_text("\n".join(lines))

    @staticmethod
    def _reminder_content(row: sqlite3.Row, now: float) -> str:
        age_minutes = max(1, int((now - float(row["opened_at"])) // 60))
        return sanitize_owner_text(
            "\n".join(
                (
                    "⏰ AMC Seat Watch incident remains unresolved",
                    f"Severity: {str(row['severity']).upper()}",
                    f"Service: {row['source']}",
                    f"Issue: {row['summary']}",
                    f"Open for: {age_minutes} minutes",
                    f"Incident: {row['incident_key']}",
                )
            )
        )

    @staticmethod
    def _recovery_content(row: sqlite3.Row, now: float) -> str:
        duration_minutes = max(0, int((now - float(row["opened_at"])) // 60))
        return sanitize_owner_text(
            "\n".join(
                (
                    "✅ AMC Seat Watch incident recovered",
                    f"Service: {row['source']}",
                    f"Issue: {row['summary']}",
                    f"Duration: {duration_minutes} minutes",
                    f"Incident: {row['incident_key']}",
                    f"Recovered: {_utc_display(now)}",
                )
            )
        )

    def report(
        self,
        incident_key: str,
        *,
        severity: str,
        source: str,
        summary: str,
        details: str = "",
        now: float | None = None,
    ) -> bool:
        """Open/reopen an incident; repeated reports update but never re-alert."""

        now = _unix_now() if now is None else float(now)
        key = _validate_identifier(incident_key, "incident key")
        source = _validate_identifier(source, "incident source")
        severity = severity.strip().lower()
        if severity not in _SEVERITIES:
            raise ValueError("severity must be info, warning, or critical")
        summary = sanitize_owner_text(summary, limit=300)
        details = sanitize_owner_text(details, limit=800)
        if not summary:
            raise ValueError("incident summary is required")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM incidents WHERE incident_key = ?", (key,)
            ).fetchone()
            if row is not None and row["state"] == "open":
                connection.execute(
                    """
                    UPDATE incidents
                       SET severity = ?, source = ?, summary = ?, details = ?,
                           updated_at = ?, occurrence_count = occurrence_count + 1
                     WHERE incident_key = ?
                    """,
                    (severity, source, summary, details, now, key),
                )
                connection.commit()
                return False

            cycle = int(row["cycle"]) + 1 if row is not None else 1
            connection.execute(
                """
                INSERT INTO incidents (
                    incident_key, cycle, state, severity, source, summary, details,
                    opened_at, updated_at, resolved_at, occurrence_count,
                    opening_delivered_at, last_delivered_at, recovery_delivered_at
                ) VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?, NULL, 1, NULL, NULL, NULL)
                ON CONFLICT(incident_key) DO UPDATE SET
                    cycle = excluded.cycle,
                    state = 'open',
                    severity = excluded.severity,
                    source = excluded.source,
                    summary = excluded.summary,
                    details = excluded.details,
                    opened_at = excluded.opened_at,
                    updated_at = excluded.updated_at,
                    resolved_at = NULL,
                    occurrence_count = 1,
                    opening_delivered_at = NULL,
                    last_delivered_at = NULL,
                    recovery_delivered_at = NULL
                """,
                (key, cycle, severity, source, summary, details, now, now),
            )
            content = self._opening_content(key, severity, source, summary, details, now)
            connection.execute(
                """
                INSERT INTO owner_outbox (
                    incident_key, cycle, event_kind, sequence, content,
                    created_at, available_at
                ) VALUES (?, ?, 'opening', 0, ?, ?, ?)
                """,
                (key, cycle, content, now, now),
            )
            connection.commit()
            return True

    def resolve(
        self,
        incident_key: str,
        *,
        now: float | None = None,
        summary: str | None = None,
        preserve_pending_opening: bool = False,
    ) -> bool:
        """Close an incident and enqueue exactly one recovery when appropriate.

        Locally detected incidents remain quiet when they recover before their
        opening is sent. Imported PostgreSQL events set ``preserve_pending_opening``
        so an already-durable opening and its later recovery are both delivered.
        """

        now = _unix_now() if now is None else float(now)
        key = _validate_identifier(incident_key, "incident key")
        resolved_summary = sanitize_owner_text(summary, limit=300) if summary else ""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM incidents WHERE incident_key = ?", (key,)
            ).fetchone()
            if row is None or row["state"] != "open":
                connection.commit()
                return False
            connection.execute(
                "UPDATE incidents SET state = 'resolved', resolved_at = ?, updated_at = ?, "
                "summary = ? WHERE incident_key = ?",
                (now, now, resolved_summary or row["summary"], key),
            )
            resolved_row = connection.execute(
                "SELECT * FROM incidents WHERE incident_key = ?", (key,)
            ).fetchone()
            assert resolved_row is not None
            if row["opening_delivered_at"] is None and not preserve_pending_opening:
                connection.execute(
                    """
                    UPDATE owner_outbox SET cancelled_at = ?, lease_until = NULL
                     WHERE incident_key = ? AND cycle = ?
                       AND delivered_at IS NULL AND cancelled_at IS NULL
                    """,
                    (now, key, row["cycle"]),
                )
            else:
                content = self._recovery_content(resolved_row, now)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO owner_outbox (
                        incident_key, cycle, event_kind, sequence, content,
                        created_at, available_at
                    ) VALUES (?, ?, 'recovery', 0, ?, ?, ?)
                    """,
                    (key, row["cycle"], content, now, now),
                )
            connection.commit()
            return True

    def enqueue_due_reminders(self, *, now: float | None = None) -> int:
        now = _unix_now() if now is None else float(now)
        threshold = now - self.reminder_seconds
        queued = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT * FROM incidents
                 WHERE state = 'open'
                   AND severity = 'critical'
                   AND opening_delivered_at IS NOT NULL
                   AND last_delivered_at <= ?
                """,
                (threshold,),
            ).fetchall()
            for row in rows:
                outstanding = connection.execute(
                    """
                    SELECT 1 FROM owner_outbox
                     WHERE incident_key = ? AND cycle = ?
                       AND delivered_at IS NULL AND cancelled_at IS NULL
                     LIMIT 1
                    """,
                    (row["incident_key"], row["cycle"]),
                ).fetchone()
                if outstanding:
                    continue
                sequence = int(
                    connection.execute(
                        """
                        SELECT COALESCE(MAX(sequence), 0) + 1
                          FROM owner_outbox
                         WHERE incident_key = ? AND cycle = ? AND event_kind = 'reminder'
                        """,
                        (row["incident_key"], row["cycle"]),
                    ).fetchone()[0]
                )
                connection.execute(
                    """
                    INSERT INTO owner_outbox (
                        incident_key, cycle, event_kind, sequence, content,
                        created_at, available_at
                    ) VALUES (?, ?, 'reminder', ?, ?, ?, ?)
                    """,
                    (
                        row["incident_key"],
                        row["cycle"],
                        sequence,
                        self._reminder_content(row, now),
                        now,
                        now,
                    ),
                )
                queued += 1
            connection.commit()
        return queued

    def queue_test_message(self, *, now: float | None = None) -> int:
        now = _unix_now() if now is None else float(now)
        content = sanitize_owner_text(
            "\n".join(
                (
                    "🧪 AMC Seat Watch owner-alert test",
                    "The owner notification path is working.",
                    f"Sent: {_utc_display(now)}",
                )
            )
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO owner_outbox (
                    incident_key, cycle, event_kind, sequence, content,
                    created_at, available_at
                ) VALUES (NULL, NULL, 'test', 0, ?, ?, ?)
                """,
                (content, now, now),
            )
            return int(cursor.lastrowid)

    def _claim(self, now: float) -> tuple[_ClaimedMessage | None, bool]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            recent = int(
                connection.execute(
                    "SELECT COUNT(*) FROM owner_deliveries WHERE delivered_at > ?",
                    (now - 3_600,),
                ).fetchone()[0]
            )
            if recent >= self.hourly_delivery_cap:
                connection.commit()
                return None, True
            row = connection.execute(
                """
                SELECT * FROM owner_outbox
                 WHERE delivered_at IS NULL AND cancelled_at IS NULL
                   AND available_at <= ?
                   AND (lease_until IS NULL OR lease_until <= ?)
                 ORDER BY available_at, id
                 LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                connection.commit()
                return None, False
            updated = connection.execute(
                """
                UPDATE owner_outbox SET lease_until = ?
                 WHERE id = ? AND (lease_until IS NULL OR lease_until <= ?)
                """,
                (now + 60, row["id"], now),
            ).rowcount
            connection.commit()
            if updated != 1:
                return None, False
            return (
                _ClaimedMessage(
                    int(row["id"]),
                    row["incident_key"],
                    int(row["cycle"]) if row["cycle"] is not None else None,
                    str(row["event_kind"]),
                    str(row["content"]),
                    int(row["attempts"]),
                ),
                False,
            )

    def _complete(
        self,
        message: _ClaimedMessage,
        result: DeliveryResult,
        now: float,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if result.disposition == DeliveryDisposition.DELIVERED:
                connection.execute(
                    """
                    UPDATE owner_outbox
                       SET delivered_at = ?, lease_until = NULL, last_error = NULL
                     WHERE id = ?
                    """,
                    (now, message.message_id),
                )
                connection.execute(
                    "INSERT INTO owner_deliveries (delivered_at, event_kind, incident_key) "
                    "VALUES (?, ?, ?)",
                    (now, message.event_kind, message.incident_key),
                )
                if message.incident_key is not None:
                    column = {
                        "opening": "opening_delivered_at",
                        "recovery": "recovery_delivered_at",
                    }.get(message.event_kind)
                    if column:
                        connection.execute(
                            f"UPDATE incidents SET {column} = ?, last_delivered_at = ? "
                            "WHERE incident_key = ? AND cycle = ?",
                            (now, now, message.incident_key, message.cycle),
                        )
                    elif message.event_kind == "reminder":
                        connection.execute(
                            "UPDATE incidents SET last_delivered_at = ? "
                            "WHERE incident_key = ? AND cycle = ?",
                            (now, message.incident_key, message.cycle),
                        )
            elif result.disposition == DeliveryDisposition.PERMANENT:
                connection.execute(
                    """
                    UPDATE owner_outbox
                       SET attempts = attempts + 1, cancelled_at = ?, lease_until = NULL,
                           last_error = ?
                     WHERE id = ?
                    """,
                    (now, result.reason[:80], message.message_id),
                )
            else:
                attempt = message.attempts + 1
                fallback = min(3_600.0, max(5.0, float(2 ** min(attempt, 10))))
                delay = result.retry_after_seconds or fallback
                connection.execute(
                    """
                    UPDATE owner_outbox
                       SET attempts = ?, available_at = ?, lease_until = NULL,
                           last_error = ?
                     WHERE id = ?
                    """,
                    (attempt, now + delay, result.reason[:80], message.message_id),
                )
            connection.commit()

    def drain(
        self,
        delivery: Any,
        *,
        now: float | None = None,
        limit: int = 10,
    ) -> DrainReport:
        now = _unix_now() if now is None else float(now)
        delivered = retry = permanent = 0
        hourly_limited = False
        for _ in range(max(1, limit)):
            message, limited = self._claim(now)
            if limited:
                hourly_limited = True
                break
            if message is None:
                break
            result = delivery.send(message.content)
            self._complete(message, result, now)
            if result.disposition == DeliveryDisposition.DELIVERED:
                delivered += 1
            elif result.disposition == DeliveryDisposition.RETRY:
                retry += 1
            else:
                permanent += 1
        return DrainReport(delivered, retry, permanent, hourly_limited)

    def incident(self, incident_key: str) -> dict[str, Any] | None:
        """Return a safe incident-state snapshot, primarily for diagnostics/tests."""

        key = _validate_identifier(incident_key, "incident key")
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT incident_key, cycle, state, severity, source, opened_at,
                       updated_at, resolved_at, occurrence_count,
                       opening_delivered_at, last_delivered_at, recovery_delivered_at
                  FROM incidents WHERE incident_key = ?
                """,
                (key,),
            ).fetchone()
        return dict(row) if row is not None else None


class OwnerNotifier:
    def __init__(
        self,
        ledger: IncidentLedger,
        delivery: DiscordOwnerWebhook,
        *,
        heartbeat_source: HeartbeatSource | None,
        heartbeat_sink: HeartbeatSink | None = None,
        incident_bridge: IncidentBridge | None = None,
        expected_services: Iterable[str] = DEFAULT_SERVICES,
        stale_after_seconds: float = 180,
    ) -> None:
        self.ledger = ledger
        self.delivery = delivery
        self.heartbeat_source = heartbeat_source
        self.heartbeat_sink = heartbeat_sink
        self.incident_bridge = incident_bridge
        self.expected_services = tuple(
            _validate_identifier(value, "service name") for value in expected_services
        )
        self.stale_after_seconds = max(30.0, float(stale_after_seconds))

    def check_health(self, *, now: float | None = None) -> None:
        now = _unix_now() if now is None else float(now)
        own_failure = self.ledger.incident("owner-notifier.failed")
        if own_failure and own_failure["state"] == "open" and own_failure[
            "opening_delivered_at"
        ] is not None:
            self.ledger.resolve("owner-notifier.failed", now=now)
        if self.heartbeat_source is None:
            self.ledger.report(
                "database.configuration",
                severity="critical",
                source="amc-owner-notifier",
                summary="PostgreSQL heartbeat source is not configured",
                now=now,
            )
            return
        try:
            if self.heartbeat_sink is not None:
                self.heartbeat_sink.touch("amc-owner-notifier", now=now)
            if self.incident_bridge is not None:
                self.incident_bridge.sync(self.ledger, now=now)
            heartbeats = {
                heartbeat.service_name: heartbeat
                for heartbeat in self.heartbeat_source.fetch(self.expected_services)
            }
        except Exception:
            # Exception details may include a DSN or endpoint. Never persist them.
            self.ledger.report(
                "database.unavailable",
                severity="critical",
                source="amc-owner-notifier",
                summary="PostgreSQL health check failed",
                details="The notifier could not read service heartbeats.",
                now=now,
            )
            return

        self.ledger.resolve("database.configuration", now=now)
        self.ledger.resolve("database.unavailable", now=now)
        for service_name in self.expected_services:
            missing_key = f"heartbeat.missing:{service_name}"
            stale_key = f"heartbeat.stale:{service_name}"
            unhealthy_key = f"heartbeat.unhealthy:{service_name}"
            heartbeat = heartbeats.get(service_name)
            if heartbeat is None:
                self.ledger.report(
                    missing_key,
                    severity="critical",
                    source=service_name,
                    summary="Service heartbeat is missing",
                    now=now,
                )
                self.ledger.resolve(stale_key, now=now)
                self.ledger.resolve(unhealthy_key, now=now)
                continue
            self.ledger.resolve(missing_key, now=now)
            age = max(0.0, now - heartbeat.observed_at)
            if age > self.stale_after_seconds:
                self.ledger.report(
                    stale_key,
                    severity="critical",
                    source=service_name,
                    summary="Service heartbeat is stale",
                    details=f"Last heartbeat was {int(age)} seconds ago.",
                    now=now,
                )
            else:
                self.ledger.resolve(stale_key, now=now)
            if heartbeat.status.lower() not in {"healthy", "running", "ok"}:
                self.ledger.report(
                    unhealthy_key,
                    severity="warning",
                    source=service_name,
                    summary="Service reported a degraded state",
                    details=f"Reported status: {sanitize_owner_text(heartbeat.status, limit=80)}",
                    now=now,
                )
            else:
                self.ledger.resolve(unhealthy_key, now=now)

    def run_once(self, *, now: float | None = None) -> DrainReport:
        now = _unix_now() if now is None else float(now)
        self.check_health(now=now)
        self.ledger.enqueue_due_reminders(now=now)
        return self.ledger.drain(self.delivery, now=now)


def _positive_number(name: str, default: float, *, integer: bool = False) -> float | int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw) if integer else float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def build_notifier_from_environment(*, require_database: bool = True) -> OwnerNotifier:
    webhook = os.environ.get("OWNER_DISCORD_WEBHOOK", "").strip()
    if not webhook:
        raise ValueError("OWNER_DISCORD_WEBHOOK is required")
    database_url = os.environ.get("AMC_DATABASE_URL", "").strip()
    if require_database and not database_url:
        raise ValueError("AMC_DATABASE_URL is required")
    path = os.environ.get(
        "OWNER_NOTIFIER_LEDGER_PATH",
        "/var/lib/amc-owner-notifier/incidents.sqlite3",
    )
    reminder_hours = float(_positive_number("OWNER_REMINDER_HOURS", 6))
    cap = int(_positive_number("OWNER_DELIVERY_HOURLY_CAP", 10, integer=True))
    stale = float(_positive_number("OWNER_HEARTBEAT_STALE_SECONDS", 180))
    services = tuple(
        part.strip()
        for part in os.environ.get(
            "OWNER_HEARTBEAT_SERVICES", ",".join(DEFAULT_SERVICES)
        ).split(",")
        if part.strip()
    )
    if not services:
        raise ValueError("OWNER_HEARTBEAT_SERVICES must contain at least one service")
    heartbeat_source = PsycopgHeartbeatSource(database_url) if database_url else None
    return OwnerNotifier(
        IncidentLedger(
            path,
            reminder_seconds=reminder_hours * 3_600,
            hourly_delivery_cap=cap,
        ),
        DiscordOwnerWebhook(webhook),
        heartbeat_source=heartbeat_source,
        heartbeat_sink=heartbeat_source,
        incident_bridge=PsycopgIncidentBridge(database_url) if database_url else None,
        expected_services=services,
        stale_after_seconds=stale,
    )


def _record_deployment_phase(
    ledger: IncidentLedger,
    *,
    phase: str,
    state: str,
    now: float | None = None,
) -> bool:
    """Record a deployment edge using only predefined, secret-free content."""

    try:
        incident_key, label = _DEPLOYMENT_PHASES[phase]
    except KeyError as exc:
        raise ValueError("unsupported deployment phase") from exc
    if state == "failed":
        return ledger.report(
            incident_key,
            severity="critical",
            source="deployment",
            summary=f"{label} failed",
            details="The automated deployment phase reported a failure.",
            now=now,
        )
    if state == "recovered":
        return ledger.resolve(
            incident_key,
            summary=f"{label} recovered",
            preserve_pending_opening=True,
            now=now,
        )
    raise ValueError("unsupported deployment incident state")


def _run_forever(notifier: OwnerNotifier, interval: float) -> int:
    stop = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    for signal_name in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_name, request_stop)
    while not stop:
        report = notifier.run_once()
        print(
            "owner-notifier: check complete "
            f"delivered={report.delivered} retries={report.retry_scheduled} "
            f"permanent={report.permanently_failed} limited={report.hourly_limited}",
            flush=True,
        )
        deadline = time.monotonic() + interval
        while not stop and time.monotonic() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AMC Seat Watch owner incident notifier")
    subcommands = parser.add_subparsers(dest="command", required=True)
    run_parser = subcommands.add_parser("run", help="run health checks continuously")
    run_parser.add_argument(
        "--interval-seconds",
        type=float,
        default=float(os.environ.get("OWNER_CHECK_INTERVAL_SECONDS", "30")),
    )
    subcommands.add_parser("check-once", help="run one health and delivery pass")
    subcommands.add_parser("test-message", help="send a labeled owner-path test")
    failure_parser = subcommands.add_parser(
        "service-failure", help="record a failed owner-notifier systemd result"
    )
    failure_parser.add_argument("--result", default="unknown")
    deployment_parser = subcommands.add_parser(
        "deployment-incident",
        help="record a predefined migration or cutover incident edge",
    )
    deployment_parser.add_argument("phase", choices=tuple(_DEPLOYMENT_PHASES))
    deployment_parser.add_argument("state", choices=("failed", "recovered"))
    args = parser.parse_args(argv)

    try:
        if args.command in {
            "test-message",
            "service-failure",
            "deployment-incident",
        }:
            notifier = build_notifier_from_environment(require_database=False)
            changed = True
            if args.command == "test-message":
                notifier.ledger.queue_test_message()
            elif args.command == "service-failure":
                notifier.ledger.report(
                    "owner-notifier.failed",
                    severity="critical",
                    source="amc-owner-notifier",
                    summary="Owner notifier service stopped unexpectedly",
                    details=(
                        "systemd result: "
                        + sanitize_owner_text(args.result, limit=80)
                    ),
                )
            else:
                changed = _record_deployment_phase(
                    notifier.ledger,
                    phase=args.phase,
                    state=args.state,
                )
            report = notifier.ledger.drain(notifier.delivery)
            print(
                f"owner-notifier: {args.command} "
                f"delivered={report.delivered} retries={report.retry_scheduled} "
                f"permanent={report.permanently_failed}",
                flush=True,
            )
            if not changed:
                return 0
            return 0 if report.delivered >= 1 else 1
        notifier = build_notifier_from_environment()
        if args.command == "check-once":
            report = notifier.run_once()
            print(
                "owner-notifier: check "
                f"delivered={report.delivered} retries={report.retry_scheduled} "
                f"permanent={report.permanently_failed}",
                flush=True,
            )
            return 0 if report.permanently_failed == 0 else 1
        if args.interval_seconds <= 0:
            parser.error("--interval-seconds must be positive")
        return _run_forever(notifier, min(float(args.interval_seconds), 60.0))
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        # Deliberately print only the exception class for unexpected I/O/DB errors;
        # connection strings and webhook URLs must never reach journald.
        if isinstance(exc, ValueError):
            print(f"owner-notifier: configuration error: {exc}", file=sys.stderr)
        else:
            print(f"owner-notifier: failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
