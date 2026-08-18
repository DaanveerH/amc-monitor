"""Exact :class:`scheduler.WorkerStore` adapter for PostgreSQL/SQLAlchemy."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time as clock
import uuid
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import and_, func, or_, select

from amc_watch.amc import AMCUpstreamCooldown, ProxyEndpointError
from amc_watch.domain import adaptive_status_interval, fixed_offset, normalize_status
from amc_watch.scheduler import (
    ClaimedJob,
    JobKind,
    SubscriptionTarget,
    WorkerPolicy,
)

from .models import (
    AvailabilityEdge,
    CatalogLookup,
    Destination,
    DiscoveryTarget,
    Guild,
    MonitorJob,
    Movie,
    PresentationFormat,
    ProxyHealth,
    RequestGateState,
    SeatObservation,
    SelectableDate,
    Showtime,
    StatusObservation,
    Subscription,
    SubscriptionFormat,
    SubscriptionMovie,
    SubscriptionTheatre,
    Theatre,
    UserOutbox,
)
from .repositories import (
    DatabaseStore,
    JobRepository,
    as_utc,
    prune_seat_observations,
    prune_status_observations,
    utc_now,
)
from .session import Database, transaction

# Monitoring horizon (days). Used as (a) an eligibility/discovery floor so a
# subscription's small ``days_ahead`` still tracks far-future showtimes, and
# (b) the depth of the date-range discovery sweep below. AMC's ``selectableDates``
# only exposes a rolling ~10-day window for a running film, so the worker sweeps
# every date in this horizon rather than trusting that list alone.
HORIZON_DAYS = 60


class SqlAlchemyWorkerRepository:
    """Worker persistence with one durable request gate and shared resources."""

    def __init__(self, database: Database | str) -> None:
        self.store = DatabaseStore(database)
        self.database = self.store.database
        self.worker_id: str | None = None
        self._prefer_status = True

    @classmethod
    def from_url(cls, url: str) -> "SqlAlchemyWorkerRepository":
        return cls(Database(url))

    def claim_jobs(
        self, worker_id: str, *, limit: int, lease_seconds: int
    ) -> Sequence[ClaimedJob]:
        self.worker_id = worker_id
        batch_limits = {
            JobKind.STATUS.value: 8,
            JobKind.SEATMAP.value: 8,
            JobKind.SELECTABLE_DATES.value: 8,
            JobKind.DISCOVERY.value: 4,
            JobKind.CATALOG.value: 1,
        }
        # A human is waiting in the setup wizard whenever a catalog job exists,
        # and catalog jobs are rare. Claim one first so the wizard resolves in
        # seconds instead of starving for minutes behind status/discovery on the
        # single serialized AMC request lane.
        rows = self.store.claim_jobs(
            worker_id=worker_id,
            limit=1,
            lease_seconds=lease_seconds,
            kinds=[JobKind.CATALOG.value],
        )
        if not rows and self._prefer_status:
            rows = self.store.claim_jobs(
                worker_id=worker_id,
                limit=min(limit, batch_limits[JobKind.STATUS.value]),
                lease_seconds=lease_seconds,
                kinds=[JobKind.STATUS.value],
            )
        if not rows:
            rows = self.store.claim_jobs(
                worker_id=worker_id,
                limit=1,
                lease_seconds=lease_seconds,
            )
            if rows:
                kind = rows[0].kind
                remaining = min(limit, batch_limits.get(kind, 1)) - 1
                if remaining > 0:
                    rows.extend(
                        self.store.claim_jobs(
                            worker_id=worker_id,
                            limit=remaining,
                            lease_seconds=lease_seconds,
                            kinds=[kind],
                        )
                    )
        self._prefer_status = not self._prefer_status
        return [
            ClaimedJob(
                id=str(row.id),
                kind=JobKind(row.kind),
                resource_key=row.dedupe_key,
                payload=dict(row.payload),
                attempts=row.attempts,
            )
            for row in rows
        ]

    def acquire_request_slot(self, *, minimum_gap_seconds: float) -> datetime:
        """Wait only for the short minimum gap; surface a durable cooldown.

        The scheduler catches ``AMCUpstreamCooldown`` and requeues the claimed job,
        so no AMC call can occur without a successfully acquired database slot.
        """

        while True:
            now = utc_now()
            self._prepare_proxy_recovery(now)
            slot = self.store.acquire_request_slot(
                minimum_gap_seconds=minimum_gap_seconds, now=now
            )
            if slot.acquired:
                return slot.available_at
            delay = max(0.0, (as_utc(slot.available_at) - now).total_seconds())
            if slot.reason == "cooldown":
                error = AMCUpstreamCooldown(429, max(60, math.ceil(delay)))
                error.persisted_cooldown = True  # type: ignore[attr-defined]
                error.cooldown_until = slot.available_at  # type: ignore[attr-defined]
                raise error
            if slot.reason == "transport_backoff":
                error = ProxyEndpointError("proxy transport backoff is active")
                error.persisted_backoff = True  # type: ignore[attr-defined]
                error.backoff_until = slot.available_at  # type: ignore[attr-defined]
                raise error
            clock.sleep(min(delay, max(minimum_gap_seconds, 0.05)))

    def _lease_owner(self) -> str:
        if not self.worker_id:
            raise RuntimeError("claim_jobs must run before completing a job")
        return self.worker_id

    def complete_job(self, job_id: str) -> None:
        if not self.store.complete_job(
            uuid.UUID(job_id), worker_id=self._lease_owner()
        ):
            raise LookupError("job lease is unavailable")

    def retry_job(self, job_id: str, *, run_at: datetime, error_code: str) -> None:
        if not self.store.fail_job(
            uuid.UUID(job_id),
            worker_id=self._lease_owner(),
            error_code=error_code,
            retry_at=run_at,
        ):
            raise LookupError("job lease is unavailable")

    def enqueue_job(
        self,
        kind: JobKind,
        resource_key: str,
        payload: Mapping[str, Any],
        *,
        run_at: datetime,
        priority: int,
    ) -> None:
        self.store.enqueue_job(
            dedupe_key=resource_key,
            kind=kind.value,
            resource_type=kind.value,
            resource_id=resource_key,
            payload=dict(payload),
            run_at=run_at,
            priority=priority,
        )

    def heartbeat(self, service: str, *, metadata: Mapping[str, Any]) -> None:
        self.store.heartbeat(
            service,
            instance_id=self.worker_id or service,
            status="degraded" if metadata.get("degraded") else "healthy",
            ttl_seconds=90,
            metadata=dict(metadata),
        )

    def set_global_cooldown(self, until: datetime, *, status_code: int) -> None:
        self.store.set_global_cooldown(until, status_code=status_code)

    def clear_global_cooldown(self) -> None:
        self.store.clear_global_cooldown()

    def set_transport_backoff(self, until: datetime) -> None:
        self.store.set_transport_backoff(until)
        now = utc_now()
        with transaction(self.database.session_factory) as session:
            for row in session.scalars(
                select(ProxyHealth).where(ProxyHealth.incident_key.is_not(None))
            ):
                row.state = "exhausted"
                row.exhausted_at = now

    def clear_transport_backoff(self) -> None:
        self.store.clear_transport_backoff()

    def open_incident(
        self, event_type: str, severity: str, metadata: Mapping[str, Any]
    ) -> None:
        self.store.open_incident(event_type, severity, metadata)

    def resolve_incident(self, event_type: str) -> None:
        self.store.resolve_incident(event_type)

    def set_active_proxy(self, index: int) -> None:
        now = utc_now()
        label = f"proxy-{index}"
        with transaction(self.database.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if gate is None:
                gate = RequestGateState(key="global")
                session.add(gate)
            gate.current_proxy_label = label
            row = session.get(ProxyHealth, label)
            if row is None:
                row = ProxyHealth(label=label, ordinal=index)
                session.add(row)
            if row.incident_key is None:
                row.state = "selected"
            for other in session.scalars(
                select(ProxyHealth).where(ProxyHealth.label != label)
            ):
                if other.state == "active":
                    other.state = "standby"

    def record_proxy_failure(self, index: int) -> None:
        now = utc_now()
        label = f"proxy-{index}"
        with transaction(self.database.session_factory) as session:
            row = session.get(ProxyHealth, label, with_for_update=True)
            if row is None:
                row = ProxyHealth(label=label, ordinal=index)
                session.add(row)
            incident_key = session.scalar(
                select(ProxyHealth.incident_key)
                .where(ProxyHealth.incident_key.is_not(None))
                .order_by(ProxyHealth.last_failure_at.desc())
                .limit(1)
            ) or f"proxy-transport:{uuid.uuid4()}"
            row.state = "transport_failed"
            row.incident_key = incident_key
            row.last_failure_at = now
            row.consecutive_transport_failures += 1

    def record_proxy_success(self, index: int) -> None:
        now = utc_now()
        label = f"proxy-{index}"
        with transaction(self.database.session_factory) as session:
            row = session.get(ProxyHealth, label, with_for_update=True)
            if row is None:
                row = ProxyHealth(label=label, ordinal=index)
                session.add(row)
            row.state = "active"
            row.last_success_at = now
            row.consecutive_transport_failures = 0
            row.exhausted_at = None
            row.incident_key = None
            for other in session.scalars(
                select(ProxyHealth).where(ProxyHealth.label != label)
            ):
                other.state = "standby"
                other.consecutive_transport_failures = 0
                other.exhausted_at = None
                other.incident_key = None

    def _prepare_proxy_recovery(self, now: datetime | None = None) -> bool:
        now = now or utc_now()
        with transaction(self.database.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if (
                gate is None
                or gate.transport_backoff_until is None
                or as_utc(gate.transport_backoff_until) > now
            ):
                return False
            gate.transport_backoff_until = None
            gate.current_proxy_label = "proxy-0"
            for row in session.scalars(
                select(ProxyHealth).where(ProxyHealth.incident_key.is_not(None))
            ):
                row.state = "standby"
                row.consecutive_transport_failures = 0
                row.exhausted_at = None
                row.incident_key = None
            return True

    def proxy_pool_state(self, endpoint_count: int) -> tuple[int, tuple[int, ...]]:
        if endpoint_count < 1:
            raise ValueError("endpoint_count must be positive")
        self._prepare_proxy_recovery()
        with transaction(self.database.session_factory) as session:
            gate = session.get(RequestGateState, "global")
            active = 0
            if gate and gate.current_proxy_label:
                row = session.get(ProxyHealth, gate.current_proxy_label)
                if row is not None and 0 <= row.ordinal < endpoint_count:
                    active = row.ordinal
            attempted = tuple(
                sorted(
                    {
                        row.ordinal
                        for row in session.scalars(
                            select(ProxyHealth).where(
                                ProxyHealth.incident_key.is_not(None),
                                ProxyHealth.state.in_(("transport_failed", "exhausted")),
                            )
                        )
                        if 0 <= row.ordinal < endpoint_count
                    }
                )
            )
            return active, attempted

    def active_proxy_index(self) -> int:
        return self.proxy_pool_state(2**31 - 1)[0]

    def record_status(
        self,
        showtime_id: str,
        *,
        status: str | None,
        is_sold_out: bool,
        is_almost_sold_out: bool,
        missing: bool,
    ) -> tuple[str | None, int]:
        now = utc_now()
        normalized = normalize_status(status)
        with transaction(self.database.session_factory) as session:
            row = session.scalar(
                select(Showtime)
                .where(Showtime.amc_showtime_id == str(showtime_id))
                .with_for_update()
            )
            if row is None:
                raise LookupError(f"unknown AMC showtime {showtime_id}")
            previous = row.normalized_status or None
            previous_sold_out = row.is_sold_out
            previous_almost_sold_out = row.is_almost_sold_out
            latest_observation = session.scalar(
                select(StatusObservation)
                .where(StatusObservation.showtime_id == row.id)
                .order_by(
                    StatusObservation.observed_at.desc(),
                    StatusObservation.id.desc(),
                )
                .limit(1)
            )
            if missing:
                row.missing_count += 1
            else:
                row.missing_count = 0
                row.active = True
                row.normalized_status = normalized
                row.is_sold_out = is_sold_out
                row.is_almost_sold_out = is_almost_sold_out
                row.last_seen_at = now
            active_count = session.scalar(
                select(func.count()).select_from(Showtime).where(
                    Showtime.active.is_(True), Showtime.starts_at > now
                )
            ) or 0
            interval = adaptive_status_interval(active_count)
            row.last_status_poll_at = now
            row.next_status_poll_at = now + timedelta(seconds=interval)
            should_record = (
                latest_observation is None
                or missing
                or latest_observation.was_missing
                or previous != (normalized or None)
                or previous_sold_out != is_sold_out
                or previous_almost_sold_out != is_almost_sold_out
            )
            if should_record:
                session.add(
                    StatusObservation(
                        showtime_id=row.id,
                        observed_at=now,
                        normalized_status=normalized,
                        raw_status=status,
                        is_sold_out=is_sold_out,
                        is_almost_sold_out=is_almost_sold_out,
                        was_missing=missing,
                    )
                )
                session.flush()
                prune_status_observations(session, row.id)
            return previous, row.missing_count

    def retire_showtime_if_confirmed_missing(
        self, showtime_id: str, *, missing_count: int
    ) -> None:
        if missing_count < 3:
            return
        with transaction(self.database.session_factory) as session:
            row = session.scalar(
                select(Showtime)
                .where(Showtime.amc_showtime_id == str(showtime_id))
                .with_for_update()
            )
            if row is not None and row.missing_count >= 3:
                row.active = False

    @staticmethod
    def _local_start(showtime: Showtime, theatre: Theatre) -> datetime:
        start = as_utc(showtime.starts_at)
        utc_offset = str((showtime.metadata_json or {}).get("utc_offset") or "")
        if re.fullmatch(r"[+-]\d{2}:?\d{2}", utc_offset):
            return start.astimezone(fixed_offset(utc_offset))
        try:
            return start.astimezone(ZoneInfo(theatre.timezone))
        except ZoneInfoNotFoundError:
            return start

    @staticmethod
    def _movie_window(
        movie: Movie, today: date, fallback_days: int
    ) -> tuple[date, date]:
        """Resolve the date range a movie may be polled and alerted for.

        ``not_before``/``not_after`` come from the movie catalog and take
        precedence over the rolling horizon, so a far-future release keeps its
        own window. A missing or malformed bound falls back to the horizon.
        """

        metadata = dict(movie.metadata_json or {})
        try:
            lower = date.fromisoformat(str(metadata.get("not_before")))
        except ValueError:
            lower = today
        try:
            upper = date.fromisoformat(str(metadata.get("not_after")))
        except ValueError:
            upper = today + timedelta(days=fallback_days)
        return max(today, lower), upper

    @staticmethod
    def _in_window(subscription: Subscription, local: datetime) -> bool:
        weekend = local.weekday() >= 5
        start = subscription.weekend_start if weekend else subscription.weekday_start
        end = subscription.weekend_end if weekend else subscription.weekday_end
        return start <= local.time().replace(tzinfo=None) <= end

    def subscriptions_for_showtime(
        self, showtime_id: str
    ) -> Sequence[SubscriptionTarget]:
        with transaction(self.database.session_factory) as session:
            showtime = session.scalar(
                select(Showtime).where(Showtime.amc_showtime_id == str(showtime_id))
            )
            if showtime is None:
                return []
            theatre = session.get(Theatre, showtime.theatre_id)
            if theatre is None:
                return []
            statement = (
                select(Subscription, Guild, Destination)
                .join(Guild, Guild.id == Subscription.guild_id)
                .join(Destination, Destination.id == Subscription.destination_id)
                .join(
                    SubscriptionMovie,
                    SubscriptionMovie.subscription_id == Subscription.id,
                )
                .join(
                    SubscriptionTheatre,
                    SubscriptionTheatre.subscription_id == Subscription.id,
                )
                .where(
                    Subscription.enabled.is_(True),
                    Guild.enabled.is_(True),
                    Destination.enabled.is_(True),
                    SubscriptionMovie.movie_id == showtime.movie_id,
                    SubscriptionTheatre.theatre_id == showtime.theatre_id,
                )
            )
            local = self._local_start(showtime, theatre)
            if showtime.format_id is None:
                return []
            remote_codes = {
                self._format_key(value)
                for value in (showtime.metadata_json or {}).get("attribute_codes", ())
                if value
            }
            presentation = session.get(PresentationFormat, showtime.format_id)
            if presentation is not None:
                remote_codes.add(self._format_key(presentation.code))
            results: list[SubscriptionTarget] = []
            for subscription, guild, destination in session.execute(statement):
                configured_codes = {
                    self._format_key(value)
                    for value in session.scalars(
                        select(PresentationFormat.code)
                        .join(
                            SubscriptionFormat,
                            SubscriptionFormat.format_id == PresentationFormat.id,
                        )
                        .where(SubscriptionFormat.subscription_id == subscription.id)
                    )
                }
                if (
                    self._in_window(subscription, local)
                    and configured_codes
                    and configured_codes & remote_codes
                ):
                    results.append(
                        SubscriptionTarget(
                            id=str(subscription.id),
                            guild_id=guild.discord_guild_id,
                            destination_channel_id=destination.discord_channel_id,
                            seat_count=subscription.seat_count,
                            seat_preset=subscription.seat_preset,
                            label=subscription.name,
                        )
                    )
            return results

    def showtime_context(self, showtime_id: str) -> Mapping[str, Any]:
        now = utc_now()
        with transaction(self.database.session_factory) as session:
            row = session.scalar(
                select(Showtime)
                .where(Showtime.amc_showtime_id == str(showtime_id))
                .with_for_update()
            )
            if row is None:
                raise LookupError(f"unknown AMC showtime {showtime_id}")
            movie = session.get(Movie, row.movie_id)
            theatre = session.get(Theatre, row.theatre_id)
            presentation = session.get(PresentationFormat, row.format_id) if row.format_id else None
            if movie is None or theatre is None:
                raise LookupError("showtime catalog is incomplete")
            local = self._local_start(row, theatre)
            row.last_seat_poll_at = now
            row.next_seat_poll_at = now + timedelta(minutes=5)
            return {
                "movie": movie.title,
                "theatre": theatre.name,
                "when_local": local.strftime("%a, %b %-d · %-I:%M %p"),
                "format": str(
                    (row.metadata_json or {}).get("group_format_name")
                    or (presentation.name if presentation else "AMC")
                ),
                "booking_url": row.booking_url or "",
            }

    def record_missing_seatmap(self, showtime_id: str) -> None:
        now = utc_now()
        with transaction(self.database.session_factory) as session:
            row = session.scalar(
                select(Showtime)
                .where(Showtime.amc_showtime_id == str(showtime_id))
                .with_for_update()
            )
            if row is not None:
                row.last_seat_poll_at = now
                row.next_seat_poll_at = now + timedelta(minutes=5)

    def apply_availability(
        self,
        subscription: SubscriptionTarget,
        showtime_id: str,
        *,
        signatures: set[str],
        alert_payload: Mapping[str, Any],
    ) -> None:
        now = utc_now()
        with transaction(self.database.session_factory) as session:
            # Read the subscription without a row lock. The worker holds only
            # SELECT on subscriptions, and PostgreSQL requires UPDATE to take a
            # FOR UPDATE lock. Nothing here mutates the subscription: the alert
            # edges below are locked instead, which is what actually serializes
            # concurrent evaluation of the same subscription and showtime.
            subscription_row = session.get(Subscription, uuid.UUID(subscription.id))
            showtime = session.scalar(
                select(Showtime)
                .where(Showtime.amc_showtime_id == str(showtime_id))
                .with_for_update()
            )
            if subscription_row is None or showtime is None:
                raise LookupError("subscription or showtime is unavailable")
            edges = list(
                session.scalars(
                    select(AvailabilityEdge)
                    .where(
                        AvailabilityEdge.subscription_id == subscription_row.id,
                        AvailabilityEdge.showtime_id == showtime.id,
                    )
                    .with_for_update()
                )
            )
            by_signature = {edge.seat_key: edge for edge in edges}
            prior = {edge.seat_key for edge in edges if edge.available}
            new_signatures = signatures - prior
            for edge in edges:
                edge.available = edge.seat_key in signatures
                if edge.available:
                    edge.last_seen_at = now
            scores = {
                str(run.get("signature")): int(run.get("score", 0))
                for run in alert_payload.get("runs") or []
            }
            for signature in signatures - by_signature.keys():
                edge = AvailabilityEdge(
                    guild_id=subscription_row.guild_id,
                    subscription_id=subscription_row.id,
                    showtime_id=showtime.id,
                    seat_key=signature,
                    available=True,
                    score=scores.get(signature),
                    first_seen_at=now,
                    last_seen_at=now,
                )
                session.add(edge)
                by_signature[signature] = edge

            compact = dict(alert_payload.get("seatmap") or {})
            compact_seats = list(compact.get("seats") or [])
            # Recommendation markers are subscription-specific presentation.
            # Persist the shared physical layout so different presets do not
            # manufacture alternating hashes for the same AMC observation.
            observed_seats = [
                {
                    **dict(seat),
                    "s": "available"
                    if seat.get("s") == "recommended"
                    else seat.get("s"),
                }
                for seat in compact_seats
            ]
            serialized = json.dumps(observed_seats, sort_keys=True, separators=(",", ":"))
            layout_hash = hashlib.sha256(serialized.encode()).hexdigest()
            latest_hash = session.scalar(
                select(SeatObservation.layout_hash)
                .where(SeatObservation.showtime_id == showtime.id)
                .order_by(SeatObservation.observed_at.desc())
                .limit(1)
            )
            if latest_hash != layout_hash:
                available_coordinates = [
                    [int(seat.get("r", 0)), int(seat.get("c", 0))]
                    for seat in observed_seats
                    if seat.get("s") == "available"
                ]
                session.add(
                    SeatObservation(
                        showtime_id=showtime.id,
                        observed_at=now,
                        layout_hash=layout_hash,
                        available_seat_count=len(available_coordinates),
                        layout=observed_seats,
                        available_coordinates=available_coordinates,
                    )
                )
                session.flush()
                prune_seat_observations(session, showtime.id)
            showtime.last_seat_poll_at = now
            showtime.next_seat_poll_at = now + timedelta(minutes=5)

            if new_signatures:
                event_key = f"availability:{subscription_row.id}:{showtime.id}:{uuid.uuid4()}"
                session.add(
                    UserOutbox(
                        guild_id=subscription_row.guild_id,
                        destination_id=subscription_row.destination_id,
                        subscription_id=subscription_row.id,
                        event_key=event_key,
                        payload=dict(alert_payload),
                        available_at=now,
                    )
                )
                for signature in new_signatures:
                    by_signature[signature].last_alerted_at = now

    def observe_selectable_dates(
        self, movie_slug: str, dates: set[date]
    ) -> tuple[bool, set[date]]:
        now = utc_now()
        with transaction(self.database.session_factory) as session:
            movie = session.scalar(
                select(Movie).where(Movie.slug == movie_slug).with_for_update()
            )
            if movie is None:
                raise LookupError(f"unknown movie {movie_slug}")
            subscription_days = list(
                session.scalars(
                    select(Subscription.days_ahead)
                    .join(
                        SubscriptionMovie,
                        SubscriptionMovie.subscription_id == Subscription.id,
                    )
                    .where(
                        Subscription.enabled.is_(True),
                        SubscriptionMovie.movie_id == movie.id,
                    )
                )
            )
            if not subscription_days:
                dates = set()
            today = now.date()
            metadata = dict(movie.metadata_json or {})
            try:
                lower = date.fromisoformat(str(metadata.get("not_before")))
            except ValueError:
                lower = today
            try:
                upper = date.fromisoformat(str(metadata.get("not_after")))
            except ValueError:
                upper = today + timedelta(days=max([HORIZON_DAYS, *subscription_days]))
            lower = max(today, lower)
            dates = {value for value in dates if lower <= value <= upper}
            initialized = movie.selectable_dates_initialized_at is not None
            existing_rows = {
                row.date: row
                for row in session.scalars(
                    select(SelectableDate).where(SelectableDate.movie_id == movie.id)
                )
            }
            new_dates = dates - existing_rows.keys() if initialized else set()
            for value in dates:
                row = existing_rows.get(value)
                if row is None:
                    session.add(
                        SelectableDate(
                            movie_id=movie.id,
                            date=value,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                    )
                else:
                    row.last_seen_at = now
            movie.selectable_dates_initialized_at = movie.selectable_dates_initialized_at or now
            movie.last_dates_poll_at = now
            movie.next_dates_poll_at = now + timedelta(minutes=5)

            theatres = list(
                session.scalars(
                    select(Theatre)
                    .join(
                        SubscriptionTheatre,
                        SubscriptionTheatre.theatre_id == Theatre.id,
                    )
                    .join(
                        SubscriptionMovie,
                        SubscriptionMovie.subscription_id
                        == SubscriptionTheatre.subscription_id,
                    )
                    .join(
                        Subscription,
                        Subscription.id == SubscriptionMovie.subscription_id,
                    )
                    .where(
                        Subscription.enabled.is_(True),
                        SubscriptionMovie.movie_id == movie.id,
                    )
                    .distinct()
                )
            )
            # Sweep every date in the horizon, not just AMC's selectable-date
            # list: a running film keeps selling beyond the ~10 days that list
            # reports, so those dates need discovery targets too.
            sweep_dates = {today + timedelta(days=offset) for offset in range(HORIZON_DAYS + 1)}
            for theatre in theatres:
                for value in sorted(dates | sweep_dates):
                    target = session.get(DiscoveryTarget, (theatre.id, value))
                    if target is None:
                        session.add(
                            DiscoveryTarget(
                                theatre_id=theatre.id,
                                date=value,
                                next_poll_at=now,
                            )
                        )
                    elif value in dates and not target.active:
                        # Reactivate a target AMC still lists that had been
                        # retired while no subscription referenced it (e.g. the
                        # initial catalog baseline). Only touch inactive targets:
                        # resetting next_poll_at on healthy active targets every
                        # 5-minute dates poll would collapse the discovery cadence.
                        target.active = True
                        target.next_poll_at = now
                    # Active targets (and swept dates AMC does not list) keep
                    # their cadence; due_resources decides whether they stay active.
            return initialized, new_dates

    def theatres_for_movie(self, movie_slug: str) -> Sequence[str]:
        with transaction(self.database.session_factory) as session:
            return list(
                session.scalars(
                    select(Theatre.slug)
                    .join(
                        SubscriptionTheatre,
                        SubscriptionTheatre.theatre_id == Theatre.id,
                    )
                    .join(
                        SubscriptionMovie,
                        SubscriptionMovie.subscription_id
                        == SubscriptionTheatre.subscription_id,
                    )
                    .join(
                        Subscription,
                        Subscription.id == SubscriptionMovie.subscription_id,
                    )
                    .join(Movie, Movie.id == SubscriptionMovie.movie_id)
                    .where(
                        Subscription.enabled.is_(True),
                        Movie.slug == movie_slug,
                    )
                    .distinct()
                    .order_by(Theatre.slug)
                )
            )

    @staticmethod
    def _format_key(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())

    def _eligible_discovery(
        self,
        session: Any,
        theatre: Theatre,
        item: Mapping[str, Any],
        starts_at: datetime,
        now: datetime,
    ) -> tuple[Movie, PresentationFormat] | None:
        movie_slug = str(item.get("movie_slug") or "")
        movie_id = str(item.get("movie_id") or "") or None
        movie_conditions = [Movie.slug == movie_slug]
        if movie_id is not None:
            movie_conditions.append(Movie.amc_movie_id == movie_id)
        movie_name = str(item.get("movie_name") or "").casefold()
        if movie_name:
            movie_conditions.append(Movie.normalized_title == movie_name)
        movie = session.scalar(
            select(Movie)
            .join(SubscriptionMovie, SubscriptionMovie.movie_id == Movie.id)
            .join(Subscription, Subscription.id == SubscriptionMovie.subscription_id)
            .join(
                SubscriptionTheatre,
                SubscriptionTheatre.subscription_id == Subscription.id,
            )
            .join(Guild, Guild.id == Subscription.guild_id)
            .join(Destination, Destination.id == Subscription.destination_id)
            .where(
                or_(*movie_conditions),
                SubscriptionTheatre.theatre_id == theatre.id,
                Subscription.enabled.is_(True),
                Guild.enabled.is_(True),
                Destination.enabled.is_(True),
            )
            .limit(1)
        )
        if movie is None:
            return None

        metadata = dict(movie.metadata_json or {})
        start = as_utc(starts_at)
        utc_offset = str(item.get("utc_offset") or "")
        if re.fullmatch(r"[+-]\d{2}:?\d{2}", utc_offset):
            local = start.astimezone(fixed_offset(utc_offset))
        else:
            try:
                local = start.astimezone(ZoneInfo(theatre.timezone))
            except ZoneInfoNotFoundError:
                local = start
        remote_codes = {
            self._format_key(value)
            for value in item.get("attribute_codes") or ()
            if value
        }
        remote_codes.add(self._format_key(item.get("format_code")))
        candidates = list(
            session.scalars(
                select(Subscription)
                .join(
                    SubscriptionMovie,
                    SubscriptionMovie.subscription_id == Subscription.id,
                )
                .join(
                    SubscriptionTheatre,
                    SubscriptionTheatre.subscription_id == Subscription.id,
                )
                .join(Guild, Guild.id == Subscription.guild_id)
                .join(Destination, Destination.id == Subscription.destination_id)
                .where(
                    Subscription.enabled.is_(True),
                    Guild.enabled.is_(True),
                    Destination.enabled.is_(True),
                    SubscriptionMovie.movie_id == movie.id,
                    SubscriptionTheatre.theatre_id == theatre.id,
                )
                .distinct()
            )
        )
        for subscription in candidates:
            try:
                lower = date.fromisoformat(str(metadata.get("not_before")))
            except ValueError:
                lower = now.date()
            try:
                upper = date.fromisoformat(str(metadata.get("not_after")))
            except ValueError:
                upper = now.date() + timedelta(days=max(subscription.days_ahead, HORIZON_DAYS))
            if not lower <= local.date() <= upper:
                continue
            if not self._in_window(subscription, local):
                continue
            formats = list(
                session.scalars(
                    select(PresentationFormat)
                    .join(
                        SubscriptionFormat,
                        SubscriptionFormat.format_id == PresentationFormat.id,
                    )
                    .where(SubscriptionFormat.subscription_id == subscription.id)
                )
            )
            match = next(
                (
                    value
                    for value in formats
                    if self._format_key(value.code) in remote_codes
                ),
                None,
            )
            if match is not None:
                return movie, match
        return None

    def _catalog_discovery(
        self,
        session: Any,
        item: Mapping[str, Any],
    ) -> tuple[Movie, PresentationFormat] | None:
        """Persist normalized wizard catalog data without enabling monitoring."""

        movie_slug = str(item.get("movie_slug") or "").strip()
        movie_id = str(item.get("movie_id") or "").strip() or None
        movie_name = str(item.get("movie_name") or "").strip()
        format_code = str(item.get("format_code") or "").strip()
        format_name = str(item.get("format_name") or format_code).strip()
        if not movie_slug or not movie_name or not format_code or not format_name:
            return None
        conditions = [Movie.slug == movie_slug]
        if movie_id:
            conditions.append(Movie.amc_movie_id == movie_id)
        movie = session.scalar(select(Movie).where(or_(*conditions)))
        if movie is None:
            movie = Movie(
                amc_movie_id=movie_id,
                slug=movie_slug,
                title=movie_name,
                normalized_title=movie_name.casefold(),
                metadata_json={"catalog_source": "theatre-bootstrap"},
            )
            session.add(movie)
        else:
            movie.amc_movie_id = movie_id or movie.amc_movie_id
            movie.slug = movie_slug
            movie.title = movie_name
            movie.normalized_title = movie_name.casefold()
        presentation = session.scalar(
            select(PresentationFormat).where(PresentationFormat.code == format_code)
        )
        if presentation is None:
            presentation = PresentationFormat(code=format_code, name=format_name)
            session.add(presentation)
        else:
            presentation.name = format_name
        session.flush()
        return movie, presentation

    def upsert_discovered_showtimes(
        self,
        theatre_slug: str,
        local_date: str,
        showtimes: Sequence[Mapping[str, Any]],
    ) -> Sequence[str]:
        now = utc_now()
        selected_date = date.fromisoformat(local_date)
        with transaction(self.database.session_factory) as session:
            theatre = session.scalar(select(Theatre).where(Theatre.slug == theatre_slug))
            if theatre is None:
                theatre = Theatre(
                    slug=theatre_slug,
                    name=theatre_slug,
                    zip_code="00000",
                )
                session.add(theatre)
                session.flush()
            target = session.get(DiscoveryTarget, (theatre.id, selected_date))
            catalog_bootstrap = target is not None and target.last_result_count == -1
            found: list[str] = []
            catalog_result_count = 0
            for item in showtimes:
                amc_showtime_id = str(item.get("showtime_id") or "")
                starts_at = item.get("showtime_at")
                if not amc_showtime_id or not isinstance(starts_at, datetime):
                    continue
                eligible = (
                    self._catalog_discovery(session, item)
                    if catalog_bootstrap
                    else self._eligible_discovery(session, theatre, item, starts_at, now)
                )
                if eligible is None:
                    continue
                movie, presentation = eligible
                row = session.scalar(
                    select(Showtime).where(Showtime.amc_showtime_id == amc_showtime_id)
                )
                created = row is None
                prior_seat_poll = None if created else row.next_seat_poll_at
                if row is None:
                    row = Showtime(
                        amc_showtime_id=amc_showtime_id,
                        movie_id=movie.id,
                        theatre_id=theatre.id,
                        format_id=presentation.id,
                        starts_at=starts_at,
                        active=not catalog_bootstrap,
                    )
                    session.add(row)
                row.movie_id = movie.id
                row.theatre_id = theatre.id
                row.format_id = presentation.id
                row.starts_at = starts_at
                row.booking_url = str(item.get("book_url") or "")
                row.normalized_status = normalize_status(item.get("status"))
                row.is_sold_out = bool(item.get("is_sold_out"))
                row.is_almost_sold_out = bool(item.get("is_almost_sold_out"))
                if not catalog_bootstrap:
                    row.active = True
                elif created:
                    row.active = False
                row.missing_count = 0
                row.last_seen_at = now
                if not catalog_bootstrap:
                    row.next_status_poll_at = row.next_status_poll_at or now
                    row.next_seat_poll_at = row.next_seat_poll_at or now
                row.metadata_json = {
                    **dict(row.metadata_json or {}),
                    "utc_offset": item.get("utc_offset"),
                    "attribute_codes": sorted(
                        {
                            str(value).casefold()
                            for value in item.get("attribute_codes") or ()
                            if value
                        }
                        | {str(item.get("format_code") or "").casefold()}
                    ),
                    "group_format_code": item.get("format_code"),
                    "group_format_name": item.get("format_name"),
                }
                catalog_result_count += 1
                # Enqueue a discovery-triggered seat check only when the showtime
                # is new or actually due for one; a recently seat-polled showtime
                # keeps its own next_seat_poll_at cadence instead of a redundant
                # refetch on every discovery pass.
                if not catalog_bootstrap and (
                    created
                    or prior_seat_poll is None
                    or as_utc(prior_seat_poll) <= now
                ):
                    found.append(amc_showtime_id)
            if target is None:
                target = DiscoveryTarget(
                    theatre_id=theatre.id,
                    date=selected_date,
                    next_poll_at=now + timedelta(minutes=15),
                )
                session.add(target)
            target.last_poll_at = now
            target.next_poll_at = now + timedelta(minutes=15)
            target.last_result_count = (
                catalog_result_count if catalog_bootstrap else len(found)
            )
            return found

    def due_resources(
        self, now: datetime, policy: WorkerPolicy
    ) -> Mapping[JobKind, Sequence[Mapping[str, Any]]]:
        with transaction(self.database.session_factory) as session:
            for expired in session.scalars(
                select(Showtime).where(
                    Showtime.active.is_(True), Showtime.starts_at <= now
                )
            ):
                expired.active = False
            active_showtimes = list(
                session.scalars(
                    select(Showtime).where(
                        Showtime.active.is_(True), Showtime.starts_at > now
                    )
                )
            )
            eligible_ids: set[uuid.UUID] = set()
            for showtime, subscription, theatre, movie in session.execute(
                select(Showtime, Subscription, Theatre, Movie)
                .select_from(Showtime)
                .join(Movie, Movie.id == Showtime.movie_id)
                .join(
                    SubscriptionMovie,
                    SubscriptionMovie.movie_id == Showtime.movie_id,
                )
                .join(
                    Subscription,
                    Subscription.id == SubscriptionMovie.subscription_id,
                )
                .join(
                    SubscriptionTheatre,
                    and_(
                        SubscriptionTheatre.subscription_id == Subscription.id,
                        SubscriptionTheatre.theatre_id == Showtime.theatre_id,
                    ),
                )
                .join(
                    SubscriptionFormat,
                    and_(
                        SubscriptionFormat.subscription_id == Subscription.id,
                        SubscriptionFormat.format_id == Showtime.format_id,
                    ),
                )
                .join(Theatre, Theatre.id == Showtime.theatre_id)
                .join(Guild, Guild.id == Subscription.guild_id)
                .join(Destination, Destination.id == Subscription.destination_id)
                .where(
                    Showtime.active.is_(True),
                    Showtime.starts_at > now,
                    Subscription.enabled.is_(True),
                    Guild.enabled.is_(True),
                    Destination.enabled.is_(True),
                )
            ):
                local = self._local_start(showtime, theatre)
                local_today = as_utc(now).astimezone(local.tzinfo).date()
                # A movie window (not_before/not_after) overrides the rolling
                # horizon exactly as it does for discovery and alert matching. A
                # far-future event booking (e.g. a December premiere sold in
                # August) must stay active, or it is discovered and then retired
                # before its seats are ever polled.
                lower, upper = self._movie_window(
                    movie,
                    local_today,
                    max(subscription.days_ahead, HORIZON_DAYS),
                )
                if lower <= local.date() <= upper and self._in_window(subscription, local):
                    eligible_ids.add(showtime.id)
            for orphan in active_showtimes:
                if orphan.id not in eligible_ids:
                    orphan.active = False
            status_rows = list(
                session.scalars(
                    select(Showtime)
                    .where(
                        Showtime.id.in_(eligible_ids),
                        or_(
                            Showtime.next_status_poll_at.is_(None),
                            Showtime.next_status_poll_at <= now,
                        ),
                    )
                    .order_by(Showtime.next_status_poll_at, Showtime.starts_at)
                )
            )
            seat_rows = list(
                session.scalars(
                    select(Showtime)
                    .where(
                        Showtime.id.in_(eligible_ids),
                        or_(
                            Showtime.next_seat_poll_at.is_(None),
                            Showtime.next_seat_poll_at <= now,
                        ),
                    )
                    .order_by(Showtime.next_seat_poll_at, Showtime.starts_at)
                )
            )
            movie_rows = list(
                session.scalars(
                    select(Movie)
                    .join(SubscriptionMovie, SubscriptionMovie.movie_id == Movie.id)
                    .join(Subscription, Subscription.id == SubscriptionMovie.subscription_id)
                    .join(Guild, Guild.id == Subscription.guild_id)
                    .join(Destination, Destination.id == Subscription.destination_id)
                    .where(
                        Subscription.enabled.is_(True),
                        Guild.enabled.is_(True),
                        Destination.enabled.is_(True),
                        or_(
                            Movie.next_dates_poll_at.is_(None),
                            Movie.next_dates_poll_at <= now,
                        ),
                    )
                    .distinct()
                    .order_by(Movie.next_dates_poll_at, Movie.slug)
                )
            )
            candidate_discovery_rows = list(
                session.execute(
                    select(DiscoveryTarget, Theatre)
                    .join(Theatre, Theatre.id == DiscoveryTarget.theatre_id)
                    .where(
                        DiscoveryTarget.active.is_(True),
                        DiscoveryTarget.next_poll_at <= now,
                    )
                    .order_by(DiscoveryTarget.next_poll_at, DiscoveryTarget.date)
                )
            )
            discovery_rows: list[tuple[DiscoveryTarget, Theatre]] = []
            # Gate on the UTC date minus one day so a theatre-local "today" is
            # not retired at UTC midnight (up to ~4-5h before local midnight for
            # US zones), which would black out discovery of same-day showtimes.
            discovery_floor = now.date() - timedelta(days=1)
            for target, theatre in candidate_discovery_rows:
                relevant = session.scalar(
                    select(func.count())
                    .select_from(SelectableDate)
                    .join(Movie, Movie.id == SelectableDate.movie_id)
                    .join(
                        SubscriptionMovie,
                        SubscriptionMovie.movie_id == Movie.id,
                    )
                    .join(
                        Subscription,
                        Subscription.id == SubscriptionMovie.subscription_id,
                    )
                    .join(
                        SubscriptionTheatre,
                        SubscriptionTheatre.subscription_id == Subscription.id,
                    )
                    .join(Guild, Guild.id == Subscription.guild_id)
                    .join(Destination, Destination.id == Subscription.destination_id)
                    .where(
                        SelectableDate.date == target.date,
                        Subscription.enabled.is_(True),
                        Guild.enabled.is_(True),
                        Destination.enabled.is_(True),
                        SubscriptionTheatre.theatre_id == theatre.id,
                    )
                )
                if not relevant and discovery_floor <= target.date <= now.date() + timedelta(days=HORIZON_DAYS):
                    # AMC's selectable-date list only spans a rolling ~10-day
                    # window for a running film, but it keeps selling further
                    # out. Treat any in-horizon date on a subscribed theatre as
                    # relevant so its showtimes are discovered even without a
                    # selectable-date row.
                    relevant = session.scalar(
                        select(func.count())
                        .select_from(Subscription)
                        .join(
                            SubscriptionTheatre,
                            SubscriptionTheatre.subscription_id == Subscription.id,
                        )
                        .join(Guild, Guild.id == Subscription.guild_id)
                        .join(Destination, Destination.id == Subscription.destination_id)
                        .where(
                            Subscription.enabled.is_(True),
                            Guild.enabled.is_(True),
                            Destination.enabled.is_(True),
                            SubscriptionTheatre.theatre_id == theatre.id,
                        )
                    )
                if (
                    target.date < discovery_floor
                    or (not relevant and target.last_result_count != -1)
                ):
                    target.active = False
                else:
                    discovery_rows.append((target, theatre))
            return {
                JobKind.STATUS: [
                    {
                        "resource_key": row.amc_showtime_id,
                        "showtime_id": row.amc_showtime_id,
                    }
                    for row in status_rows
                ],
                JobKind.SEATMAP: [
                    {
                        "resource_key": row.amc_showtime_id,
                        "showtime_id": row.amc_showtime_id,
                    }
                    for row in seat_rows
                ],
                JobKind.SELECTABLE_DATES: [
                    {
                        "resource_key": row.slug,
                        "slug": row.slug,
                        "not_before": (row.metadata_json or {}).get("not_before"),
                        "not_after": (row.metadata_json or {}).get("not_after"),
                    }
                    for row in movie_rows
                ],
                JobKind.DISCOVERY: [
                    {
                        "resource_key": f"{theatre.slug}:{target.date.isoformat()}",
                        "theatre_slug": theatre.slug,
                        "date": target.date.isoformat(),
                    }
                    for target, theatre in discovery_rows
                ],
                JobKind.CATALOG: [],
            }

    def resolve_cached_catalog(
        self, kind: str, query: Mapping[str, Any]
    ) -> Sequence[Mapping[str, Any]]:
        return self.store.resolve_cached_catalog(kind, dict(query))

    def complete_catalog_lookup(
        self, lookup_id: str, results: Sequence[Mapping[str, Any]]
    ) -> None:
        if not self.store.complete_catalog_lookup(
            lookup_id,
            [dict(value) for value in results],
            now=utc_now(),
        ):
            raise LookupError("catalog lookup is unavailable")

    def upsert_theatre_catalog(
        self, theatres: Sequence[Mapping[str, Any]]
    ) -> int:
        return self.store.upsert_theatre_catalog([dict(t) for t in theatres])

    def upsert_format_catalog(
        self, formats: Sequence[Mapping[str, Any]]
    ) -> int:
        return self.store.upsert_format_catalog([dict(f) for f in formats])

    def upsert_zip_centroid(
        self, zip_code: str, latitude: float, longitude: float
    ) -> None:
        self.store.upsert_zip_centroid(zip_code, latitude, longitude)

    def zip_centroid_exists(self, zip_code: str) -> bool:
        return self.store.zip_centroid_exists(zip_code)

    def catalog_refresh_due(
        self, now: datetime, *, max_age_seconds: float
    ) -> bool:
        return self.store.catalog_refresh_due(now, max_age_seconds=max_age_seconds)

    def mark_catalog_refreshed(self, now: datetime) -> None:
        self.store.mark_catalog_refreshed(now)

    def metrics(self) -> Mapping[str, Any]:
        return self.store.metrics()

    def import_legacy_bundle(
        self, bundle: Any, *, dry_run: bool
    ) -> Mapping[str, int]:
        """Idempotently import a bound v1/v2 bundle without creating alerts."""

        summary = {
            "subscriptions": len(bundle.subscriptions),
            "showtimes": len(bundle.showtimes),
            "alert_edges": sum(
                len(signatures)
                for by_showtime in bundle.alert_edges.values()
                for signatures in by_showtime.values()
            ),
            "selectable_dates": sum(len(values) for values in bundle.selectable_dates.values()),
            "status_observations": sum(
                1 for value in bundle.showtimes if value.get("availability") is not None
            ),
            "seat_observations": sum(
                1
                for value in bundle.showtimes
                if isinstance(value.get("seatmap"), Mapping)
                and isinstance(value.get("seatmap", {}).get("seats"), list)
            ),
        }
        if dry_run:
            return summary
        if any(
            item.guild_id is None or item.destination_channel_id is None
            for item in bundle.subscriptions
        ):
            raise ValueError("legacy subscriptions must be bound before import")

        now = utc_now()
        imported_subscriptions: dict[str, Subscription] = {}
        with transaction(self.database.session_factory) as session:
            for item in bundle.subscriptions:
                guild = session.scalar(
                    select(Guild).where(Guild.discord_guild_id == item.guild_id)
                )
                if guild is None:
                    raise LookupError(f"Discord guild {item.guild_id} is not configured")
                destination = session.scalar(
                    select(Destination).where(
                        Destination.guild_id == guild.id,
                        Destination.discord_channel_id == item.destination_channel_id,
                    )
                )
                if destination is None:
                    raise LookupError("legacy destination is not approved")
                theatres: list[Theatre] = []
                for raw in item.theatres:
                    slug = str(raw.get("slug") or raw.get("id") or "")
                    row = session.scalar(select(Theatre).where(Theatre.slug == slug))
                    if row is None:
                        row = Theatre(
                            slug=slug,
                            name=str(raw.get("name") or slug),
                            zip_code=item.zip_code,
                        )
                        session.add(row)
                        session.flush()
                    else:
                        row.name = str(raw.get("name") or row.name)
                        row.zip_code = item.zip_code
                    theatres.append(row)
                movies: list[Movie] = []
                for raw in item.movies:
                    movie_id = str(raw.get("movie_id") or "") or None
                    slug = str(raw.get("slug") or f"legacy-movie-{movie_id or hashlib.sha1(str(raw).encode()).hexdigest()[:12]}")
                    movie_conditions = [Movie.slug == slug]
                    if movie_id:
                        movie_conditions.append(Movie.amc_movie_id == movie_id)
                    row = session.scalar(select(Movie).where(or_(*movie_conditions)))
                    if row is None:
                        title = str(raw.get("name") or slug)
                        row = Movie(
                            amc_movie_id=movie_id,
                            slug=slug,
                            title=title,
                            normalized_title=title.casefold(),
                            metadata_json={
                                "not_before": raw.get("not_before"),
                                "not_after": raw.get("not_after"),
                            },
                        )
                        session.add(row)
                        session.flush()
                    else:
                        row.title = str(raw.get("name") or row.title)
                        row.normalized_title = row.title.casefold()
                        row.metadata_json = {
                            **dict(row.metadata_json or {}),
                            "not_before": raw.get("not_before"),
                            "not_after": raw.get("not_after"),
                        }
                    movies.append(row)
                formats: list[PresentationFormat] = []
                for code in item.format_codes:
                    row = session.scalar(
                        select(PresentationFormat).where(PresentationFormat.code == code)
                    )
                    if row is None:
                        row = PresentationFormat(code=code, name=code)
                        session.add(row)
                        session.flush()
                    formats.append(row)
                external_hash = hashlib.sha256(item.external_id.encode()).hexdigest()[:16]
                creation_key = f"legacy:{guild.id}:{external_hash}"
                subscription = session.scalar(
                    select(Subscription).where(Subscription.creation_key == creation_key)
                )
                if subscription is None:
                    subscription = Subscription(
                        creation_key=creation_key,
                        guild_id=guild.id,
                        destination_id=destination.id,
                        created_by_discord_user_id=guild.created_by_discord_user_id,
                        name=item.label,
                        zip_code=item.zip_code,
                        seat_count=item.seat_count,
                        days_ahead=item.days_ahead,
                        seat_preset=item.seat_preset,
                        weekday_start=self._legacy_time(item.weekday_hours[0]),
                        weekday_end=self._legacy_time(item.weekday_hours[1], end=True),
                        weekend_start=self._legacy_time(item.weekend_hours[0]),
                        weekend_end=self._legacy_time(item.weekend_hours[1], end=True),
                    )
                    session.add(subscription)
                    session.flush()
                else:
                    subscription.destination_id = destination.id
                    subscription.name = item.label
                    subscription.zip_code = item.zip_code
                    subscription.seat_count = item.seat_count
                    subscription.days_ahead = item.days_ahead
                    subscription.seat_preset = item.seat_preset
                    subscription.weekday_start = self._legacy_time(item.weekday_hours[0])
                    subscription.weekday_end = self._legacy_time(
                        item.weekday_hours[1], end=True
                    )
                    subscription.weekend_start = self._legacy_time(item.weekend_hours[0])
                    subscription.weekend_end = self._legacy_time(
                        item.weekend_hours[1], end=True
                    )
                imported_subscriptions[item.external_id] = subscription
                theatre_links = list(
                    session.scalars(
                        select(SubscriptionTheatre).where(
                            SubscriptionTheatre.subscription_id == subscription.id
                        )
                    )
                )
                movie_links = list(
                    session.scalars(
                        select(SubscriptionMovie).where(
                            SubscriptionMovie.subscription_id == subscription.id
                        )
                    )
                )
                format_links = list(
                    session.scalars(
                        select(SubscriptionFormat).where(
                            SubscriptionFormat.subscription_id == subscription.id
                        )
                    )
                )
                desired_theatres = {row.id for row in theatres}
                desired_movies = {row.id for row in movies}
                desired_formats = {row.id for row in formats}
                for link in theatre_links:
                    if link.theatre_id not in desired_theatres:
                        session.delete(link)
                for link in movie_links:
                    if link.movie_id not in desired_movies:
                        session.delete(link)
                for link in format_links:
                    if link.format_id not in desired_formats:
                        session.delete(link)
                existing_theatres = {link.theatre_id for link in theatre_links}
                existing_movies = {link.movie_id for link in movie_links}
                existing_formats = {link.format_id for link in format_links}
                session.add_all(
                    [
                        SubscriptionTheatre(
                            guild_id=guild.id,
                            subscription_id=subscription.id,
                            theatre_id=row.id,
                        )
                        for row in theatres
                        if row.id not in existing_theatres
                    ]
                    + [
                        SubscriptionMovie(
                            guild_id=guild.id,
                            subscription_id=subscription.id,
                            movie_id=row.id,
                        )
                        for row in movies
                        if row.id not in existing_movies
                    ]
                    + [
                        SubscriptionFormat(
                            guild_id=guild.id,
                            subscription_id=subscription.id,
                            format_id=row.id,
                        )
                        for row in formats
                        if row.id not in existing_formats
                    ]
                )

            initialized_slugs = set(bundle.selectable_dates_initialized) | set(
                bundle.selectable_dates
            )
            for slug in initialized_slugs:
                values = bundle.selectable_dates.get(slug, frozenset())
                movie = session.scalar(select(Movie).where(Movie.slug == slug))
                if movie is None:
                    continue
                movie.selectable_dates_initialized_at = now
                desired_dates = {date.fromisoformat(raw) for raw in values}
                for existing in session.scalars(
                    select(SelectableDate).where(SelectableDate.movie_id == movie.id)
                ):
                    if existing.date not in desired_dates:
                        session.delete(existing)
                for raw in values:
                    value = date.fromisoformat(raw)
                    if session.get(SelectableDate, (movie.id, value)) is None:
                        session.add(
                            SelectableDate(
                                movie_id=movie.id,
                                date=value,
                                first_seen_at=now,
                                last_seen_at=now,
                            )
                        )
            if bundle.cooldown_until:
                try:
                    cooldown = datetime.fromisoformat(
                        str(bundle.cooldown_until).replace("Z", "+00:00")
                    )
                except ValueError:
                    cooldown = None
                if cooldown and as_utc(cooldown) > now:
                    gate = session.get(RequestGateState, "global")
                    if gate is None:
                        gate = RequestGateState(key="global")
                        session.add(gate)
                    gate.cooldown_until = cooldown
                    gate.degraded = True

            # Import only sufficiently described showtimes; sparse legacy placeholders
            # remain counted in the dry-run summary but cannot be polled safely.
            for raw in bundle.showtimes:
                showtime_id = str(raw.get("showtime_id") or "")
                when_raw = raw.get("when_utc") or raw.get("showtime_at")
                if not showtime_id or not when_raw:
                    continue
                try:
                    starts_at = (
                        when_raw
                        if isinstance(when_raw, datetime)
                        else datetime.fromisoformat(str(when_raw).replace("Z", "+00:00"))
                    )
                except ValueError:
                    continue
                movie_name = str(raw.get("movie") or raw.get("movie_name") or "")
                movie = session.scalar(
                    select(Movie).where(Movie.normalized_title == movie_name.casefold())
                )
                if movie is None:
                    continue
                legacy_subscription = session.scalar(
                    select(Subscription)
                    .join(
                        SubscriptionMovie,
                        SubscriptionMovie.subscription_id == Subscription.id,
                    )
                    .where(
                        SubscriptionMovie.movie_id == movie.id,
                        Subscription.creation_key.like("legacy:%"),
                    )
                    .order_by(Subscription.creation_key)
                    .limit(1)
                )
                if legacy_subscription is None:
                    continue
                theatre = session.scalar(
                    select(Theatre)
                    .join(
                        SubscriptionTheatre,
                        SubscriptionTheatre.theatre_id == Theatre.id,
                    )
                    .where(
                        SubscriptionTheatre.subscription_id == legacy_subscription.id
                    )
                    .order_by(Theatre.slug)
                    .limit(1)
                )
                presentation = session.scalar(
                    select(PresentationFormat)
                    .join(
                        SubscriptionFormat,
                        SubscriptionFormat.format_id == PresentationFormat.id,
                    )
                    .where(
                        SubscriptionFormat.subscription_id == legacy_subscription.id
                    )
                    .order_by(PresentationFormat.code)
                    .limit(1)
                )
                if theatre is None or presentation is None:
                    continue
                showtime = session.scalar(
                    select(Showtime).where(Showtime.amc_showtime_id == showtime_id)
                )
                imported_status = self._legacy_status(raw.get("availability"))
                checked_raw = raw.get("last_checked_at")
                try:
                    checked_at = (
                        checked_raw
                        if isinstance(checked_raw, datetime)
                        else datetime.fromisoformat(
                            str(checked_raw).replace("Z", "+00:00")
                        )
                        if checked_raw
                        else None
                    )
                except (TypeError, ValueError):
                    checked_at = None
                if checked_at is not None:
                    checked_at = as_utc(checked_at)
                if showtime is None:
                    showtime = Showtime(
                        amc_showtime_id=showtime_id,
                        movie_id=movie.id,
                        theatre_id=theatre.id,
                        format_id=presentation.id,
                        starts_at=starts_at,
                        booking_url=str(raw.get("book_url") or ""),
                        normalized_status=imported_status,
                        is_sold_out=imported_status == "SOLDOUT",
                        active=as_utc(starts_at) > now,
                        next_status_poll_at=now,
                        next_seat_poll_at=now,
                    )
                    session.add(showtime)
                    session.flush()
                else:
                    showtime.movie_id = movie.id
                    showtime.theatre_id = theatre.id
                    showtime.format_id = presentation.id
                    showtime.starts_at = starts_at
                    showtime.booking_url = str(raw.get("book_url") or "")
                    showtime.active = as_utc(starts_at) > now
                    if (
                        imported_status
                        and (
                            checked_at is None
                            or showtime.last_status_poll_at is None
                            or as_utc(showtime.last_status_poll_at) <= checked_at
                        )
                    ):
                        showtime.normalized_status = imported_status
                        showtime.is_sold_out = imported_status == "SOLDOUT"

                metadata = dict(showtime.metadata_json or {})
                best_pairs = raw.get("best_pairs")
                if isinstance(best_pairs, list):
                    metadata["legacy_best_pairs"] = [
                        dict(value) for value in best_pairs[:20] if isinstance(value, Mapping)
                    ]
                available_count = raw.get("available_count")
                if isinstance(available_count, (int, float)):
                    metadata["legacy_available_count"] = max(0, int(available_count))
                if checked_at is not None:
                    metadata["legacy_last_checked_at"] = checked_at.isoformat()

                status_observation_key = (
                    checked_at.isoformat()
                    if checked_at is not None
                    else f"untimed:{imported_status}"
                )
                if (
                    imported_status
                    and metadata.get("legacy_status_observation_key")
                    != status_observation_key
                ):
                    session.add(
                        StatusObservation(
                            showtime_id=showtime.id,
                            observed_at=checked_at or now,
                            normalized_status=imported_status,
                            raw_status=str(raw.get("availability") or ""),
                            is_sold_out=imported_status == "SOLDOUT",
                            is_almost_sold_out=False,
                            was_missing=False,
                        )
                    )
                    metadata["legacy_status_observation_key"] = status_observation_key
                    if (
                        checked_at is not None
                        and (
                            showtime.last_status_poll_at is None
                            or as_utc(showtime.last_status_poll_at) <= checked_at
                        )
                    ):
                        showtime.last_status_poll_at = checked_at
                    session.flush()
                    prune_status_observations(session, showtime.id)

                seatmap = raw.get("seatmap")
                seats = seatmap.get("seats") if isinstance(seatmap, Mapping) else None
                if isinstance(seats, list):
                    compact_seats = [dict(value) for value in seats if isinstance(value, Mapping)]
                    serialized = json.dumps(
                        {
                            "rows": seatmap.get("rows"),
                            "columns": seatmap.get("columns"),
                            "seats": compact_seats,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    layout_hash = hashlib.sha256(serialized.encode()).hexdigest()
                    existing_seat_observation = session.scalar(
                        select(SeatObservation.id).where(
                            SeatObservation.showtime_id == showtime.id,
                            SeatObservation.layout_hash == layout_hash,
                        )
                    )
                    if existing_seat_observation is None:
                        available_coordinates = [
                            [int(value.get("r", 0)), int(value.get("c", 0))]
                            for value in compact_seats
                            if value.get("s") in {"available", "recommended"}
                        ]
                        observed_count = (
                            max(0, int(available_count))
                            if isinstance(available_count, (int, float))
                            else len(available_coordinates)
                        )
                        session.add(
                            SeatObservation(
                                showtime_id=showtime.id,
                                observed_at=checked_at or now,
                                layout_hash=layout_hash,
                                available_seat_count=observed_count,
                                layout=compact_seats,
                                available_coordinates=available_coordinates,
                            )
                        )
                        session.flush()
                        prune_seat_observations(session, showtime.id)
                    metadata["legacy_last_good_seat_hash"] = layout_hash
                    if (
                        checked_at is not None
                        and (
                            showtime.last_seat_poll_at is None
                            or as_utc(showtime.last_seat_poll_at) <= checked_at
                        )
                    ):
                        showtime.last_seat_poll_at = checked_at
                showtime.metadata_json = metadata
            session.flush()

            for subscription in imported_subscriptions.values():
                for edge in session.scalars(
                    select(AvailabilityEdge).where(
                        AvailabilityEdge.subscription_id == subscription.id
                    )
                ):
                    edge.available = False
            for external_id, by_showtime in bundle.alert_edges.items():
                subscription = imported_subscriptions.get(external_id)
                if subscription is None:
                    continue
                for amc_showtime_id, signatures in by_showtime.items():
                    showtime = session.scalar(
                        select(Showtime).where(
                            Showtime.amc_showtime_id == str(amc_showtime_id)
                        )
                    )
                    if showtime is None:
                        continue
                    for signature in signatures:
                        existing = session.scalar(
                            select(AvailabilityEdge).where(
                                AvailabilityEdge.subscription_id == subscription.id,
                                AvailabilityEdge.showtime_id == showtime.id,
                                AvailabilityEdge.seat_key == signature,
                            )
                        )
                        if existing is None:
                            session.add(
                                AvailabilityEdge(
                                    guild_id=subscription.guild_id,
                                    subscription_id=subscription.id,
                                    showtime_id=showtime.id,
                                    seat_key=signature,
                                    available=True,
                                    first_seen_at=now,
                                    last_seen_at=now,
                                    last_alerted_at=now,
                                )
                            )
                        else:
                            existing.available = True
                            existing.last_seen_at = now
                            existing.last_alerted_at = existing.last_alerted_at or now
        return summary

    @staticmethod
    def _legacy_time(value: float, *, end: bool = False) -> time:
        minutes = round(float(value) * 60)
        if minutes >= 24 * 60:
            return time(23, 59, 59) if end else time(0)
        return time(minutes // 60, minutes % 60)

    @staticmethod
    def _legacy_status(value: Any) -> str:
        normalized = normalize_status(value)
        if normalized in {"GREAT", "AVAILABLE", "OPEN"}:
            return "SELLABLE"
        if normalized in {"SOLDOUT", "SOLD", "UNAVAILABLE"}:
            return "SOLDOUT"
        return normalized


__all__ = ["SqlAlchemyWorkerRepository"]
