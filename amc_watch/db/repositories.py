"""Transactional repositories used by the worker, bot, and notifier."""

from __future__ import annotations

import hashlib
import math
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import Select, and_, delete, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from .models import (
    AvailabilityEdge,
    CatalogLookup,
    Destination,
    Guild,
    MonitorJob,
    Movie,
    OwnerIncident,
    OwnerOutbox,
    PresentationFormat,
    RequestGateState,
    SeatObservation,
    ServiceHeartbeat,
    Showtime,
    StatusObservation,
    Subscription,
    SubscriptionFormat,
    SubscriptionMovie,
    SubscriptionTheatre,
    Theatre,
    UserDelivery,
    UserOutbox,
    ZipCentroid,
)
from .session import Database, dialect_name, guild_transaction, transaction


GLOBAL_CATALOG_FORMATS = {"imax70mm": "IMAX 70MM"}
# AMC's attributes(groups:[FORMAT]) also returns language/subtitle/programming
# attributes (e.g. "arabicspoken", "singalong"). Only these real presentation
# formats are surfaced in the wizard's format picker; everything else is still
# stored for reference but not marked global_catalog.
CURATED_PRESENTATION_FORMATS = frozenset(
    {
        # IMAX family + the two 70mm codes people actively hunt.
        "imax70mm",
        "70mm",
        "imax",
        "imaxwithlaseratamc",
        "film4imax",
        # Premium large / laser / 3D.
        "dolbycinemaatamcprime",
        "laseratamc",
        "reald3d",
        "threed",
        # Panoramic / motion / chain PLFs.
        "4dx",
        "screenx",
        "bigd",
        "amcprime",
        "xl",
    }
)
# RequestGateState key that records when the national catalog was last fully
# refreshed, so the worker re-fetches on a fresh/stale catalog but not every tick.
_CATALOG_REFRESH_KEY = "catalog_refresh"


def utc_now() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    """Normalize SQLite's naive DateTime round-trips to UTC."""

    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def json_safe(value: Any) -> Any:
    """Round-trip operational metadata without leaking object representations."""

    return json.loads(
        json.dumps(
            value,
            default=lambda item: item.isoformat()
            if isinstance(item, (datetime, date))
            else str(item),
        )
    )


# Observation history is diagnostic data, not the live source of truth.  Keep
# enough transition history for incident review while bounding a small database.
STATUS_OBSERVATION_LIMIT = 64
SEAT_OBSERVATION_LIMIT = 12

AMC_TIMEZONE_ABBREVIATIONS = {
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage",
    "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
}


def prune_status_observations(
    session: Session, showtime_id: uuid.UUID, *, limit: int = STATUS_OBSERVATION_LIMIT
) -> None:
    stale_ids = list(
        session.scalars(
            select(StatusObservation.id)
            .where(StatusObservation.showtime_id == showtime_id)
            .order_by(StatusObservation.observed_at.desc(), StatusObservation.id.desc())
            .offset(limit)
        )
    )
    if stale_ids:
        session.execute(delete(StatusObservation).where(StatusObservation.id.in_(stale_ids)))


def prune_seat_observations(
    session: Session, showtime_id: uuid.UUID, *, limit: int = SEAT_OBSERVATION_LIMIT
) -> None:
    stale_ids = list(
        session.scalars(
            select(SeatObservation.id)
            .where(SeatObservation.showtime_id == showtime_id)
            .order_by(SeatObservation.observed_at.desc(), SeatObservation.id.desc())
            .offset(limit)
        )
    )
    if stale_ids:
        session.execute(delete(SeatObservation).where(SeatObservation.id.in_(stale_ids)))


@dataclass(frozen=True)
class RequestSlot:
    acquired: bool
    available_at: datetime
    reason: str | None = None


@dataclass(frozen=True)
class AlertEdgeResult:
    became_available: bool
    outbox_id: uuid.UUID | None


class GuildRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_discord_id(self, discord_guild_id: int) -> Guild | None:
        return self.session.scalar(
            select(Guild).where(Guild.discord_guild_id == discord_guild_id)
        )

    def create(
        self,
        *,
        discord_guild_id: int,
        name: str,
        created_by_discord_user_id: int,
    ) -> Guild:
        guild = Guild(
            discord_guild_id=discord_guild_id,
            name=name,
            created_by_discord_user_id=created_by_discord_user_id,
        )
        self.session.add(guild)
        self.session.flush()
        return guild

    def list_subscriptions(self, *, enabled: bool | None = None) -> list[Subscription]:
        statement = select(Subscription).order_by(Subscription.created_at, Subscription.id)
        if enabled is not None:
            statement = statement.where(Subscription.enabled.is_(enabled))
        return list(self.session.scalars(statement))


class JobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def enqueue(
        self,
        *,
        dedupe_key: str,
        kind: str,
        resource_type: str,
        resource_id: str,
        run_at: datetime,
        priority: int = 100,
        payload: dict[str, Any] | None = None,
        max_attempts: int = 10,
    ) -> MonitorJob:
        """Create or reschedule a durable deduplicated job.

        Completed and dead rows are recycled so a stable resource key never grows
        the table without bound. An already-running lease is left untouched.
        """

        job = self.session.scalar(
            select(MonitorJob)
            .where(MonitorJob.dedupe_key == dedupe_key)
            .with_for_update()
        )
        if job is None:
            job = MonitorJob(
                dedupe_key=dedupe_key,
                kind=kind,
                resource_type=resource_type,
                resource_id=resource_id,
                payload=payload or {},
                run_at=run_at,
                priority=priority,
                max_attempts=max_attempts,
            )
            self.session.add(job)
        elif job.status != "running":
            old_status = job.status
            retry_scheduled = (
                job.status == "pending" and job.last_error_code is not None
            )
            explicit_acceleration = priority == 0
            job.kind = kind
            job.resource_type = resource_type
            job.resource_id = resource_id
            job.payload = payload or {}
            if not retry_scheduled or explicit_acceleration:
                job.run_at = (
                    min(as_utc(job.run_at), as_utc(run_at))
                    if job.status == "pending"
                    else run_at
                )
                job.priority = (
                    min(job.priority, priority)
                    if job.status == "pending"
                    else priority
                )
            job.max_attempts = max_attempts
            job.status = "pending"
            job.attempts = 0 if old_status in {"succeeded", "dead"} else job.attempts
            job.claimed_by = None
            job.claimed_at = None
            job.lease_expires_at = None
            if not retry_scheduled or explicit_acceleration:
                job.last_error_code = None
        self.session.flush()
        return job

    def claim(
        self,
        *,
        worker_id: str,
        now: datetime,
        limit: int,
        lease_seconds: int,
        kinds: Sequence[str] | None = None,
        maximum_priority: int | None = None,
    ) -> list[MonitorJob]:
        due = or_(
            and_(MonitorJob.status == "pending", MonitorJob.run_at <= now),
            and_(
                MonitorJob.status == "running",
                MonitorJob.lease_expires_at.is_not(None),
                MonitorJob.lease_expires_at <= now,
            ),
        )
        statement: Select[tuple[MonitorJob]] = select(MonitorJob).where(due)
        if kinds:
            statement = statement.where(MonitorJob.kind.in_(kinds))
        if maximum_priority is not None:
            statement = statement.where(MonitorJob.priority <= maximum_priority)
        statement = (
            statement.order_by(MonitorJob.priority, MonitorJob.run_at, MonitorJob.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        jobs = list(self.session.scalars(statement))
        lease_until = now + timedelta(seconds=lease_seconds)
        for job in jobs:
            job.status = "running"
            job.claimed_by = worker_id
            job.claimed_at = now
            job.lease_expires_at = lease_until
            job.attempts += 1
        self.session.flush()
        return jobs

    def complete(self, job_id: uuid.UUID, *, worker_id: str) -> bool:
        job = self.session.scalar(
            select(MonitorJob).where(MonitorJob.id == job_id).with_for_update()
        )
        if job is None or job.status != "running" or job.claimed_by != worker_id:
            return False
        # Recurring batch membership changes frequently. Removing successful rows
        # prevents compound batch dedupe keys from growing without bound; leases
        # and dead jobs remain durable for recovery/diagnostics.
        self.session.delete(job)
        return True

    def fail(
        self,
        job_id: uuid.UUID,
        *,
        worker_id: str,
        error_code: str,
        retry_at: datetime,
    ) -> bool:
        job = self.session.scalar(
            select(MonitorJob).where(MonitorJob.id == job_id).with_for_update()
        )
        if job is None or job.status != "running" or job.claimed_by != worker_id:
            return False
        job.last_error_code = error_code[:100]
        job.claimed_by = None
        job.claimed_at = None
        job.lease_expires_at = None
        if job.attempts >= job.max_attempts:
            job.status = "dead"
        else:
            job.status = "pending"
            job.run_at = retry_at
        return True


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    @staticmethod
    def _claim_statement(model: type[UserOutbox] | type[OwnerOutbox], now: datetime, limit: int):
        return (
            select(model)
            .where(
                or_(
                    and_(model.status == "pending", model.available_at <= now),
                    and_(
                        model.status == "sending",
                        model.lease_expires_at.is_not(None),
                        model.lease_expires_at <= now,
                    ),
                )
            )
            .order_by(model.available_at, model.created_at, model.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    def claim_user(
        self,
        *,
        worker_id: str,
        now: datetime,
        limit: int,
        lease_seconds: int,
    ) -> list[UserOutbox]:
        rows = list(
            self.session.scalars(self._claim_statement(UserOutbox, now, limit))
        )
        lease_until = now + timedelta(seconds=lease_seconds)
        for row in rows:
            row.status = "sending"
            row.claimed_by = worker_id
            row.claimed_at = now
            row.lease_expires_at = lease_until
            row.attempts += 1
        self.session.flush()
        return rows

    def claim_owner(
        self,
        *,
        worker_id: str,
        now: datetime,
        limit: int,
        lease_seconds: int,
    ) -> list[OwnerOutbox]:
        rows = list(
            self.session.scalars(self._claim_statement(OwnerOutbox, now, limit))
        )
        lease_until = now + timedelta(seconds=lease_seconds)
        for row in rows:
            row.status = "sending"
            row.claimed_by = worker_id
            row.lease_expires_at = lease_until
            row.attempts += 1
        self.session.flush()
        return rows

    def mark_user_delivered(
        self, outbox_id: uuid.UUID, *, worker_id: str, discord_message_id: int | None
    ) -> bool:
        row = self.session.scalar(
            select(UserOutbox).where(UserOutbox.id == outbox_id).with_for_update()
        )
        if row is None or row.status != "sending" or row.claimed_by != worker_id:
            return False
        row.status = "delivered"
        row.delivered_at = utc_now()
        row.lease_expires_at = None
        self.session.add(
            UserDelivery(
                guild_id=row.guild_id,
                outbox_id=row.id,
                status="delivered",
                discord_message_id=discord_message_id,
            )
        )
        return True

    def retry_user(
        self,
        outbox_id: uuid.UUID,
        *,
        worker_id: str,
        retry_at: datetime,
        error_code: str,
    ) -> bool:
        row = self.session.scalar(
            select(UserOutbox).where(UserOutbox.id == outbox_id).with_for_update()
        )
        if row is None or row.status != "sending" or row.claimed_by != worker_id:
            return False
        row.last_error_code = error_code[:100]
        row.claimed_by = None
        row.claimed_at = None
        row.lease_expires_at = None
        row.status = "dead" if row.attempts >= row.max_attempts else "pending"
        row.available_at = retry_at
        self.session.add(
            UserDelivery(
                guild_id=row.guild_id,
                outbox_id=row.id,
                status="failed",
                error_code=error_code[:100],
            )
        )
        return True

    def dead_letter_user(
        self,
        outbox_id: uuid.UUID,
        *,
        worker_id: str,
        error_code: str,
    ) -> bool:
        row = self.session.scalar(
            select(UserOutbox).where(UserOutbox.id == outbox_id).with_for_update()
        )
        if row is None or row.status != "sending" or row.claimed_by != worker_id:
            return False
        row.status = "dead"
        row.last_error_code = error_code[:100]
        row.claimed_by = None
        row.claimed_at = None
        row.lease_expires_at = None
        self.session.add(
            UserDelivery(
                guild_id=row.guild_id,
                outbox_id=row.id,
                status="failed",
                error_code=error_code[:100],
            )
        )
        return True


class CatalogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_lookup(
        self,
        *,
        guild_id: uuid.UUID,
        wizard_session_id: uuid.UUID,
        kind: str,
        query: dict[str, Any],
        run_at: datetime,
    ) -> CatalogLookup:
        lookup = self.session.scalar(
            select(CatalogLookup)
            .where(
                CatalogLookup.wizard_session_id == wizard_session_id,
                CatalogLookup.kind == kind,
            )
            .with_for_update()
        )
        if lookup is None:
            lookup = CatalogLookup(
                guild_id=guild_id,
                wizard_session_id=wizard_session_id,
                kind=kind,
                query=query,
            )
            self.session.add(lookup)
            self.session.flush()
        elif lookup.query == query and lookup.status != "failed":
            # Same search already in flight or complete. Re-creating here would
            # wipe a just-completed result back to "pending" -- the wizard
            # Refresh loop that never surfaced choices. Keep the row and its job.
            return lookup
        else:
            lookup.query = query
            lookup.results = None
            lookup.status = "pending"
            lookup.error_code = None
            lookup.completed_at = None
        JobRepository(self.session).enqueue(
            dedupe_key=f"catalog:{lookup.id}",
            kind="catalog",
            resource_type="catalog_lookup",
            resource_id=str(lookup.id),
            payload={
                "resources": [
                    {
                        "resource_key": str(lookup.id),
                        "lookup_id": str(lookup.id),
                        "kind": kind,
                        **query,
                    }
                ]
            },
            run_at=run_at,
            priority=50,
        )
        return lookup

    def complete_lookup(
        self,
        lookup_id: uuid.UUID,
        results: Sequence[dict[str, Any]],
        *,
        now: datetime,
    ) -> bool:
        lookup = self.session.get(CatalogLookup, lookup_id, with_for_update=True)
        if lookup is None:
            return False
        normalized = [dict(result) for result in results]
        if lookup.kind == "theatres":
            zip_code = str((lookup.query or {}).get("zip_code") or "")
            for result in normalized:
                slug = str(result.get("slug") or result.get("id") or "").strip()
                name = str(
                    result.get("longName")
                    or result.get("name")
                    or result.get("label")
                    or slug
                ).strip()
                amc_id = str(result.get("theatreId") or "").strip() or None
                if not slug or not name:
                    continue
                conditions = [Theatre.slug == slug]
                if amc_id:
                    conditions.append(Theatre.amc_theatre_id == amc_id)
                theatre = self.session.scalar(select(Theatre).where(or_(*conditions)))
                if theatre is None:
                    theatre = Theatre(
                        amc_theatre_id=amc_id,
                        slug=slug,
                        name=name,
                        zip_code=str(result.get("postalCode") or zip_code),
                    )
                    self.session.add(theatre)
                else:
                    theatre.amc_theatre_id = amc_id or theatre.amc_theatre_id
                    theatre.slug = slug
                    theatre.name = name
                    theatre.zip_code = str(
                        result.get("postalCode") or zip_code or theatre.zip_code
                    )
                timezone_abbreviation = str(
                    result.get("timezoneAbbreviation") or ""
                ).upper()
                if timezone_abbreviation in AMC_TIMEZONE_ABBREVIATIONS:
                    theatre.timezone = AMC_TIMEZONE_ABBREVIATIONS[
                        timezone_abbreviation
                    ]
                theatre.metadata_json = {
                    **dict(theatre.metadata_json or {}),
                    "address": result.get("addressLine1"),
                    "city": result.get("city"),
                    "state": result.get("stateCode"),
                    "distance": result.get("distance"),
                    "timezone_abbreviation": result.get("timezoneAbbreviation"),
                    "utc_offset": result.get("utcOffset"),
                }
        elif lookup.kind == "movie-search":
            clean: list[dict[str, Any]] = []
            for result in normalized:
                movie_id = str(result.get("movie_id") or result.get("movieId") or "").strip()
                slug = str(result.get("slug") or "").strip()
                name = str(result.get("name") or result.get("label") or "").strip()
                if not movie_id or not slug or not name:
                    continue
                movie = self.session.scalar(
                    select(Movie).where(
                        or_(Movie.amc_movie_id == movie_id, Movie.slug == slug)
                    )
                )
                if movie is None:
                    movie = Movie(
                        amc_movie_id=movie_id,
                        slug=slug,
                        title=name,
                        normalized_title=name.casefold(),
                    )
                    self.session.add(movie)
                else:
                    movie.amc_movie_id = movie_id
                    movie.slug = slug
                    movie.title = name
                    movie.normalized_title = name.casefold()
                release_date = result.get("release_date") or result.get("releaseDateUtc")
                movie.metadata_json = {
                    **dict(movie.metadata_json or {}),
                    "release_date": release_date,
                    "catalog_source": "amc-search",
                }
                clean.append(
                    {
                        "id": slug,
                        "label": name,
                        "detail": str(release_date or ""),
                        "movie_id": movie_id,
                        "slug": slug,
                        "name": name,
                        "release_date": release_date,
                    }
                )
            for code, name in GLOBAL_CATALOG_FORMATS.items():
                presentation = self.session.scalar(
                    select(PresentationFormat).where(PresentationFormat.code == code)
                )
                if presentation is None:
                    presentation = PresentationFormat(code=code, name=name)
                    self.session.add(presentation)
                else:
                    presentation.name = name
                presentation.metadata_json = {
                    **dict(presentation.metadata_json or {}),
                    "global_catalog": True,
                }
            normalized = clean
        lookup.results = normalized
        lookup.status = "complete"
        lookup.error_code = None
        lookup.completed_at = now
        return True

    def get_lookup(
        self, wizard_session_id: uuid.UUID, kind: str
    ) -> CatalogLookup | None:
        return self.session.scalar(
            select(CatalogLookup).where(
                CatalogLookup.wizard_session_id == wizard_session_id,
                CatalogLookup.kind == kind,
            )
        )

class DatabaseStore:
    """Scheduler-facing transaction façade.

    Each method owns its transaction. Bot flows that need multiple operations under
    one RLS context should use :meth:`for_guild` and the focused repositories.
    """

    def __init__(self, database: Database | str) -> None:
        self.database = database if isinstance(database, Database) else Database(database)
        self.session_factory: sessionmaker[Session] = self.database.session_factory

    def for_guild(self, guild_id: uuid.UUID | str):
        return guild_transaction(self.session_factory, guild_id)

    def create_catalog_lookup(
        self,
        *,
        guild_id: uuid.UUID,
        wizard_session_id: uuid.UUID,
        kind: str,
        query: dict[str, Any],
        run_at: datetime | None = None,
    ) -> CatalogLookup:
        with guild_transaction(self.session_factory, guild_id) as session:
            return CatalogRepository(session).create_lookup(
                guild_id=guild_id,
                wizard_session_id=wizard_session_id,
                kind=kind,
                query=query,
                run_at=run_at or utc_now(),
            )

    def complete_catalog_lookup(
        self,
        lookup_id: uuid.UUID | str,
        results: Sequence[dict[str, Any]],
        *,
        now: datetime | None = None,
    ) -> bool:
        with transaction(self.session_factory) as session:
            return CatalogRepository(session).complete_lookup(
                uuid.UUID(str(lookup_id)), results, now=now or utc_now()
            )

    def get_catalog_lookup(
        self,
        *,
        guild_id: uuid.UUID,
        wizard_session_id: uuid.UUID,
        kind: str,
    ) -> CatalogLookup | None:
        with guild_transaction(self.session_factory, guild_id) as session:
            return CatalogRepository(session).get_lookup(wizard_session_id, kind)

    def resolve_cached_catalog(
        self, kind: str, query: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Resolve wizard choices already represented by shared catalog/showtimes."""

        with transaction(self.session_factory) as session:
            if kind == "theatres":
                statement = select(Theatre).order_by(Theatre.name)
                if query.get("zip_code"):
                    statement = statement.where(Theatre.zip_code == str(query["zip_code"]))
                return [
                    {"id": row.slug, "label": row.name, "detail": row.zip_code}
                    for row in session.scalars(statement)
                ]

            theatre_ids = tuple(str(value) for value in query.get("theatre_ids") or ())
            theatre_filter = (
                (Theatre.slug.in_(theatre_ids)) | (Theatre.amc_theatre_id.in_(theatre_ids))
                if theatre_ids
                else True
            )
            if kind == "movies":
                discovered = list(
                    session.scalars(
                        select(Movie)
                        .join(Showtime, Showtime.movie_id == Movie.id)
                        .join(Theatre, Theatre.id == Showtime.theatre_id)
                        .where(theatre_filter)
                        .distinct()
                        .order_by(Movie.title)
                    )
                )
                # Imported/presearched future titles intentionally have no
                # showtimes yet. Keep them selectable alongside movies already
                # discovered at the chosen theatres.
                without_showtimes = list(
                    session.scalars(
                        select(Movie)
                        .where(
                            ~select(Showtime.id)
                            .where(Showtime.movie_id == Movie.id)
                            .exists()
                        )
                        .order_by(Movie.title)
                    )
                )
                by_id = {row.id: row for row in (*discovered, *without_showtimes)}
                return [
                    {"id": row.slug, "label": row.title, "detail": ""}
                    for row in sorted(by_id.values(), key=lambda item: item.title.casefold())
                ]

            if kind == "formats":
                movie_ids = tuple(str(value) for value in query.get("movie_ids") or ())
                movie_filter = (
                    (Movie.slug.in_(movie_ids)) | (Movie.amc_movie_id.in_(movie_ids))
                    if movie_ids
                    else True
                )
                discovered = list(
                    session.scalars(
                        select(PresentationFormat)
                        .join(Showtime, Showtime.format_id == PresentationFormat.id)
                        .join(Theatre, Theatre.id == Showtime.theatre_id)
                        .join(Movie, Movie.id == Showtime.movie_id)
                        .where(theatre_filter, movie_filter)
                        .distinct()
                    )
                )
                preseeded = list(
                    session.scalars(
                        select(PresentationFormat)
                        .join(
                            SubscriptionFormat,
                            SubscriptionFormat.format_id == PresentationFormat.id,
                        )
                        .join(
                            Subscription,
                            Subscription.id == SubscriptionFormat.subscription_id,
                        )
                        .join(
                            SubscriptionMovie,
                            SubscriptionMovie.subscription_id == Subscription.id,
                        )
                        .join(Movie, Movie.id == SubscriptionMovie.movie_id)
                        .where(Subscription.enabled.is_(True), movie_filter)
                        .distinct()
                    )
                )
                global_formats = [
                    row
                    for row in session.scalars(select(PresentationFormat))
                    if (row.metadata_json or {}).get("global_catalog") is True
                ]
                by_id = {
                    row.id: row for row in (*discovered, *preseeded, *global_formats)
                }
                return [
                    {"id": row.code, "label": row.name, "detail": ""}
                    for row in sorted(by_id.values(), key=lambda item: item.name.casefold())
                ]

            raise ValueError(f"unsupported catalog kind: {kind}")

    # --- Static reference-data catalog (national theatres + presentation formats) ---

    def upsert_theatre_catalog(self, theatres: Sequence[Mapping[str, Any]]) -> int:
        """Upsert a page of the national theatre catalog. Idempotent by slug/amc id."""

        count = 0
        with transaction(self.session_factory) as session:
            for result in theatres:
                slug = str(result.get("slug") or "").strip()
                name = str(result.get("name") or result.get("longName") or slug).strip()
                amc_id = str(result.get("theatreId") or "").strip() or None
                if not slug or not name:
                    continue
                conditions = [Theatre.slug == slug]
                if amc_id:
                    conditions.append(Theatre.amc_theatre_id == amc_id)
                theatre = session.scalar(select(Theatre).where(or_(*conditions)))
                if theatre is None:
                    theatre = Theatre(
                        slug=slug,
                        name=name,
                        zip_code=str(result.get("postalCode") or ""),
                    )
                    session.add(theatre)
                theatre.amc_theatre_id = amc_id or theatre.amc_theatre_id
                theatre.slug = slug
                theatre.name = name
                if result.get("postalCode"):
                    theatre.zip_code = str(result.get("postalCode"))
                if result.get("latitude") is not None:
                    theatre.latitude = float(result["latitude"])
                if result.get("longitude") is not None:
                    theatre.longitude = float(result["longitude"])
                abbreviation = str(result.get("timezoneAbbreviation") or "").upper()
                if abbreviation in AMC_TIMEZONE_ABBREVIATIONS:
                    theatre.timezone = AMC_TIMEZONE_ABBREVIATIONS[abbreviation]
                theatre.metadata_json = {
                    **dict(theatre.metadata_json or {}),
                    "address": result.get("addressLine1"),
                    "city": result.get("city"),
                    "state": result.get("stateCode"),
                    "timezone_abbreviation": result.get("timezoneAbbreviation"),
                    "utc_offset": result.get("utcOffset"),
                    "brand": result.get("brand"),
                    "market_slug": result.get("marketSlug"),
                    "market_name": result.get("marketName"),
                    "catalog_source": "national-catalog",
                }
                count += 1
        return count

    def upsert_format_catalog(self, formats: Sequence[Mapping[str, Any]]) -> int:
        """Upsert a page of the global presentation-format catalog by code."""

        count = 0
        with transaction(self.session_factory) as session:
            for result in formats:
                code = str(result.get("code") or "").strip().casefold()
                name = str(result.get("name") or "").strip()
                if not code or not name:
                    continue
                presentation = session.scalar(
                    select(PresentationFormat).where(PresentationFormat.code == code)
                )
                if presentation is None:
                    presentation = PresentationFormat(code=code, name=name)
                    session.add(presentation)
                else:
                    presentation.name = name
                presentation.metadata_json = {
                    **dict(presentation.metadata_json or {}),
                    # Only real presentation formats flood the picker; language/
                    # subtitle/programming attributes are stored but not offered.
                    "global_catalog": code in CURATED_PRESENTATION_FORMATS,
                    "abbreviation": result.get("abbreviation"),
                    "sort": result.get("sort"),
                }
                count += 1
        return count

    def upsert_zip_centroid(
        self, zip_code: str, latitude: float, longitude: float
    ) -> None:
        with transaction(self.session_factory) as session:
            centroid = session.get(ZipCentroid, zip_code)
            if centroid is None:
                session.add(
                    ZipCentroid(
                        zip_code=zip_code, latitude=latitude, longitude=longitude
                    )
                )
            else:
                centroid.latitude = latitude
                centroid.longitude = longitude

    def zip_centroid_exists(self, zip_code: str) -> bool:
        with transaction(self.session_factory) as session:
            return session.get(ZipCentroid, zip_code) is not None

    def catalog_refresh_due(self, now: datetime, *, max_age_seconds: float) -> bool:
        with transaction(self.session_factory) as session:
            state = session.get(RequestGateState, _CATALOG_REFRESH_KEY)
            if state is None or state.last_success_at is None:
                return True
            return as_utc(state.last_success_at) < now - timedelta(seconds=max_age_seconds)

    def mark_catalog_refreshed(self, now: datetime) -> None:
        with transaction(self.session_factory) as session:
            state = session.get(RequestGateState, _CATALOG_REFRESH_KEY)
            if state is None:
                session.add(
                    RequestGateState(key=_CATALOG_REFRESH_KEY, last_success_at=now)
                )
            else:
                state.last_success_at = now

    def enqueue_job(
        self,
        *,
        dedupe_key: str,
        kind: str,
        resource_type: str,
        resource_id: str,
        run_at: datetime,
        priority: int = 100,
        payload: dict[str, Any] | None = None,
        max_attempts: int = 10,
    ) -> MonitorJob:
        with transaction(self.session_factory) as session:
            return JobRepository(session).enqueue(
                dedupe_key=dedupe_key,
                kind=kind,
                resource_type=resource_type,
                resource_id=resource_id,
                run_at=run_at,
                priority=priority,
                payload=payload,
                max_attempts=max_attempts,
            )

    def claim_jobs(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        now: datetime | None = None,
        kinds: Sequence[str] | None = None,
        maximum_priority: int | None = None,
    ) -> list[MonitorJob]:
        with transaction(self.session_factory) as session:
            return JobRepository(session).claim(
                worker_id=worker_id,
                now=now or utc_now(),
                limit=limit,
                lease_seconds=lease_seconds,
                kinds=kinds,
                maximum_priority=maximum_priority,
            )

    def complete_job(self, job_id: uuid.UUID, *, worker_id: str) -> bool:
        with transaction(self.session_factory) as session:
            return JobRepository(session).complete(job_id, worker_id=worker_id)

    def fail_job(
        self,
        job_id: uuid.UUID,
        *,
        worker_id: str,
        error_code: str,
        retry_at: datetime,
    ) -> bool:
        with transaction(self.session_factory) as session:
            return JobRepository(session).fail(
                job_id,
                worker_id=worker_id,
                error_code=error_code,
                retry_at=retry_at,
            )

    def acquire_request_slot(
        self,
        *,
        minimum_gap_seconds: float,
        now: datetime | None = None,
    ) -> RequestSlot:
        """Atomically acquire the global AMC request gate when it is due.

        This method never reserves a future slot. Callers receive the next eligible
        timestamp and retry then, preventing crashed workers from stranding capacity.
        """

        now = now or utc_now()
        with transaction(self.session_factory) as session:
            gate = session.get(
                RequestGateState, "global", with_for_update={"skip_locked": False}
            )
            if gate is None:
                gate = RequestGateState(key="global")
                session.add(gate)
                session.flush()
            if gate.cooldown_until is not None and as_utc(gate.cooldown_until) > as_utc(now):
                return RequestSlot(False, as_utc(gate.cooldown_until), "cooldown")
            if (
                gate.transport_backoff_until is not None
                and as_utc(gate.transport_backoff_until) > as_utc(now)
            ):
                return RequestSlot(
                    False,
                    as_utc(gate.transport_backoff_until),
                    "transport_backoff",
                )
            if gate.next_request_at is not None and as_utc(gate.next_request_at) > as_utc(now):
                return RequestSlot(False, as_utc(gate.next_request_at), "minimum_gap")
            gate.next_request_at = now + timedelta(seconds=minimum_gap_seconds)
            return RequestSlot(True, now)

    def set_cooldown(
        self,
        until: datetime,
        *,
        degraded: bool = True,
        now: datetime | None = None,
    ) -> None:
        with transaction(self.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if gate is None:
                gate = RequestGateState(key="global")
                session.add(gate)
            gate.cooldown_until = until
            gate.degraded = degraded
            gate.updated_at = now or utc_now()

    def clear_cooldown(self, *, success_at: datetime | None = None) -> None:
        at = success_at or utc_now()
        with transaction(self.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if gate is None:
                gate = RequestGateState(key="global")
                session.add(gate)
            gate.cooldown_until = None
            gate.degraded = False
            gate.last_success_at = at

    def set_transport_backoff(self, until: datetime) -> None:
        with transaction(self.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if gate is None:
                gate = RequestGateState(key="global")
                session.add(gate)
            gate.transport_backoff_until = until
            gate.degraded = True

    def clear_transport_backoff(self) -> None:
        with transaction(self.session_factory) as session:
            gate = session.get(RequestGateState, "global", with_for_update=True)
            if gate is None:
                return
            gate.transport_backoff_until = None

    def list_due_showtimes(
        self,
        *,
        poll: str,
        now: datetime | None = None,
        limit: int = 100,
    ) -> list[Showtime]:
        now = now or utc_now()
        due_column = (
            Showtime.next_status_poll_at if poll == "status" else Showtime.next_seat_poll_at
        )
        if poll not in {"status", "seats"}:
            raise ValueError("poll must be 'status' or 'seats'")
        with transaction(self.session_factory) as session:
            statement = (
                select(Showtime)
                .where(
                    Showtime.active.is_(True),
                    or_(due_column.is_(None), due_column <= now),
                )
                .order_by(due_column, Showtime.starts_at, Showtime.id)
                .limit(limit)
            )
            return list(session.scalars(statement))

    def update_status_observation(
        self,
        *,
        showtime_id: uuid.UUID,
        normalized_status: str,
        raw_status: str | None,
        is_sold_out: bool,
        is_almost_sold_out: bool,
        observed_at: datetime,
        next_poll_at: datetime,
        was_missing: bool = False,
        retire_after_misses: int = 3,
    ) -> tuple[str, str, int]:
        """Persist status and return ``(old, new, missing_count)``."""

        with transaction(self.session_factory) as session:
            showtime = session.get(Showtime, showtime_id, with_for_update=True)
            if showtime is None:
                raise LookupError(f"unknown showtime {showtime_id}")
            old_status = showtime.normalized_status
            old_sold_out = showtime.is_sold_out
            old_almost_sold_out = showtime.is_almost_sold_out
            latest_observation = session.scalar(
                select(StatusObservation)
                .where(StatusObservation.showtime_id == showtime.id)
                .order_by(
                    StatusObservation.observed_at.desc(),
                    StatusObservation.id.desc(),
                )
                .limit(1)
            )
            if was_missing:
                showtime.missing_count += 1
                if showtime.missing_count >= retire_after_misses:
                    showtime.active = False
            else:
                showtime.missing_count = 0
                showtime.active = True
                showtime.normalized_status = normalized_status
                showtime.is_sold_out = is_sold_out
                showtime.is_almost_sold_out = is_almost_sold_out
                showtime.last_seen_at = observed_at
            showtime.last_status_poll_at = observed_at
            showtime.next_status_poll_at = next_poll_at
            should_record = (
                latest_observation is None
                or was_missing
                or latest_observation.was_missing
                or old_status != normalized_status
                or old_sold_out != is_sold_out
                or old_almost_sold_out != is_almost_sold_out
            )
            if should_record:
                session.add(
                    StatusObservation(
                        showtime_id=showtime.id,
                        observed_at=observed_at,
                        normalized_status=normalized_status,
                        raw_status=raw_status,
                        is_sold_out=is_sold_out,
                        is_almost_sold_out=is_almost_sold_out,
                        was_missing=was_missing,
                    )
                )
                session.flush()
                prune_status_observations(session, showtime.id)
            return old_status, showtime.normalized_status, showtime.missing_count

    def save_seat_observation(
        self,
        *,
        showtime_id: uuid.UUID,
        observed_at: datetime,
        layout: list[dict[str, Any]],
        available_coordinates: list[list[int]],
        layout_hash: str | None,
        next_poll_at: datetime,
    ) -> SeatObservation:
        with transaction(self.session_factory) as session:
            showtime = session.get(Showtime, showtime_id, with_for_update=True)
            if showtime is None:
                raise LookupError(f"unknown showtime {showtime_id}")
            if not layout_hash:
                serialized = json.dumps(layout, sort_keys=True, separators=(",", ":"))
                layout_hash = hashlib.sha256(serialized.encode()).hexdigest()
            latest = session.scalar(
                select(SeatObservation)
                .where(SeatObservation.showtime_id == showtime_id)
                .order_by(
                    SeatObservation.observed_at.desc(), SeatObservation.id.desc()
                )
                .limit(1)
            )
            showtime.last_seat_poll_at = observed_at
            showtime.next_seat_poll_at = next_poll_at
            if latest is not None and latest.layout_hash == layout_hash:
                return latest
            observation = SeatObservation(
                showtime_id=showtime_id,
                observed_at=observed_at,
                layout=layout,
                available_coordinates=available_coordinates,
                available_seat_count=len(available_coordinates),
                layout_hash=layout_hash,
            )
            session.add(observation)
            session.flush()
            prune_seat_observations(session, showtime.id)
            return observation

    def subscription_matches_for_showtime(
        self, showtime_id: uuid.UUID
    ) -> list[Subscription]:
        with transaction(self.session_factory) as session:
            showtime = session.get(Showtime, showtime_id)
            if showtime is None or showtime.format_id is None:
                return []
            conditions: list[Any] = [
                Subscription.enabled.is_(True),
                SubscriptionMovie.movie_id == showtime.movie_id,
                SubscriptionTheatre.theatre_id == showtime.theatre_id,
            ]
            statement = (
                select(Subscription)
                .join(
                    SubscriptionMovie,
                    SubscriptionMovie.subscription_id == Subscription.id,
                )
                .join(
                    SubscriptionTheatre,
                    SubscriptionTheatre.subscription_id == Subscription.id,
                )
                .where(*conditions)
            )
            statement = statement.join(
                SubscriptionFormat,
                SubscriptionFormat.subscription_id == Subscription.id,
            ).where(SubscriptionFormat.format_id == showtime.format_id)
            return list(session.scalars(statement.distinct()))

    def upsert_alert_edge_and_outbox(
        self,
        *,
        guild_id: uuid.UUID,
        subscription_id: uuid.UUID,
        showtime_id: uuid.UUID,
        seat_key: str,
        score: int | None,
        destination_id: uuid.UUID,
        event_key: str,
        payload: dict[str, Any],
        observed_at: datetime | None = None,
    ) -> AlertEdgeResult:
        observed_at = observed_at or utc_now()
        with transaction(self.session_factory) as session:
            edge = session.scalar(
                select(AvailabilityEdge)
                .where(
                    AvailabilityEdge.subscription_id == subscription_id,
                    AvailabilityEdge.showtime_id == showtime_id,
                    AvailabilityEdge.seat_key == seat_key,
                )
                .with_for_update()
            )
            became_available = edge is None or not edge.available
            if edge is None:
                edge = AvailabilityEdge(
                    guild_id=guild_id,
                    subscription_id=subscription_id,
                    showtime_id=showtime_id,
                    seat_key=seat_key,
                    score=score,
                    available=True,
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                )
                session.add(edge)
            else:
                edge.available = True
                edge.score = score
                edge.last_seen_at = observed_at
            outbox: UserOutbox | None = None
            if became_available:
                outbox = session.scalar(
                    select(UserOutbox).where(UserOutbox.event_key == event_key)
                )
                if outbox is None:
                    outbox = UserOutbox(
                        guild_id=guild_id,
                        destination_id=destination_id,
                        subscription_id=subscription_id,
                        event_key=event_key,
                        payload=payload,
                        available_at=observed_at,
                    )
                    session.add(outbox)
                    session.flush()
                    edge.last_alerted_at = observed_at
            return AlertEdgeResult(
                became_available=became_available,
                outbox_id=outbox.id if outbox is not None else None,
            )

    def mark_unseen_edges_unavailable(
        self,
        *,
        subscription_id: uuid.UUID,
        showtime_id: uuid.UUID,
        current_seat_keys: Iterable[str],
    ) -> int:
        keys = set(current_seat_keys)
        with transaction(self.session_factory) as session:
            rows = list(
                session.scalars(
                    select(AvailabilityEdge).where(
                        AvailabilityEdge.subscription_id == subscription_id,
                        AvailabilityEdge.showtime_id == showtime_id,
                        AvailabilityEdge.available.is_(True),
                    )
                )
            )
            changed = 0
            for edge in rows:
                if edge.seat_key not in keys:
                    edge.available = False
                    changed += 1
            return changed

    def heartbeat(
        self,
        service_name: str,
        *,
        instance_id: str | None = None,
        status: str = "healthy",
        ttl_seconds: int = 90,
        details: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> ServiceHeartbeat:
        now = now or utc_now()
        instance_id = instance_id or service_name
        details = json_safe(details if details is not None else metadata or {})
        with transaction(self.session_factory) as session:
            heartbeat = session.get(ServiceHeartbeat, service_name, with_for_update=True)
            if heartbeat is None:
                heartbeat = ServiceHeartbeat(
                    service_name=service_name,
                    instance_id=instance_id,
                    status=status,
                    details=details or {},
                    started_at=now,
                    last_seen_at=now,
                    expires_at=now + timedelta(seconds=ttl_seconds),
                )
                session.add(heartbeat)
            else:
                if heartbeat.instance_id != instance_id:
                    heartbeat.started_at = now
                heartbeat.instance_id = instance_id
                heartbeat.status = status
                heartbeat.details = details or {}
                heartbeat.last_seen_at = now
                heartbeat.expires_at = now + timedelta(seconds=ttl_seconds)
            session.flush()
            return heartbeat

    def set_global_cooldown(self, until: datetime, *, status_code: int) -> None:
        self.set_cooldown(until, degraded=True)

    def clear_global_cooldown(self) -> None:
        self.clear_cooldown()

    def open_incident(
        self, event_type: str, severity: str, metadata: Mapping[str, Any]
    ) -> OwnerIncident:
        resource = str(metadata.get("resource") or "amc-worker")
        summary = str(metadata.get("summary") or event_type.replace("_", " "))
        return self.open_owner_incident(
            incident_key=f"prod:{resource}:{event_type}",
            incident_type=event_type,
            resource=resource,
            severity=severity,
            summary=summary,
            details=dict(metadata),
        )

    def resolve_incident(self, event_type: str) -> bool:
        return self.resolve_owner_incident(
            f"prod:amc-worker:{event_type}",
            summary=f"{event_type.replace('_', ' ')} recovered",
        )

    def metrics(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or utc_now()
        with transaction(self.session_factory) as session:
            active_showtimes = session.scalar(
                select(func.count()).select_from(Showtime).where(Showtime.active.is_(True))
            ) or 0
            pending_jobs = session.scalar(
                select(func.count()).select_from(MonitorJob).where(MonitorJob.status == "pending")
            ) or 0
            oldest_run_at = session.scalar(
                select(func.min(MonitorJob.run_at)).where(MonitorJob.status == "pending")
            )
            delivery_backlog_count = session.scalar(
                select(func.count())
                .select_from(UserOutbox)
                .where(UserOutbox.status.in_(("pending", "sending")))
            ) or 0
            oldest_delivery_created_at = session.scalar(
                select(func.min(UserOutbox.created_at)).where(
                    UserOutbox.status.in_(("pending", "sending"))
                )
            )
            gate = session.get(RequestGateState, "global")
            status_batches = math.ceil(active_showtimes / 8)
            cadence_seconds = max(15, math.ceil(status_batches * 3 / 0.5))
            return {
                "active_showtimes": active_showtimes,
                "pending_jobs": pending_jobs,
                # Global aggregate only: heartbeat/incident payloads intentionally
                # contain no guild, user, channel, event, or alert content.
                "user_delivery_backlog_count": delivery_backlog_count,
                "oldest_user_delivery_age_seconds": (
                    max(
                        0.0,
                        (
                            as_utc(now) - as_utc(oldest_delivery_created_at)
                        ).total_seconds(),
                    )
                    if oldest_delivery_created_at is not None
                    else 0.0
                ),
                "oldest_job_age_seconds": (
                    max(0.0, (as_utc(now) - as_utc(oldest_run_at)).total_seconds())
                    if oldest_run_at is not None
                    else 0.0
                ),
                "cooldown_until": gate.cooldown_until if gate else None,
                "transport_backoff_until": (
                    gate.transport_backoff_until if gate else None
                ),
                "degraded": bool(gate and gate.degraded),
                "projected_status_cadence_seconds": cadence_seconds,
                "capacity_utilization": min(
                    1.0, (status_batches * 3) / (cadence_seconds * 0.5)
                ),
            }

    def open_owner_incident(
        self,
        *,
        incident_key: str,
        incident_type: str,
        resource: str,
        severity: str,
        summary: str,
        details: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> OwnerIncident:
        now = now or utc_now()
        with transaction(self.session_factory) as session:
            incident = session.scalar(
                select(OwnerIncident)
                .where(OwnerIncident.incident_key == incident_key)
                .with_for_update()
            )
            should_notify = incident is None or incident.status == "resolved"
            if incident is None:
                incident = OwnerIncident(
                    incident_key=incident_key,
                    incident_type=incident_type,
                    resource=resource,
                    severity=severity,
                    summary=summary,
                    details=details or {},
                    opened_at=now,
                    updated_at=now,
                )
                session.add(incident)
                session.flush()
            else:
                incident.incident_type = incident_type
                incident.resource = resource
                incident.severity = severity
                incident.summary = summary
                incident.details = details or {}
                incident.updated_at = now
                if should_notify:
                    incident.status = "open"
                    incident.opened_at = now
                    incident.resolved_at = None
                    incident.last_reminded_at = None
            if should_notify:
                event_key = f"{incident_key}:opened:{uuid.uuid4()}"
                session.add(
                    OwnerOutbox(
                        incident_id=incident.id,
                        event_key=event_key,
                        event_type="opened",
                        payload={
                            "severity": severity,
                            "summary": summary,
                            "resource": resource,
                        },
                        available_at=now,
                    )
                )
            session.flush()
            return incident

    def resolve_owner_incident(
        self,
        incident_key: str,
        *,
        summary: str,
        now: datetime | None = None,
    ) -> bool:
        now = now or utc_now()
        with transaction(self.session_factory) as session:
            incident = session.scalar(
                select(OwnerIncident)
                .where(OwnerIncident.incident_key == incident_key)
                .with_for_update()
            )
            if incident is None or incident.status == "resolved":
                return False
            incident.status = "resolved"
            incident.summary = summary
            incident.resolved_at = now
            incident.updated_at = now
            session.add(
                OwnerOutbox(
                    incident_id=incident.id,
                    event_key=f"{incident_key}:recovery:{uuid.uuid4()}",
                    event_type="recovery",
                    payload={"severity": incident.severity, "summary": summary},
                    available_at=now,
                )
            )
            return True


__all__ = [
    "AlertEdgeResult",
    "CatalogRepository",
    "DatabaseStore",
    "GuildRepository",
    "JobRepository",
    "OutboxRepository",
    "RequestSlot",
    "utc_now",
]
