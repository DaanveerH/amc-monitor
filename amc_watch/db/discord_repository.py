"""Async Discord repository adapter backed by sync SQLAlchemy transactions."""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import re
import socket
import uuid
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.orm import Session

from amc_watch.domain import local_showtime
from amc_watch.discord_models import (
    CatalogOption,
    Destination as DiscordDestination,
    GuildConfiguration,
    MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD,
    MAX_ACTIVE_SUBSCRIPTIONS_PER_USER,
    MAX_ADJACENT_SEATS,
    MAX_MOVIES_PER_SUBSCRIPTION,
    MAX_THEATRES_PER_SUBSCRIPTION,
    MIN_ADJACENT_SEATS,
    MonitorStatus,
    RecommendedRun,
    Seat,
    SeatPreset,
    SubscriptionDraft,
    SubscriptionSummary,
    UserAlert,
    WizardSession as DiscordWizardSession,
    WizardStep,
)

from .models import (
    AuditLog,
    CatalogLookup,
    Destination,
    DiscoveryTarget,
    Guild,
    GuildAccessRole,
    Movie,
    PresentationFormat,
    SelectableDate,
    Showtime,
    Subscription,
    SubscriptionFormat,
    SubscriptionMovie,
    SubscriptionTheatre,
    Theatre,
    UserDelivery,
    UserOutbox,
    WizardSession,
    ZipCentroid,
)
from .repositories import CatalogRepository, DatabaseStore, OutboxRepository, as_utc, utc_now
from .session import Database, set_discord_guild_context, set_guild_context


def _clock(value: str) -> time:
    return time.fromisoformat(value)


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_miles = 3958.7613
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    return 2 * radius_miles * math.asin(math.sqrt(a))


class SqlAlchemyDiscordRepository:
    """Discord protocol implementation that never calls AMC.

    The public methods are asynchronous so Discord's event loop remains responsive;
    blocking psycopg operations execute in a worker thread. Every tenant method sets
    both the Discord snowflake and internal UUID transaction-local RLS settings.
    """

    def __init__(self, database: Database | str, *, lease_seconds: int = 60) -> None:
        self.store = DatabaseStore(database)
        self.database = self.store.database
        self.lease_seconds = lease_seconds
        self.instance_id = f"discord-{socket.gethostname()}-{os.getpid()}"

    @classmethod
    def from_url(cls, url: str, *, lease_seconds: int = 60) -> "SqlAlchemyDiscordRepository":
        return cls(Database(url), lease_seconds=lease_seconds)

    @contextlib.contextmanager
    def _tenant(
        self,
        discord_guild_id: int,
        *,
        create: bool = False,
        actor_user_id: int | None = None,
    ) -> Iterator[tuple[Session, Guild | None]]:
        with self.database.session_factory() as session, session.begin():
            set_discord_guild_context(session, discord_guild_id)
            guild = session.scalar(
                select(Guild).where(Guild.discord_guild_id == discord_guild_id)
            )
            if guild is None and create:
                if actor_user_id is None:
                    raise ValueError("actor_user_id is required to create a guild")
                guild = Guild(
                    discord_guild_id=discord_guild_id,
                    name=f"Discord guild {discord_guild_id}",
                    created_by_discord_user_id=actor_user_id,
                )
                session.add(guild)
                session.flush()
            if guild is not None:
                set_guild_context(session, guild.id)
            yield session, guild

    @staticmethod
    def _audit(
        session: Session,
        guild: Guild,
        actor: int | None,
        action: str,
        entity_type: str,
        entity_id: str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        session.add(
            AuditLog(
                guild_id=guild.id,
                actor_discord_user_id=actor,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                details=details or {},
            )
        )

    @staticmethod
    def _configuration(session: Session, guild: Guild) -> GuildConfiguration:
        role_id = session.scalar(
            select(GuildAccessRole.discord_role_id)
            .where(GuildAccessRole.guild_id == guild.id)
            .order_by(GuildAccessRole.created_at)
            .limit(1)
        )
        return GuildConfiguration(
            guild_id=guild.discord_guild_id,
            enabled=guild.enabled,
            operator_role_id=role_id,
            configured_by_user_id=guild.created_by_discord_user_id,
        )

    async def get_guild(self, guild_id: int) -> GuildConfiguration | None:
        return await asyncio.to_thread(self._get_guild, guild_id)

    def _get_guild(self, guild_id: int) -> GuildConfiguration | None:
        with self._tenant(guild_id) as (session, guild):
            return self._configuration(session, guild) if guild else None

    async def setup_guild(
        self, guild_id: int, actor_user_id: int, operator_role_id: int
    ) -> GuildConfiguration:
        return await asyncio.to_thread(
            self._setup_guild, guild_id, actor_user_id, operator_role_id
        )

    def _setup_guild(
        self, guild_id: int, actor_user_id: int, operator_role_id: int
    ) -> GuildConfiguration:
        with self._tenant(
            guild_id, create=True, actor_user_id=actor_user_id
        ) as (session, guild):
            assert guild is not None
            guild.enabled = True
            roles = list(
                session.scalars(
                    select(GuildAccessRole).where(GuildAccessRole.guild_id == guild.id)
                )
            )
            for role in roles:
                session.delete(role)
            # Flush deletions before re-inserting so re-running setup with the
            # same operator role does not hit the (guild_id, discord_role_id)
            # unique constraint.
            session.flush()
            session.add(
                GuildAccessRole(guild_id=guild.id, discord_role_id=operator_role_id)
            )
            self._audit(
                session,
                guild,
                actor_user_id,
                "guild.setup",
                "guild",
                str(guild.id),
            )
            session.flush()
            return self._configuration(session, guild)

    async def disable_guild(self, guild_id: int, actor_user_id: int) -> None:
        await asyncio.to_thread(self._disable_guild, guild_id, actor_user_id)

    def _disable_guild(self, guild_id: int, actor_user_id: int) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return
            guild.enabled = False
            for subscription in session.scalars(
                select(Subscription).where(Subscription.guild_id == guild.id)
            ):
                subscription.enabled = False
                subscription.configuration_revision += 1
            self._audit(session, guild, actor_user_id, "guild.disable", "guild", str(guild.id))

    async def set_operator_role(
        self, guild_id: int, actor_user_id: int, role_id: int
    ) -> None:
        await asyncio.to_thread(
            self._set_operator_role, guild_id, actor_user_id, role_id
        )

    def _set_operator_role(self, guild_id: int, actor_user_id: int, role_id: int) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            for existing in session.scalars(
                select(GuildAccessRole).where(GuildAccessRole.guild_id == guild.id)
            ):
                session.delete(existing)
            session.add(GuildAccessRole(guild_id=guild.id, discord_role_id=role_id))
            self._audit(
                session, guild, actor_user_id, "access_role.set", "discord_role", str(role_id)
            )

    async def add_destination(
        self, destination: DiscordDestination, actor_user_id: int
    ) -> None:
        await asyncio.to_thread(self._add_destination, destination, actor_user_id)

    def _add_destination(
        self, destination: DiscordDestination, actor_user_id: int
    ) -> None:
        with self._tenant(destination.guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            row = session.scalar(
                select(Destination).where(
                    Destination.guild_id == guild.id,
                    Destination.discord_channel_id == destination.channel_id,
                )
            )
            if row is None:
                row = Destination(
                    guild_id=guild.id,
                    discord_channel_id=destination.channel_id,
                    label=destination.name,
                )
                session.add(row)
            else:
                row.label = destination.name
                row.enabled = destination.enabled
                row.last_error_code = None
            self._audit(
                session,
                guild,
                actor_user_id,
                "destination.add",
                "discord_channel",
                str(destination.channel_id),
            )

    async def remove_destination(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None:
        await asyncio.to_thread(
            self._remove_destination, guild_id, channel_id, actor_user_id
        )

    def _remove_destination(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return
            row = session.scalar(
                select(Destination).where(
                    Destination.guild_id == guild.id,
                    Destination.discord_channel_id == channel_id,
                )
            )
            if row is not None:
                row.enabled = False
                row.last_error_code = "removed_by_operator"
            self._audit(
                session,
                guild,
                actor_user_id,
                "destination.remove",
                "discord_channel",
                str(channel_id),
            )

    async def list_destinations(self, guild_id: int) -> Sequence[DiscordDestination]:
        return await asyncio.to_thread(self._list_destinations, guild_id)

    def _list_destinations(self, guild_id: int) -> list[DiscordDestination]:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return []
            rows = session.scalars(
                select(Destination)
                .where(Destination.guild_id == guild.id)
                .order_by(Destination.label, Destination.discord_channel_id)
            )
            return [
                DiscordDestination(
                    guild_id=guild_id,
                    channel_id=row.discord_channel_id,
                    name=row.label,
                    enabled=row.enabled,
                )
                for row in rows
            ]

    @staticmethod
    def _wizard(row: WizardSession, discord_guild_id: int) -> DiscordWizardSession:
        return DiscordWizardSession(
            id=str(row.id),
            guild_id=discord_guild_id,
            user_id=row.discord_user_id,
            step=WizardStep(row.step),
            data=dict(row.data),
            created_at=as_utc(row.created_at),
            updated_at=as_utc(row.updated_at),
            expires_at=as_utc(row.expires_at),
        )

    async def get_active_wizard(
        self, guild_id: int, user_id: int
    ) -> DiscordWizardSession | None:
        return await asyncio.to_thread(self._get_active_wizard, guild_id, user_id)

    def _get_active_wizard(
        self, guild_id: int, user_id: int
    ) -> DiscordWizardSession | None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return None
            row = session.scalar(
                select(WizardSession)
                .where(
                    WizardSession.guild_id == guild.id,
                    WizardSession.discord_user_id == user_id,
                    WizardSession.step.not_in(("complete", "cancelled")),
                    WizardSession.expires_at > utc_now(),
                )
                .order_by(WizardSession.updated_at.desc())
                .limit(1)
            )
            return self._wizard(row, guild_id) if row else None

    async def get_wizard(
        self, session_id: str, guild_id: int
    ) -> DiscordWizardSession | None:
        return await asyncio.to_thread(self._get_wizard, session_id, guild_id)

    def _get_wizard(
        self, session_id: str, guild_id: int
    ) -> DiscordWizardSession | None:
        try:
            value = uuid.UUID(session_id)
        except ValueError:
            return None
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return None
            row = session.get(WizardSession, value)
            if row is None or row.guild_id != guild.id:
                return None
            return self._wizard(row, guild_id)

    async def save_wizard(self, wizard: DiscordWizardSession) -> None:
        await asyncio.to_thread(self._save_wizard, wizard)

    def _save_wizard(self, wizard: DiscordWizardSession) -> None:
        with self._tenant(wizard.guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            value = uuid.UUID(wizard.id)
            row = session.get(WizardSession, value)
            if row is None:
                row = WizardSession(
                    id=value,
                    guild_id=guild.id,
                    discord_user_id=wizard.user_id,
                    step=wizard.step.value,
                    data=dict(wizard.data),
                    created_at=wizard.created_at,
                    updated_at=wizard.updated_at,
                    expires_at=wizard.expires_at,
                )
                session.add(row)
            else:
                if row.guild_id != guild.id or row.discord_user_id != wizard.user_id:
                    raise PermissionError("wizard tenant or owner mismatch")
                row.step = wizard.step.value
                row.data = dict(wizard.data)
                row.updated_at = wizard.updated_at
                row.expires_at = wizard.expires_at

    async def queue_catalog_lookup(
        self,
        guild_id: int,
        session_id: str,
        kind: str,
        query: Mapping[str, Any],
    ) -> None:
        await asyncio.to_thread(
            self._queue_catalog_lookup, guild_id, session_id, kind, dict(query)
        )

    def _queue_catalog_lookup(
        self,
        guild_id: int,
        session_id: str,
        kind: str,
        query: dict[str, Any],
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            wizard_id = uuid.UUID(session_id)
            wizard = session.get(WizardSession, wizard_id)
            if wizard is None or wizard.guild_id != guild.id:
                raise LookupError("wizard session is unavailable")
            now = utc_now()
            bootstrap_count = 0
            if kind == "movies":
                bootstrap_count = self._bootstrap_selected_theatres(
                    session, query, now=now
                )
            delay_seconds = (
                min(30, 2 + math.ceil(bootstrap_count / 4) * 3)
                if bootstrap_count
                else 0
            )
            CatalogRepository(session).create_lookup(
                guild_id=guild.id,
                wizard_session_id=wizard_id,
                kind=kind,
                query=query,
                run_at=now + timedelta(seconds=delay_seconds),
            )

    @staticmethod
    def _bootstrap_selected_theatres(
        session: Session,
        query: Mapping[str, Any],
        *,
        now: datetime,
    ) -> int:
        theatre_ids = tuple(str(value) for value in query.get("theatre_ids") or ())
        if not 1 <= len(theatre_ids) <= MAX_THEATRES_PER_SUBSCRIPTION:
            raise ValueError("movie lookup requires one to three theatres")

        theatres: list[Theatre] = []
        for selected in theatre_ids:
            theatre = session.scalar(
                select(Theatre).where(
                    or_(Theatre.slug == selected, Theatre.amc_theatre_id == selected)
                )
            )
            if theatre is None:
                raise LookupError("selected theatre was not persisted by the worker")
            theatres.append(theatre)

        today = now.date()
        dates = {today}
        dates.update(
            session.scalars(
                select(SelectableDate.date)
                .where(SelectableDate.date >= today)
                .distinct()
            )
        )
        for theatre in theatres:
            for selected_date in dates:
                target = session.get(DiscoveryTarget, (theatre.id, selected_date))
                if target is None:
                    session.add(
                        DiscoveryTarget(
                            theatre_id=theatre.id,
                            date=selected_date,
                            active=True,
                            next_poll_at=now,
                            last_result_count=-1,
                        )
                    )
                elif not target.active:
                    # Reactivate a retired target for the catalog preview, but
                    # never downgrade a target already serving a live monitor
                    # into catalog-bootstrap mode: that would recreate its
                    # showtimes inactive and disrupt other tenants.
                    target.active = True
                    target.next_poll_at = now
                    target.last_result_count = -1
        return len(theatres) * len(dates)

    @staticmethod
    def _enroll_subscription_resources(
        session: Session, subscription: Subscription, *, now: datetime
    ) -> None:
        """Reactivate shared discovery/showtime resources after create/resume.

        Catalog bootstrap rows remain inert until a confirmed subscription owns
        the matching movie/theatre/format. This method makes that transition in
        the same transaction as the subscription mutation.
        """

        theatres = list(
            session.scalars(
                select(Theatre)
                .join(
                    SubscriptionTheatre,
                    SubscriptionTheatre.theatre_id == Theatre.id,
                )
                .where(SubscriptionTheatre.subscription_id == subscription.id)
            )
        )
        movie_ids = set(
            session.scalars(
                select(SubscriptionMovie.movie_id).where(
                    SubscriptionMovie.subscription_id == subscription.id
                )
            )
        )
        format_ids = set(
            session.scalars(
                select(SubscriptionFormat.format_id).where(
                    SubscriptionFormat.subscription_id == subscription.id
                )
            )
        )
        if not theatres or not movie_ids or not format_ids:
            return

        theatre_by_id = {value.id: value for value in theatres}
        dates_by_theatre: dict[uuid.UUID, set[date]] = {}
        selectable_dates = set(
            session.scalars(
                select(SelectableDate.date).where(
                    SelectableDate.movie_id.in_(movie_ids)
                )
            )
        )
        for theatre in theatres:
            theatre_now = local_showtime(
                as_utc(now),
                str((theatre.metadata_json or {}).get("utc_offset") or ""),
                theatre.timezone,
            )
            today = theatre_now.date()
            upper = today + timedelta(days=subscription.days_ahead)
            dates_by_theatre[theatre.id] = {
                today,
                *(value for value in selectable_dates if today <= value <= upper),
            }

        cached_showtimes = list(
            session.scalars(
                select(Showtime).where(
                    Showtime.theatre_id.in_(theatre_by_id),
                    Showtime.movie_id.in_(movie_ids),
                    Showtime.format_id.in_(format_ids),
                    Showtime.starts_at > now,
                )
            )
        )
        for showtime in cached_showtimes:
            theatre = theatre_by_id[showtime.theatre_id]
            local = local_showtime(
                as_utc(showtime.starts_at),
                str((showtime.metadata_json or {}).get("utc_offset") or ""),
                theatre.timezone,
            )
            today = local_showtime(
                as_utc(now),
                str((theatre.metadata_json or {}).get("utc_offset") or ""),
                theatre.timezone,
            ).date()
            if not today <= local.date() <= today + timedelta(
                days=subscription.days_ahead
            ):
                continue
            weekend = local.weekday() >= 5
            start = (
                subscription.weekend_start
                if weekend
                else subscription.weekday_start
            )
            end = (
                subscription.weekend_end if weekend else subscription.weekday_end
            )
            if not start <= local.time().replace(tzinfo=None) <= end:
                continue
            showtime.active = True
            showtime.next_status_poll_at = now
            showtime.next_seat_poll_at = now
            dates_by_theatre[theatre.id].add(local.date())

        for theatre in theatres:
            for selected_date in dates_by_theatre[theatre.id]:
                target = session.get(
                    DiscoveryTarget, (theatre.id, selected_date)
                )
                if target is None:
                    session.add(
                        DiscoveryTarget(
                            theatre_id=theatre.id,
                            date=selected_date,
                            active=True,
                            next_poll_at=now,
                        )
                    )
                else:
                    target.active = True
                    target.next_poll_at = now

    async def catalog_options(
        self, guild_id: int, session_id: str, kind: str
    ) -> Sequence[CatalogOption] | None:
        return await asyncio.to_thread(
            self._catalog_options, guild_id, session_id, kind
        )

    def _catalog_options(
        self, guild_id: int, session_id: str, kind: str
    ) -> list[CatalogOption] | None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return None
            lookup = CatalogRepository(session).get_lookup(uuid.UUID(session_id), kind)
            if lookup is None or lookup.status != "complete" or lookup.results is None:
                return None
            values: list[CatalogOption] = []
            for result in lookup.results:
                option_id = str(
                    result.get("id")
                    or result.get("slug")
                    or result.get("code")
                    or result.get("name")
                    or ""
                )
                label = str(
                    result.get("label")
                    or result.get("title")
                    or result.get("name")
                    or option_id
                )
                if option_id:
                    values.append(
                        CatalogOption(option_id, label, str(result.get("detail") or ""))
                    )
            return values

    async def nearest_theatres(
        self, zip_code: str, *, limit: int = 25
    ) -> list[CatalogOption] | None:
        """Rank theatres in the static national catalog by distance to a ZIP.

        Returns ``None`` when the ZIP has not been geocoded yet (the caller
        triggers a one-off geocode and shows a loading state); otherwise the
        nearest ``limit`` theatres as ready-to-render options.
        """

        return await asyncio.to_thread(self._nearest_theatres, str(zip_code), limit)

    def _nearest_theatres(
        self, zip_code: str, limit: int
    ) -> list[CatalogOption] | None:
        with self.database.session_factory() as session:
            centroid = session.get(ZipCentroid, zip_code)
            if centroid is None:
                return None
            lat0, lon0 = centroid.latitude, centroid.longitude
            rows = self._theatres_with_coords(session, lat0, lon0, delta=2.0)
            if not rows:
                # Sparse region: fall back to the whole coordinate-bearing catalog.
                rows = self._theatres_with_coords(session, lat0, lon0, delta=None)
            rows.sort(
                key=lambda t: _haversine_miles(lat0, lon0, t.latitude, t.longitude)
            )
            options: list[CatalogOption] = []
            for theatre in rows[:limit]:
                meta = theatre.metadata_json or {}
                detail = (
                    ", ".join(
                        piece for piece in (meta.get("city"), meta.get("state")) if piece
                    )
                    or theatre.zip_code
                )
                options.append(CatalogOption(theatre.slug, theatre.name, detail))
            return options

    @staticmethod
    def _theatres_with_coords(
        session: Session, lat0: float, lon0: float, *, delta: float | None
    ) -> list[Theatre]:
        statement = select(Theatre).where(
            Theatre.latitude.is_not(None), Theatre.longitude.is_not(None)
        )
        if delta is not None:
            statement = statement.where(
                Theatre.latitude.between(lat0 - delta, lat0 + delta),
                Theatre.longitude.between(lon0 - delta, lon0 + delta),
            )
        return list(session.scalars(statement))

    async def available_formats(
        self, theatre_ids: Sequence[str], movie_ids: Sequence[str]
    ) -> list[CatalogOption]:
        """Full static format catalog, ordered with the ones actually playing
        the chosen movie(s) at the chosen theatre(s) first and marked as such."""

        return await asyncio.to_thread(
            self._available_formats, tuple(theatre_ids), tuple(movie_ids)
        )

    def _available_formats(
        self, theatre_ids: tuple[str, ...], movie_ids: tuple[str, ...]
    ) -> list[CatalogOption]:
        with self.database.session_factory() as session:
            theatre_filter = (
                (Theatre.slug.in_(theatre_ids))
                | (Theatre.amc_theatre_id.in_(theatre_ids))
                if theatre_ids
                else True
            )
            movie_filter = (
                (Movie.slug.in_(movie_ids)) | (Movie.amc_movie_id.in_(movie_ids))
                if movie_ids
                else True
            )
            available = set(
                session.scalars(
                    select(PresentationFormat.code)
                    .join(Showtime, Showtime.format_id == PresentationFormat.id)
                    .join(Theatre, Theatre.id == Showtime.theatre_id)
                    .join(Movie, Movie.id == Showtime.movie_id)
                    .where(theatre_filter, movie_filter)
                    .distinct()
                )
            )
            # Base list = curated presentation formats (global_catalog) plus any
            # format actually playing the chosen movie(s) — never the full noisy
            # attributes(groups:[FORMAT]) set.
            formats = [
                fmt
                for fmt in session.scalars(select(PresentationFormat))
                if (fmt.metadata_json or {}).get("global_catalog") is True
                or fmt.code in available
            ]
            options = [
                CatalogOption(
                    fmt.code,
                    fmt.name,
                    "now showing" if fmt.code in available else "not showing yet",
                )
                for fmt in formats
            ]
            # Available formats first, then the rest; alphabetical within each.
            options.sort(key=lambda o: (o.detail != "now showing", o.label.casefold()))
            return options

    async def active_subscription_counts(
        self, guild_id: int, user_id: int
    ) -> tuple[int, int]:
        return await asyncio.to_thread(
            self._active_subscription_counts, guild_id, user_id
        )

    def _active_subscription_counts(self, guild_id: int, user_id: int) -> tuple[int, int]:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return 0, 0
            base = [Subscription.guild_id == guild.id, Subscription.enabled.is_(True)]
            guild_count = session.scalar(
                select(func.count()).select_from(Subscription).where(*base)
            ) or 0
            user_count = session.scalar(
                select(func.count())
                .select_from(Subscription)
                .where(*base, Subscription.created_by_discord_user_id == user_id)
            ) or 0
            return user_count, guild_count

    async def projected_status_cadence(self, draft: SubscriptionDraft) -> float:
        return await asyncio.to_thread(self._projected_status_cadence, draft)

    def _projected_status_cadence(self, draft: SubscriptionDraft) -> float:
        self._validate_draft(draft)
        with self._tenant(draft.guild_id) as (session, guild):
            if guild is None or not guild.enabled:
                raise LookupError("guild is not enabled")
            return self._projected_status_cadence_in_session(session, draft)

    @staticmethod
    def _projected_status_cadence_in_session(
        session: Session, draft: SubscriptionDraft
    ) -> float:
        now = utc_now()
        active_ids = set(
            session.scalars(
                select(Showtime.amc_showtime_id)
                .where(
                    Showtime.active.is_(True),
                    Showtime.starts_at > now,
                )
                .distinct()
            )
        )
        candidate_ids = set(
            session.scalars(
                select(Showtime.amc_showtime_id)
                .join(Theatre, Theatre.id == Showtime.theatre_id)
                .join(Movie, Movie.id == Showtime.movie_id)
                .join(
                    PresentationFormat,
                    PresentationFormat.id == Showtime.format_id,
                )
                .where(
                    Showtime.active.is_(True),
                    Showtime.starts_at > now,
                    or_(
                        Theatre.slug.in_(draft.theatre_ids),
                        Theatre.amc_theatre_id.in_(draft.theatre_ids),
                    ),
                    or_(
                        Movie.slug.in_(draft.movie_ids),
                        Movie.amc_movie_id.in_(draft.movie_ids),
                    ),
                    PresentationFormat.code == draft.format_name,
                )
                .distinct()
            )
        )
        projected = len(active_ids | candidate_ids)
        return float(max(15, math.ceil(max(projected, 1) / 8) * 6))

    @staticmethod
    def _validate_draft(
        draft: SubscriptionDraft,
    ) -> tuple[SeatPreset, time, time, time, time]:
        for value, label in (
            (draft.guild_id, "guild"),
            (draft.owner_user_id, "owner user"),
            (draft.destination_channel_id, "destination channel"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{label} ID must be positive")
        if not isinstance(draft.zip_code, str) or not re.fullmatch(
            r"\d{5}", draft.zip_code
        ):
            raise ValueError("ZIP code must contain exactly five digits")
        if not 1 <= len(draft.theatre_ids) <= MAX_THEATRES_PER_SUBSCRIPTION:
            raise ValueError("a monitor must select one to three theatres")
        if not 1 <= len(draft.movie_ids) <= MAX_MOVIES_PER_SUBSCRIPTION:
            raise ValueError("a monitor must select one to five movies")
        if any(not isinstance(value, str) for value in draft.theatre_ids):
            raise ValueError("invalid theatre selection")
        if any(not isinstance(value, str) for value in draft.movie_ids):
            raise ValueError("invalid movie selection")
        if len(set(draft.theatre_ids)) != len(draft.theatre_ids):
            raise ValueError("theatre selections must be unique")
        if len(set(draft.movie_ids)) != len(draft.movie_ids):
            raise ValueError("movie selections must be unique")
        if any(not str(value).strip() or len(str(value)) > 180 for value in draft.theatre_ids):
            raise ValueError("invalid theatre selection")
        if any(not str(value).strip() or len(str(value)) > 180 for value in draft.movie_ids):
            raise ValueError("invalid movie selection")
        if (
            isinstance(draft.adjacent_seats, bool)
            or not isinstance(draft.adjacent_seats, int)
            or not MIN_ADJACENT_SEATS
            <= draft.adjacent_seats
            <= MAX_ADJACENT_SEATS
        ):
            raise ValueError("adjacent seats must be between one and six")
        try:
            preset = SeatPreset(draft.seat_preset)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid seat preset") from exc
        if (
            not isinstance(draft.format_name, str)
            or not draft.format_name.strip()
            or len(draft.format_name) > 80
        ):
            raise ValueError("invalid presentation format")
        raw_times = (
            draft.weekday_start,
            draft.weekday_end,
            draft.weekend_start,
            draft.weekend_end,
        )
        if any(
            not isinstance(value, str)
            or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value)
            for value in raw_times
        ):
            raise ValueError("time windows must use 24-hour HH:MM values")
        weekday_start, weekday_end, weekend_start, weekend_end = map(_clock, raw_times)
        if weekday_start >= weekday_end or weekend_start >= weekend_end:
            raise ValueError("time-window start must be before its end")
        return preset, weekday_start, weekday_end, weekend_start, weekend_end

    @staticmethod
    def _catalog_entity(
        session: Session,
        model: type[Theatre] | type[Movie],
        selected_id: str,
    ) -> Theatre | Movie:
        if model is Theatre:
            row = session.scalar(
                select(Theatre).where(
                    (Theatre.slug == selected_id) | (Theatre.amc_theatre_id == selected_id)
                )
            )
            if row is None:
                raise LookupError("selected theatre is not in the current catalog")
        else:
            row = session.scalar(
                select(Movie).where((Movie.slug == selected_id) | (Movie.amc_movie_id == selected_id))
            )
            if row is None:
                raise LookupError("selected movie is not in the current catalog")
        return row

    @staticmethod
    def _lock_subscription_capacity(session: Session, guild: Guild) -> Guild:
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"subscription-capacity:{guild.id}"},
            )
        locked = session.scalar(
            select(Guild).where(Guild.id == guild.id).with_for_update()
        )
        if locked is None or not locked.enabled:
            raise LookupError("guild is not enabled")
        return locked

    @classmethod
    def _enforce_subscription_admission(
        cls,
        session: Session,
        guild: Guild,
        *,
        owner_user_id: int,
        draft: SubscriptionDraft,
    ) -> None:
        active_base = (
            Subscription.guild_id == guild.id,
            Subscription.enabled.is_(True),
        )
        guild_count = int(
            session.scalar(
                select(func.count()).select_from(Subscription).where(*active_base)
            )
            or 0
        )
        user_count = int(
            session.scalar(
                select(func.count())
                .select_from(Subscription)
                .where(
                    *active_base,
                    Subscription.created_by_discord_user_id == owner_user_id,
                )
            )
            or 0
        )
        if user_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_USER:
            raise ValueError("member active-monitor limit reached")
        if guild_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD:
            raise ValueError("guild active-monitor limit reached")
        if cls._projected_status_cadence_in_session(session, draft) > 60:
            raise ValueError("projected status latency exceeds 60 seconds")

    @staticmethod
    def _draft_for_subscription(
        session: Session, guild: Guild, row: Subscription
    ) -> SubscriptionDraft:
        destination = session.get(Destination, row.destination_id)
        if destination is None or not destination.enabled:
            raise LookupError("destination is not approved")
        theatre_ids = tuple(
            session.scalars(
                select(Theatre.slug)
                .join(
                    SubscriptionTheatre,
                    SubscriptionTheatre.theatre_id == Theatre.id,
                )
                .where(SubscriptionTheatre.subscription_id == row.id)
            )
        )
        movie_ids = tuple(
            session.scalars(
                select(Movie.slug)
                .join(SubscriptionMovie, SubscriptionMovie.movie_id == Movie.id)
                .where(SubscriptionMovie.subscription_id == row.id)
            )
        )
        format_code = session.scalar(
            select(PresentationFormat.code)
            .join(
                SubscriptionFormat,
                SubscriptionFormat.format_id == PresentationFormat.id,
            )
            .where(SubscriptionFormat.subscription_id == row.id)
            .limit(1)
        )
        if not format_code:
            raise LookupError("subscription format is unavailable")
        return SubscriptionDraft(
            guild_id=guild.discord_guild_id,
            owner_user_id=row.created_by_discord_user_id,
            zip_code=row.zip_code,
            theatre_ids=theatre_ids,
            movie_ids=movie_ids,
            format_name=format_code,
            adjacent_seats=row.seat_count,
            seat_preset=SeatPreset(row.seat_preset),
            weekday_start=row.weekday_start.strftime("%H:%M"),
            weekday_end=row.weekday_end.strftime("%H:%M"),
            weekend_start=row.weekend_start.strftime("%H:%M"),
            weekend_end=row.weekend_end.strftime("%H:%M"),
            destination_channel_id=destination.discord_channel_id,
        )

    async def create_subscription(
        self, draft: SubscriptionDraft, idempotency_key: str
    ) -> SubscriptionSummary:
        return await asyncio.to_thread(
            self._create_subscription, draft, idempotency_key
        )

    def _create_subscription(
        self, draft: SubscriptionDraft, idempotency_key: str
    ) -> SubscriptionSummary:
        if not idempotency_key or len(idempotency_key) > 80:
            raise ValueError("invalid subscription idempotency key")
        (
            preset,
            weekday_start,
            weekday_end,
            weekend_start,
            weekend_end,
        ) = self._validate_draft(draft)
        with self._tenant(draft.guild_id) as (session, guild):
            if guild is None or not guild.enabled:
                raise LookupError("guild is not enabled")
            guild = self._lock_subscription_capacity(session, guild)
            existing = session.scalar(
                select(Subscription)
                .where(
                    Subscription.guild_id == guild.id,
                    Subscription.creation_key == idempotency_key,
                )
                .with_for_update()
            )
            if existing is not None:
                return self._subscription_summary(session, guild, existing)
            self._enforce_subscription_admission(
                session,
                guild,
                owner_user_id=draft.owner_user_id,
                draft=draft,
            )
            destination = session.scalar(
                select(Destination).where(
                    Destination.guild_id == guild.id,
                    Destination.discord_channel_id == draft.destination_channel_id,
                    Destination.enabled.is_(True),
                )
            )
            if destination is None:
                raise LookupError("destination is not approved")
            theatres = [
                self._catalog_entity(session, Theatre, item)
                for item in draft.theatre_ids
            ]
            movies = [
                self._catalog_entity(session, Movie, item)
                for item in draft.movie_ids
            ]
            presentation = session.scalar(
                select(PresentationFormat).where(PresentationFormat.code == draft.format_name)
            )
            if presentation is None:
                raise LookupError("selected format is not in the current catalog")
            row = Subscription(
                creation_key=idempotency_key,
                guild_id=guild.id,
                destination_id=destination.id,
                created_by_discord_user_id=draft.owner_user_id,
                name=((draft.name or " + ".join(draft.movie_ids)).strip() or "monitor")[:120],
                zip_code=draft.zip_code,
                seat_count=draft.adjacent_seats,
                seat_preset=preset.value,
                weekday_start=weekday_start,
                weekday_end=weekday_end,
                weekend_start=weekend_start,
                weekend_end=weekend_end,
            )
            session.add(row)
            session.flush()
            session.add_all(
                [
                    SubscriptionTheatre(
                        guild_id=guild.id, subscription_id=row.id, theatre_id=item.id
                    )
                    for item in theatres
                ]
                + [
                    SubscriptionMovie(
                        guild_id=guild.id, subscription_id=row.id, movie_id=item.id
                    )
                    for item in movies
                ]
                + [
                    SubscriptionFormat(
                        guild_id=guild.id,
                        subscription_id=row.id,
                        format_id=presentation.id,
                    )
                ]
            )
            session.flush()
            self._enroll_subscription_resources(session, row, now=utc_now())
            self._audit(
                session,
                guild,
                draft.owner_user_id,
                "subscription.create",
                "subscription",
                str(row.id),
            )
            session.flush()
            return self._subscription_summary(session, guild, row)

    @staticmethod
    def _subscription_summary(
        session: Session, guild: Guild, row: Subscription
    ) -> SubscriptionSummary:
        theatres = tuple(
            session.scalars(
                select(Theatre.name)
                .join(SubscriptionTheatre, SubscriptionTheatre.theatre_id == Theatre.id)
                .where(SubscriptionTheatre.subscription_id == row.id)
                .order_by(Theatre.name)
            )
        )
        movies = tuple(
            session.scalars(
                select(Movie.title)
                .join(SubscriptionMovie, SubscriptionMovie.movie_id == Movie.id)
                .where(SubscriptionMovie.subscription_id == row.id)
                .order_by(Movie.title)
            )
        )
        presentation = session.scalar(
            select(PresentationFormat.name)
            .join(SubscriptionFormat, SubscriptionFormat.format_id == PresentationFormat.id)
            .where(SubscriptionFormat.subscription_id == row.id)
            .limit(1)
        ) or "Any format"
        destination = session.get(Destination, row.destination_id)
        return SubscriptionSummary(
            id=str(row.id),
            guild_id=guild.discord_guild_id,
            owner_user_id=row.created_by_discord_user_id,
            enabled=row.enabled,
            zip_code=row.zip_code,
            theatres=theatres,
            movies=movies,
            format_name=presentation,
            adjacent_seats=row.seat_count,
            seat_preset=SeatPreset(row.seat_preset),
            weekday_hours=f"{row.weekday_start:%H:%M}-{row.weekday_end:%H:%M}",
            weekend_hours=f"{row.weekend_start:%H:%M}-{row.weekend_end:%H:%M}",
            destination_channel_id=(destination.discord_channel_id if destination else 0),
            name=row.name or "",
        )

    async def list_subscriptions(
        self, guild_id: int, owner_user_id: int | None = None
    ) -> Sequence[SubscriptionSummary]:
        return await asyncio.to_thread(
            self._list_subscriptions, guild_id, owner_user_id
        )

    def _list_subscriptions(
        self, guild_id: int, owner_user_id: int | None
    ) -> list[SubscriptionSummary]:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return []
            statement = select(Subscription).where(Subscription.guild_id == guild.id)
            if owner_user_id is not None:
                statement = statement.where(
                    Subscription.created_by_discord_user_id == owner_user_id
                )
            rows = session.scalars(statement.order_by(Subscription.created_at, Subscription.id))
            return [self._subscription_summary(session, guild, row) for row in rows]

    def _subscription(
        self, session: Session, guild: Guild, subscription_id: str
    ) -> Subscription:
        try:
            value = uuid.UUID(subscription_id)
        except ValueError as exc:
            raise LookupError("invalid subscription ID") from exc
        row = session.get(Subscription, value)
        if row is None or row.guild_id != guild.id:
            raise LookupError("subscription not found")
        return row

    async def update_subscription(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        changes: Mapping[str, Any],
    ) -> SubscriptionSummary:
        return await asyncio.to_thread(
            self._update_subscription,
            guild_id,
            subscription_id,
            actor_user_id,
            dict(changes),
        )

    def _update_subscription(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        changes: dict[str, Any],
    ) -> SubscriptionSummary:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            row = self._subscription(session, guild, subscription_id)
            if "name" in changes:
                # The edit modal enforces a non-empty name; keep the old one if
                # a blank somehow arrives rather than storing an empty string.
                row.name = (str(changes["name"]).strip() or row.name)[:120]
            if "adjacent_seats" in changes:
                row.seat_count = int(changes["adjacent_seats"])
            if "seat_preset" in changes:
                row.seat_preset = SeatPreset(changes["seat_preset"]).value
            for attribute in (
                "weekday_start",
                "weekday_end",
                "weekend_start",
                "weekend_end",
            ):
                if attribute in changes:
                    setattr(row, attribute, _clock(str(changes[attribute])))
            if "destination_channel_id" in changes:
                destination = session.scalar(
                    select(Destination).where(
                        Destination.guild_id == guild.id,
                        Destination.discord_channel_id == int(
                            changes["destination_channel_id"]
                        ),
                        Destination.enabled.is_(True),
                    )
                )
                if destination is None:
                    raise LookupError("destination is not approved")
                row.destination_id = destination.id
            row.configuration_revision += 1
            self._audit(
                session,
                guild,
                actor_user_id,
                "subscription.update",
                "subscription",
                str(row.id),
                {"fields": sorted(changes)},
            )
            session.flush()
            return self._subscription_summary(session, guild, row)

    async def set_subscription_enabled(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        enabled: bool,
    ) -> None:
        await asyncio.to_thread(
            self._set_subscription_enabled,
            guild_id,
            subscription_id,
            actor_user_id,
            enabled,
        )

    def _set_subscription_enabled(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        enabled: bool,
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            if enabled:
                guild = self._lock_subscription_capacity(session, guild)
            row = self._subscription(session, guild, subscription_id)
            if row.enabled == enabled:
                return
            if enabled:
                draft = self._draft_for_subscription(session, guild, row)
                self._validate_draft(draft)
                self._enforce_subscription_admission(
                    session,
                    guild,
                    owner_user_id=row.created_by_discord_user_id,
                    draft=draft,
                )
            row.enabled = enabled
            row.configuration_revision += 1
            if enabled:
                self._enroll_subscription_resources(session, row, now=utc_now())
            self._audit(
                session,
                guild,
                actor_user_id,
                "subscription.resume" if enabled else "subscription.pause",
                "subscription",
                str(row.id),
            )

    async def delete_subscription(
        self, guild_id: int, subscription_id: str, actor_user_id: int
    ) -> None:
        await asyncio.to_thread(
            self._delete_subscription, guild_id, subscription_id, actor_user_id
        )

    def _delete_subscription(
        self, guild_id: int, subscription_id: str, actor_user_id: int
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            row = self._subscription(session, guild, subscription_id)
            self._audit(
                session,
                guild,
                actor_user_id,
                "subscription.delete",
                "subscription",
                str(row.id),
            )
            session.delete(row)

    async def delete_all_subscriptions(self, guild_id: int, actor_user_id: int) -> int:
        return await asyncio.to_thread(
            self._delete_all_subscriptions, guild_id, actor_user_id
        )

    def _delete_all_subscriptions(self, guild_id: int, actor_user_id: int) -> int:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                return 0
            rows = list(
                session.scalars(
                    select(Subscription).where(Subscription.guild_id == guild.id)
                )
            )
            for row in rows:
                session.delete(row)
            self._audit(
                session,
                guild,
                actor_user_id,
                "subscription.delete_all",
                "guild",
                str(guild.id),
                {"count": len(rows)},
            )
            return len(rows)

    async def monitor_status(self, guild_id: int) -> MonitorStatus:
        return await asyncio.to_thread(self._monitor_status, guild_id)

    def _monitor_status(self, guild_id: int) -> MonitorStatus:
        with self._tenant(guild_id) as (session, guild):
            active_subscriptions = 0
            if guild is not None:
                active_subscriptions = session.scalar(
                    select(func.count())
                    .select_from(Subscription)
                    .where(
                        Subscription.guild_id == guild.id,
                        Subscription.enabled.is_(True),
                    )
                ) or 0
        metrics = self.store.metrics()
        cooldown = metrics.get("cooldown_until")
        return MonitorStatus(
            healthy=not bool(metrics.get("degraded")),
            worker_state="degraded" if metrics.get("degraded") else "healthy",
            status_cadence_seconds=float(metrics["projected_status_cadence_seconds"]),
            oldest_job_age_seconds=float(metrics["oldest_job_age_seconds"]),
            cooldown_until=as_utc(cooldown) if isinstance(cooldown, datetime) else None,
            capacity_percent=float(metrics["capacity_utilization"]) * 100,
            active_subscriptions=active_subscriptions,
            active_showtimes=int(metrics["active_showtimes"]),
        )

    async def write_service_heartbeat(self, service_name: str, state: str) -> None:
        await asyncio.to_thread(
            self.store.heartbeat,
            service_name=service_name,
            instance_id=self.instance_id,
            status=state,
            ttl_seconds=90,
            details={},
        )

    async def enqueue_test_alert(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None:
        await asyncio.to_thread(
            self._enqueue_test_alert, guild_id, channel_id, actor_user_id
        )

    def _enqueue_test_alert(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is not configured")
            destination = session.scalar(
                select(Destination).where(
                    Destination.guild_id == guild.id,
                    Destination.discord_channel_id == channel_id,
                    Destination.enabled.is_(True),
                )
            )
            if destination is None:
                raise LookupError("destination is not approved")
            event_id = uuid.uuid4()
            session.add(
                UserOutbox(
                    guild_id=guild.id,
                    destination_id=destination.id,
                    event_key=f"test:{event_id}",
                    payload={
                        "is_test": True,
                        "movie": "AMC Seat Watch test",
                        "theatre": "Configured destination",
                        "when_local": "This is not a real showing",
                        "format": "Test alert",
                        "seat_count": 2,
                        "seat_preset": "center-back",
                        "booking_url": "https://www.amctheatres.com/",
                        "seatmap": {"seats": []},
                        "runs": [],
                    },
                    available_at=utc_now(),
                )
            )
            self._audit(
                session,
                guild,
                actor_user_id,
                "alert.test",
                "discord_channel",
                str(channel_id),
            )

    @staticmethod
    def _alert(row: UserOutbox, guild: Guild, destination: Destination) -> UserAlert:
        payload = dict(row.payload)
        compact = dict(payload.get("seatmap") or {})
        seats: list[Seat] = []
        for raw in compact.get("seats") or []:
            state = str(raw.get("s") or "")
            seats.append(
                Seat(
                    row=int(raw.get("r", 0)),
                    column=int(raw.get("c", 0)),
                    name=str(raw.get("n") or ""),
                    available=state in {"available", "recommended"},
                    type="Companion" if state == "accessible" else "CanReserve",
                    should_display=state != "aisle",
                )
            )
        runs: list[RecommendedRun] = []
        for raw in payload.get("runs") or []:
            runs.append(
                RecommendedRun(
                    names=tuple(str(value) for value in raw.get("names") or ()),
                    coordinates=tuple(
                        (int(value[0]), int(value[1]))
                        for value in raw.get("coordinates") or ()
                    ),
                    score=int(raw.get("score", 0)),
                )
            )
        return UserAlert(
            outbox_id=str(row.id),
            guild_id=guild.discord_guild_id,
            channel_id=destination.discord_channel_id,
            movie=str(payload.get("movie") or "AMC showing"),
            theatre=str(payload.get("theatre") or "AMC theatre"),
            when_local=str(payload.get("when_local") or "Time unavailable"),
            format_name=str(payload.get("format") or payload.get("format_name") or "AMC"),
            adjacent_seats=int(payload.get("seat_count") or 1),
            preset=SeatPreset(str(payload.get("seat_preset") or "center-back")),
            booking_url=str(payload.get("booking_url") or payload.get("book_url") or ""),
            seats=tuple(seats),
            recommendations=tuple(runs),
            is_test=bool(payload.get("is_test")),
        )

    async def claim_user_alerts(
        self, allowed_guild_ids: Sequence[int], limit: int
    ) -> Sequence[UserAlert]:
        return await asyncio.to_thread(
            self._claim_user_alerts, tuple(allowed_guild_ids), limit
        )

    def _claim_user_alerts(
        self, allowed_guild_ids: tuple[int, ...], limit: int
    ) -> list[UserAlert]:
        alerts: list[UserAlert] = []
        remaining_guilds = len(allowed_guild_ids)
        for discord_guild_id in allowed_guild_ids:
            if len(alerts) >= limit:
                break
            per_guild = max(1, math.ceil((limit - len(alerts)) / remaining_guilds))
            remaining_guilds -= 1
            with self._tenant(discord_guild_id) as (session, guild):
                if guild is None or not guild.enabled:
                    continue
                rows = OutboxRepository(session).claim_user(
                    worker_id=self.instance_id,
                    now=utc_now(),
                    limit=per_guild,
                    lease_seconds=self.lease_seconds,
                )
                for row in rows:
                    destination = session.get(Destination, row.destination_id)
                    if destination is None:
                        OutboxRepository(session).dead_letter_user(
                            row.id,
                            worker_id=self.instance_id,
                            error_code="destination_missing",
                        )
                    elif not destination.enabled:
                        OutboxRepository(session).dead_letter_user(
                            row.id,
                            worker_id=self.instance_id,
                            error_code="destination_disabled",
                        )
                    else:
                        alerts.append(self._alert(row, guild, destination))
        return alerts

    async def mark_user_alert_delivered(
        self, guild_id: int, outbox_id: str, discord_message_id: int
    ) -> None:
        await asyncio.to_thread(
            self._mark_user_alert_delivered,
            guild_id,
            outbox_id,
            discord_message_id,
        )

    def _mark_user_alert_delivered(
        self, guild_id: int, outbox_id: str, discord_message_id: int
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None or not OutboxRepository(session).mark_user_delivered(
                uuid.UUID(outbox_id),
                worker_id=self.instance_id,
                discord_message_id=discord_message_id,
            ):
                raise LookupError("alert lease is unavailable")

    async def retry_user_alert(
        self,
        guild_id: int,
        outbox_id: str,
        error_code: str,
        retry_at: datetime,
    ) -> None:
        await asyncio.to_thread(
            self._retry_user_alert,
            guild_id,
            outbox_id,
            error_code,
            retry_at,
        )

    def _retry_user_alert(
        self,
        guild_id: int,
        outbox_id: str,
        error_code: str,
        retry_at: datetime,
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None or not OutboxRepository(session).retry_user(
                uuid.UUID(outbox_id),
                worker_id=self.instance_id,
                retry_at=retry_at,
                error_code=error_code,
            ):
                raise LookupError("alert lease is unavailable")

    async def fail_user_alert(
        self,
        guild_id: int,
        outbox_id: str,
        error_code: str,
        *,
        disable_destination: bool,
    ) -> None:
        await asyncio.to_thread(
            self._fail_user_alert,
            guild_id,
            outbox_id,
            error_code,
            disable_destination,
        )

    def _fail_user_alert(
        self,
        guild_id: int,
        outbox_id: str,
        error_code: str,
        disable_destination: bool,
    ) -> None:
        with self._tenant(guild_id) as (session, guild):
            if guild is None:
                raise LookupError("guild is unavailable")
            row = session.get(UserOutbox, uuid.UUID(outbox_id), with_for_update=True)
            if row is None or row.guild_id != guild.id or row.claimed_by != self.instance_id:
                raise LookupError("alert lease is unavailable")
            if not OutboxRepository(session).dead_letter_user(
                row.id,
                worker_id=self.instance_id,
                error_code=error_code,
            ):
                raise LookupError("alert lease is unavailable")
            if disable_destination:
                destination = session.get(Destination, row.destination_id)
                if destination is not None:
                    destination.enabled = False
                    destination.last_error_code = error_code[:80]


__all__ = ["SqlAlchemyDiscordRepository"]
