"""Discord-facing domain objects and persistence boundary.

The Discord process deliberately depends on this high-level protocol instead of
SQLAlchemy models.  That keeps Discord interactions unit-testable and, more
importantly, makes it impossible for the bot to accidentally call AMC.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence
from uuid import uuid4


MAX_ACTIVE_SUBSCRIPTIONS_PER_USER = 5
MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD = 25
MAX_THEATRES_PER_SUBSCRIPTION = 3
MAX_MOVIES_PER_SUBSCRIPTION = 5
MIN_ADJACENT_SEATS = 1
MAX_ADJACENT_SEATS = 6
WIZARD_TTL = timedelta(minutes=30)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SeatPreset(StrEnum):
    CENTER = "center"
    CENTER_BACK = "center-back"
    CENTER_FRONT = "center-front"


class WizardStep(StrEnum):
    ZIP_CODE = "zip"
    THEATRES = "theatres"
    MOVIES = "movies"
    FORMAT = "format"
    SEATS = "seats"
    TIME_WINDOWS = "time-windows"
    DESTINATION = "destination"
    PREVIEW = "preview"
    COMPLETE = "complete"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class GuildConfiguration:
    guild_id: int
    enabled: bool = True
    operator_role_id: int | None = None
    configured_by_user_id: int | None = None


@dataclass(frozen=True, slots=True)
class Destination:
    guild_id: int
    channel_id: int
    name: str
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class CatalogOption:
    id: str
    label: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class SubscriptionDraft:
    guild_id: int
    owner_user_id: int
    zip_code: str
    theatre_ids: tuple[str, ...]
    movie_ids: tuple[str, ...]
    format_name: str
    adjacent_seats: int
    seat_preset: SeatPreset
    weekday_start: str
    weekday_end: str
    weekend_start: str
    weekend_end: str
    destination_channel_id: int
    name: str | None = None


@dataclass(frozen=True, slots=True)
class SubscriptionSummary:
    id: str
    guild_id: int
    owner_user_id: int
    enabled: bool
    zip_code: str
    theatres: tuple[str, ...]
    movies: tuple[str, ...]
    format_name: str
    adjacent_seats: int
    seat_preset: SeatPreset
    weekday_hours: str
    weekend_hours: str
    destination_channel_id: int
    name: str = ""


@dataclass(frozen=True, slots=True)
class MonitorStatus:
    healthy: bool
    worker_state: str
    status_cadence_seconds: float | None = None
    oldest_job_age_seconds: float | None = None
    cooldown_until: datetime | None = None
    capacity_percent: float | None = None
    active_subscriptions: int = 0
    active_showtimes: int = 0


@dataclass(frozen=True, slots=True)
class Seat:
    row: int
    column: int
    name: str
    available: bool
    type: str = "CanReserve"
    should_display: bool = True


@dataclass(frozen=True, slots=True)
class RecommendedRun:
    names: tuple[str, ...]
    coordinates: tuple[tuple[int, int], ...]
    score: int


@dataclass(frozen=True, slots=True)
class UserAlert:
    outbox_id: str
    guild_id: int
    channel_id: int
    movie: str
    theatre: str
    when_local: str
    format_name: str
    adjacent_seats: int
    preset: SeatPreset
    booking_url: str
    seats: tuple[Seat, ...] = ()
    recommendations: tuple[RecommendedRun, ...] = ()
    is_test: bool = False


@dataclass(frozen=True, slots=True)
class WizardSession:
    id: str
    guild_id: int
    user_id: int
    step: WizardStep
    data: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    expires_at: datetime = field(default_factory=lambda: utc_now() + WIZARD_TTL)

    @classmethod
    def create(cls, guild_id: int, user_id: int) -> "WizardSession":
        return cls(
            id=str(uuid4()),
            guild_id=guild_id,
            user_id=user_id,
            step=WizardStep.ZIP_CODE,
        )

    @property
    def expired(self) -> bool:
        return self.expires_at <= utc_now()

    def advance(
        self,
        step: WizardStep,
        *,
        drop: Sequence[str] = (),
        **values: Any,
    ) -> "WizardSession":
        data = dict(self.data)
        for key in drop:
            data.pop(key, None)
        data.update(values)
        now = utc_now()
        return replace(
            self,
            step=step,
            data=data,
            updated_at=now,
            expires_at=now + WIZARD_TTL,
        )


class DiscordRepository(Protocol):
    """Persistence operations required by the Discord process.

    The PostgreSQL adapter should set the guild RLS context before every
    tenant-scoped operation.  Each method is intentionally coarse grained so
    interaction handlers never manipulate database rows directly.
    """

    async def get_guild(self, guild_id: int) -> GuildConfiguration | None: ...

    async def setup_guild(
        self, guild_id: int, actor_user_id: int, operator_role_id: int
    ) -> GuildConfiguration: ...

    async def disable_guild(self, guild_id: int, actor_user_id: int) -> None: ...

    async def set_operator_role(
        self, guild_id: int, actor_user_id: int, role_id: int
    ) -> None: ...

    async def add_destination(self, destination: Destination, actor_user_id: int) -> None: ...

    async def remove_destination(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None: ...

    async def list_destinations(self, guild_id: int) -> Sequence[Destination]: ...

    async def get_active_wizard(
        self, guild_id: int, user_id: int
    ) -> WizardSession | None: ...

    async def get_wizard(
        self, session_id: str, guild_id: int
    ) -> WizardSession | None: ...

    async def save_wizard(self, session: WizardSession) -> None: ...

    async def queue_catalog_lookup(
        self,
        guild_id: int,
        session_id: str,
        kind: str,
        query: Mapping[str, Any],
    ) -> None: ...

    async def catalog_options(
        self, guild_id: int, session_id: str, kind: str
    ) -> Sequence[CatalogOption] | None: ...

    async def nearest_theatres(
        self, zip_code: str, *, limit: int = 25
    ) -> list[CatalogOption] | None: ...

    async def available_formats(
        self, theatre_ids: Sequence[str], movie_ids: Sequence[str]
    ) -> list[CatalogOption]: ...

    async def active_subscription_counts(self, guild_id: int, user_id: int) -> tuple[int, int]: ...

    async def projected_status_cadence(self, draft: SubscriptionDraft) -> float: ...

    async def create_subscription(
        self, draft: SubscriptionDraft, idempotency_key: str
    ) -> SubscriptionSummary: ...

    async def list_subscriptions(
        self, guild_id: int, owner_user_id: int | None = None
    ) -> Sequence[SubscriptionSummary]: ...

    async def update_subscription(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        changes: Mapping[str, Any],
    ) -> SubscriptionSummary: ...

    async def set_subscription_enabled(
        self,
        guild_id: int,
        subscription_id: str,
        actor_user_id: int,
        enabled: bool,
    ) -> None: ...

    async def delete_subscription(
        self, guild_id: int, subscription_id: str, actor_user_id: int
    ) -> None: ...

    async def delete_all_subscriptions(self, guild_id: int, actor_user_id: int) -> int: ...

    async def monitor_status(self, guild_id: int) -> MonitorStatus: ...

    async def write_service_heartbeat(self, service_name: str, state: str) -> None: ...

    async def enqueue_test_alert(
        self, guild_id: int, channel_id: int, actor_user_id: int
    ) -> None: ...

    async def claim_user_alerts(
        self, allowed_guild_ids: Sequence[int], limit: int
    ) -> Sequence[UserAlert]: ...

    async def mark_user_alert_delivered(
        self, guild_id: int, outbox_id: str, discord_message_id: int
    ) -> None: ...

    async def retry_user_alert(
        self, guild_id: int, outbox_id: str, error_code: str, retry_at: datetime
    ) -> None: ...

    async def fail_user_alert(
        self,
        guild_id: int,
        outbox_id: str,
        error_code: str,
        *,
        disable_destination: bool,
    ) -> None: ...


__all__ = [
    "CatalogOption",
    "Destination",
    "DiscordRepository",
    "GuildConfiguration",
    "MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD",
    "MAX_ACTIVE_SUBSCRIPTIONS_PER_USER",
    "MAX_ADJACENT_SEATS",
    "MAX_MOVIES_PER_SUBSCRIPTION",
    "MAX_THEATRES_PER_SUBSCRIPTION",
    "MIN_ADJACENT_SEATS",
    "MonitorStatus",
    "RecommendedRun",
    "Seat",
    "SeatPreset",
    "SubscriptionDraft",
    "SubscriptionSummary",
    "UserAlert",
    "WIZARD_TTL",
    "WizardSession",
    "WizardStep",
    "utc_now",
]
