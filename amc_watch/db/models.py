"""SQLAlchemy models for the shared monitor control plane.

Portable SQLAlchemy types keep the model usable with SQLite for fast tests while
PostgreSQL receives native JSONB and UUID columns in production.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    LargeBinary,
    String,
    Text,
    Time,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


JSON_VALUE = JSON().with_variant(JSONB(none_as_null=True), "postgresql")
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")


def uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)


def created_at() -> Mapped[datetime]:
    return mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


def updated_at() -> Mapped[datetime]:
    return mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class Base(DeclarativeBase):
    pass


class Guild(Base):
    __tablename__ = "guilds"

    id: Mapped[uuid.UUID] = uuid_pk()
    discord_guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by_discord_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class GuildAccessRole(Base):
    __tablename__ = "guild_access_roles"
    __table_args__ = (
        UniqueConstraint("guild_id", "discord_role_id", name="uq_guild_access_role"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    discord_role_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = created_at()


class Destination(Base):
    __tablename__ = "destinations"
    __table_args__ = (
        UniqueConstraint("guild_id", "discord_channel_id", name="uq_destination_channel"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    discord_channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    label: Mapped[str] = mapped_column(String(100), nullable=False, default="alerts")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_error_code: Mapped[str | None] = mapped_column(String(80))
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class Movie(Base):
    __tablename__ = "movies"
    __table_args__ = (
        UniqueConstraint("amc_movie_id", name="uq_movie_amc_id"),
        UniqueConstraint("slug", name="uq_movie_slug"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    amc_movie_id: Mapped[str | None] = mapped_column(String(80))
    slug: Mapped[str] = mapped_column(String(180), nullable=False)
    title: Mapped[str] = mapped_column(String(250), nullable=False)
    normalized_title: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON_VALUE, nullable=False, default=dict
    )
    selectable_dates_initialized_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_dates_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_dates_poll_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class Theatre(Base):
    __tablename__ = "theatres"
    __table_args__ = (
        UniqueConstraint("amc_theatre_id", name="uq_theatre_amc_id"),
        UniqueConstraint("slug", name="uq_theatre_slug"),
        # Supports the bounding-box prefilter used by nearest-theatre ranking
        # against the national catalog.
        Index("ix_theatres_lat_lon", "latitude", "longitude"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    amc_theatre_id: Mapped[str | None] = mapped_column(String(80))
    slug: Mapped[str] = mapped_column(String(180), nullable=False)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    zip_code: Mapped[str] = mapped_column(String(12), nullable=False, index=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="America/New_York")
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON_VALUE, nullable=False, default=dict
    )
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class PresentationFormat(Base):
    __tablename__ = "presentation_formats"

    id: Mapped[uuid.UUID] = uuid_pk()
    code: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON_VALUE, nullable=False, default=dict
    )
    created_at: Mapped[datetime] = created_at()


class ZipCentroid(Base):
    """Global ZIP -> lat/long lookup, filled on demand and cached.

    Lets the wizard turn a user's ZIP into a search point for nearest-theatre
    ranking without an AMC call once the ZIP has been geocoded once. Not tenant
    scoped, so no ``guild_id`` and no RLS (see TENANT_TABLES).
    """

    __tablename__ = "zip_centroids"

    zip_code: Mapped[str] = mapped_column(String(12), primary_key=True)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = created_at()


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (
        CheckConstraint("seat_count BETWEEN 1 AND 6", name="ck_subscription_seat_count"),
        CheckConstraint("days_ahead BETWEEN 1 AND 31", name="ck_subscription_days_ahead"),
        CheckConstraint(
            "seat_preset IN ('center', 'center-back', 'center-front')",
            name="ck_subscription_seat_preset",
        ),
        Index("ix_subscriptions_guild_active", "guild_id", "enabled"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    creation_key: Mapped[str | None] = mapped_column(String(80), unique=True)
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    destination_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("destinations.id", ondelete="RESTRICT"), nullable=False
    )
    created_by_discord_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    zip_code: Mapped[str] = mapped_column(String(12), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    seat_count: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    days_ahead: Mapped[int] = mapped_column(Integer, nullable=False, default=14)
    seat_preset: Mapped[str] = mapped_column(String(24), nullable=False, default="center-back")
    weekday_start: Mapped[time] = mapped_column(Time, nullable=False, default=time(17, 0))
    weekday_end: Mapped[time] = mapped_column(Time, nullable=False, default=time(23, 0))
    weekend_start: Mapped[time] = mapped_column(Time, nullable=False, default=time(10, 0))
    weekend_end: Mapped[time] = mapped_column(Time, nullable=False, default=time(23, 0))
    configuration_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class SubscriptionMovie(Base):
    __tablename__ = "subscription_movies"

    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), primary_key=True
    )
    movie_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True
    )
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )


class SubscriptionTheatre(Base):
    __tablename__ = "subscription_theatres"

    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), primary_key=True
    )
    theatre_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("theatres.id", ondelete="CASCADE"), primary_key=True
    )
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )


class SubscriptionFormat(Base):
    __tablename__ = "subscription_formats"

    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), primary_key=True
    )
    format_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("presentation_formats.id", ondelete="CASCADE"), primary_key=True
    )
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )


class SelectableDate(Base):
    __tablename__ = "selectable_dates"

    movie_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("movies.id", ondelete="CASCADE"), primary_key=True
    )
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    first_seen_at: Mapped[datetime] = created_at()
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DiscoveryTarget(Base):
    __tablename__ = "discovery_targets"
    __table_args__ = (
        Index("ix_discovery_targets_due", "active", "next_poll_at"),
    )

    theatre_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("theatres.id", ondelete="CASCADE"), primary_key=True
    )
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_poll_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    last_result_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = updated_at()


class Showtime(Base):
    __tablename__ = "showtimes"
    __table_args__ = (
        Index("ix_showtimes_due_status", "active", "next_status_poll_at"),
        Index("ix_showtimes_due_seats", "active", "next_seat_poll_at"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    amc_showtime_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
    movie_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("movies.id", ondelete="CASCADE"), nullable=False, index=True
    )
    theatre_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("theatres.id", ondelete="CASCADE"), nullable=False, index=True
    )
    format_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("presentation_formats.id", ondelete="SET NULL"), index=True
    )
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    booking_url: Mapped[str | None] = mapped_column(Text)
    normalized_status: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    is_sold_out: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_almost_sold_out: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    missing_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_seen_at: Mapped[datetime] = created_at()
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    next_status_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    next_seat_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_status_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seat_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON_VALUE, nullable=False, default=dict
    )


class StatusObservation(Base):
    __tablename__ = "status_observations"
    __table_args__ = (Index("ix_status_observation_showtime_time", "showtime_id", "observed_at"),)

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    showtime_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("showtimes.id", ondelete="CASCADE"), nullable=False
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    normalized_status: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    raw_status: Mapped[str | None] = mapped_column(String(100))
    is_sold_out: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_almost_sold_out: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    was_missing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class SeatObservation(Base):
    __tablename__ = "seat_observations"
    __table_args__ = (Index("ix_seat_observation_showtime_time", "showtime_id", "observed_at"),)

    id: Mapped[uuid.UUID] = uuid_pk()
    showtime_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("showtimes.id", ondelete="CASCADE"), nullable=False
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    layout_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    available_seat_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    layout: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VALUE, nullable=False, default=list)
    available_coordinates: Mapped[list[list[int]]] = mapped_column(
        JSON_VALUE, nullable=False, default=list
    )


class MonitorJob(Base):
    __tablename__ = "monitor_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'dead')",
            name="ck_monitor_job_status",
        ),
        Index("ix_monitor_jobs_claim", "status", "run_at", "priority"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    dedupe_key: Mapped[str] = mapped_column(String(300), nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    resource_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(180), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_by: Mapped[str | None] = mapped_column(String(120))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class RequestGateState(Base):
    __tablename__ = "request_gate_state"

    key: Mapped[str] = mapped_column(String(40), primary_key=True, default="global")
    next_request_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    transport_backoff_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    degraded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    current_proxy_label: Mapped[str | None] = mapped_column(String(100))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = updated_at()


class ProxyHealth(Base):
    __tablename__ = "proxy_health"

    label: Mapped[str] = mapped_column(String(100), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="unknown")
    consecutive_transport_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exhausted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    incident_key: Mapped[str | None] = mapped_column(String(180))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON_VALUE, nullable=False, default=dict
    )
    updated_at: Mapped[datetime] = updated_at()


class AvailabilityEdge(Base):
    __tablename__ = "availability_edges"
    __table_args__ = (
        UniqueConstraint(
            "subscription_id", "showtime_id", "seat_key", name="uq_availability_edge"
        ),
        Index("ix_availability_edges_guild_active", "guild_id", "available"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False
    )
    showtime_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("showtimes.id", ondelete="CASCADE"), nullable=False
    )
    seat_key: Mapped[str] = mapped_column(String(300), nullable=False)
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    score: Mapped[int | None] = mapped_column(Integer)
    first_seen_at: Mapped[datetime] = created_at()
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_alerted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UserOutbox(Base):
    __tablename__ = "user_outbox"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sending', 'delivered', 'dead')",
            name="ck_user_outbox_status",
        ),
        Index("ix_user_outbox_claim", "status", "available_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    destination_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("destinations.id", ondelete="CASCADE"), nullable=False
    )
    subscription_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="SET NULL")
    )
    event_key: Mapped[str] = mapped_column(String(300), nullable=False, unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=8)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_by: Mapped[str | None] = mapped_column(String(120))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = created_at()


class UserDelivery(Base):
    __tablename__ = "user_deliveries"

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    outbox_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("user_outbox.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempted_at: Mapped[datetime] = created_at()
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    discord_message_id: Mapped[int | None] = mapped_column(BigInteger)
    error_code: Mapped[str | None] = mapped_column(String(100))


class OwnerIncident(Base):
    __tablename__ = "owner_incidents"
    __table_args__ = (
        CheckConstraint(
            "status IN ('open', 'resolved')", name="ck_owner_incident_status"
        ),
        Index("ix_owner_incidents_open", "status", "severity", "updated_at"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    incident_key: Mapped[str] = mapped_column(String(240), nullable=False, unique=True)
    incident_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource: Mapped[str] = mapped_column(String(160), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    opened_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()
    last_reminded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OwnerOutbox(Base):
    __tablename__ = "owner_outbox"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('opened', 'reminder', 'recovery')",
            name="ck_owner_outbox_event_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'sending', 'delivered', 'dead')",
            name="ck_owner_outbox_status",
        ),
        Index("ix_owner_outbox_claim", "status", "available_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    incident_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("owner_incidents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    event_key: Mapped[str] = mapped_column(String(300), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_by: Mapped[str | None] = mapped_column(String(120))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = created_at()


class WizardSession(Base):
    __tablename__ = "wizard_sessions"
    __table_args__ = (
        Index("ix_wizard_sessions_actor", "guild_id", "discord_user_id", "expires_at"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    discord_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    step: Mapped[str] = mapped_column(String(80), nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()


class CatalogLookup(Base):
    __tablename__ = "catalog_lookups"
    __table_args__ = (
        UniqueConstraint("wizard_session_id", "kind", name="uq_catalog_lookup_kind"),
        CheckConstraint(
            "status IN ('pending', 'running', 'complete', 'failed')",
            name="ck_catalog_lookup_status",
        ),
        Index("ix_catalog_lookups_guild_status", "guild_id", "status"),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False, index=True
    )
    wizard_session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("wizard_sessions.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(40), nullable=False)
    query: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    results: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_VALUE)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    error_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = created_at()
    updated_at: Mapped[datetime] = updated_at()
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServiceHeartbeat(Base):
    __tablename__ = "service_heartbeats"

    service_name: Mapped[str] = mapped_column(String(80), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="starting")
    details: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (Index("ix_audit_log_guild_time", "guild_id", "created_at"),)

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    guild_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("guilds.id", ondelete="CASCADE"), nullable=False
    )
    actor_discord_user_id: Mapped[int | None] = mapped_column(BigInteger)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(100))
    details: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    created_at: Mapped[datetime] = created_at()


TENANT_TABLES = (
    Guild,
    GuildAccessRole,
    Destination,
    Subscription,
    SubscriptionMovie,
    SubscriptionTheatre,
    SubscriptionFormat,
    AvailabilityEdge,
    UserOutbox,
    UserDelivery,
    WizardSession,
    CatalogLookup,
    AuditLog,
)


__all__ = [mapper.class_.__name__ for mapper in Base.registry.mappers] + [
    "Base",
    "TENANT_TABLES",
]
