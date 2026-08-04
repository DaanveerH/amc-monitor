from __future__ import annotations

import os
import asyncio
import uuid
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, inspect, select, text
from sqlalchemy.dialects import postgresql

from amc_watch.db.models import (
    AuditLog,
    AvailabilityEdge,
    Base,
    CatalogLookup,
    Destination,
    DiscoveryTarget,
    Guild,
    MonitorJob,
    Movie,
    OwnerIncident,
    OwnerOutbox,
    PresentationFormat,
    RequestGateState,
    SeatObservation,
    SelectableDate,
    ServiceHeartbeat,
    Showtime,
    StatusObservation,
    Subscription,
    SubscriptionFormat,
    SubscriptionMovie,
    SubscriptionTheatre,
    Theatre,
    UserOutbox,
    UserDelivery,
    ZipCentroid,
    WizardSession,
)
from amc_watch.db.discord_repository import SqlAlchemyDiscordRepository
from amc_watch.db.worker_repository import SqlAlchemyWorkerRepository
from amc_watch.discord_models import (
    Destination as DiscordDestination,
    SeatPreset,
    SubscriptionDraft,
)
from amc_watch.amc import ProxyPool
from amc_watch.scheduler import JobKind, SharedWorker, WorkerPolicy
from amc_watch.importer import ImportBundle, ImportedSubscription
from amc_watch.db.repositories import DatabaseStore, JobRepository, as_utc, utc_now
from amc_watch.db.session import Database, guild_transaction, transaction


@pytest.fixture
def database() -> Database:
    value = Database("sqlite://")
    Base.metadata.create_all(value.engine)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture
def store(database: Database) -> DatabaseStore:
    return DatabaseStore(database)


def _future_showtime_start() -> datetime:
    """A near-future slot at 23:00 UTC (18:00-19:00 America/New_York, inside both
    the weekday 17:00-23:00 and weekend 10:00-23:00 windows), kept relative to
    now so the fixture never expires and stays inside the discovery horizon."""
    return (utc_now() + timedelta(days=3)).replace(
        hour=23, minute=0, second=0, microsecond=0
    )


def seed_monitor(database: Database) -> dict[str, object]:
    with transaction(database.session_factory) as session:
        guild = Guild(
            discord_guild_id=123,
            name="Test Guild",
            created_by_discord_user_id=456,
        )
        session.add(guild)
        session.flush()
        destination = Destination(
            guild_id=guild.id,
            discord_channel_id=789,
            label="alerts",
        )
        movie = Movie(
            amc_movie_id="feature-a",
            slug="example-feature",
            title="Example Feature",
            normalized_title="example feature",
        )
        theatre = Theatre(
            amc_theatre_id="example-theatre-13",
            slug="amc-example-8",
            name="AMC Example 8",
            zip_code="00000",
        )
        presentation = PresentationFormat(code="imax-70mm", name="IMAX 70MM")
        session.add_all((destination, movie, theatre, presentation))
        session.flush()
        subscription = Subscription(
            guild_id=guild.id,
            destination_id=destination.id,
            created_by_discord_user_id=456,
            name="Example",
            zip_code="00000",
            seat_count=2,
            seat_preset="center-back",
            weekday_start=time(17),
            weekday_end=time(23),
            weekend_start=time(10),
            weekend_end=time(23),
        )
        session.add(subscription)
        session.flush()
        session.add_all(
            (
                SubscriptionMovie(
                    guild_id=guild.id,
                    subscription_id=subscription.id,
                    movie_id=movie.id,
                ),
                SubscriptionTheatre(
                    guild_id=guild.id,
                    subscription_id=subscription.id,
                    theatre_id=theatre.id,
                ),
                SubscriptionFormat(
                    guild_id=guild.id,
                    subscription_id=subscription.id,
                    format_id=presentation.id,
                ),
            )
        )
        showtime = Showtime(
            amc_showtime_id="10001",
            movie_id=movie.id,
            theatre_id=theatre.id,
            format_id=presentation.id,
            starts_at=_future_showtime_start(),
            normalized_status="SOLDOUT",
        )
        session.add(showtime)
        # Mark the national catalog fresh so worker tests exercise steady state
        # rather than triggering the cold-start catalog-refresh sweep.
        session.add(
            RequestGateState(key="catalog_refresh", last_success_at=utc_now())
        )
        session.flush()
        return {
            "guild_id": guild.id,
            "destination_id": destination.id,
            "subscription_id": subscription.id,
            "showtime_id": showtime.id,
        }


def test_schema_contains_all_control_plane_subsystems(database: Database) -> None:
    expected = {
        "guilds",
        "destinations",
        "subscriptions",
        "movies",
        "theatres",
        "presentation_formats",
        "showtimes",
        "status_observations",
        "seat_observations",
        "monitor_jobs",
        "request_gate_state",
        "proxy_health",
        "availability_edges",
        "user_outbox",
        "owner_incidents",
        "owner_outbox",
        "wizard_sessions",
        "catalog_lookups",
        "service_heartbeats",
        "audit_log",
    }
    assert expected <= set(inspect(database.engine).get_table_names())


def test_guild_transaction_records_rls_context(database: Database) -> None:
    guild_id = uuid.uuid4()
    with guild_transaction(database.session_factory, guild_id) as session:
        assert session.info["guild_id"] == str(guild_id)


def test_deduplicated_jobs_are_claimed_and_success_rows_do_not_accumulate(store: DatabaseStore) -> None:
    now = utc_now()
    first = store.enqueue_job(
        dedupe_key="status:10001",
        kind="status",
        resource_type="showtime",
        resource_id="10001",
        run_at=now,
        priority=20,
    )
    second = store.enqueue_job(
        dedupe_key="status:10001",
        kind="status",
        resource_type="showtime",
        resource_id="10001",
        run_at=now + timedelta(minutes=1),
        priority=30,
    )
    assert first.id == second.id

    claimed = store.claim_jobs(
        worker_id="worker-a", now=now, limit=5, lease_seconds=60
    )
    assert [job.id for job in claimed] == [first.id]
    assert claimed[0].attempts == 1
    assert store.complete_job(first.id, worker_id="worker-b") is False
    assert store.complete_job(first.id, worker_id="worker-a") is True

    recycled = store.enqueue_job(
        dedupe_key="status:10001",
        kind="status",
        resource_type="showtime",
        resource_id="10001",
        run_at=now + timedelta(minutes=2),
    )
    assert recycled.id != first.id
    assert recycled.status == "pending"
    assert recycled.attempts == 0


def test_expired_job_lease_is_reclaimed(store: DatabaseStore) -> None:
    now = utc_now()
    job = store.enqueue_job(
        dedupe_key="discovery:example:2026-07-20",
        kind="discovery",
        resource_type="theatre_date",
        resource_id="example:2026-07-20",
        run_at=now,
    )
    first = store.claim_jobs(
        worker_id="dead-worker", now=now, limit=1, lease_seconds=10
    )
    assert first[0].id == job.id
    second = store.claim_jobs(
        worker_id="replacement",
        now=now + timedelta(seconds=11),
        limit=1,
        lease_seconds=10,
    )
    assert second[0].claimed_by == "replacement"
    assert second[0].attempts == 2


def test_claim_query_uses_postgresql_skip_locked(database: Database) -> None:
    statement = (
        select(MonitorJob)
        .where(MonitorJob.status == "pending")
        .with_for_update(skip_locked=True)
    )
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_global_request_gate_enforces_gap_and_cooldown(store: DatabaseStore) -> None:
    now = utc_now()
    acquired = store.acquire_request_slot(minimum_gap_seconds=3, now=now)
    assert acquired.acquired is True
    too_soon = store.acquire_request_slot(
        minimum_gap_seconds=3, now=now + timedelta(seconds=1)
    )
    assert too_soon.acquired is False
    assert too_soon.reason == "minimum_gap"

    store.set_cooldown(now + timedelta(minutes=60), now=now)
    blocked = store.acquire_request_slot(
        minimum_gap_seconds=3, now=now + timedelta(seconds=4)
    )
    assert blocked.acquired is False
    assert blocked.reason == "cooldown"
    store.clear_cooldown(success_at=now + timedelta(minutes=61))
    assert store.acquire_request_slot(
        minimum_gap_seconds=3, now=now + timedelta(minutes=61)
    ).acquired


def test_status_requires_repeated_misses_and_recovers(
    store: DatabaseStore, database: Database
) -> None:
    ids = seed_monitor(database)
    showtime_id = ids["showtime_id"]
    assert isinstance(showtime_id, uuid.UUID)
    now = utc_now()
    for count in range(1, 4):
        old, new, misses = store.update_status_observation(
            showtime_id=showtime_id,
            normalized_status="",
            raw_status=None,
            is_sold_out=False,
            is_almost_sold_out=False,
            was_missing=True,
            observed_at=now + timedelta(seconds=count),
            next_poll_at=now + timedelta(seconds=count + 15),
        )
        assert (old, new, misses) == ("SOLDOUT", "SOLDOUT", count)
    with transaction(database.session_factory) as session:
        assert session.get(Showtime, showtime_id).active is False
        assert session.scalar(select(func.count()).select_from(StatusObservation)) == 3

    old, new, misses = store.update_status_observation(
        showtime_id=showtime_id,
        normalized_status="SELLABLE",
        raw_status="Sellable",
        is_sold_out=False,
        is_almost_sold_out=False,
        observed_at=now + timedelta(minutes=1),
        next_poll_at=now + timedelta(minutes=1, seconds=15),
    )
    assert (old, new, misses) == ("SOLDOUT", "SELLABLE", 0)
    with transaction(database.session_factory) as session:
        assert session.get(Showtime, showtime_id).active is True


def test_availability_edge_and_outbox_are_transactional_and_edge_triggered(
    store: DatabaseStore, database: Database
) -> None:
    ids = seed_monitor(database)
    kwargs = {
        "guild_id": ids["guild_id"],
        "subscription_id": ids["subscription_id"],
        "showtime_id": ids["showtime_id"],
        "seat_key": "H21+H22",
        "score": 98,
        "destination_id": ids["destination_id"],
        "event_key": "availability:10001:H21+H22:1",
        "payload": {"movie": "Example Feature"},
    }
    first = store.upsert_alert_edge_and_outbox(**kwargs)
    duplicate = store.upsert_alert_edge_and_outbox(**kwargs)
    assert first.became_available is True
    assert first.outbox_id is not None
    assert duplicate.became_available is False
    assert duplicate.outbox_id is None
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(UserOutbox)) == 1
        assert session.scalar(select(func.count()).select_from(AvailabilityEdge)) == 1

    assert store.mark_unseen_edges_unavailable(
        subscription_id=ids["subscription_id"],
        showtime_id=ids["showtime_id"],
        current_seat_keys=set(),
    ) == 1
    kwargs["event_key"] = "availability:10001:H21+H22:2"
    reappeared = store.upsert_alert_edge_and_outbox(**kwargs)
    assert reappeared.became_available is True
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(UserOutbox)) == 2


def test_catalog_lookup_is_durable_and_worker_completes_it(
    store: DatabaseStore, database: Database
) -> None:
    ids = seed_monitor(database)
    now = utc_now()
    wizard_id = uuid.uuid4()
    with transaction(database.session_factory) as session:
        session.add(
            WizardSession(
                id=wizard_id,
                guild_id=ids["guild_id"],
                discord_user_id=456,
                step="theatres",
                expires_at=now + timedelta(minutes=30),
            )
        )
    lookup = store.create_catalog_lookup(
        guild_id=ids["guild_id"],
        wizard_session_id=wizard_id,
        kind="theatres",
        query={"zip_code": "00000"},
        run_at=now,
    )
    assert lookup.status == "pending"
    assert store.complete_catalog_lookup(
        lookup.id,
        [{"id": "example", "label": "AMC Example 8"}],
        now=now + timedelta(seconds=3),
    )
    loaded = store.get_catalog_lookup(
        guild_id=ids["guild_id"],
        wizard_session_id=wizard_id,
        kind="theatres",
    )
    assert loaded is not None
    assert loaded.status == "complete"
    assert loaded.results == [{"id": "example", "label": "AMC Example 8"}]
    with transaction(database.session_factory) as session:
        job = session.scalar(select(MonitorJob).where(MonitorJob.kind == "catalog"))
        assert job.payload["resources"][0]["lookup_id"] == str(lookup.id)


def test_owner_incidents_dedupe_and_emit_one_recovery(
    store: DatabaseStore, database: Database
) -> None:
    first = store.open_owner_incident(
        incident_key="worker:stale",
        incident_type="stale_heartbeat",
        resource="amc-worker",
        severity="critical",
        summary="Worker heartbeat is stale",
    )
    second = store.open_owner_incident(
        incident_key="worker:stale",
        incident_type="stale_heartbeat",
        resource="amc-worker",
        severity="critical",
        summary="Worker heartbeat remains stale",
    )
    assert first.id == second.id
    assert store.resolve_owner_incident("worker:stale", summary="Worker recovered")
    assert not store.resolve_owner_incident("worker:stale", summary="Worker recovered")
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(OwnerIncident)) == 1
        events = list(session.scalars(select(OwnerOutbox.event_type).order_by(OwnerOutbox.created_at)))
        assert events == ["opened", "recovery"]


def test_heartbeat_and_metrics_are_restart_safe(
    store: DatabaseStore, database: Database
) -> None:
    now = utc_now()
    store.heartbeat(
        service_name="amc-worker",
        instance_id="worker-a",
        status="healthy",
        ttl_seconds=90,
        details={"active_showtimes": 0},
        now=now,
    )
    store.enqueue_job(
        dedupe_key="dates:feature-a",
        kind="selectable_dates",
        resource_type="movie",
        resource_id="feature-a",
        run_at=now - timedelta(seconds=20),
    )
    metrics = store.metrics(now=now)
    assert metrics["pending_jobs"] == 1
    assert metrics["user_delivery_backlog_count"] == 0
    assert metrics["oldest_user_delivery_age_seconds"] == 0
    assert metrics["oldest_job_age_seconds"] == 20
    assert metrics["projected_status_cadence_seconds"] == 15
    with transaction(database.session_factory) as session:
        heartbeat = session.get(ServiceHeartbeat, "amc-worker")
        assert heartbeat.instance_id == "worker-a"


def test_status_and_seat_observation_history_is_deduplicated_and_bounded(
    store: DatabaseStore, database: Database
) -> None:
    ids = seed_monitor(database)
    showtime_id = ids["showtime_id"]
    assert isinstance(showtime_id, uuid.UUID)
    now = utc_now()

    # A healthy 15-second status loop keeps the latest state on Showtime but
    # does not append a diagnostic row for every unchanged sample.
    for index in range(100):
        store.update_status_observation(
            showtime_id=showtime_id,
            normalized_status="SOLDOUT",
            raw_status="SOLDOUT",
            is_sold_out=False,
            is_almost_sold_out=False,
            observed_at=now + timedelta(seconds=index * 15),
            next_poll_at=now + timedelta(seconds=(index + 1) * 15),
        )
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(StatusObservation)) == 1

    # Even pathological status flapping retains only a small transition window.
    for index in range(100):
        value = "SELLABLE" if index % 2 else "SOLDOUT"
        store.update_status_observation(
            showtime_id=showtime_id,
            normalized_status=value,
            raw_status=value,
            is_sold_out=False,
            is_almost_sold_out=False,
            observed_at=now + timedelta(hours=1, seconds=index),
            next_poll_at=now + timedelta(hours=1, seconds=index + 15),
        )
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(StatusObservation)) == 64

    layout = [{"row": "H", "seat": "21", "available": True}]
    for index in range(50):
        store.save_seat_observation(
            showtime_id=showtime_id,
            observed_at=now + timedelta(minutes=index),
            layout=layout,
            available_coordinates=[[7, 20]],
            layout_hash="stable-layout",
            next_poll_at=now + timedelta(minutes=index + 5),
        )
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(SeatObservation)) == 1

    for index in range(20):
        store.save_seat_observation(
            showtime_id=showtime_id,
            observed_at=now + timedelta(days=1, minutes=index),
            layout=[{"revision": index}],
            available_coordinates=[],
            layout_hash=f"layout-{index}",
            next_poll_at=now + timedelta(days=1, minutes=index + 5),
        )
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(SeatObservation)) == 12


def test_worker_adapter_does_not_grow_history_for_stable_polls(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    for _ in range(100):
        repository.record_status(
            "10001",
            status="SOLDOUT",
            is_sold_out=False,
            is_almost_sold_out=False,
            missing=False,
        )
    target = repository.subscriptions_for_showtime("10001")[0]
    payload = {
        "runs": [{"signature": "H21+H22", "score": 100}],
        "seatmap": {
            "seats": [
                {"r": 7, "c": 20, "s": "recommended"},
                {"r": 7, "c": 21, "s": "recommended"},
            ]
        },
    }
    for _ in range(100):
        repository.apply_availability(
            target,
            "10001",
            signatures={"H21+H22"},
            alert_payload=payload,
        )
    alternate_preset_payload = {
        **payload,
        "seatmap": {
            "seats": [
                {"r": 7, "c": 20, "s": "available"},
                {"r": 7, "c": 21, "s": "recommended"},
            ]
        },
    }
    repository.apply_availability(
        target,
        "10001",
        signatures={"H21+H22"},
        alert_payload=alternate_preset_payload,
    )
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(StatusObservation)) == 1
        assert session.scalar(select(func.count()).select_from(SeatObservation)) == 1


def test_worker_reports_sanitized_delivery_backlog_incident_and_recovery(
    database: Database,
) -> None:
    ids = seed_monitor(database)
    now = utc_now()
    with transaction(database.session_factory) as session:
        session.add(
            UserOutbox(
                guild_id=ids["guild_id"],
                destination_id=ids["destination_id"],
                subscription_id=ids["subscription_id"],
                event_key="availability:backlog-test",
                payload={"private_user_content": "must-not-leak"},
                status="pending",
                available_at=now - timedelta(minutes=10),
                created_at=now - timedelta(minutes=10),
            )
        )
    repository = SqlAlchemyWorkerRepository(database)
    worker = SharedWorker(
        repository,
        ProxyPool.from_values("http://user:secret@proxy.test:8080", []),
        policy=WorkerPolicy(delivery_backlog_age_seconds=300),
    )

    worker._heartbeat()
    with transaction(database.session_factory) as session:
        incident = session.scalar(
            select(OwnerIncident).where(
                OwnerIncident.incident_type == "delivery_backlog"
            )
        )
        assert incident is not None and incident.status == "open"
        assert incident.details["pending_count"] == 1
        assert incident.details["oldest_age_seconds"] >= 599
        assert "must-not-leak" not in str(incident.details)
        assert session.scalar(select(func.count()).select_from(OwnerOutbox)) == 1
        outbox = session.scalar(select(UserOutbox))
        outbox.status = "delivered"
        outbox.delivered_at = now

    worker._heartbeat()
    with transaction(database.session_factory) as session:
        incident = session.scalar(
            select(OwnerIncident).where(
                OwnerIncident.incident_type == "delivery_backlog"
            )
        )
        assert incident.status == "resolved"
        assert list(
            session.scalars(select(OwnerOutbox.event_type).order_by(OwnerOutbox.created_at))
        ) == ["opened", "recovery"]


def test_discord_adapter_is_tenant_scoped_idempotent_and_delivers(
    database: Database,
) -> None:
    async def scenario() -> None:
        repository = SqlAlchemyDiscordRepository(database)
        configured = await repository.setup_guild(123, 456, 999)
        assert configured.operator_role_id == 999
        await repository.add_destination(
            DiscordDestination(123, 789, "alerts"), actor_user_id=456
        )
        with transaction(database.session_factory) as session:
            session.add_all(
                (
                    Theatre(
                        amc_theatre_id="example-theatre-13",
                        slug="amc-example-8",
                        name="AMC Example 8",
                        zip_code="00000",
                    ),
                    Movie(
                        amc_movie_id="feature-a",
                        slug="example-feature",
                        title="Example Feature",
                        normalized_title="example feature",
                    ),
                    Movie(
                        amc_movie_id="feature-b",
                        slug="future-feature",
                        title="Future Feature",
                        normalized_title="future feature",
                    ),
                    PresentationFormat(code="imax-70mm", name="IMAX 70MM"),
                )
            )
        draft = SubscriptionDraft(
            guild_id=123,
            owner_user_id=456,
            zip_code="00000",
            theatre_ids=("amc-example-8",),
            movie_ids=("example-feature", "future-feature"),
            format_name="imax-70mm",
            adjacent_seats=2,
            seat_preset=SeatPreset.CENTER_BACK,
            weekday_start="17:00",
            weekday_end="23:00",
            weekend_start="10:00",
            weekend_end="23:00",
            destination_channel_id=789,
        )
        first = await repository.create_subscription(draft, "wizard-creation-key")
        second = await repository.create_subscription(draft, "wizard-creation-key")
        assert first.id == second.id
        assert len(await repository.list_subscriptions(123)) == 1

        await repository.enqueue_test_alert(123, 789, 456)
        assert await repository.claim_user_alerts((999,), 10) == []
        alerts = await repository.claim_user_alerts((123,), 10)
        assert len(alerts) == 1 and alerts[0].is_test
        await repository.mark_user_alert_delivered(123, alerts[0].outbox_id, 123456)
        await repository.write_service_heartbeat("amc-discord-bot", "healthy")

    asyncio.run(scenario())
    with transaction(database.session_factory) as session:
        assert session.scalar(select(func.count()).select_from(Subscription)) == 1
        assert session.scalar(select(func.count()).select_from(UserOutbox)) == 1
        assert session.scalar(select(func.count()).select_from(ServiceHeartbeat)) == 1


def test_static_theatre_catalog_bootstraps_discovery_and_search_persists_movie(
    database: Database,
) -> None:
    async def scenario() -> None:
        repository = SqlAlchemyDiscordRepository(database)
        await repository.setup_guild(123, 456, 999)
        wizard = DiscordWizardSession.create(123, 456)
        await repository.save_wizard(wizard)
        with transaction(database.session_factory) as session:
            session.add(
                Theatre(
                    amc_theatre_id="129",
                    slug="amc-example-8",
                    name="AMC Example 8",
                    zip_code="00000",
                )
            )

        await repository.queue_catalog_lookup(
            123,
            wizard.id,
            "movies",
            {"zip_code": "00000", "theatre_ids": ("amc-example-8",)},
        )

        await repository.queue_catalog_lookup(
            123, wizard.id, "movie-search", {"title": "Example Feature"}
        )
        with transaction(database.session_factory) as session:
            search = session.scalar(
                select(CatalogLookup).where(CatalogLookup.kind == "movie-search")
            )
            search_id = search.id
        assert DatabaseStore(database).complete_catalog_lookup(
            search_id,
            [
                {
                    "movie_id": 77123,
                    "slug": "example-feature-77123",
                    "name": "Example Feature",
                    "release_date": "2026-07-17T00:00:00Z",
                }
            ],
        )

    from amc_watch.discord_models import WizardSession as DiscordWizardSession

    asyncio.run(scenario())
    with transaction(database.session_factory) as session:
        assert session.scalar(
            select(CatalogLookup).where(CatalogLookup.kind == "theatres")
        ) is None
        theatre = session.scalar(
            select(Theatre).where(Theatre.slug == "amc-example-8")
        )
        assert theatre is not None and theatre.name == "AMC Example 8"
        target = session.scalar(
            select(DiscoveryTarget).where(DiscoveryTarget.theatre_id == theatre.id)
        )
        assert target is not None and target.active
        movie = session.scalar(select(Movie).where(Movie.slug == "example-feature-77123"))
        assert movie is not None
        assert movie.amc_movie_id == "77123"
        search = session.scalar(
            select(CatalogLookup).where(CatalogLookup.kind == "movie-search")
        )
        assert search.results[0]["id"] == "example-feature-77123"
    formats = DatabaseStore(database).resolve_cached_catalog(
        "formats",
        {
            "theatre_ids": ["amc-example-8"],
            "movie_ids": ["example-feature-77123"],
        },
    )
    assert {value["id"] for value in formats} >= {"imax70mm"}


def test_requeue_same_lookup_preserves_completed_results(database: Database) -> None:
    # Pressing the wizard's Refresh re-queues the same lookup; it must not wipe a
    # just-completed result back to pending (the loop that never showed choices).
    from amc_watch.discord_models import WizardSession as DiscordWizardSession

    async def scenario() -> None:
        repo = SqlAlchemyDiscordRepository(database)
        await repo.setup_guild(123, 456, 999)
        wizard = DiscordWizardSession.create(123, 456)
        await repo.save_wizard(wizard)
        query = {"zip_code": "00000"}
        await repo.queue_catalog_lookup(123, wizard.id, "theatres", query)
        with transaction(database.session_factory) as session:
            lookup_id = session.scalar(
                select(CatalogLookup).where(CatalogLookup.kind == "theatres")
            ).id
        DatabaseStore(database).complete_catalog_lookup(
            lookup_id,
            [{"theatreId": "129", "slug": "amc-example-8",
              "name": "AMC Example 8", "postalCode": "00000"}],
        )
        await repo.queue_catalog_lookup(123, wizard.id, "theatres", query)  # Refresh
        with transaction(database.session_factory) as session:
            lookup = session.scalar(
                select(CatalogLookup).where(CatalogLookup.kind == "theatres")
            )
            assert lookup.status == "complete"
            assert lookup.results is not None

    asyncio.run(scenario())


def test_nearest_theatres_ranks_by_distance_and_needs_centroid(
    database: Database,
) -> None:
    async def run() -> None:
        repo = SqlAlchemyDiscordRepository(database)
        with transaction(database.session_factory) as session:
            session.add_all(
                [
                    Theatre(slug="near", name="Near", zip_code="10001",
                            latitude=40.75, longitude=-73.99,
                            metadata_json={"city": "New York", "state": "NY"}),
                    Theatre(slug="mid", name="Mid", zip_code="07030",
                            latitude=40.74, longitude=-74.03, metadata_json={}),
                    Theatre(slug="far", name="Far", zip_code="90001",
                            latitude=34.05, longitude=-118.24, metadata_json={}),
                ]
            )
        # Not geocoded yet -> None so the wizard shows a loading state.
        assert await repo.nearest_theatres("00000") is None
        with transaction(database.session_factory) as session:
            session.add(
                ZipCentroid(zip_code="00000", latitude=40.7748, longitude=-73.9819)
            )
        options = await repo.nearest_theatres("00000", limit=2)
        assert [o.id for o in options] == ["near", "mid"]  # LA excluded by box + limit
        assert options[0].detail == "New York, NY"

    asyncio.run(run())


def test_available_formats_marks_availability_and_orders(
    database: Database,
) -> None:
    async def run() -> None:
        repo = SqlAlchemyDiscordRepository(database)
        with transaction(database.session_factory) as session:
            movie = Movie(amc_movie_id="m1", slug="example-feature", title="Example Feature",
                          normalized_title="example feature")
            theatre = Theatre(slug="amc-example-8", name="AMC Example 8", zip_code="00000")
            imax = PresentationFormat(code="imax70mm", name="IMAX 70MM",
                                      metadata_json={"global_catalog": True})
            dolby = PresentationFormat(code="dolbycinema", name="Dolby Cinema",
                                       metadata_json={"global_catalog": True})
            session.add_all([movie, theatre, imax, dolby])
            session.flush()
            session.add(
                Showtime(amc_showtime_id="s1", movie_id=movie.id, theatre_id=theatre.id,
                         format_id=imax.id, starts_at=_future_showtime_start(),
                         normalized_status="SELLABLE")
            )
        options = await repo.available_formats(["amc-example-8"], ["example-feature"])
        detail_by_code = {o.id: o.detail for o in options}
        assert detail_by_code["imax70mm"] == "now showing"
        assert detail_by_code["dolbycinema"] == "not showing yet"
        assert options[0].id == "imax70mm"  # available formats first

    asyncio.run(run())


def test_catalog_bootstrap_discovery_persists_inactive_choices(
    database: Database,
) -> None:
    async def setup() -> None:
        repository = SqlAlchemyDiscordRepository(database)
        await repository.setup_guild(123, 456, 999)
        wizard = DiscordWizardSession.create(123, 456)
        await repository.save_wizard(wizard)
        await repository.queue_catalog_lookup(
            123, wizard.id, "theatres", {"zip_code": "00000"}
        )
        with transaction(database.session_factory) as session:
            lookup = session.scalar(
                select(CatalogLookup).where(CatalogLookup.kind == "theatres")
            )
            lookup_id = lookup.id
        DatabaseStore(database).complete_catalog_lookup(
            lookup_id,
            [
                {
                    "theatreId": "129",
                    "slug": "amc-example-8",
                    "name": "AMC Example 8",
                    "postalCode": "00000",
                }
            ],
        )
        await repository.queue_catalog_lookup(
            123,
            wizard.id,
            "movies",
            {"zip_code": "00000", "theatre_ids": ("amc-example-8",)},
        )

    from amc_watch.discord_models import WizardSession as DiscordWizardSession

    asyncio.run(setup())
    repository = SqlAlchemyWorkerRepository(database)
    now = utc_now()
    due = repository.due_resources(now, WorkerPolicy())
    target = due[JobKind.DISCOVERY][0]
    assert target["theatre_slug"] == "amc-example-8"
    starts_at = now + timedelta(hours=4)
    activated = repository.upsert_discovered_showtimes(
        "amc-example-8",
        target["date"],
        [
            {
                "showtime_id": "catalog-only-1",
                "showtime_at": starts_at,
                "movie_id": "current-1",
                "movie_slug": "current-movie",
                "movie_name": "Current Movie",
                "format_code": "digital",
                "format_name": "Digital",
                "attribute_codes": ["digital"],
                "status": "SELLABLE",
            }
        ],
    )
    assert activated == []
    bootstrap_due = repository.due_resources(utc_now(), WorkerPolicy())
    assert bootstrap_due[JobKind.STATUS] == []
    assert bootstrap_due[JobKind.SEATMAP] == []
    with transaction(database.session_factory) as session:
        showtime = session.scalar(
            select(Showtime).where(Showtime.amc_showtime_id == "catalog-only-1")
        )
        assert showtime is not None and showtime.active is False
        assert showtime.next_status_poll_at is None
        assert session.scalar(select(Movie).where(Movie.slug == "current-movie")) is not None
    movies = repository.resolve_cached_catalog(
        "movies", {"theatre_ids": ["amc-example-8"]}
    )
    formats = repository.resolve_cached_catalog(
        "formats",
        {
            "theatre_ids": ["amc-example-8"],
            "movie_ids": ["current-movie"],
        },
    )
    assert {value["id"] for value in movies} >= {"current-movie"}
    assert {value["id"] for value in formats} >= {"digital"}

    async def confirm() -> str:
        discord_repository = SqlAlchemyDiscordRepository(database)
        await discord_repository.add_destination(
            DiscordDestination(123, 789, "alerts"), actor_user_id=456
        )
        created = await discord_repository.create_subscription(
            SubscriptionDraft(
                guild_id=123,
                owner_user_id=456,
                zip_code="00000",
                theatre_ids=("amc-example-8",),
                movie_ids=("current-movie",),
                format_name="digital",
                adjacent_seats=2,
                seat_preset=SeatPreset.CENTER_BACK,
                weekday_start="00:00",
                weekday_end="23:59",
                weekend_start="00:00",
                weekend_end="23:59",
                destination_channel_id=789,
            ),
            "catalog-confirm",
        )
        return created.id

    subscription_id = asyncio.run(confirm())
    confirmed_due = repository.due_resources(utc_now(), WorkerPolicy())
    assert [value["showtime_id"] for value in confirmed_due[JobKind.STATUS]] == [
        "catalog-only-1"
    ]
    assert [value["showtime_id"] for value in confirmed_due[JobKind.SEATMAP]] == [
        "catalog-only-1"
    ]

    async def pause() -> None:
        await SqlAlchemyDiscordRepository(database).set_subscription_enabled(
            123, subscription_id, 456, False
        )

    asyncio.run(pause())
    paused_due = repository.due_resources(utc_now(), WorkerPolicy())
    assert paused_due[JobKind.STATUS] == []
    assert paused_due[JobKind.SEATMAP] == []

    async def resume() -> None:
        await SqlAlchemyDiscordRepository(database).set_subscription_enabled(
            123, subscription_id, 456, True
        )

    asyncio.run(resume())
    resumed_due = repository.due_resources(utc_now(), WorkerPolicy())
    assert [value["showtime_id"] for value in resumed_due[JobKind.STATUS]] == [
        "catalog-only-1"
    ]

    # An initial selectable-date sample is a baseline (``new_dates`` is empty),
    # but all dates AMC currently reports still need active discovery targets.
    selected_date = utc_now().date() + timedelta(days=1)
    with transaction(database.session_factory) as session:
        movie = session.scalar(select(Movie).where(Movie.slug == "current-movie"))
        theatre = session.scalar(
            select(Theatre).where(Theatre.slug == "amc-example-8")
        )
        session.add(
            DiscoveryTarget(
                theatre_id=theatre.id,
                date=selected_date,
                active=False,
                next_poll_at=utc_now(),
            )
        )
        movie.selectable_dates_initialized_at = None

    initialized, new_dates = repository.observe_selectable_dates(
        "current-movie", {selected_date}
    )
    assert initialized is False and new_dates == set()
    with transaction(database.session_factory) as session:
        target = session.scalar(
            select(DiscoveryTarget).where(DiscoveryTarget.date == selected_date)
        )
        assert target.active is True

    starts_at = datetime.combine(selected_date, time(23), tzinfo=UTC)
    activated = repository.upsert_discovered_showtimes(
        "amc-example-8",
        selected_date.isoformat(),
        [
            {
                "showtime_id": "catalog-only-1",
                "showtime_at": starts_at,
                "movie_id": "current-1",
                "movie_slug": "current-movie",
                "movie_name": "Current Movie",
                "format_code": "digital",
                "format_name": "Digital",
                "attribute_codes": ["digital"],
                "status": "SELLABLE",
            }
        ],
    )
    assert activated == ["catalog-only-1"]


def test_catalog_jobs_are_claimed_before_others(database: Database) -> None:
    # A human is waiting in the setup wizard: catalog jobs must be claimed ahead
    # of status/discovery so the lookup resolves in seconds, not minutes.
    repository = SqlAlchemyWorkerRepository(database)
    now = utc_now()
    repository.enqueue_job(
        JobKind.STATUS,
        "status:1",
        {"resources": [{"resource_key": "1", "showtime_id": "1"}]},
        run_at=now,
        priority=10,
    )
    repository.enqueue_job(
        JobKind.CATALOG,
        "catalog:lookup-1",
        {"resources": [{"resource_key": "lookup-1", "lookup_id": "lookup-1", "kind": "theatres"}]},
        run_at=now,
        priority=50,
    )
    claimed = repository.claim_jobs("worker-catalog", limit=8, lease_seconds=90)
    assert claimed and claimed[0].kind is JobKind.CATALOG


def test_priority_seat_transition_promotes_existing_resource_job(
    database: Database,
) -> None:
    ids = seed_monitor(database)
    now = utc_now()
    with transaction(database.session_factory) as session:
        original = session.get(Showtime, ids["showtime_id"])
        original.next_seat_poll_at = now
        session.add(
            Showtime(
                amc_showtime_id="10002",
                movie_id=original.movie_id,
                theatre_id=original.theatre_id,
                format_id=original.format_id,
                starts_at=original.starts_at + timedelta(minutes=30),
                normalized_status="SOLDOUT",
                next_seat_poll_at=now,
            )
        )
    repository = SqlAlchemyWorkerRepository(database)
    SharedWorker(
        repository,
        ProxyPool.from_values("http://user:secret@proxy.test:8080", []),
    ).enqueue_due_work(now)
    repository.enqueue_job(
        JobKind.SEATMAP,
        "seatmap:10001",
        {"resources": [{"resource_key": "10001", "showtime_id": "10001"}]},
        run_at=now,
        priority=0,
    )
    with transaction(database.session_factory) as session:
        jobs = list(
            session.scalars(
                select(MonitorJob).where(MonitorJob.kind == JobKind.SEATMAP.value)
            )
        )
        assert {job.dedupe_key for job in jobs} == {
            "seatmap:10001",
            "seatmap:10002",
        }
        assert next(job for job in jobs if job.dedupe_key == "seatmap:10001").priority == 0

def test_disabled_destination_is_dead_lettered_during_claim(database: Database) -> None:
    async def scenario() -> None:
        repository = SqlAlchemyDiscordRepository(database)
        await repository.setup_guild(123, 456, 999)
        await repository.add_destination(
            DiscordDestination(123, 789, "alerts"), actor_user_id=456
        )
        await repository.enqueue_test_alert(123, 789, 456)
        await repository.remove_destination(123, 789, 456)
        assert await repository.claim_user_alerts((123,), 10) == []

    asyncio.run(scenario())
    with transaction(database.session_factory) as session:
        outbox = session.scalar(select(UserOutbox))
        assert outbox.status == "dead"
        assert outbox.last_error_code == "destination_disabled"
        delivery = session.scalar(select(UserDelivery))
        assert delivery.status == "failed"
        assert delivery.error_code == "destination_disabled"


def test_create_and_resume_enforce_transactional_member_limit(
    database: Database,
) -> None:
    from dataclasses import replace

    ids = seed_monitor(database)
    repository = SqlAlchemyDiscordRepository(database)
    draft = SubscriptionDraft(
        guild_id=123,
        owner_user_id=456,
        zip_code="00000",
        theatre_ids=("amc-example-8",),
        movie_ids=("example-feature",),
        format_name="imax-70mm",
        adjacent_seats=2,
        seat_preset=SeatPreset.CENTER_BACK,
        weekday_start="17:00",
        weekday_end="23:00",
        weekend_start="10:00",
        weekend_end="23:00",
        destination_channel_id=789,
    )

    async def scenario() -> None:
        with pytest.raises(LookupError, match="current catalog"):
            await repository.create_subscription(
                replace(draft, movie_ids=("manipulated-movie",)), "unknown-movie"
            )
        for index in range(4):
            await repository.create_subscription(draft, f"capacity-{index}")
        with pytest.raises(ValueError, match="member active-monitor limit"):
            await repository.create_subscription(draft, "capacity-over-limit")

        await repository.set_subscription_enabled(
            123, str(ids["subscription_id"]), 456, False
        )
        await repository.create_subscription(draft, "capacity-replacement")
        with pytest.raises(ValueError, match="member active-monitor limit"):
            await repository.set_subscription_enabled(
                123, str(ids["subscription_id"]), 456, True
            )

    asyncio.run(scenario())
    with transaction(database.session_factory) as session:
        original = session.get(Subscription, ids["subscription_id"])
        assert original.enabled is False
        assert (
            session.scalar(
                select(func.count())
                .select_from(Subscription)
                .where(Subscription.enabled.is_(True))
            )
            == 5
        )


def test_candidate_showtimes_are_included_in_capacity_and_draft_is_revalidated(
    database: Database,
) -> None:
    from dataclasses import replace

    ids = seed_monitor(database)
    repository = SqlAlchemyDiscordRepository(database)
    with transaction(database.session_factory) as session:
        original = session.get(Showtime, ids["showtime_id"])
        starts_at = utc_now() + timedelta(days=1)
        for index in range(88):
            session.add(
                Showtime(
                    amc_showtime_id=f"candidate-{index}",
                    movie_id=original.movie_id,
                    theatre_id=original.theatre_id,
                    format_id=original.format_id,
                    starts_at=starts_at + timedelta(minutes=index),
                    normalized_status="SELLABLE",
                    active=True,
                )
            )
    draft = SubscriptionDraft(
        guild_id=123,
        owner_user_id=456,
        zip_code="00000",
        theatre_ids=("amc-example-8",),
        movie_ids=("example-feature",),
        format_name="imax-70mm",
        adjacent_seats=2,
        seat_preset=SeatPreset.CENTER_BACK,
        weekday_start="17:00",
        weekday_end="23:00",
        weekend_start="10:00",
        weekend_end="23:00",
        destination_channel_id=789,
    )

    async def scenario() -> None:
        assert await repository.projected_status_cadence(draft) > 60
        with pytest.raises(ValueError, match="projected status latency"):
            await repository.create_subscription(draft, "over-capacity")
        with pytest.raises(ValueError, match="ZIP code"):
            await repository.create_subscription(
                replace(draft, zip_code="1002"), "invalid-zip"
            )

    asyncio.run(scenario())


def test_admission_counts_global_active_showtimes_outside_tenant_catalog(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyDiscordRepository(database)
    with transaction(database.session_factory) as session:
        movie = Movie(
            amc_movie_id="other-movie",
            slug="other-guild-movie",
            title="Other Guild Movie",
            normalized_title="other guild movie",
        )
        theatre = Theatre(
            amc_theatre_id="other-theatre",
            slug="other-guild-theatre",
            name="Other Guild Theatre",
            zip_code="10001",
        )
        presentation = PresentationFormat(code="other-format", name="Other Format")
        session.add_all((movie, theatre, presentation))
        session.flush()
        start = utc_now() + timedelta(days=1)
        for index in range(80):
            session.add(
                Showtime(
                    amc_showtime_id=f"other-guild-{index}",
                    movie_id=movie.id,
                    theatre_id=theatre.id,
                    format_id=presentation.id,
                    starts_at=start + timedelta(minutes=index),
                    active=True,
                )
            )
    draft = SubscriptionDraft(
        guild_id=123,
        owner_user_id=456,
        zip_code="00000",
        theatre_ids=("amc-example-8",),
        movie_ids=("example-feature",),
        format_name="imax-70mm",
        adjacent_seats=2,
        seat_preset=SeatPreset.CENTER_BACK,
        weekday_start="17:00",
        weekday_end="23:00",
        weekend_start="10:00",
        weekend_end="23:00",
        destination_channel_id=789,
    )

    assert asyncio.run(repository.projected_status_cadence(draft)) > 60


def test_worker_repository_drives_status_to_transactional_seat_alert(
    database: Database,
) -> None:
    ids = seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    now = utc_now()
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        showtime.starts_at = _future_showtime_start()
        showtime.next_status_poll_at = now
        showtime.next_seat_poll_at = now + timedelta(hours=1)
        movie = session.get(Movie, showtime.movie_id)
        movie.next_dates_poll_at = now + timedelta(hours=1)

    class FakeClient:
        def __init__(self):
            self.calls: list[str] = []

        def query(self, query, variables):
            self.calls.append(query)
            if "status0:showtime" in query:
                return {
                    "viewer": {
                        "status0": {
                            "status": "SELLABLE",
                            "isSoldOut": False,
                            "isAlmostSoldOut": False,
                        }
                    }
                }
            seats = []
            for row in range(12):
                for column in range(42):
                    seats.append(
                        {
                            "row": row,
                            "column": column,
                            "name": f"{'ABCDEFGHJKLM'[row]}{column + 1}",
                            "type": "CanReserve",
                            "shouldDisplay": True,
                            "available": (row, column) in {(7, 20), (7, 21)},
                        }
                    )
            return {"viewer": {"seat0": {"seatingLayout": {"seats": seats}}}}

    worker = SharedWorker(
        repository,
        ProxyPool.from_values("http://user:secret@proxy.test:8080", []),
        policy=WorkerPolicy(request_gap_seconds=0.01),
    )
    fake = FakeClient()
    worker.client = fake
    assert worker.run_once() is True
    assert worker.run_once() is True
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        assert showtime.normalized_status == "SELLABLE"
        outbox = session.scalar(select(UserOutbox))
        assert outbox is not None
        assert outbox.payload["runs"][0]["names"] == ["H21", "H22"]
        assert session.scalar(select(func.count()).select_from(AvailabilityEdge)) == 1
    assert len(fake.calls) == 2


def test_worker_selectable_dates_are_per_movie_and_schedule_discovery(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    # Near-term dates, well inside the discovery horizon. The worker still drops
    # stale (past) dates and anything beyond the horizon floor before creating
    # discovery targets, even when AMC includes them in a selectable-date reply.
    first_date = utc_now().date() + timedelta(days=1)
    second_date = first_date + timedelta(days=1)
    initialized, new_dates = repository.observe_selectable_dates(
        "example-feature", {first_date}
    )
    assert initialized is False and new_dates == set()
    initialized, new_dates = repository.observe_selectable_dates(
        "example-feature", {first_date, second_date}
    )
    assert initialized is True and new_dates == {second_date}
    with transaction(database.session_factory) as session:
        session.scalar(select(Movie).where(Movie.slug == "example-feature")).next_dates_poll_at = utc_now()
    resources = repository.due_resources(utc_now(), WorkerPolicy())
    assert resources[JobKind.SELECTABLE_DATES][0]["slug"] == "example-feature"
    assert "movie_slug" not in resources[JobKind.SELECTABLE_DATES][0]
    discovery = resources[JobKind.DISCOVERY]
    discovery_dates = {item["date"] for item in discovery}
    # Selectable dates schedule discovery; the horizon sweep adds the rest.
    assert {first_date.isoformat(), second_date.isoformat()} <= discovery_dates


def test_far_future_selectable_dates_within_horizon_floor_are_discovered(
    database: Database,
) -> None:
    # A date beyond the legacy 31-day cap but inside the horizon floor must still
    # produce a discovery target, so AMC's far-future drops are not missed even
    # when the subscription's days_ahead is small.
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    near = utc_now().date() + timedelta(days=1)
    far = utc_now().date() + timedelta(days=45)
    repository.observe_selectable_dates("example-feature", {near})  # baseline
    initialized, new_dates = repository.observe_selectable_dates(
        "example-feature", {near, far}
    )
    assert initialized is True and new_dates == {far}
    with transaction(database.session_factory) as session:
        session.scalar(
            select(Movie).where(Movie.slug == "example-feature")
        ).next_dates_poll_at = utc_now()
    resources = repository.due_resources(utc_now(), WorkerPolicy())
    discovery_dates = {item["date"] for item in resources[JobKind.DISCOVERY]}
    assert far.isoformat() in discovery_dates


def test_horizon_sweep_targets_dates_absent_from_selectabledates(
    database: Database,
) -> None:
    # AMC lists only a near date as selectable, but the horizon sweep must still
    # create and keep an active discovery target for an in-horizon date it omits
    # (the running-film case where selectableDates trails actual on-sale dates).
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    near = utc_now().date() + timedelta(days=1)
    swept = utc_now().date() + timedelta(days=40)  # inside 60-day horizon, not selectable
    repository.observe_selectable_dates("example-feature", {near})
    repository.due_resources(utc_now(), WorkerPolicy())  # runs the retirement pass
    with transaction(database.session_factory) as session:
        theatre_ids = list(session.scalars(select(Theatre.id)))
        swept_targets = [session.get(DiscoveryTarget, (tid, swept)) for tid in theatre_ids]
        assert any(t is not None and t.active for t in swept_targets), \
            "in-horizon swept date must have an active discovery target"
        assert session.scalar(
            select(func.count()).select_from(SelectableDate).where(SelectableDate.date == swept)
        ) == 0, "swept date must not be recorded as a selectable date"


def test_discovery_target_for_yesterday_survives_utc_rollover(database: Database) -> None:
    # A theatre-local "today" is yesterday in UTC for evening ET showtimes; such
    # a target must not be retired (M1) or same-day discovery blacks out ~4-5h.
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    yesterday = utc_now().date() - timedelta(days=1)
    with transaction(database.session_factory) as session:
        theatre = session.scalar(select(Theatre).where(Theatre.slug == "amc-example-8"))
        session.add(DiscoveryTarget(theatre_id=theatre.id, date=yesterday, next_poll_at=utc_now()))
    repository.due_resources(utc_now(), WorkerPolicy())  # runs the retirement pass
    with transaction(database.session_factory) as session:
        theatre = session.scalar(select(Theatre).where(Theatre.slug == "amc-example-8"))
        target = session.get(DiscoveryTarget, (theatre.id, yesterday))
        assert target is not None and target.active is True


def test_active_discovery_target_cadence_not_reset_by_dates_poll(database: Database) -> None:
    # The 5-minute selectable-dates poll must not reset an already-active
    # target's next_poll_at (M2), which would collapse the discovery cadence.
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    near = utc_now().date() + timedelta(days=1)
    repository.observe_selectable_dates("example-feature", {near})
    future = utc_now() + timedelta(hours=1)
    with transaction(database.session_factory) as session:
        theatre = session.scalar(select(Theatre).where(Theatre.slug == "amc-example-8"))
        session.get(DiscoveryTarget, (theatre.id, near)).next_poll_at = future
    repository.observe_selectable_dates("example-feature", {near})  # second poll, same date
    with transaction(database.session_factory) as session:
        theatre = session.scalar(select(Theatre).where(Theatre.slug == "amc-example-8"))
        target = session.get(DiscoveryTarget, (theatre.id, near))
        assert as_utc(target.next_poll_at) > utc_now() + timedelta(minutes=30)


def test_discovery_skips_seat_refetch_for_future_scheduled_showtime(database: Database) -> None:
    # M2: a showtime already scheduled for a future seat poll must not be
    # re-enqueued for a seat check on every discovery pass; only new or
    # due-now showtimes are returned.
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    selected_date = utc_now().date() + timedelta(days=1)
    starts_at = datetime.combine(selected_date, time(23), tzinfo=UTC)
    base = {
        "movie_slug": "example-feature", "movie_id": "feature-a", "movie_name": "Example Feature",
        "format_code": "IMAX 70MM", "format_name": "IMAX 70MM",
        "attribute_codes": ["imax-70mm"], "status": "SELLABLE",
        "is_sold_out": False, "is_almost_sold_out": False, "showtime_id": "sched-1",
        "showtime_at": starts_at,
    }
    first = repository.upsert_discovered_showtimes(
        "amc-example-8", selected_date.isoformat(), [base]
    )
    assert first == ["sched-1"]  # newly discovered -> seat check
    with transaction(database.session_factory) as session:
        st = session.scalar(select(Showtime).where(Showtime.amc_showtime_id == "sched-1"))
        st.next_seat_poll_at = utc_now() + timedelta(minutes=10)  # recently polled
    second = repository.upsert_discovered_showtimes(
        "amc-example-8", selected_date.isoformat(), [base]
    )
    assert second == []  # not due -> no redundant refetch


def test_discovery_persists_only_matching_movie_format_horizon_and_time(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    selected_date = utc_now().date() + timedelta(days=1)
    # July is EDT; 23:00 UTC is 19:00 in the theatre's configured timezone.
    eligible_start = datetime.combine(selected_date, time(23), tzinfo=UTC)
    base = {
        "movie_slug": "example-feature",
        "movie_id": "feature-a",
        "movie_name": "Example Feature",
        "format_code": "IMAX 70MM",
        "format_name": "IMAX 70MM",
        "attribute_codes": ["imax-70mm"],
        "status": "SELLABLE",
        "is_sold_out": False,
        "is_almost_sold_out": False,
    }
    found = repository.upsert_discovered_showtimes(
        "amc-example-8",
        selected_date.isoformat(),
        [
            {**base, "showtime_id": "eligible", "showtime_at": eligible_start},
            {
                **base,
                "showtime_id": "wrong-format",
                "showtime_at": eligible_start,
                "format_code": "digital",
                "attribute_codes": ["digital"],
            },
            {
                **base,
                "showtime_id": "wrong-movie",
                "showtime_at": eligible_start,
                "movie_slug": "unrelated-movie",
                "movie_id": "unrelated",
                "movie_name": "Unrelated Movie",
            },
            {
                **base,
                "showtime_id": "outside-time-window",
                "showtime_at": datetime.combine(selected_date, time(8), tzinfo=UTC),
            },
        ],
    )
    assert found == ["eligible"]
    with transaction(database.session_factory) as session:
        assert session.scalar(
            select(func.count())
            .select_from(Showtime)
            .where(Showtime.amc_showtime_id.in_((
                "eligible",
                "wrong-format",
                "wrong-movie",
                "outside-time-window",
            )))
        ) == 1
        row = session.scalar(
            select(Showtime).where(Showtime.amc_showtime_id == "eligible")
        )
        assert row is not None and row.format_id is not None
        row.format_id = None
    assert repository.subscriptions_for_showtime("eligible") == []


def test_missing_seatmap_advances_deadline(database: Database) -> None:
    ids = seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    before = utc_now()
    repository.record_missing_seatmap("10001")
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        assert as_utc(showtime.last_seat_poll_at) >= before
        assert as_utc(showtime.next_seat_poll_at) >= before + timedelta(minutes=5)


def test_non_eastern_showtime_uses_amc_utc_offset_for_window_and_display(
    database: Database,
) -> None:
    ids = seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    starts_at = datetime.combine(
        utc_now().date() + timedelta(days=1), time(4), tzinfo=UTC
    )
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        showtime.starts_at = starts_at
        showtime.next_status_poll_at = utc_now()
        showtime.next_seat_poll_at = utc_now()
        showtime.metadata_json = {
            **dict(showtime.metadata_json or {}),
            "utc_offset": "-07:00",
        }
        # Deliberately leave the theatre fallback Eastern; the per-showtime AMC
        # offset must win (04:00 UTC is 21:00 the prior day, not midnight ET).
        session.get(Theatre, showtime.theatre_id).timezone = "America/New_York"

    assert len(repository.subscriptions_for_showtime("10001")) == 1
    assert "9:00 PM" in repository.showtime_context("10001")["when_local"]
    due = repository.due_resources(utc_now(), WorkerPolicy())
    assert [value["showtime_id"] for value in due[JobKind.STATUS]] == ["10001"]


def test_worker_cooldown_requeues_without_calling_amc(database: Database) -> None:
    ids = seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    now = utc_now()
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        showtime.starts_at = datetime.combine(
            now.date() + timedelta(days=1), time(23), tzinfo=UTC
        )
        showtime.next_status_poll_at = now
        showtime.next_seat_poll_at = now + timedelta(hours=1)
        session.get(Movie, showtime.movie_id).next_dates_poll_at = now + timedelta(hours=1)
    cooldown_until = now + timedelta(minutes=5)
    repository.store.set_cooldown(cooldown_until)

    class NeverClient:
        calls = 0

        def query(self, query, variables):
            self.calls += 1
            raise AssertionError("AMC must not be called during cooldown")

    worker = SharedWorker(
        repository,
        ProxyPool.from_values("http://user:secret@proxy.test:8080", []),
        policy=WorkerPolicy(request_gap_seconds=0.01),
    )
    fake = NeverClient()
    worker.client = fake
    assert worker.run_once() is True
    assert worker.run_once() is False
    assert fake.calls == 0
    with transaction(database.session_factory) as session:
        job = session.scalar(select(MonitorJob).where(MonitorJob.kind == "status"))
        assert job.status == "pending"
        assert job.last_error_code == "upstream_cooldown"
        assert job.attempts == 1
        assert as_utc(job.run_at) == as_utc(cooldown_until)
        # Re-observing a persisted block must not turn it into a sliding cooldown.
        assert as_utc(session.get(RequestGateState, "global").cooldown_until) == as_utc(
            cooldown_until
        )


def test_worker_transport_backoff_is_persisted_and_blocks_direct_retry(
    database: Database,
) -> None:
    ids = seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    now = utc_now()
    with transaction(database.session_factory) as session:
        showtime = session.get(Showtime, ids["showtime_id"])
        showtime.starts_at = datetime.combine(
            now.date() + timedelta(days=1), time(23), tzinfo=UTC
        )
        showtime.next_status_poll_at = now
        showtime.next_seat_poll_at = now + timedelta(hours=1)
        session.get(Movie, showtime.movie_id).next_dates_poll_at = now + timedelta(hours=1)
    backoff_until = now + timedelta(minutes=5)
    repository.set_transport_backoff(backoff_until)

    class NeverClient:
        calls = 0

        def query(self, query, variables):
            self.calls += 1
            raise AssertionError("AMC must not be called during proxy backoff")

    worker = SharedWorker(
        repository,
        ProxyPool.from_values("http://user:secret@proxy.test:8080", []),
        policy=WorkerPolicy(request_gap_seconds=0.01),
    )
    fake = NeverClient()
    worker.client = fake
    assert worker.run_once() is True
    assert worker.run_once() is False
    assert fake.calls == 0
    with transaction(database.session_factory) as session:
        job = session.scalar(select(MonitorJob).where(MonitorJob.kind == "status"))
        assert job.status == "pending"
        assert job.last_error_code == "transport_backoff"
        assert job.attempts == 1
        assert as_utc(job.run_at) == as_utc(backoff_until)
        assert as_utc(
            session.get(RequestGateState, "global").transport_backoff_until
        ) == as_utc(backoff_until)


def test_active_proxy_selection_survives_repository_restart(database: Database) -> None:
    first = SqlAlchemyWorkerRepository(database)
    first.set_active_proxy(1)
    second = SqlAlchemyWorkerRepository(database)
    assert second.active_proxy_index() == 1


def test_proxy_failure_cycle_survives_restart_and_resets_after_backoff(
    database: Database,
) -> None:
    first = SqlAlchemyWorkerRepository(database)
    first.set_active_proxy(0)
    first.record_proxy_failure(0)
    first.set_active_proxy(1)
    first.record_proxy_failure(1)
    first.set_transport_backoff(utc_now() + timedelta(minutes=5))

    restarted = SqlAlchemyWorkerRepository(database)
    assert restarted.proxy_pool_state(2) == (1, (0, 1))

    with transaction(database.session_factory) as session:
        gate = session.get(RequestGateState, "global")
        gate.transport_backoff_until = utc_now() - timedelta(seconds=1)
    recovered = SqlAlchemyWorkerRepository(database)
    assert recovered.proxy_pool_state(2) == (0, ())


def test_cached_catalog_uses_shared_showtime_relations(database: Database) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    movies = repository.resolve_cached_catalog(
        "movies", {"theatre_ids": ["amc-example-8"]}
    )
    formats = repository.resolve_cached_catalog(
        "formats",
        {
            "theatre_ids": ["amc-example-8"],
            "movie_ids": ["example-feature"],
        },
    )
    assert movies == [{"id": "example-feature", "label": "Example Feature", "detail": ""}]
    assert formats == [{"id": "imax-70mm", "label": "IMAX 70MM", "detail": ""}]


def test_legacy_import_is_idempotent_and_never_creates_initial_alerts(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    bundle = ImportBundle(
        schema_version=2,
        subscriptions=(
            ImportedSubscription(
                external_id="legacy-feature-a",
                label="Legacy Example",
                zip_code="00000",
                theatres=(
                    {
                        "slug": "amc-example-8",
                        "name": "AMC Example 8",
                    },
                ),
                movies=(
                    {
                        "movie_id": "feature-a",
                        "slug": "example-feature",
                        "name": "Example Feature",
                    },
                ),
                format_codes=("imax-70mm",),
                days_ahead=14,
                weekday_hours=(17, 23),
                weekend_hours=(10, 23),
                seat_count=2,
                seat_preset="center-back",
                guild_id=123,
                destination_channel_id=789,
            ),
        ),
        selectable_dates={"example-feature": frozenset({"2026-12-18"})},
    )
    assert repository.import_legacy_bundle(bundle, dry_run=True)["subscriptions"] == 1
    repository.import_legacy_bundle(bundle, dry_run=False)
    repository.import_legacy_bundle(bundle, dry_run=False)
    with transaction(database.session_factory) as session:
        imported = list(
            session.scalars(
                select(Subscription).where(Subscription.creation_key.like("legacy:%"))
            )
        )
        assert len(imported) == 1
        assert session.scalar(select(func.count()).select_from(UserOutbox)) == 0


def test_legacy_import_preserves_last_good_status_and_seat_observation(
    database: Database,
) -> None:
    seed_monitor(database)
    repository = SqlAlchemyWorkerRepository(database)
    checked_at = utc_now() - timedelta(minutes=2)
    starts_at = utc_now() + timedelta(days=2)
    bundle = ImportBundle(
        schema_version=1,
        subscriptions=(
            ImportedSubscription(
                external_id="rich-legacy-monitor",
                label="Synthetic legacy monitor",
                zip_code="00000",
                theatres=(
                    {
                        "slug": "amc-example-8",
                        "name": "AMC Example 8",
                    },
                ),
                movies=(
                    {
                        "movie_id": "feature-a",
                        "slug": "example-feature",
                        "name": "Example Feature",
                    },
                ),
                format_codes=("imax-70mm",),
                days_ahead=14,
                weekday_hours=(17, 23),
                weekend_hours=(10, 23),
                seat_count=2,
                seat_preset="center-back",
                guild_id=123,
                destination_channel_id=789,
            ),
        ),
        showtimes=(
            {
                "showtime_id": "synthetic-rich-showtime",
                "movie": "Example Feature",
                "format": "IMAX 70MM",
                "when_local": "Synthetic local time",
                "when_utc": starts_at.isoformat(),
                "book_url": "https://example.invalid/booking",
                "availability": "sold_out",
                "available_count": 2,
                "best_pairs": [
                    {"names": ["H21", "H22"], "score": 99},
                ],
                "seatmap": {
                    "rows": 2,
                    "columns": 4,
                    "seats": [
                        {"r": 0, "c": 0, "n": "A1", "s": "taken"},
                        {"r": 1, "c": 1, "n": "B2", "s": "available"},
                        {"r": 1, "c": 2, "n": "B3", "s": "recommended"},
                    ],
                },
                "last_checked_at": checked_at.isoformat(),
            },
        ),
        alert_edges={
            "rich-legacy-monitor": {
                "synthetic-rich-showtime": frozenset({"H21+H22"})
            }
        },
    )

    summary = repository.import_legacy_bundle(bundle, dry_run=False)
    repository.import_legacy_bundle(bundle, dry_run=False)

    assert summary["status_observations"] == 1
    assert summary["seat_observations"] == 1
    with transaction(database.session_factory) as session:
        showtime = session.scalar(
            select(Showtime).where(
                Showtime.amc_showtime_id == "synthetic-rich-showtime"
            )
        )
        assert showtime.normalized_status == "SOLDOUT"
        assert as_utc(showtime.last_status_poll_at) == as_utc(checked_at)
        assert as_utc(showtime.last_seat_poll_at) == as_utc(checked_at)
        assert showtime.metadata_json["legacy_available_count"] == 2
        assert showtime.metadata_json["legacy_best_pairs"][0]["names"] == [
            "H21",
            "H22",
        ]
        statuses = list(
            session.scalars(
                select(StatusObservation).where(
                    StatusObservation.showtime_id == showtime.id
                )
            )
        )
        seats = list(
            session.scalars(
                select(SeatObservation).where(
                    SeatObservation.showtime_id == showtime.id
                )
            )
        )
        assert len(statuses) == 1 and statuses[0].normalized_status == "SOLDOUT"
        assert len(seats) == 1
        assert seats[0].available_seat_count == 2
        assert seats[0].available_coordinates == [[1, 1], [1, 2]]
        assert session.scalar(select(func.count()).select_from(UserOutbox)) == 0


def test_initial_migration_upgrades_and_downgrades_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = f"sqlite:///{tmp_path / 'migration.db'}"
    monkeypatch.setenv("AMC_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    database = Database(url)
    try:
        assert "subscriptions" in inspect(database.engine).get_table_names()
        with database.engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "20260722_0003"
    finally:
        database.dispose()
    command.downgrade(config, "base")
    database = Database(url)
    try:
        assert "subscriptions" not in inspect(database.engine).get_table_names()
    finally:
        database.dispose()


@pytest.mark.postgresql
def test_postgresql_migration_installs_forced_rls_and_service_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    monkeypatch.setenv("AMC_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    database = Database(url)
    try:
        with database.engine.connect() as connection:
            policies = set(
                connection.scalars(
                    text(
                        "SELECT tablename FROM pg_policies "
                        "WHERE schemaname='public' AND policyname='tenant_isolation'"
                    )
                )
            )
            assert {"guilds", "subscriptions", "user_outbox", "wizard_sessions"} <= policies
            forced = connection.scalar(
                text(
                    "SELECT relforcerowsecurity FROM pg_class "
                    "WHERE oid = 'public.subscriptions'::regclass"
                )
            )
            assert forced is True
            roles = set(
                connection.scalars(
                    text(
                        "SELECT rolname FROM pg_roles WHERE rolname IN "
                        "('amc_owner','amc_migrator','amc_worker','amc_bot','amc_notifier')"
                    )
                )
            )
            assert roles == {
                "amc_owner",
                "amc_migrator",
                "amc_worker",
                "amc_bot",
                "amc_notifier",
            }
    finally:
        database.dispose()
