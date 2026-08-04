"""Shared, capacity-aware polling worker.

The worker is deliberately repository-driven: Discord commands never call AMC,
and every AMC operation flows through one persisted request gate.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence

from .amc import (
    AMCUpstreamCooldown,
    FORMATS_CATALOG_QUERY,
    GraphQLClient,
    LOCATION_QUERY,
    MOVIE_SEARCH_QUERY,
    ProxyEndpointError,
    ProxyPool,
    THEATRES_CATALOG_QUERY,
    discovery_batch_query,
    parse_discovery_payload,
    parse_formats_catalog,
    parse_location_catalog,
    parse_location_centroid,
    parse_movie_catalog,
    parse_theatres_catalog,
    parse_selectable_dates,
    seatmap_batch_query,
    selectable_dates_batch_query,
    status_batch_query,
)
from .domain import (
    adaptive_status_interval,
    became_sellable,
    compact_seats,
    normalize_status,
    rank_runs,
)


class JobKind(StrEnum):
    STATUS = "status"
    SEATMAP = "seatmap"
    SELECTABLE_DATES = "selectable_dates"
    DISCOVERY = "discovery"
    CATALOG = "catalog"
    CATALOG_REFRESH = "catalog_refresh"


PRIORITY = {
    JobKind.SEATMAP: 10,
    JobKind.STATUS: 20,
    JobKind.SELECTABLE_DATES: 30,
    JobKind.DISCOVERY: 40,
    JobKind.CATALOG: 50,
    # Background national-catalog sweep: lowest priority, never claimed ahead of
    # the wizard's catalog lookups or live polling.
    JobKind.CATALOG_REFRESH: 60,
}

# Safety cap for the national-catalog pagination sweep (~900 theatres at 100/page).
MAX_CATALOG_PAGES = 50


@dataclass(frozen=True)
class ClaimedJob:
    id: str
    kind: JobKind
    resource_key: str
    payload: Mapping[str, Any]
    attempts: int = 0


@dataclass(frozen=True)
class SubscriptionTarget:
    id: str
    guild_id: int
    destination_channel_id: int
    seat_count: int
    seat_preset: str
    label: str = "AMC seats"


@dataclass(frozen=True)
class WorkerPolicy:
    request_gap_seconds: float = 3
    status_batch_size: int = 8
    seat_batch_size: int = 8
    date_batch_size: int = 8
    discovery_batch_size: int = 4
    healthy_status_floor_seconds: float = 15
    degraded_status_floor_seconds: float = 30
    full_seat_interval_seconds: float = 300
    selectable_date_interval_seconds: float = 300
    discovery_interval_seconds: float = 900
    admission_ceiling_seconds: float = 60
    lease_seconds: int = 90
    enqueue_interval_seconds: float = 3.0
    delivery_backlog_count_threshold: int = 100
    delivery_backlog_age_seconds: float = 300
    # National reference catalog is near-static; re-fetch at most weekly.
    catalog_refresh_interval_seconds: float = 7 * 24 * 3600
    catalog_page_size: int = 100


class WorkerStore(Protocol):
    """Storage contract implemented by the PostgreSQL repository façade."""

    def claim_jobs(self, worker_id: str, *, limit: int, lease_seconds: int) -> Sequence[ClaimedJob]: ...

    def acquire_request_slot(self, *, minimum_gap_seconds: float) -> datetime: ...

    def complete_job(self, job_id: str) -> None: ...

    def retry_job(self, job_id: str, *, run_at: datetime, error_code: str) -> None: ...

    def enqueue_job(
        self,
        kind: JobKind,
        resource_key: str,
        payload: Mapping[str, Any],
        *,
        run_at: datetime,
        priority: int,
    ) -> None: ...

    def heartbeat(self, service: str, *, metadata: Mapping[str, Any]) -> None: ...

    def set_global_cooldown(self, until: datetime, *, status_code: int) -> None: ...

    def clear_global_cooldown(self) -> None: ...

    def set_transport_backoff(self, until: datetime) -> None: ...

    def clear_transport_backoff(self) -> None: ...

    def open_incident(self, event_type: str, severity: str, metadata: Mapping[str, Any]) -> None: ...

    def resolve_incident(self, event_type: str) -> None: ...

    def set_active_proxy(self, index: int) -> None: ...

    def record_proxy_failure(self, index: int) -> None: ...

    def record_proxy_success(self, index: int) -> None: ...

    def record_status(
        self,
        showtime_id: str,
        *,
        status: str | None,
        is_sold_out: bool,
        is_almost_sold_out: bool,
        missing: bool,
    ) -> tuple[str | None, int]: ...

    def retire_showtime_if_confirmed_missing(self, showtime_id: str, *, missing_count: int) -> None: ...

    def subscriptions_for_showtime(self, showtime_id: str) -> Sequence[SubscriptionTarget]: ...

    def showtime_context(self, showtime_id: str) -> Mapping[str, Any]: ...

    def record_missing_seatmap(self, showtime_id: str) -> None: ...

    def apply_availability(
        self,
        subscription: SubscriptionTarget,
        showtime_id: str,
        *,
        signatures: set[str],
        alert_payload: Mapping[str, Any],
    ) -> None: ...

    def observe_selectable_dates(
        self, movie_slug: str, dates: set[date]
    ) -> tuple[bool, set[date]]: ...

    def theatres_for_movie(self, movie_slug: str) -> Sequence[str]: ...

    def upsert_discovered_showtimes(
        self, theatre_slug: str, local_date: str, showtimes: Sequence[Mapping[str, Any]]
    ) -> Sequence[str]: ...

    def due_resources(self, now: datetime, policy: WorkerPolicy) -> Mapping[JobKind, Sequence[Mapping[str, Any]]]: ...

    def complete_catalog_lookup(self, lookup_id: str, results: Sequence[Mapping[str, Any]]) -> None: ...

    def resolve_cached_catalog(
        self, kind: str, query: Mapping[str, Any]
    ) -> Sequence[Mapping[str, Any]]: ...

    def upsert_theatre_catalog(self, theatres: Sequence[Mapping[str, Any]]) -> int: ...

    def upsert_format_catalog(self, formats: Sequence[Mapping[str, Any]]) -> int: ...

    def upsert_zip_centroid(self, zip_code: str, latitude: float, longitude: float) -> None: ...

    def zip_centroid_exists(self, zip_code: str) -> bool: ...

    def catalog_refresh_due(self, now: datetime, *, max_age_seconds: float) -> bool: ...

    def mark_catalog_refreshed(self, now: datetime) -> None: ...

    def metrics(self) -> Mapping[str, Any]: ...


@dataclass
class SharedWorker:
    store: WorkerStore
    proxy_pool: ProxyPool
    policy: WorkerPolicy = field(default_factory=WorkerPolicy)
    worker_id: str = field(default_factory=lambda: f"worker-{uuid.uuid4().hex[:12]}")

    def __post_init__(self) -> None:
        self.client = GraphQLClient(self.proxy_pool.active)
        self._next_enqueue_at = datetime.now(timezone.utc)

    def enqueue_due_work(self, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)
        if self.store.catalog_refresh_due(
            now, max_age_seconds=self.policy.catalog_refresh_interval_seconds
        ):
            # One stable dedupe key: re-enqueuing while pending reschedules, and a
            # running sweep is left untouched, so the refresh never fans out.
            self.store.enqueue_job(
                JobKind.CATALOG_REFRESH,
                "catalog_refresh:global",
                {"resources": [{"resource_key": "catalog-refresh"}]},
                run_at=now,
                priority=PRIORITY[JobKind.CATALOG_REFRESH],
            )
        for kind, resources in self.store.due_resources(now, self.policy).items():
            # Persist one stable row per shared resource.  Batches are formed
            # when leases are claimed, so an urgent transition (for example a
            # newly sellable showtime) promotes the existing seat-map job
            # instead of creating a second job with a different compound key.
            for resource in resources:
                key = str(resource["resource_key"])
                self.store.enqueue_job(
                    kind,
                    f"{kind.value}:{key}",
                    {"resources": [dict(resource)]},
                    run_at=now,
                    priority=PRIORITY[kind],
                )

    def run_once(self) -> bool:
        now = datetime.now(timezone.utc)
        if now >= self._next_enqueue_at:
            # Throttle the full due-work scan. Running it on every ~1s idle loop
            # is a heavy multi-table read/write set (the ~12% idle CPU in prod);
            # transitions still enqueue their own jobs directly, so responsiveness
            # to newly sellable showtimes is unaffected.
            self.enqueue_due_work(now)
            self._next_enqueue_at = now + timedelta(
                seconds=self.policy.enqueue_interval_seconds
            )
        jobs = list(
            self.store.claim_jobs(
                self.worker_id,
                limit=max(
                    self.policy.status_batch_size,
                    self.policy.seat_batch_size,
                    self.policy.date_batch_size,
                    self.policy.discovery_batch_size,
                    1,
                ),
                lease_seconds=self.policy.lease_seconds,
            )
        )
        if not jobs:
            self._heartbeat()
            return False
        first = jobs[0]
        if any(job.kind != first.kind for job in jobs):
            # Repository implementations must lease one homogeneous batch so
            # the single global request gate always corresponds to one AMC
            # request.  Fail closed if a custom adapter violates the contract.
            run_at = datetime.now(timezone.utc) + timedelta(seconds=5)
            for job in jobs:
                self.store.retry_job(
                    job.id, run_at=run_at, error_code="mixed_job_batch"
                )
            self.store.open_incident(
                "worker_loop_error",
                "warning",
                {"job_kind": "mixed_job_batch"},
            )
            self._heartbeat()
            return True
        resources = [
            dict(resource)
            for job in jobs
            for resource in list(job.payload.get("resources") or [])
        ]
        job = ClaimedJob(
            id=first.id,
            kind=first.kind,
            resource_key="|".join(item.resource_key for item in jobs),
            payload={"resources": resources},
            attempts=max(item.attempts for item in jobs),
        )
        uses_amc = self._job_uses_amc(job)
        try:
            if uses_amc:
                self.store.acquire_request_slot(
                    minimum_gap_seconds=self.policy.request_gap_seconds
                )
            self._execute(job)
        except AMCUpstreamCooldown as exc:
            until = getattr(exc, "cooldown_until", None) or (
                datetime.now(timezone.utc) + timedelta(seconds=exc.seconds)
            )
            if getattr(exc, "persisted_cooldown", False):
                # The gate re-observed an already-persisted cooldown; no AMC call
                # was made. Requeue at that time without rotating proxies or
                # sliding the cooldown.
                for source in jobs:
                    self.store.retry_job(
                        source.id, run_at=until, error_code="upstream_cooldown"
                    )
            else:
                # A fresh AMC block (403/429/503) is usually tied to the current
                # exit IP, so prefer rotating to a new proxy and continuing over
                # an extended global cooldown. Only once every proxy has been
                # blocked this cycle do we honor the upstream cooldown.
                self.store.record_proxy_failure(self.proxy_pool.active_index)
                replacement = self.proxy_pool.failover()
                if replacement is not None:
                    self.proxy_pool = replacement
                    self.client.use_proxy(replacement.active)
                    self.store.set_active_proxy(replacement.active_index)
                    run_at = datetime.now(timezone.utc) + timedelta(
                        seconds=self.policy.request_gap_seconds
                    )
                    for source in jobs:
                        self.store.retry_job(
                            source.id,
                            run_at=run_at,
                            error_code="upstream_block_proxy_rotated",
                        )
                else:
                    self.store.set_global_cooldown(until, status_code=exc.status_code)
                    self.store.open_incident(
                        "amc_upstream_blocked",
                        "warning",
                        {
                            "status_code": exc.status_code,
                            "cooldown_until": until.isoformat(),
                        },
                    )
                    for source in jobs:
                        self.store.retry_job(
                            source.id, run_at=until, error_code="upstream_cooldown"
                        )
                    # Every proxy is blocked; after the cooldown start one fresh
                    # recovery cycle from the primary (mirrors transport exhaustion).
                    self.proxy_pool = ProxyPool(self.proxy_pool.endpoints, 0, ())
                    self.client.use_proxy(self.proxy_pool.active)
        except ProxyEndpointError as exc:
            if getattr(exc, "persisted_backoff", False):
                self.store.retry_job(
                    first.id,
                    run_at=getattr(exc, "backoff_until"),
                    error_code="transport_backoff",
                )
                for source in jobs[1:]:
                    self.store.retry_job(
                        source.id,
                        run_at=getattr(exc, "backoff_until"),
                        error_code="transport_backoff",
                    )
            else:
                self._handle_proxy_failure(jobs)
        except Exception:
            logging.exception("Worker job %s failed", job.kind.value)
            delay = min(300, 5 * 2 ** min(job.attempts, 6))
            run_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            for source in jobs:
                self.store.retry_job(
                    source.id,
                    run_at=run_at,
                    error_code="worker_error",
                )
            self.store.open_incident(
                "worker_loop_error", "warning", {"job_kind": job.kind.value}
            )
        else:
            for source in jobs:
                self.store.complete_job(source.id)
            self.store.resolve_incident("worker_loop_error")
            if uses_amc:
                self.store.clear_global_cooldown()
                self.store.clear_transport_backoff()
                self.store.record_proxy_success(self.proxy_pool.active_index)
                self.store.resolve_incident("amc_upstream_blocked")
                self.store.resolve_incident("proxy_capacity_exhausted")
                self.proxy_pool = self.proxy_pool.recovered()
        self._heartbeat()
        return True

    def _execute(self, job: ClaimedJob) -> None:
        resources = list(job.payload.get("resources") or [])
        if job.kind == JobKind.STATUS:
            self._status(resources)
        elif job.kind == JobKind.SEATMAP:
            self._seatmaps(resources)
        elif job.kind == JobKind.SELECTABLE_DATES:
            self._selectable_dates(resources)
        elif job.kind == JobKind.DISCOVERY:
            self._discovery(resources)
        elif job.kind == JobKind.CATALOG:
            self._catalog(resources)
        elif job.kind == JobKind.CATALOG_REFRESH:
            self._refresh_catalog()
        else:  # pragma: no cover - StrEnum guards this at persistence boundary
            raise ValueError(f"unsupported job kind: {job.kind}")

    @staticmethod
    def _job_uses_amc(job: ClaimedJob) -> bool:
        if job.kind != JobKind.CATALOG:
            return True
        resources = list(job.payload.get("resources") or [])
        if len(resources) != 1:
            return False
        resource = resources[0]
        lookup_kind = str(resource.get("kind") or resource.get("lookup_kind") or "").strip()
        return lookup_kind in {
            "theatres",
            "location",
            "movie-search",
            "geocode",
        }

    def _status(self, resources: Sequence[Mapping[str, Any]]) -> None:
        ids = [str(resource["showtime_id"]) for resource in resources]
        query, variables, aliases = status_batch_query(ids)
        viewer = self.client.query(query, variables).get("viewer") or {}
        for alias, showtime_id in aliases.items():
            remote = viewer.get(alias)
            raw_status = remote.get("status") if remote else None
            if not remote or not normalize_status(raw_status):
                _, missing_count = self.store.record_status(
                    showtime_id,
                    status=None,
                    is_sold_out=False,
                    is_almost_sold_out=False,
                    missing=True,
                )
                self.store.retire_showtime_if_confirmed_missing(
                    showtime_id, missing_count=missing_count
                )
                continue
            prior, _ = self.store.record_status(
                showtime_id,
                status=raw_status,
                is_sold_out=bool(remote.get("isSoldOut")),
                is_almost_sold_out=bool(remote.get("isAlmostSoldOut")),
                missing=False,
            )
            if became_sellable(prior, raw_status):
                self.store.enqueue_job(
                    JobKind.SEATMAP,
                    f"seatmap:{showtime_id}",
                    {"resources": [{"resource_key": showtime_id, "showtime_id": showtime_id}]},
                    run_at=datetime.now(timezone.utc),
                    priority=0,
                )

    def _seatmaps(self, resources: Sequence[Mapping[str, Any]]) -> None:
        ids = [str(resource["showtime_id"]) for resource in resources]
        query, variables, aliases = seatmap_batch_query(ids)
        viewer = self.client.query(query, variables).get("viewer") or {}
        for alias, showtime_id in aliases.items():
            remote = viewer.get(alias)
            if not remote:
                self.store.record_missing_seatmap(showtime_id)
                continue
            seats = list(((remote.get("seatingLayout") or {}).get("seats") or []))
            context = dict(self.store.showtime_context(showtime_id))
            for subscription in self.store.subscriptions_for_showtime(showtime_id):
                runs = rank_runs(
                    seats, count=subscription.seat_count, preset=subscription.seat_preset
                )
                highlighted = {
                    coordinate for run in runs[:1] for coordinate in run.coordinates
                }
                compact = compact_seats(seats, highlighted)
                payload = {
                    **context,
                    "showtime_id": showtime_id,
                    "subscription_id": subscription.id,
                    "subscription_label": subscription.label,
                    "seat_count": subscription.seat_count,
                    "seat_preset": subscription.seat_preset,
                    "runs": [run.as_dict() for run in runs[:10]],
                    "available_count": sum(1 for seat in seats if seat.get("available")),
                    "seatmap": compact,
                }
                self.store.apply_availability(
                    subscription,
                    showtime_id,
                    signatures={run.signature for run in runs},
                    alert_payload=payload,
                )

    def _selectable_dates(self, resources: Sequence[Mapping[str, Any]]) -> None:
        query, variables, aliases = selectable_dates_batch_query(resources)
        viewer = self.client.query(query, variables).get("viewer") or {}
        observed = parse_selectable_dates(viewer, aliases)
        now = datetime.now(timezone.utc)
        for movie_slug, dates in observed.items():
            initialized, new_dates = self.store.observe_selectable_dates(movie_slug, dates)
            if not initialized:
                continue
            for selected in new_dates:
                for theatre_slug in self.store.theatres_for_movie(movie_slug):
                    self.store.enqueue_job(
                        JobKind.DISCOVERY,
                        f"discovery:{theatre_slug}:{selected.isoformat()}",
                        {
                            "resources": [
                                {
                                    "resource_key": f"{theatre_slug}:{selected.isoformat()}",
                                    "theatre_slug": theatre_slug,
                                    "date": selected.isoformat(),
                                }
                            ]
                        },
                        run_at=now,
                        priority=PRIORITY[JobKind.DISCOVERY] - 5,
                    )

    def _discovery(self, resources: Sequence[Mapping[str, Any]]) -> None:
        query, variables, aliases = discovery_batch_query(resources)
        user = ((self.client.query(query, variables).get("viewer") or {}).get("user") or {})
        now = datetime.now(timezone.utc)
        for alias, target in aliases.items():
            showtimes = parse_discovery_payload(
                user.get(alias) or {}, theatre_slug=str(target["theatre_slug"])
            )
            ids = self.store.upsert_discovered_showtimes(
                str(target["theatre_slug"]), str(target["date"]), showtimes
            )
            for showtime_id in ids:
                self.store.enqueue_job(
                    JobKind.SEATMAP,
                    f"seatmap:{showtime_id}",
                    {"resources": [{"resource_key": showtime_id, "showtime_id": showtime_id}]},
                    run_at=now,
                    priority=PRIORITY[JobKind.SEATMAP],
                )

    def _catalog(self, resources: Sequence[Mapping[str, Any]]) -> None:
        if len(resources) != 1:
            raise ValueError("catalog jobs contain exactly one lookup")
        resource = resources[0]
        lookup_kind = str(resource.get("kind") or resource.get("lookup_kind") or "").strip()
        # Normalize the pre-Discord singular names so already-leased jobs remain
        # safe across deployment. New jobs always use the plural wizard kinds.
        lookup_kind = {"location": "theatres", "movie": "movies"}.get(
            lookup_kind, lookup_kind
        )
        lookup_id = str(resource["lookup_id"])
        raw_query = resource.get("query")
        if raw_query is None:
            # CatalogRepository historically expanded query fields into the job
            # resource. Keep those durable jobs consumable while accepting the
            # generic query object used by the Discord wizard.
            query = {
                key: value
                for key, value in resource.items()
                if key not in {"resource_key", "lookup_id", "kind", "lookup_kind"}
            }
        elif isinstance(raw_query, Mapping):
            query = dict(raw_query)
        else:
            raise ValueError("catalog lookup query must be an object")

        if lookup_kind == "geocode":
            # Resolve a user ZIP to a search point once and cache it, so the
            # wizard ranks nearest theatres from the static catalog thereafter.
            zip_code = str(query.get("zip_code") or "").strip()
            if not zip_code:
                raise ValueError("geocode lookup requires a ZIP code")
            # Already-cached ZIPs need no AMC call — nearest-theatre ranking runs
            # entirely against the static catalog once the centroid exists.
            if not self.store.zip_centroid_exists(zip_code):
                centroid = parse_location_centroid(
                    self.client.query(LOCATION_QUERY, {"query": zip_code})
                )
                if centroid is not None:
                    self.store.upsert_zip_centroid(
                        zip_code, centroid["latitude"], centroid["longitude"]
                    )
            results = []
        elif lookup_kind == "theatres":
            zip_code = str(query.get("zip_code") or "").strip()
            if not zip_code:
                raise ValueError("theatre catalog lookup requires a ZIP code")
            results = parse_location_catalog(
                self.client.query(LOCATION_QUERY, {"query": zip_code})
            )
        elif lookup_kind == "movie-search":
            title = str(query.get("title") or "").strip()
            if not 2 <= len(title) <= 100:
                raise ValueError("movie search requires a title between 2 and 100 characters")
            results = parse_movie_catalog(
                self.client.query(MOVIE_SEARCH_QUERY, {"query": title})
            )
        elif lookup_kind in {"movies", "formats"}:
            # These choices are derived from showtimes already discovered for
            # the selected theatres. Calling AMC search here would both waste a
            # globally gated request and return movies unrelated to that choice.
            results = list(self.store.resolve_cached_catalog(lookup_kind, query))
        else:
            raise ValueError("unsupported catalog lookup kind")
        self.store.complete_catalog_lookup(lookup_id, results)

    def _refresh_catalog(self) -> None:
        """Paginate and upsert the national theatre + presentation-format catalogs.

        Each page acquires its own request slot so the global gate paces the sweep;
        a cooldown mid-sweep propagates to run_once, which rotates/retries the whole
        job. Upserts are idempotent, so restarting from page one is harmless.
        """

        cursor: str | None = None
        for _ in range(MAX_CATALOG_PAGES):
            self.store.acquire_request_slot(
                minimum_gap_seconds=self.policy.request_gap_seconds
            )
            page = parse_theatres_catalog(
                self.client.query(
                    THEATRES_CATALOG_QUERY,
                    {"first": self.policy.catalog_page_size, "after": cursor},
                )
            )
            self.store.upsert_theatre_catalog(page["theatres"])
            cursor = page.get("end_cursor")
            if not page.get("has_next_page") or not cursor:
                break
        cursor = None
        for _ in range(MAX_CATALOG_PAGES):
            self.store.acquire_request_slot(
                minimum_gap_seconds=self.policy.request_gap_seconds
            )
            page = parse_formats_catalog(
                self.client.query(
                    FORMATS_CATALOG_QUERY,
                    {"first": self.policy.catalog_page_size, "after": cursor},
                )
            )
            self.store.upsert_format_catalog(page["formats"])
            cursor = page.get("end_cursor")
            if not page.get("has_next_page") or not cursor:
                break
        self.store.mark_catalog_refreshed(datetime.now(timezone.utc))

    def _handle_proxy_failure(self, jobs: Sequence[ClaimedJob]) -> None:
        self.store.record_proxy_failure(self.proxy_pool.active_index)
        replacement = self.proxy_pool.failover()
        if replacement is None:
            run_at = datetime.now(timezone.utc) + timedelta(minutes=5)
            self.store.set_transport_backoff(run_at)
            self.store.open_incident(
                "proxy_capacity_exhausted", "warning", {"endpoint_count": len(self.proxy_pool.endpoints)}
            )
            for job in jobs:
                self.store.retry_job(
                    job.id, run_at=run_at, error_code="proxy_exhausted"
                )
            # After the bounded backoff, start one fresh recovery cycle at the
            # primary. Persisted health prevents a restart during this incident
            # from retrying endpoints before that backoff expires.
            self.proxy_pool = ProxyPool(self.proxy_pool.endpoints, 0, ())
            self.client.use_proxy(self.proxy_pool.active)
            return
        self.proxy_pool = replacement
        self.client.use_proxy(replacement.active)
        self.store.set_active_proxy(replacement.active_index)
        # The failed request already consumed a globally gated slot. Requeue it;
        # never retry in place and never reset the gate on proxy selection.
        run_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.policy.request_gap_seconds
        )
        for job in jobs:
            self.store.retry_job(
                job.id,
                run_at=run_at,
                error_code="proxy_failover",
            )

    def _heartbeat(self) -> None:
        metrics = dict(self.store.metrics())
        active = int(metrics.get("active_showtimes", 0))
        degraded = bool(metrics.get("degraded", False))
        delivery_count = int(metrics.get("user_delivery_backlog_count", 0))
        delivery_age = float(metrics.get("oldest_user_delivery_age_seconds", 0.0))
        if (
            delivery_count >= self.policy.delivery_backlog_count_threshold
            or delivery_age >= self.policy.delivery_backlog_age_seconds
        ):
            self.store.open_incident(
                "delivery_backlog",
                "warning",
                {
                    "summary": "Discord user-alert delivery backlog is elevated",
                    "pending_count": delivery_count,
                    "oldest_age_seconds": round(delivery_age),
                },
            )
        else:
            self.store.resolve_incident("delivery_backlog")
        metrics["effective_status_interval_seconds"] = adaptive_status_interval(
            active,
            batch_size=self.policy.status_batch_size,
            request_gap_seconds=self.policy.request_gap_seconds,
            healthy_floor_seconds=self.policy.healthy_status_floor_seconds,
            degraded=degraded,
        )
        metrics["worker_id"] = self.worker_id
        self.store.heartbeat("amc-worker", metadata=metrics)


def load_proxy_pool_from_environment() -> ProxyPool:
    primary = os.environ.get("AMC_HTTPS_PROXY", "").strip()
    raw_backups = os.environ.get("AMC_HTTPS_PROXY_FAILOVER", "[]")
    try:
        parsed = json.loads(raw_backups)
    except json.JSONDecodeError as exc:
        raise RuntimeError("AMC_HTTPS_PROXY_FAILOVER must be a JSON array") from exc
    if not isinstance(parsed, list):
        raise RuntimeError("AMC_HTTPS_PROXY_FAILOVER must be a JSON array")
    return ProxyPool.from_values(primary, [str(value).strip() for value in parsed])


def run_forever(worker: SharedWorker, *, idle_seconds: float = 1) -> None:
    stopped = False

    def stop(*_: Any) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopped:
        try:
            worked = worker.run_once()
        except Exception:
            # A single iteration must never take the whole worker down (e.g. a
            # lost job lease or a transient managed-PostgreSQL blip inside a
            # handler). Log and keep looping; leased work recovers on expiry.
            logging.exception("worker iteration failed; continuing")
            worked = False
        if not worked:
            time.sleep(idle_seconds)
