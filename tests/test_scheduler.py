from __future__ import annotations

import unittest
import os
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

from amc_watch.amc import (
    AMCUpstreamCooldown,
    API_URL,
    GraphQLClient,
    LOCATION_QUERY,
    MOVIE_SEARCH_QUERY,
    ProxyEndpointError,
    ProxyPool,
    parse_selectable_dates,
)
from amc_watch.scheduler import ClaimedJob, JobKind, SharedWorker, SubscriptionTarget


class FakeStore:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.enqueued = []
        self.completed = []
        self.retried = []
        self.cooldown = None
        self.transport_backoff = None
        self.incidents = []
        self.incident_metadata = []
        self.resolved = []
        self.proxy_index = 0
        self.proxy_failures = []
        self.proxy_successes = []
        self.missing_seatmaps = []
        self.statuses = {}
        self.misses = {}
        self.retired = []
        self.gate_calls = 0
        self.heartbeats = []
        self.cached_catalog = {}
        self.catalog_resolutions = []
        self.completed_catalog = []
        self.upserted_theatres = []
        self.upserted_formats = []
        self.upserted_centroids = []
        self.catalog_refresh_is_due = False
        self.catalog_refreshed_at = None
        self.subscriptions = []
        self.availability = []
        self.metric_values = {
            "active_showtimes": 1,
            "user_delivery_backlog_count": 0,
            "oldest_user_delivery_age_seconds": 0,
        }

    def due_resources(self, now, policy):
        return {}

    def claim_jobs(self, worker_id, *, limit, lease_seconds):
        return [self.jobs.pop(0)] if self.jobs else []

    def acquire_request_slot(self, *, minimum_gap_seconds):
        self.gate_calls += 1
        return datetime.now(timezone.utc)

    def complete_job(self, job_id):
        self.completed.append(job_id)

    def retry_job(self, job_id, *, run_at, error_code):
        self.retried.append((job_id, error_code))

    def enqueue_job(self, kind, resource_key, payload, *, run_at, priority):
        self.enqueued.append((kind, resource_key, priority))

    def heartbeat(self, service, *, metadata):
        self.heartbeats.append((service, metadata))

    def metrics(self):
        return dict(self.metric_values)

    def set_global_cooldown(self, until, *, status_code):
        self.cooldown = (until, status_code)

    def clear_global_cooldown(self):
        self.cooldown = None

    def set_transport_backoff(self, until):
        self.transport_backoff = until

    def clear_transport_backoff(self):
        self.transport_backoff = None

    def open_incident(self, event_type, severity, metadata):
        self.incidents.append(event_type)
        self.incident_metadata.append(dict(metadata))

    def resolve_incident(self, event_type):
        self.resolved.append(event_type)

    def set_active_proxy(self, index):
        self.proxy_index = index

    def record_proxy_failure(self, index):
        self.proxy_failures.append(index)

    def record_proxy_success(self, index):
        self.proxy_successes.append(index)

    def record_status(self, showtime_id, *, status, is_sold_out, is_almost_sold_out, missing):
        prior = self.statuses.get(showtime_id)
        if missing:
            self.misses[showtime_id] = self.misses.get(showtime_id, 0) + 1
        else:
            self.misses[showtime_id] = 0
            self.statuses[showtime_id] = status
        return prior, self.misses[showtime_id]

    def retire_showtime_if_confirmed_missing(self, showtime_id, *, missing_count):
        if missing_count >= 3:
            self.retired.append(showtime_id)

    def subscriptions_for_showtime(self, showtime_id):
        return self.subscriptions

    def showtime_context(self, showtime_id):
        return {}

    def record_missing_seatmap(self, showtime_id):
        self.missing_seatmaps.append(showtime_id)

    def apply_availability(self, *args, **kwargs):
        self.availability.append((args, kwargs))

    def observe_selectable_dates(self, movie_slug, dates):
        return False, set()

    def theatres_for_movie(self, movie_slug):
        return []

    def upsert_discovered_showtimes(self, theatre_slug, local_date, showtimes):
        return []

    def resolve_cached_catalog(self, kind, query):
        self.catalog_resolutions.append((kind, query))
        return self.cached_catalog.get(kind, [])

    def complete_catalog_lookup(self, lookup_id, results):
        self.completed_catalog.append((lookup_id, list(results)))

    def upsert_theatre_catalog(self, theatres):
        self.upserted_theatres.extend(theatres)
        return len(theatres)

    def upsert_format_catalog(self, formats):
        self.upserted_formats.extend(formats)
        return len(formats)

    def upsert_zip_centroid(self, zip_code, latitude, longitude):
        self.upserted_centroids.append((zip_code, latitude, longitude))

    def zip_centroid_exists(self, zip_code):
        return any(z == zip_code for z, _lat, _lon in self.upserted_centroids)

    def catalog_refresh_due(self, now, *, max_age_seconds):
        return self.catalog_refresh_is_due

    def mark_catalog_refreshed(self, now):
        self.catalog_refreshed_at = now


class FakeClient:
    def __init__(self, result=None, error=None):
        self.result = result or {}
        self.error = error
        self.proxy_changes = []
        self.calls = []

    def query(self, query, variables):
        self.calls.append((query, variables))
        if self.error:
            raise self.error
        return self.result

    def use_proxy(self, endpoint):
        self.proxy_changes.append(endpoint)


class SequenceClient:
    """Returns a scripted response per query call (for paginated sweeps)."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.proxy_changes = []

    def query(self, query, variables):
        self.calls.append((query, variables))
        return self.responses.pop(0)

    def use_proxy(self, endpoint):
        self.proxy_changes.append(endpoint)


def status_job(identifier="j1"):
    return ClaimedJob(
        identifier,
        JobKind.STATUS,
        "status:1",
        {"resources": [{"resource_key": "1", "showtime_id": "1"}]},
    )


def catalog_job(kind, query, identifier="catalog-job"):
    return ClaimedJob(
        identifier,
        JobKind.CATALOG,
        f"catalog:{kind}",
        {
            "resources": [
                {
                    "resource_key": "lookup-1",
                    "lookup_id": "lookup-1",
                    "kind": kind,
                    "query": query,
                }
            ]
        },
    )


class SchedulerTests(unittest.TestCase):
    def worker(self, store):
        worker = SharedWorker(
            store,
            ProxyPool.from_values(
                "http://user:pass@primary.invalid:8000",
                ["http://user:pass@backup.invalid:8000"],
            ),
        )
        return worker

    def test_catalog_refresh_paginates_theatres_and_formats(self):
        store = FakeStore(
            [
                ClaimedJob(
                    "refresh-1",
                    JobKind.CATALOG_REFRESH,
                    "catalog_refresh:global",
                    {"resources": [{"resource_key": "catalog-refresh"}]},
                )
            ]
        )
        worker = self.worker(store)
        worker.client = SequenceClient(
            [
                {"viewer": {"theatres": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                    "edges": [{"node": {"theatreId": 1, "slug": "a", "name": "A",
                                        "postalCode": "10001", "latitude": 40.0,
                                        "longitude": -73.0, "ticketable": True}}]}}},
                {"viewer": {"theatres": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [{"node": {"theatreId": 2, "slug": "b", "name": "B",
                                        "postalCode": "90001", "latitude": 34.0,
                                        "longitude": -118.0, "ticketable": True}}]}}},
                {"viewer": {"attributes": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [{"node": {"code": "IMAX70MM", "name": "IMAX 70MM"}}]}}},
            ]
        )
        self.assertTrue(worker.run_once())
        self.assertEqual({t["slug"] for t in store.upserted_theatres}, {"a", "b"})
        self.assertEqual({f["code"] for f in store.upserted_formats}, {"imax70mm"})
        self.assertIsNotNone(store.catalog_refreshed_at)
        self.assertEqual(store.completed, ["refresh-1"])
        self.assertEqual(len(worker.client.calls), 3)  # 2 theatre pages + 1 format page

    def test_geocode_lookup_caches_centroid(self):
        store = FakeStore([catalog_job("geocode", {"zip_code": "00000"})])
        worker = self.worker(store)
        worker.client = FakeClient(
            {"viewer": {"location": {"edges": [
                {"node": {"latitude": 34.05, "longitude": -118.24}}
            ]}}}
        )
        self.assertTrue(worker.run_once())
        self.assertEqual(store.upserted_centroids, [("00000", 34.05, -118.24)])
        self.assertEqual(store.completed_catalog, [("lookup-1", [])])

    def test_geocode_skips_amc_when_centroid_cached(self):
        store = FakeStore([catalog_job("geocode", {"zip_code": "00000"})])
        store.upserted_centroids.append(("00000", 40.0, -73.0))  # already geocoded
        worker = self.worker(store)
        worker.client = FakeClient({"viewer": {"location": {"edges": []}}})
        self.assertTrue(worker.run_once())
        self.assertEqual(worker.client.calls, [])  # no AMC call for a cached ZIP
        self.assertEqual(store.completed_catalog, [("lookup-1", [])])

    def test_catalog_refresh_enqueued_only_when_due(self):
        store = FakeStore([])
        store.catalog_refresh_is_due = True
        worker = self.worker(store)
        worker.enqueue_due_work()
        self.assertIn(JobKind.CATALOG_REFRESH, [row[0] for row in store.enqueued])
        store.enqueued.clear()
        store.catalog_refresh_is_due = False
        worker.enqueue_due_work()
        self.assertNotIn(JobKind.CATALOG_REFRESH, [row[0] for row in store.enqueued])

    def test_sellable_transition_enqueues_immediate_seatmap(self):
        store = FakeStore([status_job()])
        store.statuses["1"] = "SOLDOUT"
        worker = self.worker(store)
        worker.client = FakeClient(
            {"viewer": {"status0": {"status": "SELLABLE", "isSoldOut": False}}}
        )
        self.assertTrue(worker.run_once())
        self.assertEqual(store.completed, ["j1"])
        self.assertEqual(store.enqueued[0][0], JobKind.SEATMAP)
        self.assertEqual(store.enqueued[0][2], 0)

    def test_homogeneous_resource_jobs_are_coalesced_into_one_batch(self):
        class BatchStore(FakeStore):
            def claim_jobs(self, worker_id, *, limit, lease_seconds):
                claimed = self.jobs[:limit]
                del self.jobs[:limit]
                return claimed

        store = BatchStore(
            [
                ClaimedJob(
                    "j1",
                    JobKind.STATUS,
                    "status:1",
                    {"resources": [{"resource_key": "1", "showtime_id": "1"}]},
                ),
                ClaimedJob(
                    "j2",
                    JobKind.STATUS,
                    "status:2",
                    {"resources": [{"resource_key": "2", "showtime_id": "2"}]},
                ),
            ]
        )
        worker = self.worker(store)
        worker.client = FakeClient(
            {
                "viewer": {
                    "status0": {"status": "SOLDOUT", "isSoldOut": True},
                    "status1": {"status": "SOLDOUT", "isSoldOut": True},
                }
            }
        )

        self.assertTrue(worker.run_once())

        self.assertEqual(len(worker.client.calls), 1)
        self.assertEqual(store.completed, ["j1", "j2"])
        self.assertEqual(store.gate_calls, 1)

    def test_graphql_proxy_ignores_no_proxy_environment_bypass(self):
        proxy = "http://user:pass@proxy.invalid:8000"
        with patch.dict(os.environ, {"NO_PROXY": "graph.amctheatres.com"}, clear=False):
            client = GraphQLClient(proxy)
            handler = next(
                value
                for value in client._opener.handlers
                if isinstance(value, urllib.request.ProxyHandler)
            )
            request = urllib.request.Request(API_URL)
            handler.proxy_open(request, proxy, "https")

        self.assertEqual(request.host, "proxy.invalid:8000")
        self.assertEqual(request._tunnel_host, "graph.amctheatres.com")

    def test_null_status_requires_repeated_misses_and_valid_status_recovers(self):
        store = FakeStore(
            [status_job("j1"), status_job("j2"), status_job("j3"), status_job("j4")]
        )
        store.statuses["1"] = "SOLDOUT"
        worker = self.worker(store)
        # A present GraphQL alias with a null status is still a missing sample.
        worker.client = FakeClient({"viewer": {"status0": {"status": None}}})
        worker.run_once()
        self.assertEqual(store.retired, [])
        worker.run_once()
        self.assertEqual(store.retired, [])
        worker.run_once()
        self.assertEqual(store.retired, ["1"])
        worker.client = FakeClient(
            {"viewer": {"status0": {"status": "SELLABLE", "isSoldOut": False}}}
        )
        worker.run_once()
        self.assertEqual(store.misses["1"], 0)
        self.assertEqual(store.statuses["1"], "SELLABLE")

    def test_missing_seatmap_alias_is_recorded_for_deadline_advancement(self):
        store = FakeStore(
            [
                ClaimedJob(
                    "seat-job",
                    JobKind.SEATMAP,
                    "seatmap:1",
                    {"resources": [{"resource_key": "1", "showtime_id": "1"}]},
                )
            ]
        )
        worker = self.worker(store)
        worker.client = FakeClient({"viewer": {"seat0": None}})

        self.assertTrue(worker.run_once())

        self.assertEqual(store.missing_seatmaps, ["1"])

    def test_seatmap_highlights_only_the_best_recommended_run(self):
        store = FakeStore(
            [
                ClaimedJob(
                    "seat-job",
                    JobKind.SEATMAP,
                    "seatmap:1",
                    {"resources": [{"resource_key": "1", "showtime_id": "1"}]},
                )
            ]
        )
        store.subscriptions = [SubscriptionTarget("sub", 10, 20, 2, "center-back")]
        seats = [
            {
                "row": row,
                "column": column,
                "name": f"R{row}-{column}",
                "available": True,
                "type": "CanReserve",
                "shouldDisplay": True,
            }
            for row in range(12)
            for column in range(30)
        ]
        worker = self.worker(store)
        worker.client = FakeClient(
            {"viewer": {"seat0": {"seatingLayout": {"seats": seats}}}}
        )

        self.assertTrue(worker.run_once())

        payload = store.availability[0][1]["alert_payload"]
        recommended = [
            seat for seat in payload["seatmap"]["seats"] if seat["s"] == "recommended"
        ]
        self.assertEqual(len(recommended), 2)

    def test_delivery_backlog_incident_uses_sanitized_aggregate_metrics_and_recovers(self):
        store = FakeStore([])
        store.metric_values.update(
            {
                "user_delivery_backlog_count": 101,
                "oldest_user_delivery_age_seconds": 42,
            }
        )
        worker = self.worker(store)

        self.assertFalse(worker.run_once())
        self.assertEqual(store.incidents, ["delivery_backlog"])
        self.assertEqual(
            set(store.incident_metadata[0]),
            {"summary", "pending_count", "oldest_age_seconds"},
        )
        self.assertNotIn("guild", str(store.incident_metadata[0]).casefold())
        store.metric_values.update(
            {
                "user_delivery_backlog_count": 0,
                "oldest_user_delivery_age_seconds": 0,
            }
        )
        self.assertFalse(worker.run_once())
        self.assertIn("delivery_backlog", store.resolved)

    def test_amc_upstream_block_rotates_proxy_and_continues(self):
        store = FakeStore([status_job()])
        worker = self.worker(store)  # primary + one backup
        worker.client = FakeClient(error=AMCUpstreamCooldown(429, 120))
        worker.run_once()
        # Rotated to the backup and requeued soon, no extended global cooldown.
        self.assertEqual(worker.proxy_pool.active_index, 1)
        self.assertEqual(store.proxy_index, 1)
        self.assertEqual(store.proxy_failures, [0])
        self.assertEqual(store.retried[0][1], "upstream_block_proxy_rotated")
        self.assertIsNone(store.cooldown)

    def test_amc_upstream_block_cools_down_when_all_proxies_blocked(self):
        store = FakeStore([status_job()])
        worker = SharedWorker(
            store,
            ProxyPool.from_values("http://user:pass@primary.invalid:8000", []),
        )
        worker.client = FakeClient(error=AMCUpstreamCooldown(429, 120))
        worker.run_once()
        # No backup to rotate to -> honor the upstream cooldown.
        self.assertEqual(store.retried[0][1], "upstream_cooldown")
        self.assertEqual(store.cooldown[1], 429)

    def test_amc_cooldown_clamps_hostile_retry_after(self):
        client = GraphQLClient("http://user:pass@proxy.invalid:8000")
        hostile = urllib.error.HTTPError(
            API_URL, 429, "Too Many Requests", {"Retry-After": "999999999"}, None
        )
        with patch.object(client._opener, "open", side_effect=hostile):
            with self.assertRaises(AMCUpstreamCooldown) as ctx:
                client.query("query{__typename}", {})
        # A hostile Retry-After must be capped, never a multi-year cooldown.
        self.assertEqual(ctx.exception.seconds, 3600)

    def test_parse_selectable_dates_tolerates_malformed_bounds(self):
        aliases = {"m0": {"slug": "example-feature", "not_before": "8/1/2026", "not_after": "nonsense"}}
        viewer = {"m0": {"dates": ["2026-07-25", "2026-08-15", "not-a-date"]}}
        # Malformed window bounds are ignored rather than raising (which would
        # poison the whole batch); valid dates kept, unparseable date skipped.
        result = parse_selectable_dates(viewer, aliases)
        self.assertEqual(result["example-feature"], {date(2026, 7, 25), date(2026, 8, 15)})

    def test_run_once_throttles_due_work_scan(self):
        store = FakeStore([])
        worker = self.worker(store)
        worker.client = FakeClient()
        worker.enqueue_due_work = Mock()
        worker.run_once()
        worker.run_once()  # immediately after -> within the throttle interval
        self.assertEqual(worker.enqueue_due_work.call_count, 1)

    def test_success_resolves_prior_worker_loop_error(self):
        store = FakeStore([status_job("failed"), status_job("recovered")])
        worker = self.worker(store)
        worker.client = FakeClient(error=RuntimeError("boom"))

        self.assertTrue(worker.run_once())
        self.assertIn("worker_loop_error", store.incidents)

        worker.client = FakeClient(
            {"viewer": {"status0": {"status": "SOLDOUT", "isSoldOut": True}}}
        )
        self.assertTrue(worker.run_once())
        self.assertIn("worker_loop_error", store.resolved)

    def test_proxy_transport_failure_uses_each_backup_without_resetting_gate(self):
        store = FakeStore([status_job()])
        worker = self.worker(store)
        client = FakeClient(error=ProxyEndpointError("down"))
        worker.client = client
        worker.run_once()
        self.assertEqual(worker.proxy_pool.active_index, 1)
        self.assertEqual(store.proxy_index, 1)
        self.assertEqual(store.gate_calls, 1)
        self.assertEqual(store.retried[0][1], "proxy_failover")
        self.assertEqual(client.proxy_changes, ["http://user:pass@backup.invalid:8000"])

    def test_theatre_catalog_looks_up_zip_at_amc(self):
        store = FakeStore([catalog_job("theatres", {"zip_code": "00000"})])
        worker = self.worker(store)
        worker.client = FakeClient(
            {
                "viewer": {
                    "location": {
                        "edges": [
                            {
                                "node": {
                                    "theatres": {
                                        "edges": [
                                            {
                                                "node": {
                                                    "theatreId": 129,
                                                    "slug": "amc-example-8",
                                                    "name": "AMC Example 8",
                                                    "postalCode": "00000",
                                                    "distance": 0.5,
                                                    "ticketable": True,
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        )

        self.assertTrue(worker.run_once())

        self.assertEqual(worker.client.calls, [(LOCATION_QUERY, {"query": "00000"})])
        self.assertEqual(store.gate_calls, 1)
        self.assertEqual(store.catalog_resolutions, [])
        self.assertEqual(store.completed_catalog[0][0], "lookup-1")
        self.assertEqual(
            store.completed_catalog[0][1][0]["slug"], "amc-example-8"
        )

    def test_movie_catalog_resolves_cache_without_calling_amc(self):
        query = {"zip_code": "00000", "theatre_ids": ["example-theatre"]}
        store = FakeStore([catalog_job("movies", query)])
        store.cached_catalog["movies"] = [
            {"id": "example-feature", "label": "Example Feature", "detail": ""}
        ]
        worker = self.worker(store)
        worker.client = Mock()

        self.assertTrue(worker.run_once())

        worker.client.query.assert_not_called()
        self.assertEqual(store.gate_calls, 0)
        self.assertEqual(store.catalog_resolutions, [("movies", query)])
        self.assertEqual(store.completed_catalog[0][1], store.cached_catalog["movies"])

    def test_explicit_movie_search_uses_amc_gate_and_normalizes_results(self):
        store = FakeStore([catalog_job("movie-search", {"title": "Example Feature"})])
        worker = self.worker(store)
        worker.client = FakeClient(
            {
                "viewer": {
                    "search": {
                        "edges": [
                            {
                                "node": {
                                    "title": "Example Feature",
                                    "type": "MOVIE",
                                    "movieId": 77123,
                                    "movie": {
                                        "movieId": 77123,
                                        "name": "Example Feature",
                                        "slug": "example-feature-77123",
                                        "releaseDateUtc": "2026-07-17T00:00:00Z",
                                    },
                                }
                            }
                        ]
                    }
                }
            }
        )

        self.assertTrue(worker.run_once())

        self.assertEqual(
            worker.client.calls, [(MOVIE_SEARCH_QUERY, {"query": "Example Feature"})]
        )
        self.assertEqual(store.gate_calls, 1)
        self.assertEqual(store.catalog_resolutions, [])
        self.assertEqual(store.completed_catalog[0][1][0]["slug"], "example-feature-77123")

    def test_format_catalog_resolves_cache_without_calling_amc(self):
        query = {
            "theatre_ids": ["example-theatre"],
            "movie_ids": ["example-feature"],
        }
        store = FakeStore([catalog_job("formats", query)])
        store.cached_catalog["formats"] = [
            {"id": "imax-70mm", "label": "IMAX 70MM", "detail": ""}
        ]
        worker = self.worker(store)
        worker.client = Mock()

        self.assertTrue(worker.run_once())

        worker.client.query.assert_not_called()
        self.assertEqual(store.gate_calls, 0)
        self.assertEqual(store.catalog_resolutions, [("formats", query)])
        self.assertEqual(store.completed_catalog[0][1], store.cached_catalog["formats"])


if __name__ == "__main__":
    unittest.main()
