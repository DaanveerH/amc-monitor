from __future__ import annotations

import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from amc_watch.owner_delivery import (
    DeliveryDisposition,
    DeliveryResult,
    DiscordOwnerWebhook,
    sanitize_owner_text,
)
from amc_watch.owner_notifier import (
    Heartbeat,
    IncidentLedger,
    OwnerNotifier,
    PsycopgIncidentBridge,
    _record_deployment_phase,
    psycopg_connection_url,
)


class FakeDelivery:
    def __init__(self, *results: DeliveryResult) -> None:
        self.results = list(results)
        self.messages: list[str] = []

    def send(self, content: str) -> DeliveryResult:
        self.messages.append(content)
        if self.results:
            return self.results.pop(0)
        return DeliveryResult(DeliveryDisposition.DELIVERED)


class FakeResponse:
    def __init__(self, status: int = 204) -> None:
        self.status = status

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class RecordingOpener:
    def __init__(self, response: object) -> None:
        self.response = response
        self.request = None

    def open(self, request: object, timeout: float) -> object:
        self.request = request
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class OwnerDeliveryTests(unittest.TestCase):
    def test_payload_redacts_urls_and_disables_all_mentions(self) -> None:
        opener = RecordingOpener(FakeResponse())
        delivery = DiscordOwnerWebhook(
            "https://discord.com/api/webhooks/123/abc", opener=opener
        )
        result = delivery.send(
            "@everyone proxy https://user:pass@proxy.example:1234 "
            "token=super-secret"
        )
        self.assertEqual(result.disposition, DeliveryDisposition.DELIVERED)
        payload = json.loads(opener.request.data)
        self.assertNotIn("super-secret", payload["content"])
        self.assertNotIn("proxy.example", payload["content"])
        self.assertNotIn("@everyone", payload["content"])
        self.assertEqual(
            payload["allowed_mentions"],
            {"parse": [], "users": [], "roles": [], "replied_user": False},
        )

    def test_discord_429_uses_retry_after_without_leaking_body(self) -> None:
        error = urllib.error.HTTPError(
            "https://discord.com/api/webhooks/redacted",
            429,
            "rate limited",
            {"Content-Type": "application/json"},
            io.BytesIO(b'{"retry_after": 7.5, "secret": "do-not-store"}'),
        )
        delivery = DiscordOwnerWebhook(
            "https://discord.com/api/webhooks/123/abc",
            opener=RecordingOpener(error),
        )
        result = delivery.send("test")
        self.assertEqual(result.disposition, DeliveryDisposition.RETRY)
        self.assertEqual(result.retry_after_seconds, 7.5)
        self.assertEqual(result.reason, "discord_http_429")
        self.assertNotIn("do-not-store", result.reason)

    def test_permanent_and_transient_http_statuses_are_classified(self) -> None:
        permanent = urllib.error.HTTPError(
            "https://discord.com/api/webhooks/redacted",
            404,
            "not found",
            {},
            io.BytesIO(b"ignored"),
        )
        transient = urllib.error.HTTPError(
            "https://discord.com/api/webhooks/redacted",
            503,
            "unavailable",
            {"Retry-After": "4"},
            io.BytesIO(b"ignored"),
        )
        one = DiscordOwnerWebhook(
            "https://discord.com/api/webhooks/123/abc",
            opener=RecordingOpener(permanent),
        ).send("one")
        two = DiscordOwnerWebhook(
            "https://discord.com/api/webhooks/123/abc",
            opener=RecordingOpener(transient),
        ).send("two")
        self.assertEqual(one.disposition, DeliveryDisposition.PERMANENT)
        self.assertEqual(two.disposition, DeliveryDisposition.RETRY)
        self.assertEqual(two.retry_after_seconds, 4)

    def test_only_discord_https_webhooks_are_accepted(self) -> None:
        for value in (
            "http://discord.com/api/webhooks/1/a",
            "https://example.com/api/webhooks/1/a",
            "https://discord.com/not-a-webhook",
            "https://user:pass@discord.com/api/webhooks/1/a",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                DiscordOwnerWebhook(value)

    def test_sanitizer_bounds_content_and_removes_controls(self) -> None:
        value = sanitize_owner_text("ok\x00 " + "x" * 2_000, limit=100)
        self.assertNotIn("\x00", value)
        self.assertEqual(len(value), 100)
        self.assertTrue(value.endswith("…"))


class IncidentLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "owner.sqlite3"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def ledger(self, **kwargs: object) -> IncidentLedger:
        return IncidentLedger(self.path, **kwargs)

    def test_ledger_is_mode_0600(self) -> None:
        self.ledger()
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(mode, 0o600)

    def test_incident_dedupes_reminds_and_recovers_once(self) -> None:
        ledger = self.ledger(reminder_seconds=60, hourly_delivery_cap=10)
        delivery = FakeDelivery()
        self.assertTrue(
            ledger.report(
                "worker.database",
                severity="critical",
                source="amc-worker",
                summary="Database unavailable",
                now=1_000,
            )
        )
        self.assertFalse(
            ledger.report(
                "worker.database",
                severity="critical",
                source="amc-worker",
                summary="Database still unavailable",
                now=1_010,
            )
        )
        self.assertEqual(ledger.drain(delivery, now=1_010).delivered, 1)
        self.assertEqual(len(delivery.messages), 1)
        self.assertEqual(ledger.enqueue_due_reminders(now=1_069), 0)
        self.assertEqual(ledger.enqueue_due_reminders(now=1_071), 1)
        self.assertEqual(ledger.drain(delivery, now=1_071).delivered, 1)
        self.assertEqual(ledger.enqueue_due_reminders(now=1_072), 0)
        self.assertTrue(ledger.resolve("worker.database", now=1_080))
        self.assertFalse(ledger.resolve("worker.database", now=1_081))
        self.assertEqual(ledger.drain(delivery, now=1_080).delivered, 1)
        self.assertEqual(len(delivery.messages), 3)
        self.assertIn("incident recovered", delivery.messages[-1])
        state = ledger.incident("worker.database")
        self.assertEqual(state["state"], "resolved")
        self.assertEqual(state["occurrence_count"], 2)

    def test_warning_incident_never_enqueues_reminders(self) -> None:
        ledger = self.ledger(reminder_seconds=60)
        delivery = FakeDelivery()
        ledger.report(
            "worker.degraded",
            severity="warning",
            source="amc-worker",
            summary="Worker is degraded",
            now=100,
        )
        self.assertEqual(ledger.drain(delivery, now=100).delivered, 1)
        self.assertEqual(ledger.enqueue_due_reminders(now=10_000), 0)
        ledger.resolve("worker.degraded", now=10_001)
        self.assertEqual(ledger.drain(delivery, now=10_001).delivered, 1)
        self.assertEqual(len(delivery.messages), 2)

    def test_deployment_phase_incidents_use_fixed_content_and_preserve_edges(self) -> None:
        ledger = self.ledger()
        self.assertTrue(
            _record_deployment_phase(
                ledger, phase="migration", state="failed", now=100
            )
        )
        self.assertTrue(
            _record_deployment_phase(
                ledger, phase="migration", state="recovered", now=101
            )
        )
        delivery = FakeDelivery()
        self.assertEqual(ledger.drain(delivery, now=101).delivered, 2)
        rendered = "\n".join(delivery.messages)
        self.assertIn("Database migration failed", rendered)
        self.assertIn("Database migration recovered", rendered)
        self.assertEqual(ledger.incident("deployment.migration")["state"], "resolved")
        with self.assertRaises(ValueError):
            _record_deployment_phase(
                ledger, phase="https://secret.example", state="failed"
            )

    def test_resolution_before_opening_delivery_is_silent(self) -> None:
        ledger = self.ledger()
        ledger.report(
            "short.outage",
            severity="warning",
            source="amc-worker",
            summary="Short outage",
            now=100,
        )
        ledger.resolve("short.outage", now=101)
        delivery = FakeDelivery()
        self.assertEqual(ledger.drain(delivery, now=102).delivered, 0)
        self.assertEqual(delivery.messages, [])

    def test_reopened_incident_gets_a_new_opening(self) -> None:
        ledger = self.ledger()
        delivery = FakeDelivery()
        ledger.report(
            "proxy.exhausted",
            severity="critical",
            source="amc-worker",
            summary="All proxy endpoints failed",
            now=100,
        )
        ledger.drain(delivery, now=100)
        ledger.resolve("proxy.exhausted", now=110)
        ledger.drain(delivery, now=110)
        ledger.report(
            "proxy.exhausted",
            severity="critical",
            source="amc-worker",
            summary="All proxy endpoints failed",
            now=200,
        )
        ledger.drain(delivery, now=200)
        self.assertEqual(len(delivery.messages), 3)
        self.assertEqual(ledger.incident("proxy.exhausted")["cycle"], 2)

    def test_hourly_cap_is_enforced_across_restarts(self) -> None:
        ledger = self.ledger(hourly_delivery_cap=2)
        for _ in range(3):
            ledger.queue_test_message(now=100)
        delivery = FakeDelivery()
        first = ledger.drain(delivery, now=100)
        self.assertEqual(first.delivered, 2)
        self.assertTrue(first.hourly_limited)
        restarted = self.ledger(hourly_delivery_cap=2)
        blocked = restarted.drain(delivery, now=3_699)
        self.assertEqual(blocked.delivered, 0)
        self.assertTrue(blocked.hourly_limited)
        allowed = restarted.drain(delivery, now=3_701)
        self.assertEqual(allowed.delivered, 1)

    def test_retry_is_scheduled_and_permanent_failure_is_cancelled(self) -> None:
        ledger = self.ledger()
        ledger.queue_test_message(now=100)
        delivery = FakeDelivery(
            DeliveryResult(DeliveryDisposition.RETRY, 7, "discord_http_429"),
            DeliveryResult(DeliveryDisposition.DELIVERED),
        )
        self.assertEqual(ledger.drain(delivery, now=100).retry_scheduled, 1)
        self.assertEqual(ledger.drain(delivery, now=106).delivered, 0)
        self.assertEqual(ledger.drain(delivery, now=107).delivered, 1)

        ledger.queue_test_message(now=200)
        failed = ledger.drain(
            FakeDelivery(
                DeliveryResult(DeliveryDisposition.PERMANENT, reason="discord_http_404")
            ),
            now=200,
        )
        self.assertEqual(failed.permanently_failed, 1)
        self.assertEqual(ledger.drain(FakeDelivery(), now=300).delivered, 0)

    def test_stored_incident_text_is_sanitized(self) -> None:
        ledger = self.ledger()
        ledger.report(
            "secrets.redacted",
            severity="critical",
            source="amc-worker",
            summary="Failed at https://user:pass@proxy.example/path",
            details="password=hunter2 @everyone",
            now=100,
        )
        delivery = FakeDelivery()
        ledger.drain(delivery, now=100)
        rendered = delivery.messages[0]
        self.assertNotIn("hunter2", rendered)
        self.assertNotIn("proxy.example", rendered)
        self.assertNotIn("@everyone", rendered)

    def test_database_open_and_recovery_events_survive_delayed_bridge_sync(self) -> None:
        ledger = self.ledger()
        bridge = PsycopgIncidentBridge("postgresql://unused")

        bridge._apply_event(
            ledger,
            event_type="opened",
            incident_key="prod:amc-worker:upstream",
            incident_type="amc_upstream_blocked",
            payload={
                "severity": "critical",
                "resource": "amc-worker",
                "summary": "AMC access entered cooldown",
            },
            event_at=100,
        )
        bridge._apply_event(
            ledger,
            event_type="recovery",
            incident_key="prod:amc-worker:upstream",
            incident_type="amc_upstream_blocked",
            payload={
                "severity": "critical",
                "summary": "AMC access recovered",
            },
            event_at=160,
        )

        delivery = FakeDelivery()
        report = ledger.drain(delivery, now=160)
        self.assertEqual(report.delivered, 2)
        self.assertEqual(len(delivery.messages), 2)
        self.assertIn("AMC access entered cooldown", delivery.messages[0])
        self.assertIn("incident recovered", delivery.messages[1])
        self.assertIn("AMC access recovered", delivery.messages[1])
        self.assertEqual(
            ledger.incident("prod:amc-worker:upstream")["state"], "resolved"
        )

    def test_database_event_type_wins_over_later_incident_state(self) -> None:
        ledger = self.ledger()
        bridge = PsycopgIncidentBridge("postgresql://unused")

        bridge._apply_event(
            ledger,
            event_type="opened",
            incident_key="prod:amc-worker:proxy",
            incident_type="proxy_capacity_exhausted",
            payload={
                "severity": "warning",
                "resource": "amc-worker",
                "summary": "All proxy endpoints failed",
            },
            event_at=200,
        )

        state = ledger.incident("prod:amc-worker:proxy")
        self.assertEqual(state["state"], "open")
        delivery = FakeDelivery()
        self.assertEqual(ledger.drain(delivery, now=200).delivered, 1)
        self.assertIn("All proxy endpoints failed", delivery.messages[0])

    def test_database_acknowledge_and_reject_are_claimant_bound(self) -> None:
        bridge = PsycopgIncidentBridge(
            "postgresql://unused", claimant="notifier-instance"
        )
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value

        bridge._acknowledge(connection, 41, mock.sentinel.now)
        acknowledge_sql, acknowledge_params = cursor.execute.call_args.args
        self.assertIn("claimed_by = %s", acknowledge_sql)
        self.assertEqual(
            acknowledge_params,
            (mock.sentinel.now, 41, "notifier-instance"),
        )

        cursor.execute.reset_mock()
        bridge._reject(connection, 42, "invalid_incident")
        reject_sql, reject_params = cursor.execute.call_args.args
        self.assertIn("claimed_by = %s", reject_sql)
        self.assertEqual(
            reject_params,
            ("invalid_incident", 42, "notifier-instance"),
        )


class FakeHeartbeatSource:
    def __init__(self, heartbeats: object) -> None:
        self.heartbeats = heartbeats

    def fetch(self, _services: object) -> object:
        if isinstance(self.heartbeats, BaseException):
            raise self.heartbeats
        return self.heartbeats


class OwnerHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.ledger = IncidentLedger(Path(self.tempdir.name) / "owner.sqlite3")
        self.delivery = FakeDelivery()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def notifier(self, source: object) -> OwnerNotifier:
        return OwnerNotifier(
            self.ledger,
            self.delivery,  # type: ignore[arg-type]
            heartbeat_source=source,  # type: ignore[arg-type]
            expected_services=("amc-worker", "amc-discord-bot"),
            stale_after_seconds=60,
        )

    def test_missing_stale_and_unhealthy_heartbeats_create_incidents(self) -> None:
        subject = self.notifier(
            FakeHeartbeatSource(
                [Heartbeat("amc-worker", 900, "degraded")]
            )
        )
        subject.run_once(now=1_000)
        keys = (
            "heartbeat.stale:amc-worker",
            "heartbeat.unhealthy:amc-worker",
            "heartbeat.missing:amc-discord-bot",
        )
        for key in keys:
            self.assertEqual(self.ledger.incident(key)["state"], "open")
        self.assertEqual(len(self.delivery.messages), 3)

    def test_database_failure_alerts_locally_then_recovers(self) -> None:
        failing = self.notifier(FakeHeartbeatSource(RuntimeError("dsn=secret")))
        failing.run_once(now=1_000)
        self.assertEqual(self.ledger.incident("database.unavailable")["state"], "open")
        self.assertNotIn("dsn=secret", "\n".join(self.delivery.messages))

        healthy = self.notifier(
            FakeHeartbeatSource(
                [
                    Heartbeat("amc-worker", 1_010, "healthy"),
                    Heartbeat("amc-discord-bot", 1_010, "running"),
                ]
            )
        )
        healthy.run_once(now=1_010)
        self.assertEqual(
            self.ledger.incident("database.unavailable")["state"], "resolved"
        )
        self.assertTrue(any("incident recovered" in item for item in self.delivery.messages))

    def test_sqlalchemy_driver_url_is_normalized_for_direct_psycopg(self) -> None:
        value = (
            "postgresql+psycopg://user:password@db.example/amc"
            "?sslmode=verify-full&sslrootcert=%2Frun%2Fca.crt"
        )
        normalized = psycopg_connection_url(value)
        self.assertTrue(normalized.startswith("postgresql://"))
        self.assertNotIn("+psycopg", normalized)
        self.assertIn("sslmode=verify-full", normalized)


class DeploymentTLSWrapperTests(unittest.TestCase):
    def test_database_ca_is_private_and_removed_before_exec(self) -> None:
        wrapper = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "exec-with-db-ca.py"
        )
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary)
            environment = os.environ.copy()
            environment.update(
                {
                    "RUNTIME_DIRECTORY": str(runtime),
                    "AMC_DATABASE_CA_CERT": (
                        "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n"
                        "-----END CERTIFICATE-----"
                    ),
                    "AMC_DATABASE_URL": (
                        "postgresql+psycopg://user:password@db.example/amc"
                        "?sslmode=require"
                    ),
                }
            )
            assertion_program = """
import os
import stat
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
assert 'AMC_DATABASE_CA_CERT' not in os.environ
assert os.environ['PGSSLMODE'] == 'verify-full'
certificate = Path(os.environ['PGSSLROOTCERT'])
assert stat.S_IMODE(certificate.stat().st_mode) == 0o600
query = parse_qs(urlsplit(os.environ['AMC_DATABASE_URL']).query)
assert urlsplit(os.environ['AMC_DATABASE_URL']).scheme == 'postgresql+psycopg'
assert query['sslmode'] == ['verify-full']
assert query['sslrootcert'] == [str(certificate)]
"""
            subprocess.run(
                [
                    sys.executable,
                    str(wrapper),
                    sys.executable,
                    "-c",
                    assertion_program,
                ],
                check=True,
                env=environment,
                capture_output=True,
                text=True,
            )


if __name__ == "__main__":
    unittest.main()
