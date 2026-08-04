from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from amc_watch.importer import bind_bundle, load_legacy_bundle


class ImporterTests(unittest.TestCase):
    def write(self, path: Path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def test_imports_v1_without_creating_a_destination_binding(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root / "config.json",
                {
                    "theatre_slug": "amc-example-8",
                    "theatre_name": "AMC Example 8",
                    "zip_code": "00000",
                    "movie_contains": ["Example Feature", "Future Feature"],
                    "future_movies": [
                        {
                            "slug": "future-feature-200",
                            "not_before": "2026-12-17",
                            "not_after": "2027-01-17",
                        }
                    ],
                    "format_codes": ["imax70mm"],
                    "tickets": 2,
                },
            )
            self.write(
                root / "data" / "state.json",
                {
                    "service": {"cooldown_until": "2026-07-19T20:00:00Z"},
                    "showtimes": [{"showtime_id": "123"}],
                    "notified": {"123": ["H15+H16"]},
                    "schedule": {"known_selectable_dates": ["2026-12-18"]},
                },
            )
            bundle = load_legacy_bundle(root)
        self.assertEqual(bundle.schema_version, 1)
        self.assertEqual(bundle.subscriptions[0].seat_count, 2)
        self.assertIsNone(bundle.subscriptions[0].guild_id)
        self.assertEqual(bundle.alert_edges["example-theatre-imax-70mm"]["123"], {"H15+H16"})
        self.assertEqual(bundle.selectable_dates["future-feature-200"], {"2026-12-18"})

    def test_imports_v2_edges_per_subscription_and_binds_explicitly(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(root / "config.json", {})
            self.write(
                root / "subscriptions.json",
                {
                    "version": 2,
                    "subscriptions": [
                        {
                            "id": "example-theatre-imax-70mm",
                            "zip_code": "00000",
                            "theatres": [{"slug": "amc-example-8"}],
                            "movies": [{"movie_id": 100, "name": "Example Feature"}],
                            "format_codes": ["imax70mm"],
                            "weekday_hours": [17, 23],
                            "weekend_hours": [10, 23],
                            "tickets": 3,
                            "seat_profile": {"preset": "center"},
                        }
                    ],
                },
            )
            self.write(
                root / "data" / "state.json",
                {
                    "schema_version": 2,
                    "notified": {"example-theatre-imax-70mm": {"456": ["G14+G15+G16"]}},
                },
            )
            bundle = bind_bundle(load_legacy_bundle(root), guild_id=11, destination_channel_id=22)
        self.assertEqual(bundle.schema_version, 2)
        self.assertEqual(bundle.subscriptions[0].seat_count, 3)
        self.assertEqual(bundle.subscriptions[0].guild_id, 11)
        self.assertEqual(bundle.subscriptions[0].destination_channel_id, 22)

    def test_imports_latest_main_selectable_date_baselines_per_movie(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root / "config.json",
                {
                    "movie_contains": ["Example Feature", "Future Feature"],
                    "future_movies": [
                        {"slug": "example-feature-100"},
                        {"slug": "future-feature-200"},
                    ],
                },
            )
            self.write(
                root / "data" / "state.json",
                {
                    "schedule": {
                        "known_selectable_dates": ["legacy-flat-must-not-win"],
                        "known_selectable_dates_by_movie": {
                            "example-feature-100": ["2026-07-20"],
                            "future-feature-200": ["2026-12-18", "2026-12-19"],
                        },
                        "selectable_dates_initialized": [
                            "example-feature-100",
                            "future-feature-200",
                        ],
                    }
                },
            )
            bundle = load_legacy_bundle(root)

        self.assertEqual(
            bundle.selectable_dates,
            {
                "example-feature-100": {"2026-07-20"},
                "future-feature-200": {"2026-12-18", "2026-12-19"},
            },
        )
        self.assertEqual(
            bundle.selectable_dates_initialized,
            {"example-feature-100", "future-feature-200"},
        )

    def test_preserves_latest_main_rich_showtime_and_schedule_seat_timestamp(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root / "config.json",
                {
                    "movie_contains": ["Example Feature"],
                    "future_movies": [{"slug": "example-feature"}],
                },
            )
            self.write(
                root / "data" / "state.json",
                {
                    "schedule": {
                        "seat_checked_at": {
                            "synthetic-showtime-1": "2026-07-19T12:34:56Z"
                        }
                    },
                    "showtimes": [
                        {
                            "showtime_id": "synthetic-showtime-1",
                            "movie": "Example Feature",
                            "format": "IMAX 70MM",
                            "when_local": "Synthetic local time",
                            "when_utc": "2026-07-22T23:00:00Z",
                            "book_url": "https://example.invalid/booking",
                            "availability": "sold_out",
                            "available_count": 0,
                            "best_pairs": [],
                            "seatmap": {
                                "rows": 2,
                                "columns": 4,
                                "seats": [
                                    {"r": 0, "c": 0, "n": "A1", "s": "taken"},
                                    {"r": 1, "c": 1, "n": "B2", "s": "available"},
                                ],
                            },
                        }
                    ],
                },
            )
            bundle = load_legacy_bundle(root)

        showtime = bundle.showtimes[0]
        self.assertEqual(showtime["last_checked_at"], "2026-07-19T12:34:56Z")
        self.assertEqual(showtime["available_count"], 0)
        self.assertEqual(showtime["seatmap"]["seats"][1]["n"], "B2")


if __name__ == "__main__":
    unittest.main()
