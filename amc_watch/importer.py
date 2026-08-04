"""Idempotent import of the v1 and v2 file-backed monitor formats."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class ImportedSubscription:
    external_id: str
    label: str
    zip_code: str
    theatres: tuple[Mapping[str, Any], ...]
    movies: tuple[Mapping[str, Any], ...]
    format_codes: tuple[str, ...]
    days_ahead: int
    weekday_hours: tuple[float, float]
    weekend_hours: tuple[float, float]
    seat_count: int
    seat_preset: str
    guild_id: int | None = None
    destination_channel_id: int | None = None


@dataclass(frozen=True)
class ImportBundle:
    schema_version: int
    subscriptions: tuple[ImportedSubscription, ...]
    showtimes: tuple[Mapping[str, Any], ...] = ()
    alert_edges: Mapping[str, Mapping[str, frozenset[str]]] = field(default_factory=dict)
    cooldown_until: str | None = None
    selectable_dates: Mapping[str, frozenset[str]] = field(default_factory=dict)
    selectable_dates_initialized: frozenset[str] = frozenset()
    source_files: tuple[str, ...] = ()


class ImportStore(Protocol):
    def import_legacy_bundle(self, bundle: ImportBundle, *, dry_run: bool) -> Mapping[str, int]: ...


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return value


def _movie_id_from_slug(slug: str | None) -> int | None:
    match = re.search(r"-(\d+)$", str(slug or ""))
    return int(match.group(1)) if match else None


def _v1_subscription(config: Mapping[str, Any]) -> ImportedSubscription:
    future = {str(movie.get("slug")): movie for movie in config.get("future_movies") or []}
    movies: list[dict[str, Any]] = []
    for name in config.get("movie_contains") or []:
        normalized_name = re.sub(r"[^a-z0-9]+", " ", str(name).casefold()).strip()
        matching = next(
            (
                movie
                for movie in future.values()
                if normalized_name
                in re.sub(r"[^a-z0-9]+", " ", str(movie.get("slug")).casefold()).strip()
            ),
            {},
        )
        slug = matching.get("slug")
        movies.append(
            {
                "movie_id": _movie_id_from_slug(slug),
                "name": str(name),
                "slug": slug,
                "not_before": matching.get("not_before"),
                "not_after": matching.get("not_after"),
            }
        )
    return ImportedSubscription(
        external_id="example-theatre-imax-70mm",
        label="Imported example monitor",
        zip_code=str(config.get("zip_code") or "00000"),
        theatres=(
            {
                "slug": str(config.get("theatre_slug") or "amc-example-8"),
                "name": str(config.get("theatre_name") or "AMC Example 8"),
            },
        ),
        movies=tuple(movies),
        format_codes=tuple(str(value) for value in config.get("format_codes") or ["imax70mm"]),
        days_ahead=int(config.get("days_ahead", 14)),
        weekday_hours=tuple(float(value) for value in config.get("weekday_hours") or [17, 23]),
        weekend_hours=tuple(float(value) for value in config.get("weekend_hours") or [10, 23]),
        seat_count=int(config.get("tickets", 2)),
        seat_preset="center-back",
    )


def _v2_subscriptions(config: Mapping[str, Any]) -> tuple[ImportedSubscription, ...]:
    result: list[ImportedSubscription] = []
    for rule in config.get("subscriptions") or []:
        profile = rule.get("seat_profile") or {}
        result.append(
            ImportedSubscription(
                external_id=str(rule["id"]),
                label=str(rule.get("label") or rule["id"]),
                zip_code=str(rule["zip_code"]),
                theatres=tuple(dict(value) for value in rule.get("theatres") or []),
                movies=tuple(dict(value) for value in rule.get("movies") or []),
                format_codes=tuple(str(value) for value in rule.get("format_codes") or []),
                days_ahead=int(rule.get("days_ahead", 14)),
                weekday_hours=tuple(float(value) for value in rule.get("weekday_hours") or [17, 23]),
                weekend_hours=tuple(float(value) for value in rule.get("weekend_hours") or [10, 23]),
                seat_count=int(rule.get("tickets", 2)),
                seat_preset=str(profile.get("preset") or "center-back"),
            )
        )
    return tuple(result)


def _alert_edges(
    state: Mapping[str, Any], subscriptions: tuple[ImportedSubscription, ...], schema_version: int
) -> dict[str, dict[str, frozenset[str]]]:
    notified = state.get("notified") or {}
    output: dict[str, dict[str, frozenset[str]]] = {}
    if schema_version == 2:
        for subscription in subscriptions:
            by_showtime = notified.get(subscription.external_id) or {}
            output[subscription.external_id] = {
                str(showtime_id): frozenset(str(value) for value in signatures)
                for showtime_id, signatures in by_showtime.items()
            }
    elif subscriptions:
        output[subscriptions[0].external_id] = {
            str(showtime_id): frozenset(str(value) for value in signatures)
            for showtime_id, signatures in notified.items()
        }
    return output


def load_legacy_bundle(root: Path) -> ImportBundle:
    config_path = root / "config.json"
    subscriptions_path = root / "subscriptions.json"
    state_candidates = (root / "data" / "state.json", root / "state.json")
    state_path = next((path for path in state_candidates if path.exists()), None)
    state = _read_object(state_path) if state_path else {}

    if subscriptions_path.exists():
        subscriptions_config = _read_object(subscriptions_path)
        if int(subscriptions_config.get("version", 0)) != 2:
            raise ValueError("subscriptions.json is not schema version 2")
        schema_version = 2
        subscriptions = _v2_subscriptions(subscriptions_config)
        source_files = [str(subscriptions_path)]
    else:
        schema_version = 1
        subscriptions = (_v1_subscription(_read_object(config_path)),)
        source_files = [str(config_path)]
    if state_path:
        source_files.append(str(state_path))

    service = state.get("service") or {}
    schedule = state.get("schedule") or {}
    date_state: dict[str, frozenset[str]] = {
        str(slug): frozenset(str(value) for value in values)
        for slug, values in (schedule.get("known_selectable_dates_by_movie") or {}).items()
    }
    initialized = frozenset(
        str(value) for value in schedule.get("selectable_dates_initialized") or ()
    )
    raw_dates = schedule.get("known_selectable_dates") or []
    future_slugs = [
        str(movie.get("slug"))
        for subscription in subscriptions
        for movie in subscription.movies
        if movie.get("slug")
    ]
    if not date_state and len(set(future_slugs)) == 1 and raw_dates:
        date_state[future_slugs[0]] = frozenset(str(value) for value in raw_dates)
        initialized = frozenset({future_slugs[0]})

    # Latest-main keeps the per-showtime seat timestamp in both places. Older
    # snapshots may have only schedule.seat_checked_at, so normalize it onto the
    # showtime without mutating or exposing the source state.
    seat_checked_at = schedule.get("seat_checked_at") or {}
    showtimes: list[dict[str, Any]] = []
    for raw_showtime in state.get("showtimes") or []:
        showtime = dict(raw_showtime)
        showtime_id = str(showtime.get("showtime_id") or "")
        if not showtime.get("last_checked_at") and showtime_id:
            checked_at = seat_checked_at.get(showtime_id)
            if checked_at:
                showtime["last_checked_at"] = str(checked_at)
        showtimes.append(showtime)

    return ImportBundle(
        schema_version=schema_version,
        subscriptions=subscriptions,
        showtimes=tuple(showtimes),
        alert_edges=_alert_edges(state, subscriptions, schema_version),
        cooldown_until=service.get("cooldown_until"),
        selectable_dates=date_state,
        selectable_dates_initialized=initialized or frozenset(date_state),
        source_files=tuple(source_files),
    )


def bind_bundle(
    bundle: ImportBundle, *, guild_id: int, destination_channel_id: int
) -> ImportBundle:
    return ImportBundle(
        schema_version=bundle.schema_version,
        subscriptions=tuple(
            ImportedSubscription(
                **{
                    **subscription.__dict__,
                    "guild_id": guild_id,
                    "destination_channel_id": destination_channel_id,
                }
            )
            for subscription in bundle.subscriptions
        ),
        showtimes=bundle.showtimes,
        alert_edges=bundle.alert_edges,
        cooldown_until=bundle.cooldown_until,
        selectable_dates=bundle.selectable_dates,
        selectable_dates_initialized=bundle.selectable_dates_initialized,
        source_files=bundle.source_files,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--guild-id", type=int)
    parser.add_argument("--channel-id", type=int)
    parser.add_argument("--json", action="store_true", help="print a secret-free import summary")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the idempotent import using AMC_DATABASE_URL",
    )
    args = parser.parse_args()
    bundle = load_legacy_bundle(args.root)
    if bool(args.guild_id) != bool(args.channel_id):
        parser.error("--guild-id and --channel-id must be supplied together")
    if args.guild_id:
        bundle = bind_bundle(bundle, guild_id=args.guild_id, destination_channel_id=args.channel_id)
    summary: Mapping[str, Any] = {
        "schema_version": bundle.schema_version,
        "subscriptions": [value.external_id for value in bundle.subscriptions],
        "showtimes": len(bundle.showtimes),
        "alert_edges": sum(len(value) for value in bundle.alert_edges.values()),
        "bound": all(value.guild_id is not None for value in bundle.subscriptions),
        "source_files": list(bundle.source_files),
    }
    if args.apply:
        if not args.guild_id:
            parser.error("--apply requires --guild-id and --channel-id")
        database_url = (
            os.environ.get("AMC_DATABASE_URL")
            or os.environ.get("DATABASE_URL")
            or ""
        )
        if not database_url:
            parser.error("AMC_DATABASE_URL is required with --apply")
        from amc_watch.db.worker_repository import SqlAlchemyWorkerRepository

        summary = {
            **summary,
            "imported": dict(
                SqlAlchemyWorkerRepository.from_url(database_url).import_legacy_bundle(
                    bundle, dry_run=False
                )
            ),
        }
    print(json.dumps(summary, indent=2) if args.json else summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
