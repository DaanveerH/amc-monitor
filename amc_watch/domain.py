"""Pure domain logic shared by the worker, Discord bot, and import tools."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SEAT_PRESETS: dict[str, dict[str, float]] = {
    "center": {
        "min_row_fraction": 0.25,
        "max_row_fraction": 0.88,
        "max_center_offset": 0.30,
        "ideal_row_fraction": 0.58,
    },
    "center-back": {
        "min_row_fraction": 0.25,
        "max_row_fraction": 0.88,
        "max_center_offset": 0.30,
        "ideal_row_fraction": 0.68,
    },
    "center-front": {
        "min_row_fraction": 0.18,
        "max_row_fraction": 0.72,
        "max_center_offset": 0.30,
        "ideal_row_fraction": 0.45,
    },
}

SELLABLE_STATUSES = frozenset({"SELLABLE", "ALMOSTSOLDOUT", "ALMOSTFULL"})
BOOK_URL = "https://www.amctheatres.com/showtimes/{showtime_id}/seats"


@dataclass(frozen=True)
class SeatRun:
    names: tuple[str, ...]
    coordinates: tuple[tuple[int, int], ...]
    score: int
    row_fraction: float
    center_offset: float

    @property
    def signature(self) -> str:
        return "+".join(self.names)

    def as_dict(self) -> dict[str, Any]:
        return {
            "names": list(self.names),
            "coordinates": [list(value) for value in self.coordinates],
            "score": self.score,
            "row_fraction": round(self.row_fraction, 3),
            "center_offset": round(self.center_offset, 3),
            "signature": self.signature,
        }


@dataclass(frozen=True)
class SubscriptionSpec:
    guild_id: int
    user_id: int
    destination_channel_id: int
    zip_code: str
    theatre_slugs: tuple[str, ...]
    movie_ids: tuple[int, ...]
    format_codes: tuple[str, ...]
    seat_count: int = 2
    seat_preset: str = "center-back"
    weekday_hours: tuple[float, float] = (17, 23)
    weekend_hours: tuple[float, float] = (10, 23)
    days_ahead: int = 14

    def validate(self) -> None:
        if not re.fullmatch(r"\d{5}", self.zip_code):
            raise ValueError("ZIP code must contain exactly five digits")
        if not 1 <= self.seat_count <= 6:
            raise ValueError("seat_count must be between 1 and 6")
        if self.seat_preset not in SEAT_PRESETS:
            raise ValueError(f"unknown seat preset: {self.seat_preset}")
        if not 1 <= self.days_ahead <= 31:
            raise ValueError("days_ahead must be between 1 and 31")
        if not 1 <= len(self.theatre_slugs) <= 3:
            raise ValueError("a subscription must select one to three theatres")
        if not 1 <= len(self.movie_ids) <= 5:
            raise ValueError("a subscription must select one to five movies")
        if not self.format_codes:
            raise ValueError("at least one format is required")
        for name, bounds in (
            ("weekday_hours", self.weekday_hours),
            ("weekend_hours", self.weekend_hours),
        ):
            start, end = bounds
            if not 0 <= start <= end <= 24:
                raise ValueError(f"invalid {name}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_status(value: Any) -> str:
    return "".join(character for character in str(value or "") if character.isalnum()).upper()


def became_sellable(previous: Any, current: Any) -> bool:
    prior = normalize_status(previous)
    new = normalize_status(current)
    return bool(prior) and prior not in SELLABLE_STATUSES and new in SELLABLE_STATUSES


def adaptive_status_interval(
    active_showtimes: int,
    *,
    batch_size: int = 8,
    request_gap_seconds: float = 3,
    request_share: float = 0.5,
    healthy_floor_seconds: float = 15,
    degraded: bool = False,
) -> float:
    """Return a capacity-safe cadence for one shared serialized request lane."""
    if active_showtimes < 0:
        raise ValueError("active_showtimes cannot be negative")
    if batch_size < 1 or request_gap_seconds <= 0 or not 0 < request_share <= 1:
        raise ValueError("invalid scheduler capacity settings")
    batches = max(1, math.ceil(active_showtimes / batch_size))
    capacity_interval = batches * request_gap_seconds / request_share
    floor = max(healthy_floor_seconds, 30 if degraded else healthy_floor_seconds)
    return max(floor, capacity_interval)


def projected_status_interval(
    active_showtime_ids: Iterable[str], candidate_showtime_ids: Iterable[str]
) -> float:
    return adaptive_status_interval(len(set(active_showtime_ids) | set(candidate_showtime_ids)))


def capacity_allows(
    active_showtime_ids: Iterable[str],
    candidate_showtime_ids: Iterable[str],
    *,
    ceiling_seconds: float = 60,
) -> bool:
    return projected_status_interval(active_showtime_ids, candidate_showtime_ids) <= ceiling_seconds


def seat_is_normal(seat: Mapping[str, Any]) -> bool:
    return bool(
        seat.get("name")
        and seat.get("available")
        and seat.get("type") == "CanReserve"
        and seat.get("shouldDisplay", True) is not False
    )


def rank_runs(
    seats: Iterable[Mapping[str, Any]],
    *,
    count: int,
    preset: str = "center-back",
) -> list[SeatRun]:
    """Rank adjacent reservable runs inside the selected comfortable-view zone."""
    if not 1 <= count <= 6:
        raise ValueError("count must be between 1 and 6")
    try:
        zone = SEAT_PRESETS[preset]
    except KeyError as exc:
        raise ValueError(f"unknown seat preset: {preset}") from exc

    all_seats = list(seats)
    display_seats = [
        seat
        for seat in all_seats
        if seat.get("name")
        and seat.get("type") == "CanReserve"
        and seat.get("shouldDisplay", True) is not False
    ]
    if not display_seats:
        return []

    rows = sorted({int(seat["row"]) for seat in display_seats})
    row_index = {row: index for index, row in enumerate(rows)}
    row_members: dict[int, list[Mapping[str, Any]]] = {
        row: [seat for seat in display_seats if int(seat["row"]) == row]
        for row in rows
    }
    available = {
        (int(seat["row"]), int(seat["column"])): seat
        for seat in all_seats
        if seat_is_normal(seat)
    }

    results: list[SeatRun] = []
    for row, column in sorted(available):
        coordinates = tuple((row, column + offset) for offset in range(count))
        members = [available.get(coordinate) for coordinate in coordinates]
        if any(member is None for member in members):
            continue

        minimum = min(int(seat["column"]) for seat in row_members[row])
        maximum = max(int(seat["column"]) for seat in row_members[row])
        span = max(maximum - minimum, 1)
        row_center = (minimum + maximum) / 2
        run_center = column + (count - 1) / 2
        center_offset = abs(run_center - row_center) / (span / 2)
        row_fraction = row_index[row] / max(len(rows) - 1, 1)
        if not zone["min_row_fraction"] <= row_fraction <= zone["max_row_fraction"]:
            continue
        if center_offset > zone["max_center_offset"]:
            continue

        vertical_offset = abs(row_fraction - zone["ideal_row_fraction"])
        score = round(max(0, 100 - center_offset * 55 - vertical_offset * 45))
        results.append(
            SeatRun(
                names=tuple(str(member["name"]) for member in members if member),
                coordinates=coordinates,
                score=score,
                row_fraction=row_fraction,
                center_offset=center_offset,
            )
        )
    return sorted(results, key=lambda run: (-run.score, run.names))


def compact_seats(
    seats: Iterable[Mapping[str, Any]], highlighted: Iterable[tuple[int, int]] = ()
) -> dict[str, Any]:
    source = list(seats)
    selected = set(highlighted)
    compact: list[dict[str, Any]] = []
    for seat in source:
        row, column = int(seat["row"]), int(seat["column"])
        seat_type = str(seat.get("type") or "")
        if seat_type == "NotASeat" or not seat.get("name") or seat.get("shouldDisplay", True) is False:
            status = "aisle"
        elif seat_type != "CanReserve":
            status = "accessible"
        elif (row, column) in selected:
            status = "recommended"
        elif seat.get("available"):
            status = "available"
        else:
            status = "taken"
        compact.append({"r": row, "c": column, "n": seat.get("name") or "", "s": status})
    return {
        "rows": max((int(seat["row"]) for seat in source), default=-1) + 1,
        "columns": max((int(seat["column"]) for seat in source), default=-1) + 1,
        "seats": compact,
    }


def mobile_seatmap(
    seatmap: Mapping[str, Any] | None,
    *,
    preset: str = "center-back",
    max_columns: int = 26,
) -> str:
    """Render only the preset's useful rows and centered columns for phones."""
    if not seatmap or preset not in SEAT_PRESETS:
        return ""
    seats = list(seatmap.get("seats") or [])
    if not seats:
        return ""
    all_rows = sorted({int(seat["r"]) for seat in seats})
    denominator = max(len(all_rows) - 1, 1)
    zone = SEAT_PRESETS[preset]
    rows = [
        row
        for index, row in enumerate(all_rows)
        if zone["min_row_fraction"] <= index / denominator <= zone["max_row_fraction"]
    ]
    usable_columns = sorted({int(seat["c"]) for seat in seats if seat.get("s") != "aisle"})
    if not rows or not usable_columns:
        return ""

    first, last = usable_columns[0], usable_columns[-1]
    row_center = (first + last) / 2
    half_zone = max(1, int((last - first + 1) * zone["max_center_offset"]))
    first = max(first, math.floor(row_center - half_zone))
    last = min(last, math.ceil(row_center + half_zone))
    if last - first + 1 > max_columns:
        first = round(row_center - (max_columns - 1) / 2)
        last = first + max_columns - 1

    by_coordinate = {(int(seat["r"]), int(seat["c"])): seat for seat in seats}
    columns = range(first, last + 1)
    output = [f"    {'── SCREEN ──':^{last - first + 1}}"]
    symbols = {
        "recommended": "◆",
        "available": "○",
        "taken": "·",
        "accessible": "×",
    }
    for row in rows:
        named = [seat for seat in seats if int(seat["r"]) == row and seat.get("n")]
        label = re.sub(r"[^A-Za-z]", "", str(named[0]["n"])) if named else "?"
        rendered = "".join(
            symbols.get(str((by_coordinate.get((row, column)) or {}).get("s")), " ")
            for column in columns
        )
        output.append(f"{(label or '?'):>2}  {rendered}")
    return "\n".join(output)


def fixed_offset(value: str | None) -> timezone:
    match = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", str(value or ""))
    if not match:
        return timezone.utc
    minutes = int(match.group(2)) * 60 + int(match.group(3))
    return timezone(timedelta(minutes=-minutes if match.group(1) == "-" else minutes))


def local_showtime(
    when_utc: datetime,
    utc_offset: str | None,
    timezone_name: str | None = None,
) -> datetime:
    if when_utc.tzinfo is None:
        when_utc = when_utc.replace(tzinfo=timezone.utc)
    if re.fullmatch(r"[+-]\d{2}:?\d{2}", str(utc_offset or "")):
        return when_utc.astimezone(fixed_offset(utc_offset))
    if timezone_name:
        try:
            return when_utc.astimezone(ZoneInfo(timezone_name))
        except ZoneInfoNotFoundError:
            pass
    return when_utc.astimezone(timezone.utc)


def in_time_window(local: datetime, weekday: Sequence[float], weekend: Sequence[float]) -> bool:
    start, end = weekend if local.weekday() >= 5 else weekday
    value = local.hour + local.minute / 60
    return float(start) <= value <= float(end)
