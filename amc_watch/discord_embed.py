"""Pure seat ranking/cropping plus Discord alert presentation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

import discord

from .discord_models import RecommendedRun, Seat, SeatPreset, UserAlert


AMC_HOME_URL = "https://www.amctheatres.com/"


@dataclass(frozen=True, slots=True)
class PresetZone:
    min_row_fraction: float
    max_row_fraction: float
    center_width: float
    ideal_row_fraction: float


PRESET_ZONES: Mapping[SeatPreset, PresetZone] = {
    SeatPreset.CENTER: PresetZone(0.25, 0.88, 0.30, 0.58),
    SeatPreset.CENTER_BACK: PresetZone(0.25, 0.88, 0.30, 0.68),
    SeatPreset.CENTER_FRONT: PresetZone(0.18, 0.72, 0.30, 0.45),
}


def _seat(value: Seat | Mapping[str, Any]) -> Seat:
    if isinstance(value, Seat):
        return value
    return Seat(
        row=int(value.get("row", value.get("r", 0))),
        column=int(value.get("column", value.get("c", 0))),
        name=str(value.get("name", value.get("n", "")) or ""),
        available=bool(value.get("available", value.get("s") == "available")),
        type=str(value.get("type") or "CanReserve"),
        should_display=value.get("shouldDisplay", value.get("should_display", True)) is not False,
    )


def _displayable(seat: Seat) -> bool:
    return bool(seat.name and seat.should_display and seat.type != "NotASeat")


def _reservable(seat: Seat) -> bool:
    return bool(_displayable(seat) and seat.available and seat.type == "CanReserve")


def rank_adjacent_runs(
    seats: Iterable[Seat | Mapping[str, Any]],
    count: int,
    preset: SeatPreset | str,
) -> list[RecommendedRun]:
    """Rank 1-6 contiguous normal seats inside the chosen comfort zone."""

    if not 1 <= count <= 6:
        raise ValueError("Adjacent seat count must be between 1 and 6")
    preset = SeatPreset(preset)
    zone = PRESET_ZONES[preset]
    all_seats = [_seat(value) for value in seats]
    layout = [seat for seat in all_seats if _displayable(seat)]
    if not layout:
        return []
    rows = sorted({seat.row for seat in layout})
    row_index = {row: index for index, row in enumerate(rows)}
    available = {(seat.row, seat.column): seat for seat in all_seats if _reservable(seat)}
    results: list[RecommendedRun] = []

    for row in rows:
        row_layout = [seat for seat in layout if seat.row == row]
        min_col = min(seat.column for seat in row_layout)
        max_col = max(seat.column for seat in row_layout)
        span = max(max_col - min_col, 1)
        row_fraction = row_index[row] / max(len(rows) - 1, 1)
        if not zone.min_row_fraction <= row_fraction <= zone.max_row_fraction:
            continue
        for first_col in range(min_col, max_col - count + 2):
            run = tuple(available.get((row, first_col + offset)) for offset in range(count))
            if any(seat is None for seat in run):
                continue
            seats_in_run = tuple(seat for seat in run if seat is not None)
            run_center = first_col + (count - 1) / 2
            row_center = (min_col + max_col) / 2
            center_offset = abs(run_center - row_center) / max(span / 2, 1)
            # center_width is the full preferred band.  Offset is normalized to
            # half-row width, so the same numeric threshold gives a 30% band.
            if center_offset > zone.center_width:
                continue
            vertical_offset = abs(row_fraction - zone.ideal_row_fraction)
            score = round(max(0, 100 - center_offset * 55 - vertical_offset * 45))
            results.append(
                RecommendedRun(
                    names=tuple(seat.name for seat in seats_in_run),
                    coordinates=tuple((seat.row, seat.column) for seat in seats_in_run),
                    score=score,
                )
            )
    return sorted(results, key=lambda run: (-run.score, run.names))


def render_mobile_seat_map(
    seats: Iterable[Seat | Mapping[str, Any]],
    highlighted: Iterable[tuple[int, int]],
    preset: SeatPreset | str,
    *,
    max_columns: int = 22,
) -> str:
    """Render only eligible rows and the preferred center window for phones."""

    preset = SeatPreset(preset)
    zone = PRESET_ZONES[preset]
    all_seats = [_seat(value) for value in seats]
    layout = [seat for seat in all_seats if _displayable(seat)]
    if not layout:
        return ""
    rows = sorted({seat.row for seat in layout})
    row_index = {row: index for index, row in enumerate(rows)}
    selected_rows = [
        row
        for row in rows
        if zone.min_row_fraction
        <= row_index[row] / max(len(rows) - 1, 1)
        <= zone.max_row_fraction
    ]
    if not selected_rows:
        return ""

    all_columns = sorted({seat.column for seat in layout})
    min_col, max_col = all_columns[0], all_columns[-1]
    center = (min_col + max_col) / 2
    desired_count = max(1, math.ceil((max_col - min_col + 1) * zone.center_width))
    column_count = min(max_columns, desired_count)
    first_col = math.ceil(center - (column_count - 1) / 2)
    last_col = first_col + column_count - 1
    if first_col < min_col:
        first_col, last_col = min_col, min_col + column_count - 1
    if last_col > max_col:
        first_col, last_col = max_col - column_count + 1, max_col
    columns = list(range(first_col, last_col + 1))

    by_coord = {(seat.row, seat.column): seat for seat in all_seats}
    highlighted_set = set(highlighted)
    output = [f"    {'── SCREEN ──':^{len(columns)}}"]
    for row in selected_rows:
        row_seats = [seat for seat in layout if seat.row == row]
        label = re.sub(r"[^A-Za-z]", "", row_seats[0].name)[:3] or "?"
        symbols: list[str] = []
        for column in columns:
            seat = by_coord.get((row, column))
            if seat is None or not _displayable(seat):
                symbols.append(" ")
            elif (row, column) in highlighted_set:
                symbols.append("◆")
            elif seat.type != "CanReserve":
                symbols.append("×")
            elif seat.available:
                symbols.append("○")
            else:
                symbols.append("·")
        output.append(f"{label:>3} {''.join(symbols)}")
    return "\n".join(output)


def safe_booking_url(value: str) -> str:
    """Allow only HTTPS AMC URLs in clickable buttons and embeds."""

    try:
        parsed = urlparse(value)
    except ValueError:
        return AMC_HOME_URL
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not (
        host == "amctheatres.com" or host.endswith(".amctheatres.com")
    ):
        return AMC_HOME_URL
    return value[:512]


def _clean(value: str, limit: int) -> str:
    value = discord.utils.escape_markdown(str(value), as_needed=True)
    value = discord.utils.escape_mentions(value)
    return value[:limit]


def build_alert_embed(alert: UserAlert) -> discord.Embed:
    best = alert.recommendations[0] if alert.recommendations else None
    highlighted = best.coordinates if best else ()
    seat_map = render_mobile_seat_map(alert.seats, highlighted, alert.preset)
    count = len(alert.recommendations)
    label = "qualifying option" if count == 1 else "qualifying options"
    lines = [f"**{count} {label} available**"]
    for index, run in enumerate(alert.recommendations[:3], start=1):
        names = " + ".join(_clean(name, 12) for name in run.names)
        lines.append(f"**#{index}** `{names}` · {run.score}/100")
    if seat_map:
        lines.extend(
            (
                "",
                f"**{alert.preset.value.replace('-', ' ').title()} view**",
                "_Front/back rows and far edges are hidden._",
                f"```\n{seat_map}\n```",
                "`◆ recommended` `○ open` `· taken` `× accessible`",
            )
        )
    if alert.is_test:
        lines.insert(0, "**TEST ALERT — no availability claim**")
    title_prefix = "Test: " if alert.is_test else "Great seats opened — "
    embed = discord.Embed(
        title=(title_prefix + _clean(alert.movie, 220))[:256],
        url=safe_booking_url(alert.booking_url),
        description="\n".join(lines)[:4096],
        color=discord.Color.from_rgb(216, 180, 91),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Showing", value=_clean(alert.when_local, 1024), inline=True)
    embed.add_field(name="Format", value=_clean(alert.format_name, 1024), inline=True)
    embed.add_field(name="Tickets", value=f"{alert.adjacent_seats} adjacent", inline=True)
    embed.set_footer(
        text=(f"{_clean(alert.theatre, 160)} · Purchase manually")[:2048]
    )
    return embed


class BookingLinkView(discord.ui.View):
    def __init__(self, booking_url: str):
        super().__init__(timeout=None)
        self.add_item(
            discord.ui.Button(
                label="Open AMC",
                style=discord.ButtonStyle.link,
                url=safe_booking_url(booking_url),
            )
        )


__all__ = [
    "AMC_HOME_URL",
    "BookingLinkView",
    "PRESET_ZONES",
    "PresetZone",
    "build_alert_embed",
    "rank_adjacent_runs",
    "render_mobile_seat_map",
    "safe_booking_url",
]
