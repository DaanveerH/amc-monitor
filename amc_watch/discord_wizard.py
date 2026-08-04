"""Persistent subscription wizard state machine."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import time
from typing import Any, Mapping, Sequence

from .discord_models import (
    CatalogOption,
    DiscordRepository,
    MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD,
    MAX_ACTIVE_SUBSCRIPTIONS_PER_USER,
    MAX_ADJACENT_SEATS,
    MAX_MOVIES_PER_SUBSCRIPTION,
    MAX_THEATRES_PER_SUBSCRIPTION,
    MIN_ADJACENT_SEATS,
    SeatPreset,
    SubscriptionDraft,
    SubscriptionSummary,
    WizardSession,
    WizardStep,
)


# Curated seat areas, shown as a dropdown instead of asking the user to type a
# preset name. Keyed by SeatPreset so the label stays in sync with the enum.
SEAT_PRESET_LABELS: dict[SeatPreset, str] = {
    SeatPreset.CENTER: "Center of the auditorium",
    SeatPreset.CENTER_BACK: "Center, toward the back",
    SeatPreset.CENTER_FRONT: "Center, toward the front",
}

# Common viewing windows offered as a dropdown; "Custom…" opens a modal for
# anything not covered here. Each entry is
# (key, label, (weekday_start, weekday_end, weekend_start, weekend_end)).
TIME_WINDOW_PRESETS: tuple[tuple[str, str, tuple[str, str, str, str]], ...] = (
    ("anytime", "Any showtime · all day", ("00:00", "23:59", "00:00", "23:59")),
    ("evenings", "Evenings · 5:00 PM–11:00 PM", ("17:00", "23:00", "17:00", "23:00")),
    (
        "evenings-matinees",
        "Evenings + weekend matinees",
        ("17:00", "23:00", "10:00", "23:00"),
    ),
    ("daytime", "Daytime · 10:00 AM–6:00 PM", ("10:00", "18:00", "10:00", "18:00")),
    ("late-night", "Late night · 8:00 PM–midnight", ("20:00", "23:59", "20:00", "23:59")),
)
_TIME_WINDOW_PRESETS_BY_KEY: dict[str, tuple[str, str, str, str]] = {
    key: values for key, _, values in TIME_WINDOW_PRESETS
}

# Linear order the wizard walks, enabling Back/Forward navigation. COMPLETE and
# CANCELLED are terminal and not part of the reversible flow.
STEP_ORDER: tuple[WizardStep, ...] = (
    WizardStep.ZIP_CODE,
    WizardStep.THEATRES,
    WizardStep.MOVIES,
    WizardStep.FORMAT,
    WizardStep.SEATS,
    WizardStep.TIME_WINDOWS,
    WizardStep.DESTINATION,
    WizardStep.PREVIEW,
)

# The session.data keys each step must have set before it counts as complete.
STEP_REQUIRED_KEYS: dict[WizardStep, tuple[str, ...]] = {
    WizardStep.ZIP_CODE: ("zip_code",),
    WizardStep.THEATRES: ("theatre_ids",),
    WizardStep.MOVIES: ("movie_ids",),
    WizardStep.FORMAT: ("format_name",),
    WizardStep.SEATS: ("adjacent_seats", "seat_preset"),
    WizardStep.TIME_WINDOWS: (
        "weekday_start",
        "weekday_end",
        "weekend_start",
        "weekend_end",
    ),
    WizardStep.DESTINATION: ("destination_channel_id",),
    WizardStep.PREVIEW: (),
}


def step_complete(session: WizardSession, step: WizardStep) -> bool:
    return all(key in session.data for key in STEP_REQUIRED_KEYS[step])


def _auto_monitor_name(data: Mapping[str, Any]) -> str:
    """Fallback monitor name derived from the chosen movies (mirrors the repo)."""
    return (" + ".join(str(m) for m in (data.get("movie_ids") or [])))[:80]


class WizardInputError(ValueError):
    pass


class CatalogLookupPending(WizardInputError):
    pass


@dataclass(frozen=True, slots=True)
class WizardPrompt:
    title: str
    description: str
    field_label: str | None = None
    placeholder: str | None = None
    options: tuple[CatalogOption, ...] = ()
    can_confirm: bool = False
    maximum: int = 1
    loading: bool = False


def normalize_zip_code(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"\d{5}", value):
        raise WizardInputError("Enter a five-digit US ZIP code.")
    return value


def _parse_clock(value: str) -> time:
    try:
        hour_text, minute_text = value.strip().split(":", 1)
        parsed = time(hour=int(hour_text), minute=int(minute_text))
    except (TypeError, ValueError) as exc:
        raise WizardInputError("Use 24-hour HH:MM times, such as 17:00-23:00.") from exc
    return parsed


def parse_window(value: str) -> tuple[str, str]:
    try:
        start_text, end_text = value.strip().split("-", 1)
    except ValueError as exc:
        raise WizardInputError("Use START-END, such as 17:00-23:00.") from exc
    start, end = _parse_clock(start_text), _parse_clock(end_text)
    if start >= end:
        raise WizardInputError("The start time must be before the end time.")
    return start.strftime("%H:%M"), end.strftime("%H:%M")


def parse_time_windows(value: str) -> tuple[str, str, str, str]:
    try:
        weekday, weekend = value.split(";", 1)
    except ValueError as exc:
        raise WizardInputError(
            "Enter weekday and weekend windows separated by a semicolon."
        ) from exc
    weekday_start, weekday_end = parse_window(weekday)
    weekend_start, weekend_end = parse_window(weekend)
    return weekday_start, weekday_end, weekend_start, weekend_end


def parse_seat_preference(value: str) -> tuple[int, SeatPreset]:
    pieces = value.lower().strip().split()
    if len(pieces) != 2:
        raise WizardInputError("Enter a count and preset, such as `2 center-back`.")
    try:
        count = int(pieces[0])
        preset = SeatPreset(pieces[1])
    except (ValueError, KeyError) as exc:
        raise WizardInputError(
            "Preset must be center, center-back, or center-front."
        ) from exc
    if not MIN_ADJACENT_SEATS <= count <= MAX_ADJACENT_SEATS:
        raise WizardInputError("Choose between 1 and 6 adjacent seats.")
    return count, preset


def select_catalog_options(
    raw: str,
    options: Sequence[CatalogOption],
    *,
    maximum: int,
    label: str,
) -> tuple[str, ...]:
    tokens = [token.strip() for token in raw.split(",") if token.strip()]
    if not tokens:
        raise WizardInputError(f"Choose at least one {label}.")
    if len(tokens) > maximum:
        raise WizardInputError(f"Choose no more than {maximum} {label}.")
    by_id = {option.id.casefold(): option.id for option in options}
    selected: list[str] = []
    for token in tokens:
        if token.isdigit() and 1 <= int(token) <= len(options):
            option_id = options[int(token) - 1].id
        else:
            option_id = by_id.get(token.casefold(), "")
        if not option_id:
            raise WizardInputError(f"`{token}` is not one of the current {label} choices.")
        if option_id not in selected:
            selected.append(option_id)
    return tuple(selected)


class WizardController:
    def __init__(self, repository: DiscordRepository):
        self.repository = repository

    async def start(self, guild_id: int, user_id: int) -> WizardSession:
        existing = await self.repository.get_active_wizard(guild_id, user_id)
        if existing and not existing.expired and existing.step not in {
            WizardStep.COMPLETE,
            WizardStep.CANCELLED,
        }:
            return existing
        user_count, guild_count = await self.repository.active_subscription_counts(
            guild_id, user_id
        )
        if user_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_USER:
            raise WizardInputError("You already have the beta limit of 5 active monitors.")
        if guild_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD:
            raise WizardInputError("This server already has the beta limit of 25 monitors.")
        session = WizardSession.create(guild_id, user_id)
        await self.repository.save_wizard(session)
        return session

    async def load_for_user(
        self, session_id: str, guild_id: int, user_id: int
    ) -> WizardSession:
        session = await self.repository.get_wizard(session_id, guild_id)
        if session is None or session.expired:
            raise WizardInputError("This setup session expired. Run `/amc create` again.")
        if session.guild_id != guild_id or session.user_id != user_id:
            raise WizardInputError("This setup session belongs to another user or server.")
        return session

    async def submit_text(self, session: WizardSession, raw: str) -> WizardSession:
        if session.expired:
            raise WizardInputError("This setup session expired. Run `/amc create` again.")
        if session.step is WizardStep.ZIP_CODE:
            zip_code = normalize_zip_code(raw)
            # Changing an earlier answer invalidates the choices derived from it.
            drop = (
                ()
                if zip_code == session.data.get("zip_code")
                else ("theatre_ids", "movie_ids", "movie_search", "format_name")
            )
            updated = session.advance(WizardStep.THEATRES, drop=drop, zip_code=zip_code)
            await self.repository.save_wizard(updated)
            # Theatres come from the static national catalog ranked by distance;
            # we just need the ZIP geocoded once (cached in zip_centroids).
            await self.repository.queue_catalog_lookup(
                session.guild_id, session.id, "geocode", {"zip_code": zip_code}
            )
            return updated
        if session.step is WizardStep.THEATRES:
            options = await self._ready_options(session, "theatres")
            theatre_ids = select_catalog_options(
                raw,
                options,
                maximum=MAX_THEATRES_PER_SUBSCRIPTION,
                label="theatres",
            )
            drop = (
                ()
                if theatre_ids == tuple(session.data.get("theatre_ids") or ())
                else ("movie_ids", "movie_search", "format_name")
            )
            updated = session.advance(
                WizardStep.MOVIES, drop=drop, theatre_ids=theatre_ids
            )
            await self.repository.save_wizard(updated)
            await self.repository.queue_catalog_lookup(
                session.guild_id,
                session.id,
                "movies",
                {"zip_code": session.data["zip_code"], "theatre_ids": theatre_ids},
            )
            return updated
        if session.step is WizardStep.MOVIES:
            value = raw.strip()
            if value.casefold().startswith("search:"):
                title = value.split(":", 1)[1].strip()
                if not 2 <= len(title) <= 100:
                    raise WizardInputError(
                        "Enter a movie title between 2 and 100 characters after `search:`."
                    )
                updated = session.advance(WizardStep.MOVIES, movie_search=title)
                await self.repository.save_wizard(updated)
                await self.repository.queue_catalog_lookup(
                    session.guild_id,
                    session.id,
                    "movie-search",
                    {"title": title},
                )
                return updated
            options = await self._ready_options(session, "movies")
            movie_ids = select_catalog_options(
                raw,
                options,
                maximum=MAX_MOVIES_PER_SUBSCRIPTION,
                label="movies",
            )
            drop = (
                ()
                if movie_ids == tuple(session.data.get("movie_ids") or ())
                else ("format_name",)
            )
            updated = session.advance(WizardStep.FORMAT, drop=drop, movie_ids=movie_ids)
            await self.repository.save_wizard(updated)
            # Formats are served synchronously from the static catalog (marked by
            # availability) — no worker lookup to queue.
            return updated
        if session.step is WizardStep.FORMAT:
            options = await self._ready_options(session, "formats")
            selected = select_catalog_options(raw, options, maximum=1, label="formats")
            updated = session.advance(WizardStep.SEATS, format_name=selected[0])
        elif session.step is WizardStep.SEATS:
            adjacent_seats, preset = parse_seat_preference(raw)
            updated = session.advance(
                WizardStep.TIME_WINDOWS,
                adjacent_seats=adjacent_seats,
                seat_preset=preset.value,
            )
        elif session.step is WizardStep.TIME_WINDOWS:
            weekday_start, weekday_end, weekend_start, weekend_end = parse_time_windows(raw)
            updated = session.advance(
                WizardStep.DESTINATION,
                weekday_start=weekday_start,
                weekday_end=weekday_end,
                weekend_start=weekend_start,
                weekend_end=weekend_end,
            )
        elif session.step is WizardStep.PREVIEW:
            # The PREVIEW "Set name…" modal routes here.
            return await self.set_name(session, raw)
        else:
            raise WizardInputError("Use the controls shown for this setup step.")
        await self.repository.save_wizard(updated)
        return updated

    async def choose_destination(
        self, session: WizardSession, channel_id: int
    ) -> WizardSession:
        if session.step is not WizardStep.DESTINATION:
            raise WizardInputError("The destination cannot be selected at this step.")
        destinations = await self.repository.list_destinations(session.guild_id)
        allowed = {
            destination.channel_id
            for destination in destinations
            if destination.enabled
        }
        if channel_id not in allowed:
            raise WizardInputError(
                "That channel is not an approved destination. Ask an operator to add it first."
            )
        updated = session.advance(
            WizardStep.PREVIEW, destination_channel_id=int(channel_id)
        )
        await self.repository.save_wizard(updated)
        return updated

    async def go_to(self, session: WizardSession, step: WizardStep) -> WizardSession:
        if step not in STEP_ORDER:
            raise WizardInputError("You cannot navigate to that step.")
        current_index = STEP_ORDER.index(session.step) if session.step in STEP_ORDER else 0
        target_index = STEP_ORDER.index(step)
        # Backward is always allowed; forward requires every earlier step complete.
        if target_index > current_index:
            for earlier in STEP_ORDER[:target_index]:
                if not step_complete(session, earlier):
                    raise WizardInputError("Finish the current step before moving on.")
        updated = session.advance(step)
        await self.repository.save_wizard(updated)
        return updated

    async def go_back(self, session: WizardSession) -> WizardSession:
        if session.step not in STEP_ORDER or STEP_ORDER.index(session.step) == 0:
            raise WizardInputError("You are already at the first step.")
        return await self.go_to(session, STEP_ORDER[STEP_ORDER.index(session.step) - 1])

    async def go_forward(self, session: WizardSession) -> WizardSession:
        if session.step not in STEP_ORDER:
            raise WizardInputError("There is nothing to move forward to.")
        if not step_complete(session, session.step):
            raise WizardInputError("Finish this step before moving on.")
        index = STEP_ORDER.index(session.step)
        if index + 1 >= len(STEP_ORDER):
            raise WizardInputError("You are already at the final step.")
        return await self.go_to(session, STEP_ORDER[index + 1])

    async def set_name(self, session: WizardSession, raw: str) -> WizardSession:
        if session.step is not WizardStep.PREVIEW:
            raise WizardInputError("Set the name from the confirmation step.")
        name = raw.strip()
        if not name:
            updated = session.advance(WizardStep.PREVIEW, drop=("name",))
        elif len(name) > 80:
            raise WizardInputError("Names must be 80 characters or fewer.")
        else:
            updated = session.advance(WizardStep.PREVIEW, name=name)
        await self.repository.save_wizard(updated)
        return updated

    async def set_seat_count(
        self, session: WizardSession, count: int
    ) -> WizardSession:
        if session.step is not WizardStep.SEATS:
            raise WizardInputError("Seats cannot be set at this step.")
        if not MIN_ADJACENT_SEATS <= count <= MAX_ADJACENT_SEATS:
            raise WizardInputError("Choose between 1 and 6 seats.")
        # Merge the pick into the draft without leaving the SEATS step, so the
        # count and area can be chosen in either order from one card.
        updated = session.advance(WizardStep.SEATS, adjacent_seats=count)
        await self.repository.save_wizard(updated)
        return updated

    async def set_seat_preset(
        self, session: WizardSession, preset: str
    ) -> WizardSession:
        if session.step is not WizardStep.SEATS:
            raise WizardInputError("Seats cannot be set at this step.")
        try:
            value = SeatPreset(preset)
        except ValueError as exc:
            raise WizardInputError("Pick one of the offered seat areas.") from exc
        updated = session.advance(WizardStep.SEATS, seat_preset=value.value)
        await self.repository.save_wizard(updated)
        return updated

    async def finish_seats(self, session: WizardSession) -> WizardSession:
        if session.step is not WizardStep.SEATS:
            raise WizardInputError("Seats cannot be confirmed at this step.")
        if "adjacent_seats" not in session.data or "seat_preset" not in session.data:
            raise WizardInputError("Pick a seat count and a seat area first.")
        updated = session.advance(WizardStep.TIME_WINDOWS)
        await self.repository.save_wizard(updated)
        return updated

    async def choose_time_window_preset(
        self, session: WizardSession, key: str
    ) -> WizardSession:
        if session.step is not WizardStep.TIME_WINDOWS:
            raise WizardInputError("Time windows cannot be set at this step.")
        preset = _TIME_WINDOW_PRESETS_BY_KEY.get(key)
        if preset is None:
            raise WizardInputError("Pick one of the offered time windows.")
        weekday_start, weekday_end, weekend_start, weekend_end = preset
        updated = session.advance(
            WizardStep.DESTINATION,
            weekday_start=weekday_start,
            weekday_end=weekday_end,
            weekend_start=weekend_start,
            weekend_end=weekend_end,
        )
        await self.repository.save_wizard(updated)
        return updated

    async def retry_lookup(self, session: WizardSession) -> None:
        data = session.data
        if session.step is WizardStep.THEATRES:
            # Only the one-off ZIP geocode is async; theatres themselves are served
            # synchronously from the static catalog.
            kind, query = "geocode", {"zip_code": data["zip_code"]}
        elif session.step is WizardStep.MOVIES:
            if data.get("movie_search"):
                kind, query = "movie-search", {"title": data["movie_search"]}
            else:
                kind, query = "movies", {
                    "zip_code": data["zip_code"],
                    "theatre_ids": data["theatre_ids"],
                }
        elif session.step is WizardStep.FORMAT:
            # Formats are synchronous (static catalog) — nothing to queue.
            return
        else:
            raise WizardInputError("There is no catalog lookup at this step.")
        await self.repository.queue_catalog_lookup(session.guild_id, session.id, kind, query)

    async def confirm(self, session: WizardSession) -> SubscriptionSummary:
        if session.step is not WizardStep.PREVIEW:
            raise WizardInputError("Complete every setup step before confirming.")
        draft = self._draft(session)
        user_count, guild_count = await self.repository.active_subscription_counts(
            session.guild_id, session.user_id
        )
        if user_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_USER:
            raise WizardInputError("You already have the beta limit of 5 active monitors.")
        if guild_count >= MAX_ACTIVE_SUBSCRIPTIONS_PER_GUILD:
            raise WizardInputError("This server already has the beta limit of 25 monitors.")
        projected = await self.repository.projected_status_cadence(draft)
        if projected > 60:
            raise WizardInputError(
                "This monitor would push projected status latency above 60 seconds. "
                "Pause another monitor or try fewer theatres."
            )
        subscription = await self.repository.create_subscription(
            draft, idempotency_key=session.id
        )
        await self.repository.save_wizard(session.advance(WizardStep.COMPLETE))
        return subscription

    async def cancel(self, session: WizardSession) -> None:
        await self.repository.save_wizard(session.advance(WizardStep.CANCELLED))

    async def prompt(self, session: WizardSession) -> WizardPrompt:
        if session.step is WizardStep.ZIP_CODE:
            return WizardPrompt(
                "1 · ZIP code",
                "Enter the five-digit ZIP code to search around.",
                "ZIP code",
                "00000",
            )
        if session.step in {WizardStep.THEATRES, WizardStep.MOVIES, WizardStep.FORMAT}:
            options = await self._step_options(session)
            title = {
                WizardStep.THEATRES: "2 · Theatres",
                WizardStep.MOVIES: "3 · Movies",
                WizardStep.FORMAT: "4 · Format",
            }[session.step]
            maximum = {
                WizardStep.THEATRES: 3,
                WizardStep.MOVIES: 5,
                WizardStep.FORMAT: 1,
            }[session.step]
            if options is None:
                return WizardPrompt(
                    title,
                    "⏳ Loading choices from AMC… this updates on its own.",
                    maximum=maximum,
                    loading=True,
                )
            if not options:
                if session.step is WizardStep.MOVIES:
                    return WizardPrompt(
                        title,
                        "No titles cached for these theatres yet — tap **Search by title**, "
                        "or **Refresh** to check again.",
                        maximum=maximum,
                    )
                return WizardPrompt(
                    title,
                    "No matching choices were found. Cancel and try again.",
                    maximum=maximum,
                )
            instruction = (
                f"Select up to {maximum} from the menu below."
                if maximum > 1
                else "Select one from the menu below."
            )
            if session.step is WizardStep.MOVIES:
                instruction += " Not listed? Tap **Search by title**."
            return WizardPrompt(
                title,
                instruction,
                options=tuple(options[:25]),
                maximum=maximum,
            )
        if session.step is WizardStep.SEATS:
            data = session.data
            count = data.get("adjacent_seats")
            preset = data.get("seat_preset")
            lines = [
                "Pick how many adjacent seats to watch for and which part of the "
                "auditorium, then tap **Continue**."
            ]
            if count is not None and preset is not None:
                label = SEAT_PRESET_LABELS.get(SeatPreset(preset), preset)
                lines.append(
                    f"Selected: **{count} adjacent seat"
                    f"{'s' if count != 1 else ''}** · {label}."
                )
            return WizardPrompt("5 · Seats", "\n".join(lines))
        if session.step is WizardStep.TIME_WINDOWS:
            return WizardPrompt(
                "6 · Time windows",
                "Choose a viewing window from the menu, or tap **Custom…** to set "
                "your own weekday and weekend hours.",
            )
        if session.step is WizardStep.DESTINATION:
            return WizardPrompt(
                "7 · Alert channel",
                "Choose one of the channels approved by an operator.",
            )
        if session.step is WizardStep.PREVIEW:
            data = session.data
            name = str(data.get("name") or _auto_monitor_name(data))
            description = (
                f"Name: **{name}**\n"
                f"ZIP `{data['zip_code']}` · {len(data['theatre_ids'])} theatre(s) · "
                f"{len(data['movie_ids'])} movie(s)\n"
                f"`{data['format_name']}` · {data['adjacent_seats']} adjacent · "
                f"`{data['seat_preset']}`\n"
                f"Weekdays `{data['weekday_start']}-{data['weekday_end']}` · "
                f"Weekends `{data['weekend_start']}-{data['weekend_end']}`\n"
                f"Alerts: <#{data['destination_channel_id']}>\n"
                "Tap **Set name…** to rename, or **Create monitor** to finish."
            )
            return WizardPrompt("8 · Confirm monitor", description, can_confirm=True)
        if session.step is WizardStep.COMPLETE:
            name = str(session.data.get("name") or _auto_monitor_name(session.data))
            return WizardPrompt(
                "Monitor created",
                f"**{name}** is active. Use `/amc list` anytime.",
            )
        return WizardPrompt("Setup cancelled", "Run `/amc create` whenever you are ready.")

    async def _step_options(
        self, session: WizardSession
    ) -> Sequence[CatalogOption] | None:
        """Options for the current catalog step. Theatres and formats come from
        the static catalog (synchronous); movies stay live via worker lookups.
        ``None`` means still loading (theatres: ZIP not geocoded; movies: lookup
        pending)."""
        if session.step is WizardStep.THEATRES:
            return await self.repository.nearest_theatres(str(session.data["zip_code"]))
        if session.step is WizardStep.FORMAT:
            return await self.repository.available_formats(
                session.data["theatre_ids"], session.data["movie_ids"]
            )
        return await self._movie_options(session)

    async def _ready_options(
        self, session: WizardSession, kind: str
    ) -> Sequence[CatalogOption]:
        options = await self._step_options(session)
        if options is None:
            raise CatalogLookupPending(
                "Still looking up choices. This updates on its own in a moment."
            )
        if not options:
            if kind == "movies":
                raise WizardInputError(
                    "No cached movies were found. Enter `search: movie title` instead."
                )
            raise WizardInputError("No matching choices were found. Cancel and try again.")
        return options

    async def _movie_options(
        self, session: WizardSession
    ) -> Sequence[CatalogOption] | None:
        cached = await self.repository.catalog_options(
            session.guild_id, session.id, "movies"
        )
        if cached is None:
            return None
        searched: Sequence[CatalogOption] = ()
        if session.data.get("movie_search"):
            result = await self.repository.catalog_options(
                session.guild_id, session.id, "movie-search"
            )
            if result is None:
                return None
            searched = result
        merged: dict[str, CatalogOption] = {}
        for option in (*cached, *searched):
            merged.setdefault(option.id, option)
        return tuple(merged.values())

    @staticmethod
    def _draft(session: WizardSession) -> SubscriptionDraft:
        data = session.data
        required = {
            "zip_code",
            "theatre_ids",
            "movie_ids",
            "format_name",
            "adjacent_seats",
            "seat_preset",
            "weekday_start",
            "weekday_end",
            "weekend_start",
            "weekend_end",
            "destination_channel_id",
        }
        if missing := required - data.keys():
            raise WizardInputError("Setup is incomplete: " + ", ".join(sorted(missing)))
        return SubscriptionDraft(
            guild_id=session.guild_id,
            owner_user_id=session.user_id,
            zip_code=str(data["zip_code"]),
            theatre_ids=tuple(data["theatre_ids"]),
            movie_ids=tuple(data["movie_ids"]),
            format_name=str(data["format_name"]),
            adjacent_seats=int(data["adjacent_seats"]),
            seat_preset=SeatPreset(data["seat_preset"]),
            weekday_start=str(data["weekday_start"]),
            weekday_end=str(data["weekday_end"]),
            weekend_start=str(data["weekend_start"]),
            weekend_end=str(data["weekend_end"]),
            destination_channel_id=int(data["destination_channel_id"]),
            name=(str(data["name"]) if data.get("name") else None),
        )


__all__ = [
    "CatalogLookupPending",
    "SEAT_PRESET_LABELS",
    "STEP_ORDER",
    "STEP_REQUIRED_KEYS",
    "TIME_WINDOW_PRESETS",
    "WizardController",
    "WizardInputError",
    "WizardPrompt",
    "normalize_zip_code",
    "parse_seat_preference",
    "parse_time_windows",
    "parse_window",
    "select_catalog_options",
    "step_complete",
]
