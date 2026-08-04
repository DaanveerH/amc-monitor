"""Guild-only Discord application for AMC Seat Watch.

This module owns Discord I/O only.  AMC catalog requests are represented as
repository jobs by :mod:`amc_watch.discord_wizard`; no AMC client is imported.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import replace
from datetime import timedelta
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from .discord_embed import BookingLinkView, build_alert_embed
from .discord_models import (
    Destination,
    DiscordRepository,
    MAX_ADJACENT_SEATS,
    MIN_ADJACENT_SEATS,
    SeatPreset,
    UserAlert,
    WizardSession,
    WizardStep,
    utc_now,
)
from .discord_security import (
    DiscordBotSettings,
    DiscordGuard,
    InteractionDenied,
    validate_destination_channel,
)
from .discord_wizard import (
    SEAT_PRESET_LABELS,
    STEP_ORDER,
    TIME_WINDOW_PRESETS,
    WizardController,
    WizardInputError,
    WizardPrompt,
    parse_time_windows,
    parse_window,
    step_complete,
)


LOGGER = logging.getLogger(__name__)
NO_MENTIONS = discord.AllowedMentions.none()


async def _safe_ephemeral(interaction: discord.Interaction[Any], content: str) -> None:
    content = discord.utils.escape_mentions(content)[:1900]
    kwargs = {"content": content, "ephemeral": True, "allowed_mentions": NO_MENTIONS}
    if interaction.response.is_done():
        await interaction.followup.send(**kwargs)
    else:
        await interaction.response.send_message(**kwargs)


class UserOutboxDispatcher:
    """Lease and deliver user alerts without exposing webhook URLs."""

    def __init__(
        self,
        client: discord.Client,
        repository: DiscordRepository,
        *,
        allowed_guild_ids: frozenset[int] = frozenset(),
        poll_seconds: float = 2.0,
        batch_size: int = 10,
    ):
        self.client = client
        self.repository = repository
        self.allowed_guild_ids = allowed_guild_ids
        self.poll_seconds = poll_seconds
        self.batch_size = batch_size
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="discord-user-outbox")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run(self) -> None:
        await self.client.wait_until_ready()
        while not self._stop.is_set():
            try:
                delivered = await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Do not include payloads, URLs, or exception strings in logs.
                LOGGER.error(
                    "Discord outbox sweep failed error_type=%s", type(exc).__name__
                )
                delivered = 0
            delay = 0 if delivered >= self.batch_size else self.poll_seconds
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                pass

    async def dispatch_once(self) -> int:
        alerts = await self.repository.claim_user_alerts(
            tuple(sorted(self.allowed_guild_ids)), self.batch_size
        )
        for alert in alerts:
            await self._deliver(alert)
        return len(alerts)

    async def _deliver(self, alert: UserAlert) -> None:
        try:
            channel = self.client.get_channel(alert.channel_id)
            if channel is None:
                channel = await self.client.fetch_channel(alert.channel_id)
            if not isinstance(channel, (discord.TextChannel, discord.Thread)) and not callable(
                getattr(channel, "send", None)
            ):
                await self.repository.fail_user_alert(
                    alert.guild_id,
                    alert.outbox_id,
                    "invalid_destination",
                    disable_destination=True,
                )
                return
            message = await channel.send(
                embed=build_alert_embed(alert),
                view=BookingLinkView(alert.booking_url),
                allowed_mentions=NO_MENTIONS,
            )
        except (discord.Forbidden, discord.NotFound):
            await self.repository.fail_user_alert(
                alert.guild_id,
                alert.outbox_id,
                "destination_unavailable",
                disable_destination=True,
            )
        except discord.HTTPException as exc:
            status = int(getattr(exc, "status", 0) or 0)
            if status in {403, 404}:
                await self.repository.fail_user_alert(
                    alert.guild_id,
                    alert.outbox_id,
                    f"discord_http_{status}",
                    disable_destination=True,
                )
                return
            retry_after = float(getattr(exc, "retry_after", 0) or 0)
            delay = min(max(retry_after, 5), 900)
            await self.repository.retry_user_alert(
                alert.guild_id,
                alert.outbox_id,
                f"discord_http_{status or 'error'}",
                utc_now() + timedelta(seconds=delay),
            )
        except Exception as exc:
            LOGGER.error(
                "Discord alert delivery failed outbox_id=%s error_type=%s",
                alert.outbox_id,
                type(exc).__name__,
            )
            await self.repository.retry_user_alert(
                alert.guild_id,
                alert.outbox_id,
                "discord_delivery_error",
                utc_now() + timedelta(seconds=30),
            )
        else:
            await self.repository.mark_user_alert_delivered(
                alert.guild_id, alert.outbox_id, int(message.id)
            )


class ServiceHeartbeatLoop:
    """Publish liveness independently of Discord command traffic."""

    SERVICE_NAME = "amc-discord-bot"

    def __init__(self, repository: DiscordRepository, interval_seconds: float = 30.0):
        self.repository = repository
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="discord-service-heartbeat")

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.repository.write_service_heartbeat(self.SERVICE_NAME, "healthy")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.error(
                    "Discord service heartbeat failed error_type=%s", type(exc).__name__
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_seconds)
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        try:
            await self.repository.write_service_heartbeat(self.SERVICE_NAME, "stopping")
        except Exception as exc:
            LOGGER.error(
                "Final Discord service heartbeat failed error_type=%s", type(exc).__name__
            )


class WizardTextModal(discord.ui.Modal):
    def __init__(
        self,
        cog: "SeatWatchCog",
        session: WizardSession,
        prompt: WizardPrompt,
    ):
        super().__init__(title=prompt.title[:45], timeout=900)
        self.cog = cog
        self.session_id = session.id
        self.answer = discord.ui.TextInput(
            label=(prompt.field_label or "Value")[:45],
            placeholder=(prompt.placeholder or "")[:100] or None,
            required=True,
            max_length=300,
        )
        self.add_item(self.answer)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self.cog.guard.require_operator(interaction)
            guild, member = self.cog.guard.require_guild(interaction)
            session = await self.cog.wizard.load_for_user(
                self.session_id, guild.id, member.id
            )
            updated = await self.cog.wizard.submit_text(session, str(self.answer.value))
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


class MovieSearchModal(discord.ui.Modal):
    def __init__(self, cog: "SeatWatchCog", session: WizardSession):
        super().__init__(title="Search movies", timeout=900)
        self.cog = cog
        self.session_id = session.id
        self.query = discord.ui.TextInput(
            label="Movie title",
            placeholder="Movie title",
            required=True,
            min_length=2,
            max_length=100,
        )
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self.cog.guard.require_operator(interaction)
            guild, member = self.cog.guard.require_guild(interaction)
            session = await self.cog.wizard.load_for_user(
                self.session_id, guild.id, member.id
            )
            updated = await self.cog.wizard.submit_text(
                session, f"search: {self.query.value}"
            )
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


class WizardView(discord.ui.View):
    def __init__(
        self,
        cog: "SeatWatchCog",
        session: WizardSession,
        prompt: WizardPrompt,
    ):
        super().__init__(timeout=900)
        self.cog = cog
        self.session_id = session.id
        self.step = session.step
        catalog_steps = {WizardStep.THEATRES, WizardStep.MOVIES, WizardStep.FORMAT}
        if session.step in catalog_steps and prompt.options:
            # Re-mark prior picks so navigating Back shows what was chosen.
            data = session.data
            previously = {
                WizardStep.THEATRES: tuple(data.get("theatre_ids") or ()),
                WizardStep.MOVIES: tuple(data.get("movie_ids") or ()),
                WizardStep.FORMAT: (
                    (data["format_name"],) if data.get("format_name") else ()
                ),
            }[session.step]
            previously = {str(value) for value in previously}
            select = discord.ui.Select(
                custom_id=f"amc:wizard:pick:{session.id}",
                placeholder="Choose…",
                min_values=1,
                max_values=max(1, min(prompt.maximum, len(prompt.options))),
                options=[
                    discord.SelectOption(
                        label=option.label[:100],
                        value=option.id[:100],
                        description=(option.detail[:100] if option.detail else None),
                        default=(option.id in previously),
                    )
                    for option in prompt.options[:25]
                ],
            )
            select.callback = self.pick_selected
            self.add_item(select)
            if session.step is WizardStep.MOVIES:
                self._add_search_button(session)
        elif session.step in catalog_steps:
            # Movie search stays available regardless of load state.
            if session.step is WizardStep.MOVIES:
                self._add_search_button(session)
            # While the lookup is in flight the card auto-refreshes from a
            # background poller (see SeatWatchCog._await_catalog), so no manual
            # button is shown. Refresh is only a fallback for the terminal
            # states: an empty result, or a lookup that timed out.
            if not prompt.loading:
                retry = discord.ui.Button(
                    label="Refresh",
                    style=discord.ButtonStyle.primary,
                    custom_id=f"amc:wizard:retry:{session.id}",
                )
                retry.callback = self.retry_clicked
                self.add_item(retry)
        elif session.step is WizardStep.SEATS:
            chosen_count = session.data.get("adjacent_seats")
            count_select = discord.ui.Select(
                custom_id=f"amc:wizard:seatcount:{session.id}",
                placeholder="How many adjacent seats?",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(
                        label=f"{n} adjacent seat{'s' if n != 1 else ''}",
                        value=str(n),
                        default=(chosen_count == n),
                    )
                    for n in range(MIN_ADJACENT_SEATS, MAX_ADJACENT_SEATS + 1)
                ],
            )
            count_select.callback = self.seat_count_selected
            self.add_item(count_select)
            chosen_preset = session.data.get("seat_preset")
            preset_select = discord.ui.Select(
                custom_id=f"amc:wizard:seatpreset:{session.id}",
                placeholder="Which part of the auditorium?",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(
                        label=label,
                        value=preset.value,
                        default=(chosen_preset == preset.value),
                    )
                    for preset, label in SEAT_PRESET_LABELS.items()
                ],
            )
            preset_select.callback = self.seat_preset_selected
            self.add_item(preset_select)
            # No per-step Continue — the shared "Next ›" nav button advances once
            # both seat selects are chosen (step_complete).
        elif session.step is WizardStep.TIME_WINDOWS:
            window_select = discord.ui.Select(
                custom_id=f"amc:wizard:timewindow:{session.id}",
                placeholder="Choose a viewing window",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(label=label, value=key)
                    for key, label, _ in TIME_WINDOW_PRESETS
                ],
            )
            window_select.callback = self.time_window_selected
            self.add_item(window_select)
            custom = discord.ui.Button(
                label="Custom…",
                style=discord.ButtonStyle.secondary,
                custom_id=f"amc:wizard:timecustom:{session.id}",
            )
            custom.callback = self.time_custom_clicked
            self.add_item(custom)
        elif prompt.field_label:
            button = discord.ui.Button(
                label="Continue",
                style=discord.ButtonStyle.primary,
                custom_id=f"amc:wizard:continue:{session.id}",
            )
            button.callback = self.continue_clicked
            self.add_item(button)
        elif session.step is WizardStep.DESTINATION:
            select = discord.ui.ChannelSelect(
                custom_id=f"amc:wizard:destination:{session.id}",
                channel_types=[discord.ChannelType.text, discord.ChannelType.news],
                placeholder="Choose an approved alert channel",
                min_values=1,
                max_values=1,
            )
            select.callback = self.destination_selected
            self.add_item(select)
        elif prompt.can_confirm:
            confirm = discord.ui.Button(
                label="Create monitor",
                style=discord.ButtonStyle.success,
                custom_id=f"amc:wizard:confirm:{session.id}",
            )
            confirm.callback = self.confirm_clicked
            self.add_item(confirm)
        if session.step not in {WizardStep.COMPLETE, WizardStep.CANCELLED}:
            self._add_nav_row(session)

    def _add_nav_row(self, session: WizardSession) -> None:
        # A shared bottom row on row 4 so it never collides with the per-step
        # selects/buttons above it.
        in_order = session.step in STEP_ORDER
        if in_order and STEP_ORDER.index(session.step) > 0:
            back = discord.ui.Button(
                label="‹ Back",
                style=discord.ButtonStyle.secondary,
                custom_id=f"amc:wizard:back:{session.id}",
                row=4,
            )
            back.callback = self.back_clicked
            self.add_item(back)
        if (
            in_order
            and session.step is not WizardStep.PREVIEW
            and step_complete(session, session.step)
        ):
            nxt = discord.ui.Button(
                label="Next ›",
                style=discord.ButtonStyle.primary,
                custom_id=f"amc:wizard:next:{session.id}",
                row=4,
            )
            nxt.callback = self.forward_clicked
            self.add_item(nxt)
        if session.step is WizardStep.PREVIEW:
            set_name = discord.ui.Button(
                label="Set name…",
                style=discord.ButtonStyle.secondary,
                custom_id=f"amc:wizard:name:{session.id}",
                row=4,
            )
            set_name.callback = self.name_clicked
            self.add_item(set_name)
        cancel = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.secondary,
            custom_id=f"amc:wizard:cancel:{session.id}",
            row=4,
        )
        cancel.callback = self.cancel_clicked
        self.add_item(cancel)

    def _add_search_button(self, session: WizardSession) -> None:
        search = discord.ui.Button(
            label="Search by title",
            style=discord.ButtonStyle.secondary,
            custom_id=f"amc:wizard:search:{session.id}",
        )
        search.callback = self.search_clicked
        self.add_item(search)

    async def _session(self, interaction: discord.Interaction[Any]) -> WizardSession:
        await self.cog.guard.require_operator(interaction)
        guild, member = self.cog.guard.require_guild(interaction)
        return await self.cog.wizard.load_for_user(self.session_id, guild.id, member.id)

    async def continue_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            prompt = await self.cog.wizard.prompt(session)
            await interaction.response.send_modal(WizardTextModal(self.cog, session, prompt))
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def retry_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            await self.cog.wizard.retry_lookup(session)
            # Re-render the wizard so the picker appears the moment the lookup is
            # ready. retry_lookup is idempotent (it never resets a completed
            # lookup), so tapping Refresh repeatedly is safe.
            await self.cog.send_wizard(interaction, session)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def pick_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            select = next(
                item
                for item in self.children
                if isinstance(item, discord.ui.Select)
                and not isinstance(item, discord.ui.ChannelSelect)
            )
            # The select values ARE CatalogOption ids, which submit_text already
            # accepts (comma-separated) — no numbered-list typing needed.
            updated = await self.cog.wizard.submit_text(session, ",".join(select.values))
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def search_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            await interaction.response.send_modal(MovieSearchModal(self.cog, session))
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    @staticmethod
    def _selected_value(interaction: discord.Interaction[Any]) -> str:
        values = (interaction.data or {}).get("values") or []
        if not values:
            raise WizardInputError("Choose an option from the menu.")
        return str(values[0])

    async def seat_count_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            count = int(self._selected_value(interaction))
            updated = await self.cog.wizard.set_seat_count(session, count)
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))
        except ValueError:
            await _safe_ephemeral(interaction, "Pick a seat count from the menu.")

    async def seat_preset_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            updated = await self.cog.wizard.set_seat_preset(
                session, self._selected_value(interaction)
            )
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def back_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            updated = await self.cog.wizard.go_back(session)
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def forward_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            updated = await self.cog.wizard.go_forward(session)
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def name_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            prompt = WizardPrompt(
                "Name this monitor",
                "A friendly label so it's easy to find in /amc list and /amc edit.",
                field_label="Monitor name",
                placeholder="IMAX Fridays",
            )
            await interaction.response.send_modal(
                WizardTextModal(self.cog, session, prompt)
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def time_window_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            updated = await self.cog.wizard.choose_time_window_preset(
                session, self._selected_value(interaction)
            )
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def time_custom_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            prompt = WizardPrompt(
                "Custom time windows",
                "Use local 24-hour time as weekday window;weekend window.",
                field_label="Weekday; weekend",
                placeholder="17:00-23:00;10:00-23:00",
            )
            await interaction.response.send_modal(
                WizardTextModal(self.cog, session, prompt)
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def destination_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            select = next(
                item for item in self.children if isinstance(item, discord.ui.ChannelSelect)
            )
            guild = interaction.guild
            assert guild is not None
            selected = select.values[0]
            channel = guild.get_channel(int(selected.id))
            if channel is None:
                channel = await interaction.client.fetch_channel(int(selected.id))
            # Auto-approve the picked channel so onboarding needs no prior
            # /amc-admin destination-add. The operator gate + channel perms are
            # still enforced above.
            await self.cog.ensure_destination_approved(
                guild, interaction.user.id, channel
            )
            updated = await self.cog.wizard.choose_destination(session, int(channel.id))
            await self.cog.send_wizard(interaction, updated)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def confirm_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            await self.cog.wizard.confirm(session)
            completed = await self.cog.wizard.load_for_user(
                session.id, session.guild_id, session.user_id
            )
            # Land the card on the COMPLETE prompt (edit-in-place to the end).
            await self.cog.send_wizard(interaction, completed)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def cancel_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            session = await self._session(interaction)
            await self.cog.wizard.cancel(session)
            await _safe_ephemeral(interaction, "Monitor setup cancelled.")
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


_EDIT_FIELDS: tuple[tuple[str, str], ...] = (
    ("name", "Name"),
    ("adjacent_seats", "Adjacent seats"),
    ("seat_preset", "Seat area"),
    ("time_windows", "Viewing windows"),
    ("destination", "Alert channel"),
)
_EDIT_FIELD_LABELS = dict(_EDIT_FIELDS)
_TIME_WINDOW_VALUES = {key: values for key, _label, values in TIME_WINDOW_PRESETS}


class MonitorFieldModal(discord.ui.Modal):
    """Free-text entry for the two edit fields that need it: name and a custom
    viewing window."""

    def __init__(self, view: "MonitorEditView", field: str, *, title: str, label: str, placeholder: str):
        super().__init__(title=title[:45], timeout=900)
        self.view_ref = view
        self.field = field
        self.value_input = discord.ui.TextInput(
            label=label[:45],
            placeholder=placeholder[:100] or None,
            required=True,
            max_length=100,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        try:
            guild, member = await self.view_ref._guarded(interaction)
            raw = str(self.value_input.value)
            if self.field == "name":
                changes: dict[str, Any] = {"name": raw}
            else:
                ws, we, kws, kwe = parse_time_windows(raw)
                changes = {
                    "weekday_start": ws,
                    "weekday_end": we,
                    "weekend_start": kws,
                    "weekend_end": kwe,
                }
            await self.view_ref.apply_changes(interaction, guild, member, changes)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


class MonitorEditView(discord.ui.View):
    """Select-driven monitor editor: monitor picker → field picker → value control,
    all as one edited-in-place ephemeral card."""

    def __init__(self, cog: "SeatWatchCog", subscriptions: Sequence[Any]):
        super().__init__(timeout=900)
        self.cog = cog
        self.subscriptions = {s.id: s for s in subscriptions}
        self.selected_id: str | None = None
        self.selected_field: str | None = None
        self.note: str | None = None
        self._rebuild()

    @property
    def _current(self) -> Any:
        return self.subscriptions.get(self.selected_id or "")

    def render_embed(self) -> discord.Embed:
        if self.selected_id is None:
            description = "Pick a monitor to edit."
        elif self.selected_field is None:
            current = self._current
            description = (
                f"Editing **{(current.name or current.id[:8])}** — "
                "choose a field to change."
            )
        else:
            description = f"Set a new value for **{_EDIT_FIELD_LABELS[self.selected_field]}**."
        if self.note:
            description = f"{self.note}\n\n{description}"
        return discord.Embed(
            title="Edit monitor",
            description=description[:4096],
            color=discord.Color.from_rgb(216, 180, 91),
        )

    def _rebuild(self) -> None:
        self.clear_items()
        if self.selected_id is None:
            self._add_monitor_picker()
        elif self.selected_field is None:
            self._add_field_picker()
        else:
            self._add_value_control()
        if self.selected_id is not None:
            back = discord.ui.Button(label="‹ Back", style=discord.ButtonStyle.secondary, row=4)
            back.callback = self.back_clicked
            self.add_item(back)
        cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, row=4)
        cancel.callback = self.cancel_clicked
        self.add_item(cancel)

    def _add_monitor_picker(self) -> None:
        options = []
        for summary in list(self.subscriptions.values())[:25]:
            state = "active" if summary.enabled else "paused"
            detail = (
                f"{state} · {summary.adjacent_seats} seats · "
                f"{summary.seat_preset.value} · {summary.format_name}"
            )
            options.append(
                discord.SelectOption(
                    label=(summary.name or summary.id[:8])[:100],
                    value=summary.id,
                    description=detail[:100],
                )
            )
        select = discord.ui.Select(
            placeholder="Which monitor?", min_values=1, max_values=1, options=options
        )
        select.callback = self.monitor_selected
        self.add_item(select)

    def _add_field_picker(self) -> None:
        select = discord.ui.Select(
            placeholder="What do you want to change?",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label=label, value=key) for key, label in _EDIT_FIELDS],
        )
        select.callback = self.field_selected
        self.add_item(select)

    def _add_value_control(self) -> None:
        field = self.selected_field
        if field == "adjacent_seats":
            select = discord.ui.Select(
                placeholder="How many adjacent seats?",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(label=f"{n} adjacent seat{'s' if n != 1 else ''}", value=str(n))
                    for n in range(MIN_ADJACENT_SEATS, MAX_ADJACENT_SEATS + 1)
                ],
            )
            select.callback = self.seat_count_selected
            self.add_item(select)
        elif field == "seat_preset":
            select = discord.ui.Select(
                placeholder="Which part of the auditorium?",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(label=label, value=preset.value)
                    for preset, label in SEAT_PRESET_LABELS.items()
                ],
            )
            select.callback = self.seat_preset_selected
            self.add_item(select)
        elif field == "time_windows":
            select = discord.ui.Select(
                placeholder="Choose a viewing window",
                min_values=1,
                max_values=1,
                options=[
                    discord.SelectOption(label=label, value=key)
                    for key, label, _ in TIME_WINDOW_PRESETS
                ],
            )
            select.callback = self.time_window_selected
            self.add_item(select)
            custom = discord.ui.Button(
                label="Custom…", style=discord.ButtonStyle.secondary, row=1
            )
            custom.callback = self.time_custom_clicked
            self.add_item(custom)
        elif field == "destination":
            select = discord.ui.ChannelSelect(
                channel_types=[discord.ChannelType.text, discord.ChannelType.news],
                placeholder="Pick an alert channel",
                min_values=1,
                max_values=1,
            )
            select.callback = self.destination_selected
            self.add_item(select)
        elif field == "name":
            button = discord.ui.Button(
                label="Enter name…", style=discord.ButtonStyle.primary, row=1
            )
            button.callback = self.name_clicked
            self.add_item(button)

    async def _guarded(self, interaction: discord.Interaction[Any]) -> tuple[Any, Any]:
        await self.cog.guard.require_operator(interaction)
        return self.cog.guard.require_guild(interaction)

    async def _reedit(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.response.is_done():
            await interaction.edit_original_response(
                embed=self.render_embed(), view=self, allowed_mentions=NO_MENTIONS
            )
        else:
            await interaction.response.edit_message(embed=self.render_embed(), view=self)

    @staticmethod
    def _selected_value(interaction: discord.Interaction[Any]) -> str:
        values = (interaction.data or {}).get("values") or []
        if not values:
            raise WizardInputError("Choose an option from the menu.")
        return str(values[0])

    async def apply_changes(
        self, interaction: discord.Interaction[Any], guild: Any, member: Any, changes: dict[str, Any]
    ) -> None:
        updated = await self.cog.repository.update_subscription(
            guild.id, self.selected_id, member.id, changes
        )
        self.subscriptions[updated.id] = updated
        self.note = f"✅ Updated **{updated.name or updated.id[:8]}**."
        self.selected_field = None
        self._rebuild()
        await self._reedit(interaction)

    async def monitor_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            self.selected_id = self._selected_value(interaction)
            self.selected_field = None
            self.note = None
            self._rebuild()
            await self._reedit(interaction)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def field_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            self.selected_field = self._selected_value(interaction)
            self.note = None
            self._rebuild()
            await self._reedit(interaction)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def seat_count_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            guild, member = await self._guarded(interaction)
            await self.apply_changes(
                interaction, guild, member, {"adjacent_seats": int(self._selected_value(interaction))}
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def seat_preset_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            guild, member = await self._guarded(interaction)
            await self.apply_changes(
                interaction, guild, member,
                {"seat_preset": SeatPreset(self._selected_value(interaction)).value},
            )
        except (InteractionDenied, WizardInputError, ValueError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def time_window_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            guild, member = await self._guarded(interaction)
            ws, we, kws, kwe = _TIME_WINDOW_VALUES[self._selected_value(interaction)]
            await self.apply_changes(
                interaction, guild, member,
                {"weekday_start": ws, "weekday_end": we, "weekend_start": kws, "weekend_end": kwe},
            )
        except (InteractionDenied, WizardInputError, KeyError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def time_custom_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            await interaction.response.send_modal(
                MonitorFieldModal(
                    self, "time_windows", title="Custom time windows",
                    label="Weekday; weekend", placeholder="17:00-23:00;10:00-23:00",
                )
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def name_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            await interaction.response.send_modal(
                MonitorFieldModal(
                    self, "name", title="Rename monitor",
                    label="Monitor name", placeholder="IMAX Fridays",
                )
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def destination_selected(self, interaction: discord.Interaction[Any]) -> None:
        try:
            guild, member = await self._guarded(interaction)
            select = next(
                item for item in self.children if isinstance(item, discord.ui.ChannelSelect)
            )
            selected = select.values[0]
            channel = guild.get_channel(int(selected.id))
            if channel is None:
                channel = await interaction.client.fetch_channel(int(selected.id))
            await self.cog.ensure_destination_approved(guild, member.id, channel)
            await self.apply_changes(
                interaction, guild, member, {"destination_channel_id": int(channel.id)}
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def back_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            if self.selected_field is not None:
                self.selected_field = None
            else:
                self.selected_id = None
            self.note = None
            self._rebuild()
            await self._reedit(interaction)
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))

    async def cancel_clicked(self, interaction: discord.Interaction[Any]) -> None:
        try:
            await self._guarded(interaction)
            self.clear_items()
            self.stop()
            await interaction.response.edit_message(
                content="Edit cancelled.", embed=None, view=self
            )
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


class ConfirmActionView(discord.ui.View):
    def __init__(self, cog: "SeatWatchCog", action: str, actor_user_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.action = action
        self.actor_user_id = actor_user_id
        confirm = discord.ui.Button(
            label="Confirm",
            style=discord.ButtonStyle.danger,
            custom_id=f"amc:confirm:{action}:{actor_user_id}",
        )
        confirm.callback = self.confirm
        self.add_item(confirm)

    async def confirm(self, interaction: discord.Interaction[Any]) -> None:
        try:
            if int(interaction.user.id) != self.actor_user_id:
                raise InteractionDenied("Only the person who opened this confirmation can use it.")
            await self.cog.guard.require_operator(interaction)
            guild = interaction.guild
            assert guild is not None
            if self.action == "delete-all":
                count = await self.cog.repository.delete_all_subscriptions(
                    guild.id, interaction.user.id
                )
                message = f"Deleted {count} monitor{'s' if count != 1 else ''}."
            elif self.action == "disable":
                await self.cog.repository.disable_guild(guild.id, interaction.user.id)
                message = "AMC Seat Watch is disabled for this server."
            else:
                raise InteractionDenied("Unknown confirmation action.")
            await _safe_ephemeral(interaction, message)
            self.stop()
        except (InteractionDenied, WizardInputError) as exc:
            await _safe_ephemeral(interaction, str(exc))


class SeatWatchCog(commands.Cog):
    # Slash names are /amc and /amc-admin; the Python attributes stay `monitor`/
    # `monitor_admin` so the @monitor.command(...) decorators are untouched.
    monitor = app_commands.Group(
        name="amc",
        description="Create and manage AMC seat monitors",
        guild_only=True,
    )
    monitor_admin = app_commands.Group(
        name="amc-admin",
        description="Configure AMC Seat Watch for this server",
        guild_only=True,
    )

    def __init__(
        self,
        bot: commands.Bot,
        repository: DiscordRepository,
        settings: DiscordBotSettings,
    ):
        self.bot = bot
        self.repository = repository
        self.settings = settings
        self.guard = DiscordGuard(repository, settings)
        self.wizard = WizardController(repository)
        # One in-flight catalog poller per wizard session, so the loading card
        # updates itself instead of asking the user to tap Refresh.
        self._catalog_pollers: dict[str, asyncio.Task[None]] = {}

    async def cog_app_command_error(
        self, interaction: discord.Interaction[Any], error: app_commands.AppCommandError
    ) -> None:
        original = getattr(error, "original", error)
        if isinstance(original, (InteractionDenied, WizardInputError)):
            await _safe_ephemeral(interaction, str(original))
            return
        LOGGER.error(
            "Discord command failed command=%s error_type=%s",
            getattr(interaction.command, "qualified_name", "unknown"),
            type(original).__name__,
        )
        await _safe_ephemeral(interaction, "That command failed safely. Please try again.")

    @staticmethod
    def _wizard_embed(prompt: WizardPrompt) -> discord.Embed:
        return discord.Embed(
            title=prompt.title[:256],
            description=prompt.description[:4096],
            color=discord.Color.from_rgb(216, 180, 91),
        )

    async def send_wizard(
        self,
        interaction: discord.Interaction[Any],
        session: WizardSession,
        *,
        initial: bool = False,
    ) -> None:
        # Kick off the catalog lookup as soon as a lookup step is shown, so the
        # user gets a loading state instead of having to trigger it manually.
        # Idempotent, so it never resets an in-flight or completed lookup.
        if session.step in {WizardStep.THEATRES, WizardStep.MOVIES, WizardStep.FORMAT}:
            await self.wizard.retry_lookup(session)
        prompt = await self.wizard.prompt(session)
        embed = self._wizard_embed(prompt)
        view = WizardView(self, session, prompt)
        # One persistent ephemeral card: `/amc create` sends it once; every
        # later step edits that same message in place instead of spamming a new
        # ephemeral per step/refresh.
        if initial:
            await interaction.response.send_message(
                embed=embed, view=view, ephemeral=True, allowed_mentions=NO_MENTIONS
            )
        elif interaction.response.is_done():
            await interaction.edit_original_response(
                embed=embed, view=view, allowed_mentions=NO_MENTIONS
            )
        else:
            await interaction.response.edit_message(embed=embed, view=view)
        # If the card is showing a loading state, poll the lookup in the
        # background and edit this same message in place the moment it resolves.
        if prompt.loading:
            self._spawn_catalog_poller(interaction, session)

    # Catalog lookups run on the worker's serialized AMC lane; they usually
    # resolve in a few seconds. Poll a little longer than that before giving up.
    _CATALOG_POLL_INTERVAL = 1.5
    _CATALOG_POLL_ATTEMPTS = 20

    def _spawn_catalog_poller(
        self, interaction: discord.Interaction[Any], session: WizardSession
    ) -> None:
        existing = self._catalog_pollers.get(session.id)
        if existing is not None and not existing.done():
            existing.cancel()
        task = asyncio.create_task(self._await_catalog(interaction, session))
        self._catalog_pollers[session.id] = task

    async def _await_catalog(
        self, interaction: discord.Interaction[Any], session: WizardSession
    ) -> None:
        target_step = session.step
        try:
            for _ in range(self._CATALOG_POLL_ATTEMPTS):
                await asyncio.sleep(self._CATALOG_POLL_INTERVAL)
                current = await self.repository.get_wizard(session.id, session.guild_id)
                # The user cancelled, advanced, or the session expired — a newer
                # render owns the card now, so this poller must not touch it.
                if current is None or current.step != target_step:
                    return
                prompt = await self.wizard.prompt(current)
                if not prompt.loading:
                    await interaction.edit_original_response(
                        embed=self._wizard_embed(prompt),
                        view=WizardView(self, current, prompt),
                        allowed_mentions=NO_MENTIONS,
                    )
                    return
            # Still loading after the budget elapsed: surface a manual Refresh so
            # the user is never permanently stuck if the worker is down.
            current = await self.repository.get_wizard(session.id, session.guild_id)
            if current is None or current.step != target_step:
                return
            prompt = replace(
                await self.wizard.prompt(current),
                loading=False,
                description="AMC is taking longer than usual. Tap **Refresh** to check again.",
            )
            await interaction.edit_original_response(
                embed=self._wizard_embed(prompt),
                view=WizardView(self, current, prompt),
                allowed_mentions=NO_MENTIONS,
            )
        except asyncio.CancelledError:
            raise
        except discord.HTTPException:
            # The card was dismissed or the interaction token expired; nothing
            # left to update.
            return
        finally:
            if self._catalog_pollers.get(session.id) is asyncio.current_task():
                self._catalog_pollers.pop(session.id, None)

    @monitor.command(name="create", description="Create or resume a seat monitor")
    async def monitor_create(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        session = await self.wizard.start(guild.id, member.id)
        await self.send_wizard(interaction, session, initial=True)

    @monitor.command(name="list", description="List this server's seat monitors")
    async def monitor_list(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        guild, _ = self.guard.require_guild(interaction)
        subscriptions = await self.repository.list_subscriptions(guild.id)
        embed = discord.Embed(
            title="AMC Seat Watch monitors",
            color=discord.Color.from_rgb(216, 180, 91),
        )
        if not subscriptions:
            embed.description = "No monitors are configured."
        for subscription in subscriptions[:25]:
            state = "active" if subscription.enabled else "paused"
            title = f"{subscription.name or 'monitor'} · {subscription.id[:8]}"
            value = (
                f"{state} · {subscription.adjacent_seats} adjacent · "
                f"{subscription.seat_preset.value} · {subscription.format_name}\n"
                f"{', '.join(subscription.movies)[:600]}\n"
                f"Alerts: <#{subscription.destination_channel_id}> · id `{subscription.id}`"
            )
            embed.add_field(name=title[:256], value=value[:1024], inline=False)
        await interaction.response.send_message(
            embed=embed, ephemeral=True, allowed_mentions=NO_MENTIONS
        )

    async def ensure_destination_approved(
        self, guild: Any, actor_user_id: int, channel: Any
    ) -> None:
        """Validate a channel and approve it as a Destination if it isn't already,
        so picking an alert channel needs no separate /amc-admin destination-add."""
        validate_destination_channel(
            channel, expected_guild_id=guild.id, bot_member=guild.me
        )
        approved = {
            item.channel_id
            for item in await self.repository.list_destinations(guild.id)
            if item.enabled
        }
        if int(channel.id) not in approved:
            await self.repository.add_destination(
                Destination(guild.id, int(channel.id), getattr(channel, "name", "alerts")),
                actor_user_id,
            )

    @monitor.command(name="edit", description="Edit a monitor: name, seats, times, or alert channel")
    async def monitor_edit(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        guild, _ = self.guard.require_guild(interaction)
        subscriptions = list(await self.repository.list_subscriptions(guild.id))
        if not subscriptions:
            await _safe_ephemeral(interaction, "No monitors yet — run `/amc create`.")
            return
        view = MonitorEditView(self, subscriptions)
        await interaction.response.send_message(
            embed=view.render_embed(), view=view, ephemeral=True, allowed_mentions=NO_MENTIONS
        )

    async def _set_enabled(
        self, interaction: discord.Interaction[Any], monitor_id: str, enabled: bool
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        await self.repository.set_subscription_enabled(
            guild.id, monitor_id, member.id, enabled
        )
        await _safe_ephemeral(
            interaction, f"Monitor `{monitor_id}` is {'active' if enabled else 'paused'}."
        )

    @monitor.command(name="pause", description="Pause a monitor")
    async def monitor_pause(
        self, interaction: discord.Interaction[Any], monitor_id: str
    ) -> None:
        await self._set_enabled(interaction, monitor_id, False)

    @monitor.command(name="resume", description="Resume a monitor")
    async def monitor_resume(
        self, interaction: discord.Interaction[Any], monitor_id: str
    ) -> None:
        await self._set_enabled(interaction, monitor_id, True)

    @monitor.command(name="delete", description="Delete one monitor")
    async def monitor_delete(
        self, interaction: discord.Interaction[Any], monitor_id: str
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        await self.repository.delete_subscription(guild.id, monitor_id, member.id)
        await _safe_ephemeral(interaction, f"Deleted monitor `{monitor_id}`.")

    @monitor.command(name="delete-all", description="Delete all monitors in this server")
    async def monitor_delete_all(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        await interaction.response.send_message(
            "This permanently deletes every monitor in this server. Confirm within 60 seconds.",
            view=ConfirmActionView(self, "delete-all", interaction.user.id),
            ephemeral=True,
            allowed_mentions=NO_MENTIONS,
        )

    @monitor.command(name="test", description="Queue a clearly labelled test alert")
    async def monitor_test(
        self,
        interaction: discord.Interaction[Any],
        destination: discord.TextChannel | None = None,
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        approved = [
            item
            for item in await self.repository.list_destinations(guild.id)
            if item.enabled
        ]
        if destination is None:
            if not approved:
                raise WizardInputError("Add an approved destination first.")
            destination = guild.get_channel(approved[0].channel_id)
        if destination is None:
            raise WizardInputError("The destination channel is unavailable.")
        validate_destination_channel(
            destination, expected_guild_id=guild.id, bot_member=guild.me
        )
        if destination.id not in {item.channel_id for item in approved}:
            raise WizardInputError("That channel is not an approved destination.")
        await self.repository.enqueue_test_alert(guild.id, destination.id, member.id)
        await _safe_ephemeral(interaction, "Test alert queued.")

    @monitor.command(name="status", description="Show cadence, queue age, and capacity")
    async def monitor_status(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        guild, _ = self.guard.require_guild(interaction)
        status = await self.repository.monitor_status(guild.id)
        embed = discord.Embed(
            title="AMC Seat Watch status",
            color=discord.Color.green() if status.healthy else discord.Color.orange(),
        )
        cadence = (
            f"{status.status_cadence_seconds:.1f}s"
            if status.status_cadence_seconds is not None
            else "not measured"
        )
        queue_age = (
            f"{status.oldest_job_age_seconds:.1f}s"
            if status.oldest_job_age_seconds is not None
            else "empty"
        )
        capacity = (
            f"{status.capacity_percent:.0f}%"
            if status.capacity_percent is not None
            else "not measured"
        )
        cooldown = (
            discord.utils.format_dt(status.cooldown_until, style="R")
            if status.cooldown_until
            else "none"
        )
        embed.description = (
            f"**Worker:** {discord.utils.escape_markdown(status.worker_state)}\n"
            f"**Status cadence:** {cadence}\n"
            f"**Oldest job:** {queue_age}\n"
            f"**Capacity:** {capacity}\n"
            f"**Cooldown:** {cooldown}\n"
            f"**Active:** {status.active_subscriptions} monitors · "
            f"{status.active_showtimes} showtimes"
        )
        await interaction.response.send_message(
            embed=embed, ephemeral=True, allowed_mentions=NO_MENTIONS
        )

    @monitor_admin.command(name="setup", description="Enable the app and choose its operator role")
    async def admin_setup(
        self,
        interaction: discord.Interaction[Any],
        operator_role: discord.Role,
        destination: discord.TextChannel | None = None,
    ) -> None:
        await self.guard.require_bootstrap(interaction)
        guild, member = self.guard.require_guild(interaction)
        self._validate_role(guild, operator_role)
        if destination is not None:
            validate_destination_channel(
                destination, expected_guild_id=guild.id, bot_member=guild.me
            )
        await self.repository.setup_guild(guild.id, member.id, operator_role.id)
        if destination is not None:
            await self.repository.add_destination(
                Destination(guild.id, destination.id, destination.name), member.id
            )
        await _safe_ephemeral(
            interaction,
            f"AMC Seat Watch is enabled for <@&{operator_role.id}>."
            + (f" Alerts may be sent to <#{destination.id}>." if destination else ""),
        )

    @monitor_admin.command(name="destination-add", description="Approve an alert channel")
    async def admin_destination_add(
        self, interaction: discord.Interaction[Any], channel: discord.TextChannel
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        validate_destination_channel(channel, expected_guild_id=guild.id, bot_member=guild.me)
        await self.repository.add_destination(
            Destination(guild.id, channel.id, channel.name), member.id
        )
        await _safe_ephemeral(interaction, f"Approved <#{channel.id}> for alerts.")

    @monitor_admin.command(name="destination-remove", description="Remove an alert channel")
    async def admin_destination_remove(
        self, interaction: discord.Interaction[Any], channel: discord.TextChannel
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        if channel.guild.id != guild.id:
            raise InteractionDenied("Choose a channel from this server.")
        await self.repository.remove_destination(guild.id, channel.id, member.id)
        await _safe_ephemeral(interaction, f"Removed <#{channel.id}> from alert destinations.")

    @monitor_admin.command(name="access-role", description="Change the required operator role")
    async def admin_access_role(
        self, interaction: discord.Interaction[Any], role: discord.Role
    ) -> None:
        await self.guard.require_operator(interaction)
        guild, member = self.guard.require_guild(interaction)
        self._validate_role(guild, role)
        await self.repository.set_operator_role(guild.id, member.id, role.id)
        await _safe_ephemeral(interaction, f"Operator access now requires <@&{role.id}>.")

    @monitor_admin.command(name="status", description="Show server app configuration")
    async def admin_status(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        guild, _ = self.guard.require_guild(interaction)
        config = await self.repository.get_guild(guild.id)
        destinations = await self.repository.list_destinations(guild.id)
        channels = "\n".join(
            f"<#{item.channel_id}> — {'enabled' if item.enabled else 'disabled'}"
            for item in destinations
        ) or "none"
        embed = discord.Embed(
            title="AMC Seat Watch configuration",
            description=(
                f"**Enabled:** {'yes' if config and config.enabled else 'no'}\n"
                f"**Operator role:** <@&{config.operator_role_id}>\n"
                f"**Destinations:**\n{channels}"
            ),
            color=discord.Color.from_rgb(216, 180, 91),
        )
        await interaction.response.send_message(
            embed=embed, ephemeral=True, allowed_mentions=NO_MENTIONS
        )

    @monitor_admin.command(name="disable", description="Disable monitoring in this server")
    async def admin_disable(self, interaction: discord.Interaction[Any]) -> None:
        await self.guard.require_operator(interaction)
        await interaction.response.send_message(
            "This disables the app and all monitors in this server. Confirm within 60 seconds.",
            view=ConfirmActionView(self, "disable", interaction.user.id),
            ephemeral=True,
            allowed_mentions=NO_MENTIONS,
        )

    @staticmethod
    def _validate_role(guild: discord.Guild, role: discord.Role) -> None:
        if role.guild.id != guild.id:
            raise InteractionDenied("Choose a role from this server.")
        if role.is_default() or role.managed:
            raise InteractionDenied("Choose a normal, assignable role instead of @everyone.")


class SeatWatchBot(commands.Bot):
    def __init__(
        self,
        repository: DiscordRepository,
        settings: DiscordBotSettings,
    ):
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.repository = repository
        self.settings = settings
        self.dispatcher = UserOutboxDispatcher(
            self,
            repository,
            allowed_guild_ids=settings.allowed_guild_ids,
            poll_seconds=settings.outbox_poll_seconds,
            batch_size=settings.outbox_batch_size,
        )
        self.heartbeat = ServiceHeartbeatLoop(
            repository, interval_seconds=settings.heartbeat_seconds
        )

    async def _sync_guild_commands(self, guild_id: int) -> None:
        # Guild-scoped sync appears instantly (global sync can take ~1h). This is
        # a small, allowlisted beta, so publish the command set to every approved
        # guild rather than relying on slow global propagation.
        guild = discord.Object(id=guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

    async def setup_hook(self) -> None:
        await self.add_cog(SeatWatchCog(self, self.repository, self.settings))
        for guild_id in self.settings.allowed_guild_ids:
            try:
                await self._sync_guild_commands(guild_id)
            except discord.HTTPException:
                # The bot may not be in every allowlisted guild yet; on_guild_join
                # syncs the rest when it is invited.
                LOGGER.warning("Command sync skipped for guild", extra={"guild_id": guild_id})
        self.dispatcher.start()
        self.heartbeat.start()

    async def on_guild_join(self, guild: discord.Guild) -> None:
        if guild.id not in self.settings.allowed_guild_ids:
            LOGGER.warning("Leaving a guild outside the invite allowlist", extra={"guild_id": guild.id})
            await guild.leave()
            return
        # Publish commands to a freshly invited allowlisted guild without waiting
        # for a restart or global propagation.
        await self._sync_guild_commands(guild.id)

    async def close(self) -> None:
        await self.heartbeat.stop()
        await self.dispatcher.stop()
        await super().close()


def build_bot(
    repository: DiscordRepository,
    settings: DiscordBotSettings | None = None,
) -> SeatWatchBot:
    return SeatWatchBot(repository, settings or DiscordBotSettings.from_env())


def run_bot(
    repository: DiscordRepository,
    *,
    settings: DiscordBotSettings | None = None,
    token: str | None = None,
) -> None:
    token = token or os.environ.get("DISCORD_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")
    build_bot(repository, settings).run(token, log_handler=None)


__all__ = [
    "NO_MENTIONS",
    "SeatWatchBot",
    "SeatWatchCog",
    "ServiceHeartbeatLoop",
    "UserOutboxDispatcher",
    "build_bot",
    "run_bot",
]
