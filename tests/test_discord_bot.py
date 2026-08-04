import ast
import asyncio
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import discord

from amc_watch.discord_bot import (
    MonitorEditView,
    NO_MENTIONS,
    SeatWatchBot,
    SeatWatchCog,
    ServiceHeartbeatLoop,
    UserOutboxDispatcher,
)
from amc_watch.discord_models import (
    CatalogOption,
    Destination,
    GuildConfiguration,
    MonitorStatus,
    RecommendedRun,
    Seat,
    SeatPreset,
    SubscriptionSummary,
    UserAlert,
    WizardStep,
)
from amc_watch.discord_security import (
    DiscordBotSettings,
    DiscordGuard,
    InteractionDenied,
    validate_destination_channel,
)
from amc_watch.discord_wizard import WizardController, WizardInputError


class FakeRepository:
    def __init__(self):
        self.guilds = {}
        self.destinations = {}
        self.wizards = {}
        self.catalog = {}
        self.nearest = None  # nearest_theatres result (None = ZIP not geocoded)
        self.available_formats_result = []  # available_formats result
        self.lookups = []
        self.subscriptions = []
        self.created_by_key = {}
        self.projected = 15.0
        self.alerts = []
        self.delivered = []
        self.retried = []
        self.failed = []
        self.heartbeats = []
        self.claimed_for = ()
        self.updates = []
        self.added_destinations = []

    async def get_guild(self, guild_id):
        return self.guilds.get(guild_id)

    async def setup_guild(self, guild_id, actor_user_id, operator_role_id):
        config = GuildConfiguration(guild_id, True, operator_role_id, actor_user_id)
        self.guilds[guild_id] = config
        return config

    async def disable_guild(self, guild_id, actor_user_id):
        self.guilds[guild_id] = replace(self.guilds[guild_id], enabled=False)

    async def set_operator_role(self, guild_id, actor_user_id, role_id):
        self.guilds[guild_id] = replace(self.guilds[guild_id], operator_role_id=role_id)

    async def add_destination(self, destination, actor_user_id):
        self.destinations[(destination.guild_id, destination.channel_id)] = destination
        self.added_destinations.append(destination.channel_id)

    async def remove_destination(self, guild_id, channel_id, actor_user_id):
        self.destinations.pop((guild_id, channel_id), None)

    async def list_destinations(self, guild_id):
        return [value for (gid, _), value in self.destinations.items() if gid == guild_id]

    async def get_active_wizard(self, guild_id, user_id):
        return next(
            (
                value
                for value in self.wizards.values()
                if value.guild_id == guild_id
                and value.user_id == user_id
                and value.step not in {WizardStep.COMPLETE, WizardStep.CANCELLED}
            ),
            None,
        )

    async def get_wizard(self, session_id, guild_id):
        session = self.wizards.get(session_id)
        return session if session is not None and session.guild_id == guild_id else None

    async def save_wizard(self, session):
        self.wizards[session.id] = session

    async def queue_catalog_lookup(self, guild_id, session_id, kind, query):
        self.lookups.append((session_id, kind, query))

    async def catalog_options(self, guild_id, session_id, kind):
        return self.catalog.get((session_id, kind))

    async def nearest_theatres(self, zip_code, *, limit=25):
        return self.nearest

    async def available_formats(self, theatre_ids, movie_ids):
        return list(self.available_formats_result)

    async def active_subscription_counts(self, guild_id, user_id):
        guild = [item for item in self.subscriptions if item.guild_id == guild_id and item.enabled]
        user = [item for item in guild if item.owner_user_id == user_id]
        return len(user), len(guild)

    async def projected_status_cadence(self, draft):
        return self.projected

    async def create_subscription(self, draft, idempotency_key):
        if idempotency_key in self.created_by_key:
            return self.created_by_key[idempotency_key]
        summary = SubscriptionSummary(
            id=f"monitor-{len(self.subscriptions) + 1}",
            guild_id=draft.guild_id,
            owner_user_id=draft.owner_user_id,
            enabled=True,
            zip_code=draft.zip_code,
            theatres=draft.theatre_ids,
            movies=draft.movie_ids,
            format_name=draft.format_name,
            adjacent_seats=draft.adjacent_seats,
            seat_preset=draft.seat_preset,
            weekday_hours=f"{draft.weekday_start}-{draft.weekday_end}",
            weekend_hours=f"{draft.weekend_start}-{draft.weekend_end}",
            destination_channel_id=draft.destination_channel_id,
            name=(draft.name or " + ".join(draft.movie_ids)),
        )
        self.subscriptions.append(summary)
        self.created_by_key[idempotency_key] = summary
        return summary

    async def list_subscriptions(self, guild_id, owner_user_id=None):
        return [item for item in self.subscriptions if item.guild_id == guild_id]

    async def update_subscription(self, guild_id, subscription_id, actor_user_id, changes):
        self.updates.append((subscription_id, dict(changes)))
        idx = next(i for i, item in enumerate(self.subscriptions) if item.id == subscription_id)
        item = self.subscriptions[idx]
        mapping = dict(changes)
        if "seat_preset" in mapping:
            mapping["seat_preset"] = SeatPreset(mapping["seat_preset"])
        item = replace(item, **{k: v for k, v in mapping.items() if hasattr(item, k)})
        self.subscriptions[idx] = item
        return item

    async def set_subscription_enabled(self, guild_id, subscription_id, actor_user_id, enabled):
        return None

    async def delete_subscription(self, guild_id, subscription_id, actor_user_id):
        self.subscriptions = [item for item in self.subscriptions if item.id != subscription_id]

    async def delete_all_subscriptions(self, guild_id, actor_user_id):
        count = len([item for item in self.subscriptions if item.guild_id == guild_id])
        self.subscriptions = [item for item in self.subscriptions if item.guild_id != guild_id]
        return count

    async def monitor_status(self, guild_id):
        return MonitorStatus(True, "healthy", 15, 0, None, 25, 1, 10)

    async def write_service_heartbeat(self, service_name, state):
        self.heartbeats.append((service_name, state))

    async def enqueue_test_alert(self, guild_id, channel_id, actor_user_id):
        return None

    async def claim_user_alerts(self, allowed_guild_ids, limit):
        self.claimed_for = tuple(allowed_guild_ids)
        result, self.alerts = self.alerts[:limit], self.alerts[limit:]
        return result

    async def mark_user_alert_delivered(self, guild_id, outbox_id, discord_message_id):
        self.delivered.append((outbox_id, discord_message_id))

    async def retry_user_alert(self, guild_id, outbox_id, error_code, retry_at):
        self.retried.append((outbox_id, error_code, retry_at))

    async def fail_user_alert(
        self, guild_id, outbox_id, error_code, *, disable_destination
    ):
        self.failed.append((outbox_id, error_code, disable_destination))


class FakeChannel:
    def __init__(self, channel_id=20, guild_id=10, permissions=None):
        self.id = channel_id
        self.guild = SimpleNamespace(id=guild_id)
        self.name = "alerts"
        self.sent = []
        self._permissions = permissions or SimpleNamespace(
            view_channel=True, send_messages=True, embed_links=True
        )

    def permissions_for(self, member):
        return self._permissions

    async def send(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(id=999)


class FakeClient:
    def __init__(self, channel=None):
        self.channel = channel

    def get_channel(self, channel_id):
        return self.channel

    async def fetch_channel(self, channel_id):
        return self.channel

    async def wait_until_ready(self):
        return None


def interaction(guild_id=10, user_id=1, owner_id=1, role_ids=(), permissions=None):
    guild = SimpleNamespace(id=guild_id, owner_id=owner_id)
    member = SimpleNamespace(
        id=user_id,
        roles=[SimpleNamespace(id=value) for value in role_ids],
        guild_permissions=permissions
        or SimpleNamespace(administrator=False, manage_guild=False),
    )
    return SimpleNamespace(guild=guild, user=member)


class DiscordSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = FakeRepository()
        self.settings = DiscordBotSettings(frozenset({10}), frozenset({1}))
        self.guard = DiscordGuard(self.repo, self.settings)

    async def test_dm_and_uninvited_guild_are_denied(self):
        with self.assertRaises(InteractionDenied):
            self.guard.require_guild(SimpleNamespace(guild=None, user=SimpleNamespace(id=1)))
        with self.assertRaises(InteractionDenied):
            self.guard.require_guild(interaction(guild_id=11))

    async def test_bootstrap_requires_privilege_and_explicit_bootstrap_user(self):
        await self.guard.require_bootstrap(interaction(user_id=1, owner_id=1))
        with self.assertRaises(InteractionDenied):
            await self.guard.require_bootstrap(interaction(user_id=2, owner_id=2))
        with self.assertRaises(InteractionDenied):
            await self.guard.require_bootstrap(interaction(user_id=1, owner_id=2))

    async def test_subsequent_mutation_requires_exact_operator_role(self):
        self.repo.guilds[10] = GuildConfiguration(10, True, 99, 1)
        with self.assertRaises(InteractionDenied):
            await self.guard.require_operator(interaction(user_id=1, owner_id=1))
        await self.guard.require_operator(interaction(user_id=2, role_ids=(99,)))

    async def test_super_user_bypasses_operator_role(self):
        settings = DiscordBotSettings(frozenset({10}), super_user_ids=frozenset({7}))
        guard = DiscordGuard(self.repo, settings)
        self.repo.guilds[10] = GuildConfiguration(10, True, 99, 1)  # operator role 99
        # Super-user with no operator role is allowed; a non-super without it is not.
        await guard.require_operator(interaction(user_id=7, role_ids=()))
        with self.assertRaises(InteractionDenied):
            await guard.require_operator(interaction(user_id=8, role_ids=()))

    async def test_super_user_can_bootstrap_any_allowlisted_guild(self):
        settings = DiscordBotSettings(frozenset({10}), super_user_ids=frozenset({7}))
        guard = DiscordGuard(self.repo, settings)
        # Not the owner and not an admin, but a super-user → allowed to set up.
        await guard.require_bootstrap(interaction(user_id=7, owner_id=999))

    async def test_channel_permissions_are_all_required(self):
        validate_destination_channel(
            FakeChannel(), expected_guild_id=10, bot_member=object()
        )
        missing = SimpleNamespace(view_channel=True, send_messages=True, embed_links=False)
        with self.assertRaisesRegex(InteractionDenied, "Embed Links"):
            validate_destination_channel(
                FakeChannel(permissions=missing),
                expected_guild_id=10,
                bot_member=object(),
            )

    def test_empty_invite_allowlist_fails_closed(self):
        with self.assertRaises(ValueError):
            DiscordBotSettings(frozenset())


class WizardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = FakeRepository()
        self.controller = WizardController(self.repo)
        self.session = await self.controller.start(10, 1)

    async def test_full_wizard_persists_and_confirms(self):
        session = await self.controller.submit_text(self.session, "00000")
        self.assertEqual(session.step, WizardStep.THEATRES)
        self.assertEqual(self.repo.lookups[-1][1], "geocode")
        self.repo.nearest = [CatalogOption("example", "AMC Example 8")]
        session = await self.controller.submit_text(session, "1")
        self.repo.catalog[(session.id, "movies")] = [
            CatalogOption("feature-a", "Example Feature"),
            CatalogOption("feature-b", "Future Feature"),
        ]
        session = await self.controller.submit_text(session, "1,2")
        self.repo.available_formats_result = [CatalogOption("imax70mm", "IMAX 70MM")]
        session = await self.controller.submit_text(session, "1")
        session = await self.controller.submit_text(session, "2 center-back")
        session = await self.controller.submit_text(
            session, "17:00-23:00;10:00-23:00"
        )
        self.repo.destinations[(10, 20)] = Destination(10, 20, "alerts")
        session = await self.controller.choose_destination(session, 20)
        subscription = await self.controller.confirm(session)
        self.assertEqual(subscription.movies, ("feature-a", "feature-b"))
        self.assertEqual(subscription.adjacent_seats, 2)
        self.assertEqual(subscription.seat_preset, SeatPreset.CENTER_BACK)
        self.assertEqual(self.repo.wizards[session.id].step, WizardStep.COMPLETE)
        duplicate = await self.controller.confirm(session)
        self.assertEqual(duplicate.id, subscription.id)
        self.assertEqual(len(self.repo.subscriptions), 1)

    async def test_active_session_resumes(self):
        same = await self.controller.start(10, 1)
        self.assertEqual(same.id, self.session.id)

    async def test_movie_step_can_search_future_title_through_worker_lookup(self):
        session = await self.controller.submit_text(self.session, "00000")
        self.repo.nearest = [CatalogOption("t", "Theatre")]
        session = await self.controller.submit_text(session, "1")
        self.repo.catalog[(session.id, "movies")] = []

        session = await self.controller.submit_text(session, "search: Example Feature")

        self.assertEqual(session.step, WizardStep.MOVIES)
        self.assertEqual(session.data["movie_search"], "Example Feature")
        self.assertEqual(
            self.repo.lookups[-1],
            (session.id, "movie-search", {"title": "Example Feature"}),
        )
        self.repo.catalog[(session.id, "movie-search")] = [
            CatalogOption("example-feature", "Example Feature")
        ]
        prompt = await self.controller.prompt(session)
        self.assertEqual([option.id for option in prompt.options], ["example-feature"])
        session = await self.controller.submit_text(session, "1")
        self.assertEqual(session.data["movie_ids"], ("example-feature",))

    async def _advance_to_seats(self):
        session = await self.controller.submit_text(self.session, "00000")
        self.repo.nearest = [CatalogOption("t", "T")]
        session = await self.controller.submit_text(session, "t")
        self.repo.catalog[(session.id, "movies")] = [CatalogOption("m", "M")]
        session = await self.controller.submit_text(session, "m")
        self.repo.available_formats_result = [CatalogOption("f", "F")]
        session = await self.controller.submit_text(session, "f")
        self.assertEqual(session.step, WizardStep.SEATS)
        return session

    async def test_native_seat_and_time_selection(self):
        session = await self._advance_to_seats()
        # Picks can arrive in any order and stay on the SEATS step until Continue.
        session = await self.controller.set_seat_preset(session, "center-back")
        self.assertEqual(session.step, WizardStep.SEATS)
        session = await self.controller.set_seat_count(session, 3)
        self.assertEqual(session.step, WizardStep.SEATS)
        session = await self.controller.finish_seats(session)
        self.assertEqual(session.step, WizardStep.TIME_WINDOWS)
        self.assertEqual(session.data["adjacent_seats"], 3)
        self.assertEqual(session.data["seat_preset"], "center-back")

        session = await self.controller.choose_time_window_preset(
            session, "evenings-matinees"
        )
        self.assertEqual(session.step, WizardStep.DESTINATION)
        self.assertEqual(session.data["weekday_start"], "17:00")
        self.assertEqual(session.data["weekday_end"], "23:00")
        self.assertEqual(session.data["weekend_start"], "10:00")
        self.assertEqual(session.data["weekend_end"], "23:00")

    async def test_finish_seats_requires_both_picks(self):
        session = await self._advance_to_seats()
        with self.assertRaises(WizardInputError):
            await self.controller.finish_seats(session)
        session = await self.controller.set_seat_count(session, 2)
        with self.assertRaises(WizardInputError):
            await self.controller.finish_seats(session)

    async def test_native_inputs_are_validated(self):
        session = await self._advance_to_seats()
        with self.assertRaises(WizardInputError):
            await self.controller.set_seat_count(session, 9)
        with self.assertRaises(WizardInputError):
            await self.controller.set_seat_preset(session, "front-left")
        session = await self.controller.set_seat_count(session, 2)
        session = await self.controller.set_seat_preset(session, "center")
        session = await self.controller.finish_seats(session)
        with self.assertRaises(WizardInputError):
            await self.controller.choose_time_window_preset(session, "nope")

    async def _walk_to_preview(self):
        session = await self.controller.submit_text(self.session, "00000")
        self.repo.nearest = [CatalogOption("example", "AMC Example")]
        session = await self.controller.submit_text(session, "example")
        self.repo.catalog[(session.id, "movies")] = [CatalogOption("feature-a", "Example Feature")]
        session = await self.controller.submit_text(session, "feature-a")
        self.repo.available_formats_result = [CatalogOption("imax70mm", "IMAX 70MM")]
        session = await self.controller.submit_text(session, "imax70mm")
        session = await self.controller.set_seat_count(session, 2)
        session = await self.controller.set_seat_preset(session, "center")
        session = await self.controller.finish_seats(session)
        session = await self.controller.choose_time_window_preset(session, "evenings")
        self.repo.destinations[(10, 20)] = Destination(10, 20, "alerts")
        session = await self.controller.choose_destination(session, 20)
        self.assertEqual(session.step, WizardStep.PREVIEW)
        return session

    async def test_back_navigation_preserves_data_and_forward_gating(self):
        session = await self._walk_to_preview()
        session = await self.controller.go_to(session, WizardStep.THEATRES)
        self.assertEqual(session.step, WizardStep.THEATRES)
        # Everything entered later is preserved when navigating back.
        self.assertEqual(tuple(session.data["theatre_ids"]), ("example",))
        self.assertEqual(tuple(session.data["movie_ids"]), ("feature-a",))
        self.assertEqual(session.data["format_name"], "imax70mm")
        # Forward is allowed because every earlier step is complete.
        session = await self.controller.go_forward(session)
        self.assertEqual(session.step, WizardStep.MOVIES)

    async def test_forward_blocked_on_incomplete_step(self):
        session = await self.controller.submit_text(self.session, "00000")  # -> THEATRES
        with self.assertRaises(WizardInputError):
            await self.controller.go_forward(session)
        with self.assertRaises(WizardInputError):
            await self.controller.go_to(session, WizardStep.SEATS)
        session = await self.controller.go_back(session)  # THEATRES -> ZIP
        self.assertEqual(session.step, WizardStep.ZIP_CODE)

    async def test_changed_theatre_invalidates_downstream(self):
        session = await self._walk_to_preview()
        session = await self.controller.go_to(session, WizardStep.THEATRES)
        self.repo.nearest = [
            CatalogOption("example", "AMC Example"),
            CatalogOption("empire", "AMC Empire"),
        ]
        session = await self.controller.submit_text(session, "empire")
        self.assertEqual(tuple(session.data["theatre_ids"]), ("empire",))
        self.assertNotIn("movie_ids", session.data)  # downstream dropped
        self.assertNotIn("format_name", session.data)
        self.assertEqual(self.repo.lookups[-1][1], "movies")
        self.assertEqual(tuple(self.repo.lookups[-1][2]["theatre_ids"]), ("empire",))

    async def test_custom_name_and_fallback(self):
        session = await self._walk_to_preview()
        session = await self.controller.set_name(session, "IMAX Fridays")
        self.assertEqual(session.data["name"], "IMAX Fridays")
        subscription = await self.controller.confirm(session)
        self.assertEqual(subscription.name, "IMAX Fridays")

    async def test_blank_name_falls_back_to_derived(self):
        session = await self._walk_to_preview()
        session = await self.controller.set_name(session, "Temp")
        session = await self.controller.set_name(session, "   ")  # clear
        self.assertNotIn("name", session.data)
        subscription = await self.controller.confirm(session)
        self.assertEqual(subscription.name, "feature-a")  # derived from movie ids

    async def test_catalog_pending_and_capacity_admission_are_safe(self):
        session = await self.controller.submit_text(self.session, "00000")
        with self.assertRaises(WizardInputError):
            await self.controller.submit_text(session, "1")
        self.repo.nearest = [CatalogOption("t", "T")]
        session = await self.controller.submit_text(session, "1")
        self.repo.catalog[(session.id, "movies")] = [CatalogOption("m", "M")]
        session = await self.controller.submit_text(session, "1")
        self.repo.available_formats_result = [CatalogOption("f", "F")]
        session = await self.controller.submit_text(session, "1")
        session = await self.controller.submit_text(session, "2 center")
        session = await self.controller.submit_text(session, "17:00-23:00;10:00-23:00")
        self.repo.destinations[(10, 20)] = Destination(10, 20, "alerts")
        session = await self.controller.choose_destination(session, 20)
        self.repo.projected = 61
        with self.assertRaisesRegex(WizardInputError, "above 60 seconds"):
            await self.controller.confirm(session)


class DiscordApplicationTests(unittest.IsolatedAsyncioTestCase):
    def test_every_required_command_is_registered(self):
        self.assertEqual(SeatWatchCog.monitor.name, "amc")
        self.assertEqual(SeatWatchCog.monitor_admin.name, "amc-admin")
        self.assertEqual(
            {command.name for command in SeatWatchCog.monitor.commands},
            {"create", "list", "edit", "pause", "resume", "delete", "delete-all", "test", "status"},
        )
        self.assertEqual(
            {command.name for command in SeatWatchCog.monitor_admin.commands},
            {"setup", "destination-add", "destination-remove", "access-role", "status", "disable"},
        )

    def test_bot_requests_only_standard_guild_intent(self):
        bot = SeatWatchBot(FakeRepository(), DiscordBotSettings(frozenset({10})))
        self.assertTrue(bot.intents.guilds)
        self.assertFalse(bot.intents.message_content)
        self.assertFalse(bot.intents.members)

    def test_discord_process_has_no_amc_client_import(self):
        for filename in (
            "discord_bot.py",
            "discord_models.py",
            "discord_security.py",
            "discord_wizard.py",
        ):
            tree = ast.parse((Path("amc_watch") / filename).read_text())
            imports = {
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for alias in node.names
            }
            imports |= {
                node.module or ""
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
            }
            self.assertNotIn("amc_seat_monitor", imports)
            self.assertNotIn("amc_watch.amc", imports)

    async def test_outbox_delivers_without_mentions_and_marks_once(self):
        repo = FakeRepository()
        seats = (
            Seat(7, 20, "H21", True),
            Seat(7, 21, "H22", True),
        )
        repo.alerts.append(
            UserAlert(
                "out-1",
                10,
                20,
                "Example Feature",
                "AMC Example 8",
                "7:00 PM",
                "IMAX 70MM",
                2,
                SeatPreset.CENTER_BACK,
                "https://www.amctheatres.com/showtimes/123",
                seats,
                (RecommendedRun(("H21", "H22"), ((7, 20), (7, 21)), 99),),
            )
        )
        channel = FakeChannel()
        dispatcher = UserOutboxDispatcher(
            FakeClient(channel), repo, allowed_guild_ids=frozenset({10})
        )
        self.assertEqual(await dispatcher.dispatch_once(), 1)
        self.assertEqual(repo.claimed_for, (10,))
        self.assertEqual(repo.delivered, [("out-1", 999)])
        self.assertEqual(channel.sent[0]["allowed_mentions"], NO_MENTIONS)
        self.assertEqual(channel.sent[0]["view"].children[0].label, "Open AMC")

    async def test_service_heartbeat_uses_stable_name(self):
        repo = FakeRepository()
        heartbeat = ServiceHeartbeatLoop(repo, interval_seconds=30)
        task = asyncio.create_task(heartbeat.run())
        await asyncio.sleep(0)
        self.assertEqual(repo.heartbeats[0], ("amc-discord-bot", "healthy"))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class FakeResponse:
    def __init__(self):
        self._done = False
        self.sent = []
        self.edits = []

    def is_done(self):
        return self._done

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        self._done = True

    async def edit_message(self, **kwargs):
        self.edits.append(kwargs)
        self._done = True


class FakeInteraction:
    def __init__(self, guild_id=10, user_id=1):
        self.response = FakeResponse()
        self.original_edits = []
        self.guild = SimpleNamespace(id=guild_id, owner_id=user_id)
        self.user = SimpleNamespace(id=user_id)

    async def edit_original_response(self, **kwargs):
        self.original_edits.append(kwargs)


def _selects(view):
    return [c for c in view.children if isinstance(c, discord.ui.Select)]


def _labels(view):
    return {getattr(c, "label", None) for c in view.children}


class WizardAutoLoadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = FakeRepository()
        self.settings = DiscordBotSettings(frozenset({10}), frozenset({1}))
        self.cog = SeatWatchCog(SimpleNamespace(), self.repo, self.settings)
        self.cog._CATALOG_POLL_INTERVAL = 0.0
        session = await self.cog.wizard.start(10, 1)
        # Advancing past ZIP lands on THEATRES with a catalog lookup pending.
        self.session = await self.cog.wizard.submit_text(session, "00000")

    async def test_loading_card_has_no_refresh_and_auto_resolves(self):
        inter = FakeInteraction()
        await self.cog.send_wizard(inter, self.session)

        # The loading card carries no Select and no manual Refresh — just Cancel.
        loading_view = inter.response.edits[-1]["view"]
        self.assertEqual(_selects(loading_view), [])
        self.assertNotIn("Refresh", _labels(loading_view))
        self.assertIn(self.session.id, self.cog._catalog_pollers)

        # The ZIP geocode resolves; nearest theatres appear and the poller edits
        # the same card in place.
        self.repo.nearest = [CatalogOption("example", "AMC Example 8")]
        await self.cog._catalog_pollers[self.session.id]

        final_view = inter.original_edits[-1]["view"]
        self.assertEqual(len(_selects(final_view)), 1)
        self.assertNotIn("Refresh", _labels(final_view))
        # The poller cleans up after itself.
        self.assertNotIn(self.session.id, self.cog._catalog_pollers)

    async def test_poller_falls_back_to_refresh_on_timeout(self):
        self.cog._CATALOG_POLL_ATTEMPTS = 1
        inter = FakeInteraction()
        await self.cog.send_wizard(inter, self.session)

        # Lookup never resolves within budget → surface a manual Refresh.
        await self.cog._catalog_pollers[self.session.id]

        timed_out_view = inter.original_edits[-1]["view"]
        self.assertIn("Refresh", _labels(timed_out_view))
        self.assertIn(
            "longer than usual", inter.original_edits[-1]["embed"].description
        )

    async def test_seats_step_renders_selects_not_a_text_modal(self):
        from amc_watch.discord_bot import WizardView

        session = self.session.advance(WizardStep.SEATS)
        prompt = await self.cog.wizard.prompt(session)
        # No free-text field means no "type it in a modal" Continue path.
        self.assertIsNone(prompt.field_label)
        view = WizardView(self.cog, session, prompt)
        self.assertEqual(len(_selects(view)), 2)
        # The shared nav row replaces the old per-step Continue button.
        self.assertNotIn("Continue", _labels(view))
        self.assertIn("‹ Back", _labels(view))

    async def test_time_windows_step_renders_preset_select_and_custom(self):
        from amc_watch.discord_bot import WizardView

        session = self.session.advance(WizardStep.TIME_WINDOWS)
        prompt = await self.cog.wizard.prompt(session)
        self.assertIsNone(prompt.field_label)
        view = WizardView(self.cog, session, prompt)
        self.assertEqual(len(_selects(view)), 1)
        self.assertIn("Custom…", _labels(view))

    async def test_poller_skips_edit_when_user_advanced(self):
        inter = FakeInteraction()
        await self.cog.send_wizard(inter, self.session)
        task = self.cog._catalog_pollers[self.session.id]

        # User moved on to MOVIES before the theatres poll fired; the stale
        # poller must not clobber the newer card.
        self.repo.nearest = [CatalogOption("t", "T")]
        advanced = await self.cog.wizard.submit_text(self.session, "t")
        self.assertEqual(advanced.step, WizardStep.MOVIES)

        await task
        self.assertEqual(inter.original_edits, [])


class MonitorEditFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = FakeRepository()
        self.repo.guilds[10] = GuildConfiguration(10, True, 99, 1)  # operator role 99
        self.settings = DiscordBotSettings(frozenset({10}), frozenset({1}))
        self.cog = SeatWatchCog(SimpleNamespace(), self.repo, self.settings)
        self.repo.subscriptions.append(
            SubscriptionSummary(
                id="mon-1", guild_id=10, owner_user_id=1, enabled=True, zip_code="00000",
                theatres=("example",), movies=("feature-a",), format_name="IMAX 70MM",
                adjacent_seats=2, seat_preset=SeatPreset.CENTER,
                weekday_hours="17:00-23:00", weekend_hours="10:00-23:00",
                destination_channel_id=20, name="Example",
            )
        )

    def _interaction(self, values=None):
        resp = FakeResponse()
        guild = SimpleNamespace(id=10, owner_id=1, me=object())
        member = SimpleNamespace(
            id=1,
            roles=[SimpleNamespace(id=99)],
            guild_permissions=SimpleNamespace(administrator=False, manage_guild=False),
        )
        inter = SimpleNamespace(
            response=resp, guild=guild, user=member,
            data={"values": list(values or [])}, original_edits=[],
        )

        async def edit_original(**kwargs):
            inter.original_edits.append(kwargs)

        inter.edit_original_response = edit_original
        return inter

    async def test_edit_seat_count_updates_and_returns_to_field_picker(self):
        view = MonitorEditView(self.cog, list(self.repo.subscriptions))
        await view.monitor_selected(self._interaction(values=["mon-1"]))
        self.assertEqual(view.selected_id, "mon-1")
        await view.field_selected(self._interaction(values=["adjacent_seats"]))
        self.assertEqual(view.selected_field, "adjacent_seats")
        await view.seat_count_selected(self._interaction(values=["4"]))
        self.assertIn(("mon-1", {"adjacent_seats": 4}), self.repo.updates)
        self.assertIsNone(view.selected_field)  # back to the field picker for more edits

    async def test_edit_back_navigates_field_then_monitor(self):
        view = MonitorEditView(self.cog, list(self.repo.subscriptions))
        await view.monitor_selected(self._interaction(values=["mon-1"]))
        await view.field_selected(self._interaction(values=["seat_preset"]))
        await view.back_clicked(self._interaction())  # value -> field picker
        self.assertEqual(view.selected_id, "mon-1")
        self.assertIsNone(view.selected_field)
        await view.back_clicked(self._interaction())  # field -> monitor picker
        self.assertIsNone(view.selected_id)

    async def test_ensure_destination_approved_is_idempotent(self):
        guild = SimpleNamespace(id=10, me=object())
        channel = FakeChannel(channel_id=555, guild_id=10)
        await self.cog.ensure_destination_approved(guild, 1, channel)
        self.assertIn(555, self.repo.added_destinations)
        self.repo.added_destinations.clear()
        await self.cog.ensure_destination_approved(guild, 1, channel)
        self.assertEqual(self.repo.added_destinations, [])  # already approved


class BotCommandSyncTests(unittest.IsolatedAsyncioTestCase):
    def _bot(self, allowed):
        bot = SeatWatchBot(FakeRepository(), DiscordBotSettings(frozenset(allowed)))
        calls = []
        bot.tree.copy_global_to = lambda *, guild: calls.append(("copy", guild.id))

        async def fake_sync(*, guild=None):
            calls.append(("sync", guild.id if guild is not None else None))

        bot.tree.sync = fake_sync
        return bot, calls

    async def test_sync_publishes_to_a_guild(self):
        bot, calls = self._bot({10, 11})
        await bot._sync_guild_commands(10)
        self.assertEqual(calls, [("copy", 10), ("sync", 10)])

    async def test_guild_join_syncs_allowlisted_and_leaves_others(self):
        bot, _ = self._bot({10})
        synced, left = [], []

        async def fake_sync_guild(gid):
            synced.append(gid)

        bot._sync_guild_commands = fake_sync_guild

        async def leave():
            left.append(True)

        await bot.on_guild_join(SimpleNamespace(id=10, leave=leave))
        await bot.on_guild_join(SimpleNamespace(id=999, leave=leave))
        self.assertEqual(synced, [10])  # only the allowlisted guild
        self.assertEqual(left, [True])  # only the uninvited guild left


if __name__ == "__main__":
    unittest.main()
