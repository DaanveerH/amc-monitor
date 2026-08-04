"""Invite, authorization, and Discord destination checks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

import discord

from .discord_models import DiscordRepository, GuildConfiguration


class InteractionDenied(RuntimeError):
    """A safe, user-facing rejection for an interaction."""


def _parse_ids(raw: str) -> frozenset[int]:
    values: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError as exc:
            raise ValueError("Discord ID allowlists must contain integers") from exc
        if value <= 0:
            raise ValueError("Discord IDs must be positive")
        values.add(value)
    return frozenset(values)


@dataclass(frozen=True, slots=True)
class DiscordBotSettings:
    allowed_guild_ids: frozenset[int]
    bootstrap_user_ids: frozenset[int] = frozenset()
    # Discord user IDs that may run every command in any allowlisted guild,
    # bypassing the per-guild operator role (e.g. the app owner).
    super_user_ids: frozenset[int] = frozenset()
    test_guild_id: int | None = None
    outbox_poll_seconds: float = 2.0
    outbox_batch_size: int = 10
    heartbeat_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.allowed_guild_ids:
            raise ValueError("DISCORD_ALLOWED_GUILD_IDS must not be empty")
        if self.test_guild_id is not None and self.test_guild_id not in self.allowed_guild_ids:
            raise ValueError("DISCORD_TEST_GUILD_ID must be in DISCORD_ALLOWED_GUILD_IDS")
        if not 0.5 <= self.outbox_poll_seconds <= 60:
            raise ValueError("Outbox polling must be between 0.5 and 60 seconds")
        if not 1 <= self.outbox_batch_size <= 100:
            raise ValueError("Outbox batch size must be between 1 and 100")
        if not 5 <= self.heartbeat_seconds <= 300:
            raise ValueError("Service heartbeat must be between 5 and 300 seconds")

    @classmethod
    def from_env(cls) -> "DiscordBotSettings":
        allowed = _parse_ids(os.environ.get("DISCORD_ALLOWED_GUILD_IDS", ""))
        bootstrap = _parse_ids(os.environ.get("DISCORD_BOOTSTRAP_USER_IDS", ""))
        supers = _parse_ids(os.environ.get("DISCORD_SUPER_USER_IDS", ""))
        test_raw = os.environ.get("DISCORD_TEST_GUILD_ID", "").strip()
        return cls(
            allowed_guild_ids=allowed,
            bootstrap_user_ids=bootstrap,
            super_user_ids=supers,
            test_guild_id=int(test_raw) if test_raw else None,
            outbox_poll_seconds=float(os.environ.get("DISCORD_OUTBOX_POLL_SECONDS", "2")),
            outbox_batch_size=int(os.environ.get("DISCORD_OUTBOX_BATCH_SIZE", "10")),
            heartbeat_seconds=float(os.environ.get("DISCORD_HEARTBEAT_SECONDS", "30")),
        )


class DiscordGuard:
    """Centralizes authorization so every command gets the same policy."""

    def __init__(self, repository: DiscordRepository, settings: DiscordBotSettings):
        self.repository = repository
        self.settings = settings

    def require_guild(self, interaction: discord.Interaction[Any]) -> tuple[Any, Any]:
        guild = interaction.guild
        member = interaction.user
        if guild is None:
            raise InteractionDenied("AMC Seat Watch commands are available only in a server.")
        if guild.id not in self.settings.allowed_guild_ids:
            raise InteractionDenied("This server is not in the AMC Seat Watch beta.")
        return guild, member

    async def require_bootstrap(
        self, interaction: discord.Interaction[Any]
    ) -> GuildConfiguration | None:
        guild, member = self.require_guild(interaction)
        config = await self.repository.get_guild(guild.id)
        member_id = int(member.id)
        # A global super-user may set up any allowlisted guild.
        if member_id in self.settings.super_user_ids:
            return config
        role_ids = {int(role.id) for role in getattr(member, "roles", ())}
        permissions = getattr(member, "guild_permissions", None)
        privileged = bool(
            member_id == int(guild.owner_id)
            or getattr(permissions, "administrator", False)
            or getattr(permissions, "manage_guild", False)
            or (config and config.operator_role_id in role_ids)
        )
        if not privileged:
            raise InteractionDenied(
                "Only the server owner, a Manage Server administrator, or the configured "
                "operator role can set up AMC Seat Watch."
            )
        # Before the first setup, an optional user-ID allowlist prevents a different
        # administrator in an invited guild from claiming the installation.
        if (
            config is None
            and self.settings.bootstrap_user_ids
            and member_id not in self.settings.bootstrap_user_ids
        ):
            raise InteractionDenied("You are not an approved bootstrap user for this beta.")
        return config

    async def require_operator(
        self, interaction: discord.Interaction[Any]
    ) -> GuildConfiguration:
        guild, member = self.require_guild(interaction)
        config = await self.repository.get_guild(guild.id)
        if config is None or not config.enabled:
            raise InteractionDenied("AMC Seat Watch has not been enabled in this server.")
        # A global super-user may run every command in any enabled allowlisted
        # guild, bypassing the per-guild operator role.
        if int(member.id) in self.settings.super_user_ids:
            return config
        if config.operator_role_id is None:
            raise InteractionDenied("An operator role must be configured first.")
        role_ids = {int(role.id) for role in getattr(member, "roles", ())}
        if config.operator_role_id not in role_ids:
            raise InteractionDenied("The configured AMC Seat Watch operator role is required.")
        return config


def validate_destination_channel(
    channel: Any,
    *,
    expected_guild_id: int,
    bot_member: Any,
) -> None:
    """Reject cross-guild or non-writable delivery destinations."""

    guild = getattr(channel, "guild", None)
    if guild is None or int(guild.id) != expected_guild_id:
        raise InteractionDenied("Choose a text channel from this server.")
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        # Tests and adapters may use a duck-typed channel.  It must at least expose
        # permissions_for and send; real Discord objects remain restricted above.
        if not callable(getattr(channel, "permissions_for", None)) or not callable(
            getattr(channel, "send", None)
        ):
            raise InteractionDenied("Choose a writable text channel.")
    permissions = channel.permissions_for(bot_member)
    missing = [
        label
        for attribute, label in (
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
        )
        if not getattr(permissions, attribute, False)
    ]
    if missing:
        raise InteractionDenied(
            "I need these permissions in that channel: " + ", ".join(missing) + "."
        )


def ids_csv(values: Iterable[int]) -> str:
    """Stable formatting for diagnostics without exposing any secrets."""

    return ",".join(str(value) for value in sorted(set(values)))


__all__ = [
    "DiscordBotSettings",
    "DiscordGuard",
    "InteractionDenied",
    "ids_csv",
    "validate_destination_channel",
]
