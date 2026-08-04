from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path
from unittest import mock

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260719_0001_initial_control_plane.py"


def load_initial_migration():
    spec = importlib.util.spec_from_file_location("amc_initial_control_plane", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_heartbeat_grants_and_policies_are_service_scoped() -> None:
    migration = load_initial_migration()
    assert "service_heartbeats" not in migration.BOT_MUTATE
    assert "service_heartbeats" not in migration.WORKER_MUTATE

    with mock.patch.object(migration.op, "execute") as execute:
        migration._set_ownership_and_grants()
    grant_sql = "\n".join(str(call.args[0]) for call in execute.call_args_list)
    assert (
        "REVOKE ALL ON TABLE service_heartbeats FROM amc_worker, amc_bot, amc_notifier"
        in grant_sql
    )
    assert (
        "GRANT SELECT, INSERT, UPDATE ON TABLE service_heartbeats "
        "TO amc_worker, amc_bot, amc_notifier"
        in grant_sql
    )
    assert "GRANT SELECT ON TABLE owner_incidents, owner_outbox TO amc_notifier" in grant_sql
    assert "GRANT UPDATE ON TABLE owner_outbox TO amc_notifier" in grant_sql
    assert "GRANT UPDATE ON TABLE owner_incidents" not in grant_sql
    notifier_grants = [
        str(call.args[0])
        for call in execute.call_args_list
        if "TO amc_notifier" in str(call.args[0])
    ]
    assert all("user_outbox" not in statement for statement in notifier_grants)

    with mock.patch.object(migration.op, "execute") as execute:
        migration._enable_service_heartbeat_rls()
    policy_sql = "\n".join(str(call.args[0]) for call in execute.call_args_list)
    assert "ALTER TABLE service_heartbeats FORCE ROW LEVEL SECURITY" in policy_sql
    assert "FOR SELECT TO amc_bot USING (service_name = 'amc-discord-bot')" in policy_sql
    assert "FOR INSERT TO amc_bot WITH CHECK (service_name = 'amc-discord-bot')" in policy_sql
    assert "FOR UPDATE TO amc_bot USING (service_name = 'amc-discord-bot')" in policy_sql
    assert "FOR SELECT TO amc_notifier USING (true)" in policy_sql
    assert "WITH CHECK (service_name = 'amc-owner-notifier')" in policy_sql


@pytest.mark.postgresql
def test_postgresql_bot_cannot_spoof_or_delete_other_service_heartbeats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    monkeypatch.setenv("AMC_DATABASE_URL", url)
    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM service_heartbeats"))
            connection.execute(
                text(
                    """
                    INSERT INTO service_heartbeats (
                        service_name, instance_id, status, details,
                        started_at, last_seen_at, expires_at
                    ) VALUES
                        ('amc-worker', 'worker-1', 'healthy', '{}'::jsonb,
                         now(), now(), now() + interval '2 minutes'),
                        ('amc-discord-bot', 'bot-1', 'healthy', '{}'::jsonb,
                         now(), now(), now() + interval '2 minutes'),
                        ('amc-owner-notifier', 'notifier-1', 'healthy', '{}'::jsonb,
                         now(), now(), now() + interval '2 minutes')
                    """
                )
            )

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_bot"))
            visible = set(
                connection.scalars(text("SELECT service_name FROM service_heartbeats"))
            )
            assert visible == {"amc-discord-bot"}
            assert (
                connection.execute(
                    text(
                        "UPDATE service_heartbeats SET status='spoofed' "
                        "WHERE service_name='amc-worker'"
                    )
                ).rowcount
                == 0
            )
            assert (
                connection.execute(
                    text(
                        "UPDATE service_heartbeats SET status='healthy' "
                        "WHERE service_name='amc-discord-bot'"
                    )
                ).rowcount
                == 1
            )

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_bot"))
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        """
                        INSERT INTO service_heartbeats (
                            service_name, instance_id, status, details,
                            started_at, last_seen_at, expires_at
                        ) VALUES (
                            'amc-worker-spoof', 'bot-1', 'healthy', '{}'::jsonb,
                            now(), now(), now() + interval '2 minutes'
                        )
                        """
                    )
                )

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_bot"))
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "DELETE FROM service_heartbeats "
                        "WHERE service_name='amc-discord-bot'"
                    )
                )

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_notifier"))
            visible = set(
                connection.scalars(text("SELECT service_name FROM service_heartbeats"))
            )
            assert visible == {
                "amc-worker",
                "amc-discord-bot",
                "amc-owner-notifier",
            }
            assert (
                connection.execute(
                    text(
                        "UPDATE service_heartbeats SET status='spoofed' "
                        "WHERE service_name='amc-worker'"
                    )
                ).rowcount
                == 0
            )
            assert (
                connection.execute(
                    text(
                        "UPDATE service_heartbeats SET status='healthy' "
                        "WHERE service_name='amc-owner-notifier'"
                    )
                ).rowcount
                == 1
            )
    finally:
        engine.dispose()


@pytest.mark.postgresql
def test_postgresql_runtime_roles_are_least_privilege_and_tenant_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not configured")
    monkeypatch.setenv("AMC_DATABASE_URL", url)
    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
    engine = create_engine(url)
    first_guild = uuid.uuid4()
    second_guild = uuid.uuid4()
    first_snowflake = 1_000_000_000_000_000 + first_guild.int % 8_000_000_000_000_000
    second_snowflake = 1_000_000_000_000_000 + second_guild.int % 8_000_000_000_000_000
    try:
        with engine.begin() as connection:
            for guild_id, snowflake, name in (
                (first_guild, first_snowflake, "First tenant"),
                (second_guild, second_snowflake, "Second tenant"),
            ):
                connection.execute(
                    text(
                        """
                        INSERT INTO guilds (
                            id, discord_guild_id, name, enabled,
                            created_by_discord_user_id
                        ) VALUES (:id, :snowflake, :name, true, :snowflake)
                        ON CONFLICT (discord_guild_id) DO NOTHING
                        """
                    ),
                    {"id": guild_id, "snowflake": snowflake, "name": name},
                )

            for role in ("amc_worker", "amc_bot", "amc_notifier"):
                unsafe = connection.execute(
                    text(
                        """
                        SELECT rolsuper, rolcreatedb, rolcreaterole, rolbypassrls
                          FROM pg_roles WHERE rolname = :role
                        """
                    ),
                    {"role": role},
                ).one()
                assert not any(unsafe)
                assert connection.scalar(
                    text("SELECT has_schema_privilege(:role, 'public', 'CREATE')"),
                    {"role": role},
                ) is False

            assert connection.scalar(
                text(
                    "SELECT has_table_privilege('amc_notifier', "
                    "'owner_incidents', 'SELECT')"
                )
            ) is True
            assert connection.scalar(
                text(
                    "SELECT has_table_privilege('amc_notifier', "
                    "'owner_incidents', 'UPDATE')"
                )
            ) is False
            assert connection.scalar(
                text(
                    "SELECT has_table_privilege('amc_notifier', "
                    "'owner_outbox', 'UPDATE')"
                )
            ) is True
            for table_name in (
                "subscriptions",
                "seat_observations",
                "proxy_health",
                "user_outbox",
            ):
                assert connection.scalar(
                    text("SELECT has_table_privilege('amc_notifier', :table, 'SELECT')"),
                    {"table": table_name},
                ) is False

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_bot"))
            connection.execute(
                text("SELECT set_config('app.guild_id', :guild_id, true)"),
                {"guild_id": str(first_guild)},
            )
            visible = set(connection.scalars(text("SELECT id FROM guilds")))
            assert visible == {first_guild}
            assert connection.execute(
                text("UPDATE guilds SET name='blocked' WHERE id=:guild_id"),
                {"guild_id": second_guild},
            ).rowcount == 0

        with engine.connect() as connection:
            connection.execute(text("SET ROLE amc_bot"))
            connection.execute(
                text("SELECT set_config('app.guild_id', :guild_id, true)"),
                {"guild_id": str(first_guild)},
            )
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        """
                        INSERT INTO destinations (
                            id, guild_id, discord_channel_id, label, enabled
                        ) VALUES (:id, :guild_id, 12345, 'cross-tenant', true)
                        """
                    ),
                    {"id": uuid.uuid4(), "guild_id": second_guild},
                )
    finally:
        engine.dispose()
