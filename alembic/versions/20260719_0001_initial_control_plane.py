"""Initial PostgreSQL control plane, roles, grants, and tenant RLS.

Revision ID: 20260719_0001
Revises: None
Create Date: 2026-07-19
"""

from __future__ import annotations

from collections.abc import Iterable

from alembic import op

from amc_watch.db.models import Base


revision = "20260719_0001"
down_revision = None
branch_labels = None
depends_on = None


ROLE_NAMES = ("amc_owner", "amc_migrator", "amc_worker", "amc_bot", "amc_notifier")

TENANT_TABLES = (
    "guilds",
    "guild_access_roles",
    "destinations",
    "subscriptions",
    "subscription_movies",
    "subscription_theatres",
    "subscription_formats",
    "availability_edges",
    "user_outbox",
    "user_deliveries",
    "wizard_sessions",
    "catalog_lookups",
    "audit_log",
)

BOT_MUTATE = (
    "guilds",
    "guild_access_roles",
    "destinations",
    "subscriptions",
    "subscription_movies",
    "subscription_theatres",
    "subscription_formats",
    "user_outbox",
    "user_deliveries",
    "wizard_sessions",
    "catalog_lookups",
    "audit_log",
)
BOT_READ = (
    "movies",
    "theatres",
    "presentation_formats",
    "showtimes",
    "availability_edges",
    "request_gate_state",
)
WORKER_MUTATE = (
    "movies",
    "theatres",
    "presentation_formats",
    "selectable_dates",
    "discovery_targets",
    "showtimes",
    "status_observations",
    "seat_observations",
    "monitor_jobs",
    "request_gate_state",
    "proxy_health",
    "availability_edges",
    "user_outbox",
    "owner_incidents",
    "owner_outbox",
    "catalog_lookups",
    "audit_log",
)
WORKER_READ = (
    "guilds",
    "guild_access_roles",
    "destinations",
    "subscriptions",
    "subscription_movies",
    "subscription_theatres",
    "subscription_formats",
    "wizard_sessions",
)

DROP_ORDER = (
    "audit_log",
    "service_heartbeats",
    "catalog_lookups",
    "wizard_sessions",
    "owner_outbox",
    "owner_incidents",
    "user_deliveries",
    "user_outbox",
    "availability_edges",
    "proxy_health",
    "request_gate_state",
    "monitor_jobs",
    "seat_observations",
    "status_observations",
    "showtimes",
    "discovery_targets",
    "selectable_dates",
    "subscription_formats",
    "subscription_theatres",
    "subscription_movies",
    "subscriptions",
    "presentation_formats",
    "theatres",
    "movies",
    "destinations",
    "guild_access_roles",
    "guilds",
)


def _quoted(items: Iterable[str]) -> str:
    return ", ".join(f'"{item}"' for item in items)


def _create_roles() -> None:
    for role in ROLE_NAMES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
              END IF;
            END
            $$
            """
        )
    # Membership grants are handled in alembic/env.py (autocommit phase) so
    # they commit before this transaction needs them; see env.py for why.


def _set_ownership_and_grants() -> None:
    # Schema grants must precede ownership transfer: PostgreSQL 15+ requires
    # the *new* owner role to hold CREATE on the schema before ALTER TABLE
    # ... OWNER TO is accepted.
    op.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    op.execute("GRANT CREATE, USAGE ON SCHEMA public TO amc_owner, amc_migrator")
    for table in Base.metadata.sorted_tables:
        op.execute(f'ALTER TABLE "{table.name}" OWNER TO amc_owner')
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC")
    op.execute("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC")
    for role in ("amc_migrator", "amc_worker", "amc_bot", "amc_notifier"):
        op.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
    op.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO amc_migrator")
    op.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO amc_migrator")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {_quoted(BOT_MUTATE)} TO amc_bot")
    op.execute(f"GRANT SELECT ON TABLE {_quoted(BOT_READ)} TO amc_bot")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {_quoted(WORKER_MUTATE)} TO amc_worker")
    op.execute(f"GRANT SELECT ON TABLE {_quoted(WORKER_READ)} TO amc_worker")
    # apply_availability serializes edge writes with SELECT ... FOR UPDATE on
    # the parent subscription row, which requires the UPDATE privilege.
    op.execute("GRANT UPDATE ON TABLE subscriptions TO amc_worker")
    op.execute("REVOKE ALL ON TABLE service_heartbeats FROM amc_worker, amc_bot, amc_notifier")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON TABLE service_heartbeats "
        "TO amc_worker, amc_bot, amc_notifier"
    )
    op.execute("GRANT SELECT ON TABLE owner_incidents, owner_outbox TO amc_notifier")
    op.execute("GRANT UPDATE ON TABLE owner_outbox TO amc_notifier")
    op.execute(
        "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO amc_worker, amc_bot"
    )


def _enable_rls() -> None:
    privileged = " OR ".join(
        (
            "pg_has_role(current_user, 'amc_worker', 'member')",
            "pg_has_role(current_user, 'amc_migrator', 'member')",
            "pg_has_role(current_user, 'amc_owner', 'member')",
        )
    )
    for table in TENANT_TABLES:
        op.execute(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY')
        if table == "guilds":
            tenant_match = (
                "id = NULLIF(current_setting('app.guild_id', true), '')::uuid "
                "OR discord_guild_id::text = "
                "NULLIF(current_setting('app.discord_guild_id', true), '')"
            )
        else:
            tenant_match = (
                "guild_id = NULLIF(current_setting('app.guild_id', true), '')::uuid"
            )
        expression = f"({tenant_match} OR {privileged})"
        op.execute(
            f'CREATE POLICY tenant_isolation ON "{table}" '
            f"FOR ALL USING ({expression}) WITH CHECK ({expression})"
        )


def _enable_service_heartbeat_rls() -> None:
    """Let each runtime writer touch only its own liveness row."""

    op.execute("ALTER TABLE service_heartbeats ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE service_heartbeats FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY heartbeat_admin_all ON service_heartbeats "
        "FOR ALL TO amc_owner, amc_migrator USING (true) WITH CHECK (true)"
    )
    for role, service_name in (
        ("amc_bot", "amc-discord-bot"),
        ("amc_worker", "amc-worker"),
    ):
        op.execute(
            f"CREATE POLICY heartbeat_{role}_select ON service_heartbeats "
            f"FOR SELECT TO {role} USING (service_name = '{service_name}')"
        )
        op.execute(
            f"CREATE POLICY heartbeat_{role}_insert ON service_heartbeats "
            f"FOR INSERT TO {role} WITH CHECK (service_name = '{service_name}')"
        )
        op.execute(
            f"CREATE POLICY heartbeat_{role}_update ON service_heartbeats "
            f"FOR UPDATE TO {role} USING (service_name = '{service_name}') "
            f"WITH CHECK (service_name = '{service_name}')"
        )
    op.execute(
        "CREATE POLICY heartbeat_notifier_select ON service_heartbeats "
        "FOR SELECT TO amc_notifier USING (true)"
    )
    op.execute(
        "CREATE POLICY heartbeat_notifier_insert ON service_heartbeats "
        "FOR INSERT TO amc_notifier "
        "WITH CHECK (service_name = 'amc-owner-notifier')"
    )
    op.execute(
        "CREATE POLICY heartbeat_notifier_update ON service_heartbeats "
        "FOR UPDATE TO amc_notifier "
        "USING (service_name = 'amc-owner-notifier') "
        "WITH CHECK (service_name = 'amc-owner-notifier')"
    )


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind, checkfirst=False)
    if bind.dialect.name != "postgresql":
        return
    _create_roles()
    _set_ownership_and_grants()
    _enable_rls()
    _enable_service_heartbeat_rls()


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for table in TENANT_TABLES:
            op.execute(f'DROP POLICY IF EXISTS tenant_isolation ON "{table}"')
    for table in DROP_ORDER:
        op.drop_table(table)
    # Service roles may be shared by login roles or later databases; provisioning
    # removes them explicitly after revoking memberships.
