"""Alembic environment for AMC Seat Watch."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from amc_watch.db.models import Base


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def database_url() -> str:
    url = os.environ.get("AMC_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("AMC_DATABASE_URL is required to run database migrations")
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=False,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        if connection.dialect.name == "postgresql":
            # Bootstrap role grants must COMMIT before the migration transaction
            # begins: PostgreSQL only honours newly granted role membership (for
            # SET ROLE / ownership transfer) after the granting transaction ends.
            # Non-superuser CREATEROLE logins such as managed PostgreSQL's doadmin
            # auto-administer roles they create themselves (and PG16+ forbids
            # re-granting ADMIN OPTION to one's own grantor), so grant only the
            # memberships actually missing.
            # Create the service group roles up front so membership grants can
            # commit before the migration transaction begins. The migration's
            # own CREATE ROLE blocks are idempotent no-ops after this.
            for role in ("amc_owner", "amc_migrator", "amc_worker", "amc_bot", "amc_notifier"):
                connection.exec_driver_sql(
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
            connection.exec_driver_sql(
                """
                DO $$
                BEGIN
                  IF NOT pg_has_role('amc_migrator', 'amc_owner', 'MEMBER') THEN
                    GRANT amc_owner TO amc_migrator WITH ADMIN OPTION;
                  END IF;
                  IF NOT pg_has_role(current_user, 'amc_migrator', 'MEMBER') THEN
                    GRANT amc_migrator TO CURRENT_USER WITH ADMIN OPTION;
                  END IF;
                  IF NOT pg_has_role(current_user, 'amc_owner', 'USAGE') THEN
                    GRANT amc_owner TO CURRENT_USER;
                  END IF;
                END
                $$
                """
            )
            connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
