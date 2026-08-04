"""Engine and transaction helpers with PostgreSQL tenant context support."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator

from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool


class Database:
    """Small sync database wrapper shared by the worker and repositories."""

    def __init__(self, url: str, *, echo: bool = False, pool_size: int = 3) -> None:
        kwargs: dict[str, object] = {"echo": echo, "pool_pre_ping": True}
        if url in {"sqlite://", "sqlite:///:memory:"}:
            kwargs.update(
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        elif not url.startswith("sqlite"):
            kwargs.update(pool_size=pool_size, max_overflow=0)
        self.engine = create_engine(url, **kwargs)
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", _enable_sqlite_foreign_keys)
        self.session_factory = sessionmaker(
            bind=self.engine, class_=Session, expire_on_commit=False
        )

    def dispose(self) -> None:
        self.engine.dispose()


def _enable_sqlite_foreign_keys(dbapi_connection: object, _: object) -> None:
    cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def set_guild_context(session: Session, guild_id: uuid.UUID | str) -> None:
    """Set the RLS guild for the current PostgreSQL transaction.

    SQLite has no RLS and remains a deliberate test-only compatibility path.
    """

    value = str(guild_id)
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        session.execute(
            text("SELECT set_config('app.guild_id', :guild_id, true)"),
            {"guild_id": value},
        )
    session.info["guild_id"] = value


def set_discord_guild_context(session: Session, discord_guild_id: int) -> None:
    """Set the snowflake context used to resolve or bootstrap a guild row."""

    value = str(discord_guild_id)
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        session.execute(
            text("SELECT set_config('app.discord_guild_id', :guild_id, true)"),
            {"guild_id": value},
        )
    session.info["discord_guild_id"] = value


@contextlib.contextmanager
def guild_transaction(
    session_factory: sessionmaker[Session], guild_id: uuid.UUID | str
) -> Iterator[Session]:
    """Open a transaction whose PostgreSQL RLS context is one Discord guild."""

    with session_factory() as session, session.begin():
        set_guild_context(session, guild_id)
        yield session


@contextlib.contextmanager
def transaction(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    with session_factory() as session, session.begin():
        yield session


def dialect_name(session: Session) -> str:
    bind: Engine | None = session.get_bind()  # type: ignore[assignment]
    return bind.dialect.name if bind is not None else ""


__all__ = [
    "Database",
    "dialect_name",
    "guild_transaction",
    "set_discord_guild_context",
    "set_guild_context",
    "transaction",
]
