"""Bind pre-created PostgreSQL login roles to least-privilege service groups.

Run only with a transient migration identity after ``alembic upgrade``.
Passwords are created and rotated outside this process; this command never reads,
prints, or changes them.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from sqlalchemy import create_engine, text


MEMBERSHIPS = {
    "amc_worker_login": "amc_worker",
    "amc_bot_login": "amc_bot",
    "amc_notifier_login": "amc_notifier",
    "amc_migrator_login": "amc_migrator",
}
SERVICE_GROUPS = ("amc_worker", "amc_bot", "amc_notifier")
PRIVILEGED_GROUPS = ("amc_owner", "amc_migrator")
ALL_GROUPS = (*SERVICE_GROUPS, *PRIVILEGED_GROUPS)
RUNTIME_LOGINS = frozenset(MEMBERSHIPS) - {"amc_migrator_login"}


def _database_url() -> str:
    value = os.environ.get("AMC_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not value:
        raise RuntimeError("AMC_DATABASE_URL is required")
    return value


def provision(url: str, *, check_only: bool = False) -> dict[str, Any]:
    engine = create_engine(url, pool_pre_ping=True)
    try:
        if engine.dialect.name != "postgresql":
            raise RuntimeError("service roles can only be provisioned in PostgreSQL")
        with engine.begin() as connection:
            known = set(
                connection.scalars(
                    text(
                        "SELECT rolname FROM pg_roles WHERE rolname = ANY(:roles)"
                    ),
                    {"roles": list(MEMBERSHIPS) + list(MEMBERSHIPS.values()) + list(PRIVILEGED_GROUPS)},
                )
            )
            missing = (
                set(MEMBERSHIPS)
                | set(MEMBERSHIPS.values())
                | set(PRIVILEGED_GROUPS)
            ) - known
            if missing:
                raise RuntimeError("missing required database roles: " + ", ".join(sorted(missing)))

            if not check_only:
                connection.execute(text("REVOKE CREATE ON SCHEMA public FROM PUBLIC"))
                # Managed PostgreSQL's doadmin is a non-superuser CREATEROLE
                # without CREATEDB; PG16 forbids touching any attribute the
                # caller does not itself hold (even setting it negative), and
                # SUPERUSER/BYPASSRLS require superuser outright. Logins it
                # creates already lack all of these — the attribute assertion
                # below enforces them. Only INHERIT is universally alterable.
                for login, intended_group in MEMBERSHIPS.items():
                    # Names are fixed constants, never user-controlled identifiers.
                    connection.execute(text(f"ALTER ROLE {login} INHERIT"))
                    for group in ALL_GROUPS:
                        if group != intended_group:
                            connection.execute(text(f"REVOKE {group} FROM {login}"))
                    connection.execute(text(f"GRANT {intended_group} TO {login}"))
                    if login in RUNTIME_LOGINS:
                        connection.execute(
                            text(f"REVOKE CREATE ON SCHEMA public FROM {login}")
                        )

            attributes = {
                row.rolname: {
                    "login": bool(row.rolcanlogin),
                    "superuser": bool(row.rolsuper),
                    "createdb": bool(row.rolcreatedb),
                    "createrole": bool(row.rolcreaterole),
                    "bypassrls": bool(row.rolbypassrls),
                }
                for row in connection.execute(
                    text(
                        "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolbypassrls FROM pg_roles "
                        "WHERE rolname = ANY(:roles)"
                    ),
                    {"roles": list(MEMBERSHIPS)},
                )
            }
            # Check direct grants, not pg_has_role's transitive result. The
            # migrator legitimately inherits amc_owner through amc_migrator,
            # but the login must not receive that privileged role directly.
            memberships = {
                login: {
                    group: bool(
                        connection.scalar(
                            text(
                                "SELECT EXISTS ("
                                "SELECT 1 FROM pg_auth_members membership "
                                "JOIN pg_roles granted "
                                "ON granted.oid = membership.roleid "
                                "JOIN pg_roles member "
                                "ON member.oid = membership.member "
                                "WHERE member.rolname = :login "
                                "AND granted.rolname = :group)"
                            ),
                            {"login": login, "group": group},
                        )
                    )
                    for group in ALL_GROUPS
                }
                for login in MEMBERSHIPS
            }
            create_privilege = {
                login: bool(
                    connection.scalar(
                        text("SELECT has_schema_privilege(:login, 'public', 'CREATE')"),
                        {"login": login},
                    )
                )
                for login in MEMBERSHIPS
            }
            for login, group in MEMBERSHIPS.items():
                values = attributes[login]
                if not values["login"] or any(
                    values[name]
                    for name in ("superuser", "createdb", "createrole", "bypassrls")
                ):
                    raise RuntimeError(f"unsafe attributes remain on {login}")
                if not memberships[login][group]:
                    raise RuntimeError(f"{login} is not a member of {group}")
                unexpected = [
                    role
                    for role, present in memberships[login].items()
                    if present and role != group
                ]
                if unexpected:
                    raise RuntimeError(
                        f"{login} has unexpected service memberships: {', '.join(unexpected)}"
                    )
                if login in RUNTIME_LOGINS and create_privilege[login]:
                    raise RuntimeError(f"{login} still has CREATE on public schema")
                if login == "amc_migrator_login" and not create_privilege[login]:
                    raise RuntimeError(
                        "amc_migrator_login cannot create migration objects"
                    )
            return {
                "checked": sorted(MEMBERSHIPS),
                "memberships": {login: group for login, group in MEMBERSHIPS.items()},
                "safe": True,
            }
    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify without changing roles")
    args = parser.parse_args()
    summary = provision(_database_url(), check_only=args.check)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
