# AMC Seat Watch

AMC Seat Watch is a self-hosted Discord app that watches AMC showtimes for
adjacent seats and posts a link to AMC's seat picker when a match appears. It
does not reserve or purchase tickets, require an AMC account, or expose a
public web server.

> [!IMPORTANT]
> This is an independent project and is not affiliated with or endorsed by
> AMC Theatres. Operators are responsible for complying with applicable terms,
> laws, and rate limits.

## How it works

The application is split into three outbound-only processes:

| Process | Responsibility |
| --- | --- |
| `amc-worker` | Discovers showtimes, checks status and seats, evaluates monitors, and writes alert events. This is the only process that calls AMC. |
| `amc-discord-bot` | Handles guild-scoped commands and sends user alerts. It has no AMC proxy credentials. |
| `amc-owner-notifier` | Sends operational incidents through a separate webhook and retains a local outage ledger. |

PostgreSQL stores subscriptions, catalog data, shared observations, jobs,
outboxes, incidents, and audit records. Separate database roles and forced
row-level security protect guild data and keep each service least-privileged.

User commands are available under `/amc`, including `create`, `list`, `edit`,
`pause`, `resume`, `delete`, `test`, and `status`. Administrators use
`/amc-admin` to configure approved guilds, roles, and destination channels.

The setup wizard lets a member choose nearby theatres, movies, presentation
format, adjacent-seat count, seating area, and preferred viewing windows.
Alerts highlight a recommended adjacent run and link to AMC for manual
purchase.

## Design boundaries

- Only the worker may call AMC or receive AMC proxy credentials.
- The bot and notifier use separate credentials and delivery paths.
- AMC requests pass through one PostgreSQL-backed request gate with a
  three-second minimum gap and shared cooldowns for upstream rate limits.
- The app has no inbound application listener, public dashboard, checkout
  automation, or direct-message workflow.
- User-supplied webhook URLs are not accepted.

See [docs/architecture.md](docs/architecture.md) for the data flow and security
model.

## Requirements

- Python 3.11 or newer
- [uv](https://docs.astral.sh/uv/)
- PostgreSQL 17 for the full database and RLS test suite
- A Discord application and bot token
- An outbound HTTP(S) proxy for the worker

## Local setup

Install the locked dependencies:

```bash
uv sync --all-groups --frozen
```

Create service-specific PostgreSQL databases or roles, then copy
`.env.example` into your secret manager or local environment. Do not commit a
filled environment file.

Apply migrations with a migration-capable database identity:

```bash
uv run alembic upgrade head
uv run python -m amc_watch.db.provision_roles
```

Start each process with only the environment variables it needs:

```bash
uv run python -m amc_watch.worker
uv run python -m amc_watch.bot
uv run python -m amc_watch.owner_notifier
```

The production topology is intentionally not encoded in this public
repository. Use your preferred process supervisor and secret manager, keep the
three service identities separate, and never run more than one AMC worker for
the same request gate.

## Configuration

`.env.example` documents the supported keys. At minimum:

- the worker needs `AMC_DATABASE_URL` and `AMC_HTTPS_PROXY`;
- the bot needs `AMC_DATABASE_URL`, `DISCORD_BOT_TOKEN`, and
  `DISCORD_ALLOWED_GUILD_IDS`;
- the notifier needs `AMC_DATABASE_URL` and `OWNER_DISCORD_WEBHOOK`.

Database connections should use certificate verification in hosted
environments. Store the CA and credentials in a secret manager, inject them at
runtime, and avoid putting secret values in command arguments or logs.
`scripts/exec-with-db-ca.py` can materialize an injected CA in a private runtime
directory before launching a service.

## Development

Run the same checks used by CI:

```bash
uv lock --check
uv run python -m compileall -q amc_watch tests
uv run pytest -q
git diff --check
```

PostgreSQL and RLS tests require `TEST_DATABASE_URL` pointing to a disposable
database. Tests use synthetic IDs, names, and locations only.

## Security and privacy

The public history is intentionally a clean snapshot. It excludes private
deployment details, runtime state, credentials, personal preferences, and
historical Git metadata. See [SECURITY.md](SECURITY.md) before reporting a
potential vulnerability.

Never commit database URLs, certificates, Discord tokens, webhooks, proxy URLs,
service tokens, logs, runtime databases, or session material.

## Contributing

Small, focused issues and pull requests are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) first.

## License

No open-source license is currently included. Public visibility does not grant
permission to copy, modify, or redistribute the code.
