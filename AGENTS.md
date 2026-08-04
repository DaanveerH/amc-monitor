# Repository instructions

## Start here

Read `README.md`, `context.md`, and the relevant source and tests before making
non-trivial changes.

## Invariants

- `amc-worker` is the only process allowed to call AMC or receive AMC proxy
  credentials.
- Keep user alerts and operational incidents on separate credentials and
  delivery paths.
- Preserve guild isolation, forced PostgreSQL RLS, and service-specific
  least-privilege roles.
- Do not add a public listener, dashboard, webhook receiver, purchase flow, or
  direct AMC fallback without an explicit product decision.
- Do not print or commit database URLs, CA contents, Discord tokens, webhooks,
  proxy URLs, service tokens, IDs from a live system, runtime state, or personal
  data.

## Code changes

- Keep changes focused and match the existing style.
- Add a regression test for each behavior fix. Prefer the real SQLAlchemy
  repository when a bug crosses controller and repository boundaries.
- Treat Discord's interaction response window as a hard boundary. Acknowledge
  before database or network work.
- Do not add broad exception handling that hides logging, retry, or tenant
  safety behavior.
- Generate and inspect Alembic migrations locally. Never apply migrations to a
  non-disposable database as part of development or CI.
- Use synthetic names, IDs, locations, URLs, and credentials in fixtures and
  documentation.

Run before handoff:

```bash
uv lock --check
uv run python -m compileall -q amc_watch tests
uv run pytest -q
git diff --check
```

PostgreSQL/RLS tests require a disposable `TEST_DATABASE_URL`.

## Repository boundaries

- `amc_watch/` contains the application runtime.
- `alembic/` contains schema changes and database security policies.
- `tests/` contains unit, SQLite integration, PostgreSQL, RLS, and security
  coverage.
- `docs/` contains public architecture and protocol notes only.
- `scripts/` contains provider-neutral runtime helpers.
- Deployment topology, credentials, runtime artifacts, and private incident
  records do not belong in this repository.
