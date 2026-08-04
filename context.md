# Repository context

This file gives maintainers and coding agents a short, public-safe handoff. It
describes repository behavior, not any live deployment.

## Current architecture

- `amc_watch/` is the supported runtime.
- `amc-worker` is the only process that calls AMC.
- `amc-discord-bot` owns guild-scoped commands and user delivery.
- `amc-owner-notifier` owns operational incident delivery.
- PostgreSQL is the control plane. Forced RLS and service-specific roles are
  security boundaries, not optional deployment details.
- Alembic owns schema and policy changes.

## Product boundaries

- Self-hosted and outbound-only.
- No ticket purchase or reservation automation.
- No AMC customer login, cookies, or payment data.
- No public listener, dashboard, inbound webhook receiver, or direct messages.
- No fallback that lets the bot call AMC directly.

## Development baseline

```bash
uv lock --check
uv run python -m compileall -q amc_watch tests
uv run pytest -q
git diff --check
```

PostgreSQL/RLS tests require a disposable `TEST_DATABASE_URL`; CI provides
PostgreSQL 17. Fixtures must use synthetic names, IDs, locations, and URLs.

## Change guidance

1. Read `README.md`, `AGENTS.md`, and the relevant source and tests.
2. Keep changes narrow and add regression coverage for behavior fixes.
3. Preserve acknowledgement of Discord interactions before slow work.
4. Preserve guild isolation, request serialization, outbox idempotency, and
   service-specific credentials.
5. Never add production topology, personal data, credentials, runtime state, or
   private incident history to this repository.

The public repository deliberately omits deployment-specific automation. A
private deployment may use any process supervisor and secret manager that
preserve the documented security boundaries.
