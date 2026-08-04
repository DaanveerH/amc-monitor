# Architecture

AMC Seat Watch separates upstream access, Discord interaction handling, and
operational alerting into three processes backed by PostgreSQL.

```text
Discord users -> amc-discord-bot -> PostgreSQL jobs/outboxes
                                      ^            |
                                      |            v
AMC GraphQL <- proxy <- amc-worker ---+       Discord alerts
                                      |
                                      +-> amc-owner-notifier -> owner incidents
```

## Worker

The worker is the sole AMC client. It claims jobs, enforces a shared request
gate, discovers showtimes, polls status and seat maps, evaluates subscriptions,
and writes transactional outbox rows. Upstream cooldowns are stored centrally
so restarts and multiple worker loops cannot bypass pacing.

## Discord bot

The bot maintains the outbound Discord Gateway connection. It validates guild,
role, channel, and ownership boundaries before reading or writing tenant data.
Catalog work is queued for the worker; the bot never receives AMC proxy
credentials.

## Owner notifier

The notifier evaluates service heartbeats and delivery health. It uses a
separate webhook from user alerts and a local SQLite ledger so database outages
can still be reported without creating a recursive dependency.

## Database security

PostgreSQL stores both control-plane and observation data. Alembic migrations
define forced row-level security policies and least-privilege roles for the
worker, bot, notifier, and migrator. Application-level authorization complements
RLS but does not replace it.

## Network model

All application connections are outbound. The project does not require an HTTP
server, public port, inbound webhook, or AMC account session. AMC calls are
anonymous, low-volume reads and should be routed through the worker's configured
proxy with conservative pacing.
