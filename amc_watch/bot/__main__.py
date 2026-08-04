"""Run the outbound-only AMC Seat Watch Discord process."""

from __future__ import annotations

import logging
import os

from amc_watch.db.discord_repository import SqlAlchemyDiscordRepository
from amc_watch.discord_bot import run_bot
from amc_watch.discord_security import DiscordBotSettings


def main() -> None:
    database_url = (
        os.environ.get("AMC_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
    )
    if not database_url:
        raise RuntimeError("AMC_DATABASE_URL is required")
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    repository = SqlAlchemyDiscordRepository.from_url(database_url)
    run_bot(repository, settings=DiscordBotSettings.from_env())


if __name__ == "__main__":
    main()
