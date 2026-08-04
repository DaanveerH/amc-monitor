"""Executable PostgreSQL-backed AMC worker service."""

from __future__ import annotations

import os

from .db.worker_repository import SqlAlchemyWorkerRepository
from .amc import ProxyPool
from .scheduler import SharedWorker, load_proxy_pool_from_environment, run_forever


def main() -> int:
    database_url = (
        os.environ.get("AMC_DATABASE_URL") or os.environ.get("DATABASE_URL") or ""
    ).strip()
    if not database_url:
        raise RuntimeError("AMC_DATABASE_URL is required")
    repository = SqlAlchemyWorkerRepository.from_url(database_url)
    pool = load_proxy_pool_from_environment()
    active_index, attempted_indices = repository.proxy_pool_state(len(pool.endpoints))
    if active_index >= len(pool.endpoints):
        active_index = 0
    pool = ProxyPool(pool.endpoints, active_index, attempted_indices)
    repository.set_active_proxy(active_index)
    worker = SharedWorker(repository, pool)
    run_forever(worker, idle_seconds=float(os.environ.get("AMC_WORKER_IDLE_SECONDS", "1")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
