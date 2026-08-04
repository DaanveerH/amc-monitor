"""Grant amc_bot the discovery/enrollment writes its wizard + /monitor commands need.

The initial control-plane grants left amc_bot unable to enqueue catalog lookups
(monitor_jobs), bootstrap/enroll discovery targets (discovery_targets), read
selectable dates, or activate showtimes on enrollment (showtimes UPDATE). Without
these, `/monitor create`, the setup wizard, and `/monitor status` fail with
permission denied. This is a NEW migration (not an amendment of the initial one)
so it actually runs against the already-provisioned production database.
"""
from __future__ import annotations

from alembic import op

revision = "20260721_0002"
down_revision = "20260719_0001"
branch_labels = None
depends_on = None

_GRANTS = (
    "GRANT SELECT, INSERT, UPDATE ON TABLE monitor_jobs TO amc_bot",
    "GRANT SELECT, INSERT, UPDATE ON TABLE discovery_targets TO amc_bot",
    "GRANT SELECT ON TABLE selectable_dates TO amc_bot",
    "GRANT UPDATE ON TABLE showtimes TO amc_bot",
)

_REVOKES = (
    "REVOKE SELECT, INSERT, UPDATE ON TABLE monitor_jobs FROM amc_bot",
    "REVOKE SELECT, INSERT, UPDATE ON TABLE discovery_targets FROM amc_bot",
    "REVOKE SELECT ON TABLE selectable_dates FROM amc_bot",
    "REVOKE UPDATE ON TABLE showtimes FROM amc_bot",
)


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    for statement in _GRANTS:
        op.execute(statement)


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    for statement in _REVOKES:
        op.execute(statement)
