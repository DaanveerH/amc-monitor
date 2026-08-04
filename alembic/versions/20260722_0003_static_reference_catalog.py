"""Static reference-data catalog: numeric theatre coords + zip_centroids.

Backs serving the wizard's theatre step from a static national catalog:
- Convert theatres.latitude/longitude from text to double precision and index
  them for a bounding-box nearest-theatre prefilter. (These columns were always
  NULL before -- the live theatre upsert never populated them -- so the type
  change is data-safe.)
- Add a global zip_centroids table (ZIP -> lat/long), filled on demand and
  cached, so the bot can rank nearest theatres without an AMC call after a ZIP
  is geocoded once. Global reference data: no guild_id, no RLS; worker writes,
  bot reads.

This is a NEW migration (not an amendment of the initial one) so it runs against
the already-provisioned production database. All statements are written to be
idempotent so they are also correct on a fresh deploy, where the initial
migration's create_all already builds the current model schema.
"""
from __future__ import annotations

from alembic import op

revision = "20260722_0003"
down_revision = "20260721_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    # theatres.latitude/longitude: text -> double precision (only if still text).
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'theatres' AND column_name = 'latitude'
              AND data_type = 'character varying'
          ) THEN
            ALTER TABLE theatres
              ALTER COLUMN latitude TYPE double precision
                USING NULLIF(latitude, '')::double precision,
              ALTER COLUMN longitude TYPE double precision
                USING NULLIF(longitude, '')::double precision;
          END IF;
        END
        $$
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_theatres_lat_lon "
        "ON theatres (latitude, longitude)"
    )
    # Global ZIP centroid cache. IF NOT EXISTS so a fresh deploy (already created
    # by create_all) is a no-op here.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS zip_centroids (
          zip_code varchar(12) PRIMARY KEY,
          latitude double precision NOT NULL,
          longitude double precision NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute('ALTER TABLE zip_centroids OWNER TO amc_owner')
    op.execute("GRANT ALL PRIVILEGES ON TABLE zip_centroids TO amc_migrator")
    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE zip_centroids TO amc_worker")
    op.execute("GRANT SELECT ON TABLE zip_centroids TO amc_bot")


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute("DROP TABLE IF EXISTS zip_centroids")
    op.execute("DROP INDEX IF EXISTS ix_theatres_lat_lon")
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'theatres' AND column_name = 'latitude'
              AND data_type = 'double precision'
          ) THEN
            ALTER TABLE theatres
              ALTER COLUMN latitude TYPE varchar(32) USING latitude::text,
              ALTER COLUMN longitude TYPE varchar(32) USING longitude::text;
          END IF;
        END
        $$
        """
    )
