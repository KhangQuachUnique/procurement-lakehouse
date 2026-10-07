"""Seven-table Bronze metadata contract. Changes require an Alembic revision.

Cross-row lifecycle rules (lease fencing, verified files, atomic publication) are
transaction-service responsibilities; these tables do not implement that service.
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

SCHEMA = "bronze_meta"
metadata = sa.MetaData(
    schema=SCHEMA,
    naming_convention={
        "pk": "pk_%(table_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "ix": "ix_%(table_name)s_%(column_0_name)s",
    },
)


def timestamp(name, *, nullable=False, default=False):
    return sa.Column(
        name,
        sa.DateTime(timezone=True),
        nullable=nullable,
        server_default=sa.text("CURRENT_TIMESTAMP") if default else None,
    )


partitions = sa.Table(
    "partitions",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("source", sa.Text, nullable=False),
    sa.Column("resource", sa.Text, nullable=False),
    sa.Column("source_date", sa.Date, nullable=False),
    sa.Column("current_commit_id", UUID(as_uuid=True)),
    timestamp("created_at", default=True),
    sa.UniqueConstraint("source", "resource", "source_date", name="uq_partition_identity"),
    sa.CheckConstraint("source <> '' AND resource <> ''", name="identity_nonempty"),
    sa.ForeignKeyConstraint(
        ["id", "current_commit_id"],
        ["bronze_meta.commits.partition_id", "bronze_meta.commits.id"],
        name="fk_partition_current_commit",
        use_alter=True,
    ),
)

attempts = sa.Table(
    "attempts",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "partition_id",
        UUID(as_uuid=True),
        sa.ForeignKey("bronze_meta.partitions.id"),
        nullable=False,
    ),
    sa.Column("request_id", UUID(as_uuid=True), nullable=False, unique=True),
    sa.Column("dagster_run_id", sa.Text),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False, server_default="running"),
    sa.Column("base_commit_id", UUID(as_uuid=True)),
    sa.Column("owner_id", UUID(as_uuid=True), nullable=False),
    sa.Column("lease_generation", sa.BigInteger, nullable=False),
    sa.Column("config", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("failure_reason", sa.Text),
    timestamp("started_at", default=True),
    timestamp("completed_at", nullable=True),
    sa.UniqueConstraint("partition_id", "id", name="uq_attempt_partition_id"),
    sa.UniqueConstraint(
        "partition_id", "id", "owner_id", "lease_generation", name="uq_attempt_lease_identity"
    ),
    sa.CheckConstraint(
        "kind IN ('ingestion', 'refresh', 'compaction', 'import', 'repair')", name="kind"
    ),
    sa.CheckConstraint(
        "status IN ('running', 'success', 'failed', 'canceled', 'abandoned')", name="status"
    ),
    sa.CheckConstraint("lease_generation > 0", name="generation_positive"),
    sa.CheckConstraint("jsonb_typeof(config) = 'object'", name="config_object"),
    sa.CheckConstraint(
        "(status = 'running' AND completed_at IS NULL) OR "
        "(status <> 'running' AND completed_at IS NOT NULL AND completed_at >= started_at)",
        name="completion",
    ),
    sa.ForeignKeyConstraint(
        ["partition_id", "base_commit_id"],
        ["bronze_meta.commits.partition_id", "bronze_meta.commits.id"],
        name="fk_attempt_base_commit",
        use_alter=True,
    ),
)
sa.Index("ix_attempt_partition_history", attempts.c.partition_id, attempts.c.started_at.desc())
sa.Index("ix_attempt_dagster_run", attempts.c.dagster_run_id)

attempt_pages = sa.Table(
    "attempt_pages",
    metadata,
    sa.Column(
        "attempt_id", UUID(as_uuid=True), sa.ForeignKey("bronze_meta.attempts.id"), primary_key=True
    ),
    sa.Column("page_number", sa.Integer, primary_key=True),
    sa.Column("page_size", sa.Integer, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("search_items", sa.BigInteger, nullable=False, server_default="0"),
    sa.Column("bronze_records", sa.BigInteger, nullable=False, server_default="0"),
    sa.Column("error_count", sa.Integer, nullable=False, server_default="0"),
    timestamp("started_at", default=True),
    timestamp("completed_at", nullable=True),
    sa.CheckConstraint("page_number >= 0 AND page_size > 0", name="page_bounds"),
    sa.CheckConstraint(
        "search_items >= 0 AND bronze_records >= 0 AND error_count >= 0", name="counts_nonnegative"
    ),
    sa.CheckConstraint("status IN ('running', 'success', 'failed')", name="status"),
    sa.CheckConstraint(
        "(status = 'running' AND completed_at IS NULL) OR "
        "(status <> 'running' AND completed_at IS NOT NULL AND completed_at >= started_at)",
        name="completion",
    ),
)

partition_leases = sa.Table(
    "partition_leases",
    metadata,
    sa.Column(
        "partition_id",
        UUID(as_uuid=True),
        sa.ForeignKey("bronze_meta.partitions.id"),
        primary_key=True,
    ),
    sa.Column("attempt_id", UUID(as_uuid=True)),
    sa.Column("owner_id", UUID(as_uuid=True)),
    sa.Column("generation", sa.BigInteger, nullable=False, server_default="0"),
    timestamp("heartbeat_at", nullable=True),
    timestamp("expires_at", nullable=True),
    sa.CheckConstraint("generation >= 0", name="generation_nonnegative"),
    sa.CheckConstraint(
        "(attempt_id IS NULL AND owner_id IS NULL AND heartbeat_at IS NULL AND expires_at IS NULL) OR "
        "(attempt_id IS NOT NULL AND owner_id IS NOT NULL AND heartbeat_at IS NOT NULL "
        "AND expires_at IS NOT NULL AND generation > 0 AND expires_at > heartbeat_at)",
        name="holder_complete",
    ),
    sa.ForeignKeyConstraint(
        ["partition_id", "attempt_id", "owner_id", "generation"],
        [
            "bronze_meta.attempts.partition_id",
            "bronze_meta.attempts.id",
            "bronze_meta.attempts.owner_id",
            "bronze_meta.attempts.lease_generation",
        ],
        name="fk_lease_attempt_owner_generation",
    ),
)
sa.Index(
    "ix_lease_expiry",
    partition_leases.c.expires_at,
    postgresql_where=partition_leases.c.attempt_id.is_not(None),
)

commits = sa.Table(
    "commits",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "partition_id",
        UUID(as_uuid=True),
        sa.ForeignKey("bronze_meta.partitions.id"),
        nullable=False,
    ),
    sa.Column("attempt_id", UUID(as_uuid=True), nullable=False, unique=True),
    sa.Column("parent_commit_id", UUID(as_uuid=True)),
    sa.Column("data_version", UUID(as_uuid=True), nullable=False),
    sa.Column("record_count", sa.BigInteger, nullable=False),
    sa.Column("file_count", sa.Integer, nullable=False),
    sa.Column("verification", JSONB, nullable=False),
    timestamp("committed_at", default=True),
    sa.UniqueConstraint("partition_id", "id", name="uq_commit_partition_id"),
    sa.ForeignKeyConstraint(
        ["partition_id", "attempt_id"],
        ["bronze_meta.attempts.partition_id", "bronze_meta.attempts.id"],
        name="fk_commit_attempt_partition",
    ),
    sa.ForeignKeyConstraint(
        ["partition_id", "parent_commit_id"],
        ["bronze_meta.commits.partition_id", "bronze_meta.commits.id"],
        name="fk_commit_parent_partition",
    ),
    sa.CheckConstraint("parent_commit_id IS NULL OR parent_commit_id <> id", name="not_own_parent"),
    sa.CheckConstraint(
        "record_count >= 0 AND file_count >= 0 AND (record_count = 0 OR file_count > 0)",
        name="counts",
    ),
    sa.CheckConstraint("jsonb_typeof(verification) = 'object'", name="verification_object"),
)
sa.Index("ix_commit_partition_history", commits.c.partition_id, commits.c.committed_at.desc())

commit_files = sa.Table(
    "commit_files",
    metadata,
    sa.Column(
        "commit_id", UUID(as_uuid=True), sa.ForeignKey("bronze_meta.commits.id"), primary_key=True
    ),
    sa.Column("file_number", sa.Integer, primary_key=True),
    sa.Column("table_name", sa.Text, nullable=False),
    sa.Column("bucket", sa.Text, nullable=False),
    sa.Column("object_key", sa.Text, nullable=False),
    sa.Column("row_count", sa.BigInteger, nullable=False),
    sa.Column("size_bytes", sa.BigInteger, nullable=False),
    sa.Column("sha256", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.Integer, nullable=False),
    sa.UniqueConstraint("bucket", "object_key", name="uq_committed_object"),
    sa.CheckConstraint(
        "file_number >= 0 AND row_count >= 0 AND size_bytes > 0 AND schema_version > 0",
        name="bounds",
    ),
    sa.CheckConstraint("table_name <> '' AND bucket <> '' AND object_key <> ''", name="names"),
    sa.CheckConstraint("sha256 ~ '^[0-9a-f]{64}$'", name="sha256"),
)

errors = sa.Table(
    "errors",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "attempt_id", UUID(as_uuid=True), sa.ForeignKey("bronze_meta.attempts.id"), nullable=False
    ),
    # A fetch can fail before any page row exists; this is context, not a page FK.
    sa.Column("page_number", sa.Integer),
    sa.Column("source_record_id", sa.Text),
    sa.Column("stage", sa.Text, nullable=False),
    sa.Column("error_type", sa.Text, nullable=False),
    sa.Column("message", sa.Text, nullable=False),
    sa.Column("http_status", sa.Integer),
    sa.Column("details", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    timestamp("occurred_at", default=True),
    sa.CheckConstraint("page_number IS NULL OR page_number >= 0", name="page_number"),
    sa.CheckConstraint(
        "http_status IS NULL OR http_status BETWEEN 100 AND 599", name="http_status"
    ),
    sa.CheckConstraint("stage <> '' AND error_type <> ''", name="classification"),
    sa.CheckConstraint("jsonb_typeof(details) = 'object'", name="details_object"),
)
sa.Index("ix_error_attempt_time", errors.c.attempt_id, errors.c.occurred_at)
sa.Index("ix_error_type_time", errors.c.error_type, errors.c.occurred_at.desc())
