"""Initial seven-table application schema; frozen SQL, independent of runtime models.

Revision ID: 0001_bronze_core
"""

from alembic import op

revision = "0001_bronze_core"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
CREATE SCHEMA bronze_meta
""")
    op.execute("""
CREATE TABLE bronze_meta.partitions (
	id UUID NOT NULL, 
	source TEXT NOT NULL, 
	resource TEXT NOT NULL, 
	source_date DATE NOT NULL, 
	current_commit_id UUID, 
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	CONSTRAINT pk_partitions PRIMARY KEY (id), 
	CONSTRAINT uq_partition_identity UNIQUE (source, resource, source_date), 
	CONSTRAINT ck_partitions_identity_nonempty CHECK (source <> '' AND resource <> '')
)
""")
    op.execute("""
CREATE TABLE bronze_meta.attempts (
	id UUID NOT NULL, 
	partition_id UUID NOT NULL, 
	request_id UUID NOT NULL, 
	dagster_run_id TEXT, 
	kind TEXT NOT NULL, 
	status TEXT DEFAULT 'running' NOT NULL, 
	base_commit_id UUID, 
	owner_id UUID NOT NULL, 
	lease_generation BIGINT NOT NULL, 
	config JSONB DEFAULT '{}'::jsonb NOT NULL, 
	failure_reason TEXT, 
	started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	CONSTRAINT pk_attempts PRIMARY KEY (id), 
	CONSTRAINT uq_attempt_partition_id UNIQUE (partition_id, id), 
	CONSTRAINT uq_attempt_lease_identity UNIQUE (partition_id, id, owner_id, lease_generation), 
	CONSTRAINT ck_attempts_kind CHECK (kind IN ('ingestion', 'refresh', 'compaction', 'import', 'repair')), 
	CONSTRAINT ck_attempts_status CHECK (status IN ('running', 'success', 'failed', 'canceled', 'abandoned')), 
	CONSTRAINT ck_attempts_generation_positive CHECK (lease_generation > 0), 
	CONSTRAINT ck_attempts_config_object CHECK (jsonb_typeof(config) = 'object'), 
	CONSTRAINT ck_attempts_completion CHECK ((status = 'running' AND completed_at IS NULL) OR (status <> 'running' AND completed_at IS NOT NULL AND completed_at >= started_at)), 
	CONSTRAINT fk_attempts_partition_id_partitions FOREIGN KEY(partition_id) REFERENCES bronze_meta.partitions (id), 
	CONSTRAINT uq_attempts_request_id UNIQUE (request_id)
)
""")
    op.execute("""
CREATE INDEX ix_attempt_dagster_run ON bronze_meta.attempts (dagster_run_id)
""")
    op.execute("""
CREATE INDEX ix_attempt_partition_history ON bronze_meta.attempts (partition_id, started_at DESC)
""")
    op.execute("""
CREATE TABLE bronze_meta.attempt_pages (
	attempt_id UUID NOT NULL, 
	page_number INTEGER NOT NULL, 
	page_size INTEGER NOT NULL, 
	status TEXT NOT NULL, 
	search_items BIGINT DEFAULT '0' NOT NULL, 
	bronze_records BIGINT DEFAULT '0' NOT NULL, 
	error_count INTEGER DEFAULT '0' NOT NULL, 
	started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	completed_at TIMESTAMP WITH TIME ZONE, 
	CONSTRAINT pk_attempt_pages PRIMARY KEY (attempt_id, page_number), 
	CONSTRAINT ck_attempt_pages_page_bounds CHECK (page_number >= 0 AND page_size > 0), 
	CONSTRAINT ck_attempt_pages_counts_nonnegative CHECK (search_items >= 0 AND bronze_records >= 0 AND error_count >= 0), 
	CONSTRAINT ck_attempt_pages_status CHECK (status IN ('running', 'success', 'failed')), 
	CONSTRAINT ck_attempt_pages_completion CHECK ((status = 'running' AND completed_at IS NULL) OR (status <> 'running' AND completed_at IS NOT NULL AND completed_at >= started_at)), 
	CONSTRAINT fk_attempt_pages_attempt_id_attempts FOREIGN KEY(attempt_id) REFERENCES bronze_meta.attempts (id)
)
""")
    op.execute("""
CREATE TABLE bronze_meta.partition_leases (
	partition_id UUID NOT NULL, 
	attempt_id UUID, 
	owner_id UUID, 
	generation BIGINT DEFAULT '0' NOT NULL, 
	heartbeat_at TIMESTAMP WITH TIME ZONE, 
	expires_at TIMESTAMP WITH TIME ZONE, 
	CONSTRAINT pk_partition_leases PRIMARY KEY (partition_id), 
	CONSTRAINT ck_partition_leases_generation_nonnegative CHECK (generation >= 0), 
	CONSTRAINT ck_partition_leases_holder_complete CHECK ((attempt_id IS NULL AND owner_id IS NULL AND heartbeat_at IS NULL AND expires_at IS NULL) OR (attempt_id IS NOT NULL AND owner_id IS NOT NULL AND heartbeat_at IS NOT NULL AND expires_at IS NOT NULL AND generation > 0 AND expires_at > heartbeat_at)), 
	CONSTRAINT fk_lease_attempt_owner_generation FOREIGN KEY(partition_id, attempt_id, owner_id, generation) REFERENCES bronze_meta.attempts (partition_id, id, owner_id, lease_generation), 
	CONSTRAINT fk_partition_leases_partition_id_partitions FOREIGN KEY(partition_id) REFERENCES bronze_meta.partitions (id)
)
""")
    op.execute("""
CREATE INDEX ix_lease_expiry ON bronze_meta.partition_leases (expires_at) WHERE attempt_id IS NOT NULL
""")
    op.execute("""
CREATE TABLE bronze_meta.commits (
	id UUID NOT NULL, 
	partition_id UUID NOT NULL, 
	attempt_id UUID NOT NULL, 
	parent_commit_id UUID, 
	data_version UUID NOT NULL, 
	record_count BIGINT NOT NULL, 
	file_count INTEGER NOT NULL, 
	verification JSONB NOT NULL, 
	committed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	CONSTRAINT pk_commits PRIMARY KEY (id), 
	CONSTRAINT uq_commit_partition_id UNIQUE (partition_id, id), 
	CONSTRAINT fk_commit_attempt_partition FOREIGN KEY(partition_id, attempt_id) REFERENCES bronze_meta.attempts (partition_id, id), 
	CONSTRAINT fk_commit_parent_partition FOREIGN KEY(partition_id, parent_commit_id) REFERENCES bronze_meta.commits (partition_id, id), 
	CONSTRAINT ck_commits_not_own_parent CHECK (parent_commit_id IS NULL OR parent_commit_id <> id), 
	CONSTRAINT ck_commits_counts CHECK (record_count >= 0 AND file_count >= 0 AND (record_count = 0 OR file_count > 0)), 
	CONSTRAINT ck_commits_verification_object CHECK (jsonb_typeof(verification) = 'object'), 
	CONSTRAINT fk_commits_partition_id_partitions FOREIGN KEY(partition_id) REFERENCES bronze_meta.partitions (id), 
	CONSTRAINT uq_commits_attempt_id UNIQUE (attempt_id)
)
""")
    op.execute("""
CREATE INDEX ix_commit_partition_history ON bronze_meta.commits (partition_id, committed_at DESC)
""")
    op.execute("""
CREATE TABLE bronze_meta.errors (
	id UUID NOT NULL, 
	attempt_id UUID NOT NULL, 
	page_number INTEGER, 
	source_record_id TEXT, 
	stage TEXT NOT NULL, 
	error_type TEXT NOT NULL, 
	message TEXT NOT NULL, 
	http_status INTEGER, 
	details JSONB DEFAULT '{}'::jsonb NOT NULL, 
	occurred_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	CONSTRAINT pk_errors PRIMARY KEY (id), 
	CONSTRAINT ck_errors_page_number CHECK (page_number IS NULL OR page_number >= 0), 
	CONSTRAINT ck_errors_http_status CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599), 
	CONSTRAINT ck_errors_classification CHECK (stage <> '' AND error_type <> ''), 
	CONSTRAINT ck_errors_details_object CHECK (jsonb_typeof(details) = 'object'), 
	CONSTRAINT fk_errors_attempt_id_attempts FOREIGN KEY(attempt_id) REFERENCES bronze_meta.attempts (id)
)
""")
    op.execute("""
CREATE INDEX ix_error_attempt_time ON bronze_meta.errors (attempt_id, occurred_at)
""")
    op.execute("""
CREATE INDEX ix_error_type_time ON bronze_meta.errors (error_type, occurred_at DESC)
""")
    op.execute("""
CREATE TABLE bronze_meta.commit_files (
	commit_id UUID NOT NULL, 
	file_number INTEGER NOT NULL, 
	table_name TEXT NOT NULL, 
	bucket TEXT NOT NULL, 
	object_key TEXT NOT NULL, 
	row_count BIGINT NOT NULL, 
	size_bytes BIGINT NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	schema_version INTEGER NOT NULL, 
	CONSTRAINT pk_commit_files PRIMARY KEY (commit_id, file_number), 
	CONSTRAINT uq_committed_object UNIQUE (bucket, object_key), 
	CONSTRAINT ck_commit_files_bounds CHECK (file_number >= 0 AND row_count >= 0 AND size_bytes > 0 AND schema_version > 0), 
	CONSTRAINT ck_commit_files_names CHECK (table_name <> '' AND bucket <> '' AND object_key <> ''), 
	CONSTRAINT ck_commit_files_sha256 CHECK (sha256 ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT fk_commit_files_commit_id_commits FOREIGN KEY(commit_id) REFERENCES bronze_meta.commits (id)
)
""")
    op.execute("""
ALTER TABLE bronze_meta.partitions ADD CONSTRAINT fk_partition_current_commit FOREIGN KEY(id, current_commit_id) REFERENCES bronze_meta.commits (partition_id, id)
""")
    op.execute("""
ALTER TABLE bronze_meta.attempts ADD CONSTRAINT fk_attempt_base_commit FOREIGN KEY(partition_id, base_commit_id) REFERENCES bronze_meta.commits (partition_id, id)
""")


def downgrade():
    op.execute("""
ALTER TABLE bronze_meta.partitions DROP CONSTRAINT fk_partition_current_commit
""")
    op.execute("""
ALTER TABLE bronze_meta.attempts DROP CONSTRAINT fk_attempt_base_commit
""")
    op.execute("""
DROP TABLE bronze_meta.commit_files
""")
    op.execute("""
DROP TABLE bronze_meta.errors
""")
    op.execute("""
DROP TABLE bronze_meta.commits
""")
    op.execute("""
DROP TABLE bronze_meta.partition_leases
""")
    op.execute("""
DROP TABLE bronze_meta.attempt_pages
""")
    op.execute("""
DROP TABLE bronze_meta.attempts
""")
    op.execute("""
DROP TABLE bronze_meta.partitions
""")
    op.execute("""
DROP SCHEMA bronze_meta
""")
