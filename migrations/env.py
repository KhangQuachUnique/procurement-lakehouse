"""Migrate only the application schema, using an explicit APP_DATABASE_URL."""

from alembic import context

from procurement.infrastructure.database import application_database_url, create_application_engine
from procurement.metadata.postgres.schema import SCHEMA, metadata


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name == SCHEMA
    return True


def configure(connection=None):
    options = {
        "target_metadata": metadata,
        "include_schemas": True,
        "include_name": include_name,
        "compare_type": True,
        "compare_server_default": True,
    }
    if connection is None:
        context.configure(url=application_database_url(), literal_binds=True, **options)
    else:
        context.configure(connection=connection, **options)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    configure()
else:
    supplied = context.config.attributes.get("connection")
    if supplied is not None:
        configure(supplied)
    else:
        engine = create_application_engine()
        try:
            with engine.connect() as connection:
                configure(connection)
        finally:
            engine.dispose()
