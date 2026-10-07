"""CLI entry point for migrating historical S3 manifests to PostgreSQL metadata."""

from procurement.tools.import_metadata import main

if __name__ == "__main__":
    main()
