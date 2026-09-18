"""Cooperative interruption shared by the scheduler and ingestion lifecycle."""


class IngestionInterrupted(RuntimeError):
    pass


INTERRUPTIONS = (KeyboardInterrupt, SystemExit, IngestionInterrupted)
