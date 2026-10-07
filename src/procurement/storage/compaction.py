"""Offline, append-only daily Bronze compaction published through ordinary SUCCESS commits (legacy shim)."""

import sys

from procurement.compaction import executor

# Alias module so monkeypatching procurement.storage.compaction directly mutates executor
sys.modules[__name__] = executor
