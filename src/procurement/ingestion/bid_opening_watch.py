"""Compatibility shim for late bid opening watch.

Delegates to `procurement.watcher`.
"""
import sys

from procurement.watcher import service

sys.modules[__name__] = service
