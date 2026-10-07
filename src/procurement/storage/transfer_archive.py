"""Compatibility shim for transfer archive.

Delegates to `procurement.transfer.archive`.
"""
import sys

from procurement.transfer import archive

sys.modules[__name__] = archive
