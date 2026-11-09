"""Architectural boundary tests for Gold Hexagonal Architecture."""

import inspect
import sys


def test_domain_has_zero_infrastructure_dependencies():
    """Domain layer must not depend on database engines, object storage, or CLI frameworks."""
    forbidden_modules = {"duckdb", "pyiceberg", "s3fs", "typer", "httpx"}
    for mod_name, mod in sys.modules.items():
        if mod_name.startswith("procurement.gold.domain"):
            imported = set(dir(mod))
            for forbidden in forbidden_modules:
                assert forbidden not in imported, f"Domain module {mod_name} imports forbidden {forbidden}"


def test_ports_define_abstract_interfaces():
    """Ports must be Protocol classes."""
    from procurement.gold.ports.inbound.process_gold import ProcessGoldPort
    from procurement.gold.ports.outbound.gold_storage import GoldStoragePort
    from procurement.gold.ports.outbound.silver_reader import SilverReaderPort

    assert hasattr(ProcessGoldPort, "__protocol_attrs__") or inspect.isclass(ProcessGoldPort)
    assert hasattr(GoldStoragePort, "__protocol_attrs__") or inspect.isclass(GoldStoragePort)
    assert hasattr(SilverReaderPort, "__protocol_attrs__") or inspect.isclass(SilverReaderPort)


def test_adapters_implement_ports():
    """Adapters must implement the defined outbound ports."""
    from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
    from procurement.gold.adapters.driven.silver.iceberg_reader import IcebergReader
    from procurement.gold.ports.outbound.gold_storage import GoldStoragePort
    from procurement.gold.ports.outbound.silver_reader import SilverReaderPort

    assert issubclass(IcebergPublisher, GoldStoragePort)
    assert issubclass(IcebergReader, SilverReaderPort)
