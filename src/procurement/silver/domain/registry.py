"""Registry for Silver resource transformers."""

from typing import TypeVar

from procurement.silver.domain.protocols import SilverTransformerProtocol

T = TypeVar("T", bound=type[SilverTransformerProtocol])

_REGISTRY: dict[str, type[SilverTransformerProtocol]] = {}


def register_transformer(table_name: str):
    """Decorator to register a transformer class for a Bronze table."""

    def decorator(cls: T) -> T:
        _REGISTRY[table_name] = cls
        return cls

    return decorator


def get_transformer(table_name: str) -> SilverTransformerProtocol | None:
    """Retrieve an instantiated transformer for table_name, or None."""
    _ensure_loaded()
    cls = _REGISTRY.get(table_name)
    return cls() if cls else None


def require_transformer(table_name: str) -> SilverTransformerProtocol:
    """Retrieve an instantiated transformer for table_name, raising if missing."""
    transformer = get_transformer(table_name)
    if transformer is None:
        raise ValueError(f"Silver transformer not implemented: {table_name}")
    return transformer


def transformer_contracts(tables: set[str]) -> dict[str, dict]:
    """Only relevant transformers participate in a build's cache contract."""
    return {table: dict(require_transformer(table).revision_contracts) for table in sorted(tables)}


def _ensure_loaded() -> None:
    """Ensure transformer modules are imported and registered."""
    import procurement.silver.domain.transformers  # noqa: F401


__all__ = [
    "get_transformer",
    "register_transformer",
    "require_transformer",
    "transformer_contracts",
]
