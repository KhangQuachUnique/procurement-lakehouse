"""Registry for Silver resource transformers.

Allows decoupled registration so each developer can work in their own module
and simply decorate their transformer class with `@register_transformer`.
"""


from procurement.processing.silver.protocols import SilverTransformerProtocol

_REGISTRY: dict[str, SilverTransformerProtocol] = {}


def register_transformer(table_name: str):
    """Decorator to register a transformer class for a specific Bronze table."""

    def decorator(cls: type[SilverTransformerProtocol]):
        instance = cls()
        _REGISTRY[table_name] = instance
        return cls

    return decorator


def get_transformer(table_name: str) -> SilverTransformerProtocol | None:
    """Retrieve registered transformer for a Bronze table."""
    return _REGISTRY.get(table_name)


def list_registered_tables() -> list[str]:
    """List all table names that have registered transformers."""
    return sorted(_REGISTRY.keys())
