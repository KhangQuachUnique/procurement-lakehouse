"""Namespace-bound catalog guard for source writers, including final pin/cleanup paths."""

import re


class NamespaceCatalog:
    def __init__(self, catalog, namespace):
        self.catalog, self.namespace = catalog, namespace

    def _check(self, namespace):
        if namespace != self.namespace:
            raise ValueError("Catalog access outside the writer namespace refused")

    def table(self, namespace, name):
        self._check(namespace)
        return self.catalog.table(namespace, name)

    def pin(self, namespace, *args, **kwargs):
        self._check(namespace)
        return self.catalog.pin(namespace, *args, **kwargs)

    def create_namespace_if_not_exists(self, namespace):
        self._check(namespace)
        if hasattr(self.catalog, "create_namespace_if_not_exists"):
            return self.catalog.create_namespace_if_not_exists(namespace)
        return None

    def request(self, method, path, **kwargs):
        match = re.match(r"^namespaces/([a-z][a-z0-9_]*)/tables(?:/|$)", path.lstrip("/"))
        if match is None:
            raise ValueError("Unscoped catalog request refused")
        self._check(match[1])
        return self.catalog.request(method, path, **kwargs)

    def close(self):
        self.catalog.close()
