"""Compact connected components on immutable, token-sorted bipartite edges.

No token dictionary or token vertices in RAM: a token's first node is its anchor.
Rebuild this structure for a new edge set; union-only edits cannot model deletion.
Canonical labels are minimum numeric IDs (assigned in original-key order).
"""

import hashlib
import json
import os
import sys
from array import array
from pathlib import Path
from uuid import uuid4


def _sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024**2), b""):
            h.update(block)
    return h.hexdigest()


def _directory_sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class UnionFind:
    def __init__(self, count):
        if type(count) is not int or not 0 <= count <= 2**32 - 1:
            raise ValueError("Union-Find requires a nonnegative uint32 node count")
        if array("I").itemsize != 4:
            raise ValueError("This runtime requires four-byte unsigned integers")
        self.parent = array("I", range(count))
        self.minimum = array("I", range(count))
        self.rank = bytearray(count)
        self.edges = 0
        self.tokens = 0
        self.last_token = None
        self.last_node = None
        self.anchor = None

    @property
    def bytes(self):
        return (len(self.parent) + len(self.minimum)) * 4 + len(self.rank)

    def find(self, node):
        parent = self.parent
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(self, first, second):
        a, b = self.find(first), self.find(second)
        if a == b:
            return
        if self.rank[a] < self.rank[b]:
            a, b = b, a
        self.parent[b] = a
        self.minimum[a] = min(self.minimum[a], self.minimum[b])
        if self.rank[a] == self.rank[b]:
            self.rank[a] += 1

    def feed(self, rows):
        for token, node in rows:
            if (
                not isinstance(token, str)
                or type(node) is not int
                or not 0 <= node < len(self.parent)
            ):
                raise ValueError("Invalid token/node edge")
            if self.last_token is not None and (token, node) <= (self.last_token, self.last_node):
                raise ValueError("Edges must be distinct and sorted by token,node ID")
            if token != self.last_token:
                self.anchor = node
                self.tokens += 1
            else:
                self.union(self.anchor, node)
            self.last_token, self.last_node = token, node
            self.edges += 1

    def label(self, node):
        return self.minimum[self.find(node)]

    def save(self, directory, identity):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        generation = uuid4().hex
        destination = directory / generation
        destination.mkdir()
        hashes = {}
        for name, values in (
            ("parent", self.parent),
            ("minimum", self.minimum),
            ("rank", self.rank),
        ):
            path = destination / (name + ".bin")
            with path.open("xb") as stream:
                stream.write(memoryview(values))
                stream.flush()
                os.fsync(stream.fileno())
            hashes[name] = _sha(path)
        manifest = {
            "version": 1,
            "identity": identity,
            "count": len(self.parent),
            "byteorder": sys.byteorder,
            "itemsize": 4,
            "hashes": hashes,
            "edges": self.edges,
            "tokens": self.tokens,
            "last_token": self.last_token,
            "last_node": self.last_node,
            "anchor": self.anchor,
        }
        path = destination / "manifest.json"
        with path.open("x") as stream:
            json.dump(manifest, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        _directory_sync(destination)
        pointer = directory / (generation + ".tmp")
        with pointer.open("x") as stream:
            json.dump({"generation": generation, "manifest_sha256": _sha(path)}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pointer, directory / "CURRENT")
        _directory_sync(directory)

    @classmethod
    def load(cls, directory, identity):
        if array("I").itemsize != 4:
            raise ValueError("This runtime requires four-byte unsigned integers")
        directory = Path(directory)
        pointer = json.loads((directory / "CURRENT").read_text())
        generation = pointer["generation"]
        if len(generation) != 32 or any(c not in "0123456789abcdef" for c in generation):
            raise ValueError("Invalid Union-Find checkpoint generation")
        base = directory / generation
        path = base / "manifest.json"
        if _sha(path) != pointer["manifest_sha256"]:
            raise ValueError("Union-Find manifest checksum mismatch")
        data = json.loads(path.read_text())
        if (data["version"], data["identity"], data["byteorder"], data["itemsize"]) != (
            1,
            identity,
            sys.byteorder,
            4,
        ):
            raise ValueError("Union-Find checkpoint identity/format mismatch")
        # Do not allocate initial arrays only to replace them during restore.
        result = cls.__new__(cls)
        for name in ("parent", "minimum", "rank"):
            path = base / (name + ".bin")
            width = 1 if name == "rank" else 4
            if path.stat().st_size != data["count"] * width or _sha(path) != data["hashes"][name]:
                raise ValueError("Union-Find array checksum/size mismatch")
            if name == "rank":
                values = bytearray(path.read_bytes())
            else:
                values = array("I")
                with path.open("rb") as stream:
                    values.fromfile(stream, data["count"])
            setattr(result, name, values)
        for name in ("edges", "tokens", "last_token", "last_node", "anchor"):
            setattr(result, name, data[name])
        return result
