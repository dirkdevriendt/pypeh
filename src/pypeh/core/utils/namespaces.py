from __future__ import annotations

import logging

from typing import Dict

logger = logging.getLogger(__name__)


class PrefixMap:
    def __init__(self, prefixes: Dict[str, str]):
        """
        example_prefixes = {
            "foaf": "https://xmlns.com/foaf/0.1/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "schema": "https://schema.org/",
        }
        """
        self.prefixes = prefixes
        self.reverse_map = {uri: prefix for prefix, uri in prefixes.items()}

    def expand(self, curie: str) -> str:
        """Expand a CURIE to a full URI."""
        prefix, suffix = curie.split(":", 1)
        if prefix in self.prefixes:
            return f"{self.prefixes[prefix]}{suffix}"
        raise ValueError(f"Unknown prefix: {prefix}")

    def compress(self, uri: str) -> str:
        """Compress a full URI to a CURIE if possible."""
        for ns, prefix in self.reverse_map.items():
            if uri.startswith(ns):
                return f"{prefix}:{uri[len(ns):]}"
        return uri  # fallback to full URI if no prefix match


class ImportMapTrieNode:
    def __init__(self):
        self.children = {}
        self.connection_str = None


class ImportMap:
    """
    Implementation of a trie with dict-like behaviour. For any namespace the
    closest matching namespace and its connection string will be returned.
    """

    def __init__(self):
        self.root = ImportMapTrieNode()
        self._data = set()

    def insert(self, namespace, connection_str):
        self._data.add(namespace)
        parts = self._split_namespace(namespace)
        node = self.root
        for part in parts:
            if part not in node.children:
                node.children[part] = ImportMapTrieNode()
            node = node.children[part]
        node.connection_str = connection_str

    def match(self, uri_or_curie):
        # TODO: if curie first convert to uri
        parts = self._split_namespace(uri_or_curie)
        node = self.root
        last_match = None
        for part in parts:
            if part in node.children:
                node = node.children[part]
                if node.connection_str:
                    last_match = node.connection_str
            else:
                break
        return last_match

    def __getitem__(self, key):
        return self.match(key)

    def __setitem__(self, key, value):
        return self.insert(key, value)

    def __contains__(self, key):
        value = self.match(key)
        return value is not None

    def get(self, key, default=None):
        ret = self.match(key)
        if ret is None:
            return default
        return ret

    def keys(self):
        return list(self._data)

    def values(self):
        return [self.match(key) for key in self._data]

    def items(self):
        return {key: self.match(key) for key in self._data}

    def __iter__(self):
        return iter(self.keys())

    def _split_namespace(self, uri):
        # Split only on "/" and "#"
        return [p for p in uri.replace("#", "/").split("/") if p]
