from __future__ import annotations

import hashlib
import json
import logging

from dataclasses import is_dataclass
from typing import Dict, Callable, Type, Any


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
        self.connection_str: str | None = None


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


class NamespaceManager:
    def __init__(self):
        self.namespaces: Dict[str, str] = {}  # prefix -> base IRI
        self.class_prefixes: Dict[Type, str] = {}  # dataclass → prefix
        self.suffix_strategy: Callable[[Any], str] = self.default_suffix()

    def bind(self, namespace_label: str, base_iri: str):
        if not base_iri.endswith("/"):
            base_iri += "/"
        self.namespaces[namespace_label] = base_iri

    def register_class(self, cls: Type, prefix: str):
        if not is_dataclass(cls):
            raise TypeError(f"{cls} is not a dataclass")
        self.class_prefixes[cls] = prefix

    def default_suffix(self, length: int = 16):
        def _hash_suffix(obj):
            d = obj.__dict__
            canonical = json.dumps(d, sort_keys=True, separators=(",", ":"))
            h = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            return h[:length]

        return _hash_suffix

    def set_suffix_strategy(self, func: Callable[[Any], str]):
        # signature of func should be def f(obj):
        self.suffix_strategy = func

    def mint(self, obj, identifying_field: str = "id") -> str:
        # minted IRI will be of form namespace/prefix/suffix
        cls = obj.__class__
        data = obj.__dict__.copy()
        data.pop(identifying_field, None)

        if cls not in self.class_prefixes:
            raise ValueError(f"No prefix registered for class {cls.__name__}")

        prefix = self.class_prefixes[cls]

        if prefix not in self.namespaces:
            raise ValueError(f"No namespace bound for prefix '{prefix}'")

        base = self.namespaces[prefix]
        suffix = self.suffix_strategy(obj)

        return f"{base}{prefix}/{suffix}"

    def mint_and_set(self, obj, identifying_field: str = "id"):
        iri = self.mint(obj, identifying_field=identifying_field)
        setattr(obj, identifying_field, iri)
