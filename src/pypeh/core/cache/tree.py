"""
This module provides functionality for creating and manipulating an in-memory tree-like
representation of data.

Usage:
    Use this module to define in-memory data structures containing data adhering to the
    PEH-model.

"""

import logging

from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import Dict, Generic, Optional, Type, TypeVar, Union

from linkml_runtime.loaders import yaml_loader
from peh import NamedThing, EntityList

logger = logging.getLogger(__name__)


# Type variable for generic storage
T = TypeVar("T", bound=NamedThing)


class StorageBackend(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def store(self, entity_type: str, entity_id: str, entity: T) -> None:
        """Store an entity"""
        pass

    @abstractmethod
    def retrieve(self, entity_type: str, entity_id: str) -> Optional[T]:
        """Retrieve an entity"""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all stored data"""
        pass


class TreeNode(Generic[T]):
    """Represents a node in the entity tree"""

    def __init__(self, entity: T):
        self.entity = entity
        self.children: Dict[str, "TreeNode[T]"] = {}
        self.parent: Optional["TreeNode[T]"] = None

    def add_child(self, child: "TreeNode[T]") -> None:
        """Add a child node"""
        self.children[child.entity.id] = child
        child.parent = self


class InMemoryTreeStorage(StorageBackend):
    """In-memory storage implementation using a tree structure"""

    def __init__(self):
        self._storage: Dict[str, Dict[str, TreeNode]] = defaultdict(dict)

    def store(self, entity_type: str, entity_id: str, entity: T) -> None:
        """Store an entity in the tree structure"""
        node = TreeNode(entity)
        self._storage[entity_type][entity_id] = node
        self._build_relationships(node)

    def retrieve(self, entity_type: str, entity_id: str) -> Optional[T]:
        """Retrieve an entity from storage"""
        try:
            return self._storage[entity_type][entity_id].entity
        except KeyError:
            logger.debug(f"Entity not found: {entity_type}/{entity_id}")
            return None

    def clear(self) -> None:
        """Clear all stored data"""
        self._storage.clear()

    def _build_relationships(self, node: TreeNode) -> None:
        """Build relationships between entities in the tree"""
        entity = node.entity
        for property_name in entity._keys():
            property_value = getattr(entity, property_name)
            if property_value is None:
                continue

            if isinstance(property_value, NamedThing):
                child_node = TreeNode(property_value)
                node.add_child(child_node)
                self._build_relationships(child_node)
            elif isinstance(property_value, (list, tuple)):
                for item in property_value:
                    if isinstance(item, NamedThing):
                        child_node = TreeNode(item)
                        node.add_child(child_node)
                        self._build_relationships(child_node)


class EntityManager:
    """High-level entity management class"""

    def __init__(self, storage_backend: StorageBackend):
        self.storage = storage_backend

    def save_entity(self, entity: T) -> None:
        """Save an entity to storage"""
        entity_type = entity.__class__.__name__
        self.storage.store(entity_type, entity.id, entity)

    def get_entity(self, entity_type: Union[str, Type[T]], entity_id: str) -> Optional[T]:
        """Retrieve an entity from storage"""
        if isinstance(entity_type, type):
            entity_type = entity_type.__name__
        return self.storage.retrieve(entity_type, entity_id)

    def load_from_yaml(self, file_path: Union[str, Path]) -> None:
        """Load entities from a YAML file"""
        entity_list = yaml_loader.load(str(file_path), EntityList)
        for entity_list_name in entity_list._keys():
            for entity in getattr(entity_list, entity_list_name):
                self.save_entity(entity)

    def load_from_folder(self, folder_path: Union[str, Path]) -> None:
        """Load entities from all YAML files in a folder"""
        folder = Path(folder_path)
        for file_path in folder.glob("**/*.yaml"):
            self.load_from_yaml(file_path)


# Future RDF implementation example:
"""
class RDFStorage(StorageBackend):
    def __init__(self):
        self.graph = rdflib.Graph()
        
    def store(self, entity_type: str, entity_id: str, entity: T) -> None:
        # Convert entity to RDF and add to graph
        pass
        
    def retrieve(self, entity_type: str, entity_id: str) -> Optional[T]:
        # Query graph and convert result back to entity
        pass
        
    def clear(self) -> None:
        self.graph = rdflib.Graph()
"""
