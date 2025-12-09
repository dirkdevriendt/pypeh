from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Any


@dataclass(frozen=True, order=True)
class Node:
    dataset_label: str
    field_label: str


@dataclass(frozen=True)
class Edge:
    parent: Node
    child: Node


class Graph:
    # NOTE: This graph can only be traversed root to leaves
    def __init__(self) -> None:
        self.graph = defaultdict(set)
        self.nodes: set[Node] = set()
        self.arg_names: dict[Edge, str | None] = {}
        self.compute_fns: dict[Node, Callable[..., Any]] = {}

    def _add_node(self, node: Node) -> None:
        self.nodes.add(node)
        if node not in self.graph:
            self.graph[node]

    def add_edge(self, parent: Node, child: Node, arg_name: str | None = None) -> None:
        self._add_node(parent)
        self._add_node(child)
        self.graph[parent].add(child)
        edge = Edge(parent, child)
        self.arg_names[edge] = arg_name

    @property
    def edges(self):
        edges = []
        for edge in self.arg_names:
            edges.append((edge.parent, edge.child))
        return edges

    def get_children(self, node: Node) -> set[Node]:
        return self.graph.get(node, set())

    def get_parents(self, node: Node) -> set[Node]:
        parents = set()
        for parent, children in self.graph.items():
            if node in children:
                parents.add(parent)
        return parents

    def topological_sort(self) -> list[Node]:
        in_degree = defaultdict(int)

        for node in self.nodes:
            in_degree[node] = 0

        for parent in self.graph:
            for child in self.graph[parent]:
                in_degree[child] += 1

        queue = deque([node for node in self.nodes if in_degree[node] == 0])

        sorted_nodes = []
        while queue:
            node = queue.popleft()
            sorted_nodes.append(node)

            for child_node in self.graph[node]:
                in_degree[child_node] -= 1
                if in_degree[child_node] == 0:
                    queue.append(child_node)

        if len(sorted_nodes) != len(self.nodes):
            remaining = [node for node in self.nodes if node not in sorted_nodes]
            remaining.sort()
            raise ValueError(f"Circular dependency detected! Remaining variables: {remaining}")

        return sorted_nodes
