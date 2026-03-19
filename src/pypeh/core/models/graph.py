from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable

from pypeh.core.models.internal_data_layout import JoinSpec
from pypeh.core.utils.function_utils import _extract_callable


@dataclass(frozen=True, order=True)
class Node:
    """
    A frozen variant of peh.ContextualFieldReference.
    """

    dataset_label: str
    field_label: str


class Delayed:
    def __init__(self, map_fn: Callable, output_dtype):
        self.map_fn = map_fn
        self.arg_sources = {}  # refers to kwarg represented by the parent
        self.join_specs: list[list[JoinSpec]] = []
        self.output_dtype = output_dtype

    def add_parent(self, parent: Node, map_name: str, join_specs: list[JoinSpec] | None = None):
        self.arg_sources[map_name] = parent
        if join_specs is not None:
            self.join_specs.append(join_specs)

    @property
    def parents(self) -> list[Node]:
        return list(self.arg_sources.values())


@dataclass
class ExecutionStep:
    node: Node
    compute: Callable


@dataclass
class ExecutionPlan:
    steps: list[ExecutionStep]

    def run(self, datasets: dict, base_fields: dict):
        for step in self.steps:
            result = step.compute(datasets, node=step.node, base_fields=base_fields)
            datasets[step.node.dataset_label] = result

    def __len__(self):
        return len(self.steps)


class Graph:
    # NOTE: This graph can only be traversed root to leaves
    def __init__(self) -> None:
        self.graph = defaultdict(set)
        self.nodes: set[Node] = set()
        self.delayed_fns: dict[Node, Delayed] = {}
        self.execution_plan: ExecutionPlan | None = None

    def _reset_execution_plan(self):
        if self.execution_plan is not None:
            self.execution_plan = None

    def _add_node(self, node: Node) -> None:
        if node not in self.nodes:
            self.nodes.add(node)
            self.graph[node]

    def _add_computation(self, node: Node, map_fn: Callable, output_dtype: str) -> None:
        self.delayed_fns[node] = Delayed(map_fn=map_fn, output_dtype=output_dtype)

    def add_node(self, node: Node, node_fn: Callable, output_dtype):
        self._add_node(node)
        self._add_computation(node, node_fn, output_dtype)

    def add_edge(
        self, parent: Node, child: Node, map_name: str | None = None, join_spec: list[JoinSpec] | None = None
    ) -> None:
        # TODO: improve map name, refers to kwarg represented by the parent
        self._add_node(parent)
        self._add_node(child)
        if map_name is not None:
            if child in self.delayed_fns:
                child_delayed = self.delayed_fns[child]
                child_delayed.add_parent(parent, map_name, join_spec)
            else:
                raise ValueError("No Delayed function has been defined for node {child}")

        self.graph[parent].add(child)
        self._reset_execution_plan()

    @property
    def edges(self):
        edges = []
        for parent, children in self.graph.items():
            for child in children:
                edges.append((parent, child))
        return edges

    def get_children(self, node: Node) -> set[Node]:
        return self.graph.get(node, set())

    def get_parents(self, node: Node) -> set[Node]:
        # NOTE: this does not scale well
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

    def add_calculation_target(
        self,
        target: Node,
        function_name: str,
        result_dtype: str,
    ):
        child = target
        map_fn = _extract_callable(function_name)
        self.add_node(child, node_fn=map_fn, output_dtype=result_dtype)

    def add_calculation_source(
        self,
        source: Node,
        target: Node,
        source_mapping_name: str,
        join_spec: list[JoinSpec] | None = None,
    ):
        child = target
        parent = source
        self.add_edge(parent, child, map_name=source_mapping_name, join_spec=join_spec)
