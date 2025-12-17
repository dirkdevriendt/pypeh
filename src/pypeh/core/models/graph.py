import importlib
import peh_model.peh as peh

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.internal_data_layout import JoinSpec
from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface


@dataclass(frozen=True, order=True)
class Node:
    dataset_label: str
    field_label: str


class Delayed:
    def __init__(self, map_fn: Callable, output_dtype):
        self.map_fn = map_fn
        self.arg_sources = {}
        self.join_specs: list[list[JoinSpec]] = []
        self.output_dtype = output_dtype

    def add_parent(self, parent: Node, map_name: str, join_specs: list[JoinSpec] | None = None):
        self.arg_sources[map_name] = parent
        if join_specs is not None:
            self.join_specs.append(join_specs)

    @property
    def parents(self) -> list[Node]:
        return list(self.arg_sources.values())

    def build_callable(self, adapter):
        """
        adapter: an object implementing adapter.apply_map(...) and adapter.apply_join(...)
        """
        map_fn = self.map_fn
        arg_sources = self.arg_sources
        join_specs = self.join_specs
        output_dtype = adapter.map_type(self.output_dtype)

        def _apply(datasets: dict, *, node: Node, base_fields: dict):
            """
            datasets: dict of dataset_label → dataset object (lazy or eager)
            parent_results: mapping from parent Node → computed result for this node
            """
            ds = datasets[node.dataset_label]
            # Apply all joins
            if join_specs:
                ds = adapter.apply_joins(
                    datasets=datasets,
                    join_specs=join_specs,
                    node=node,
                )

            # Build column expressions for the map function
            kwargs = {}
            for arg_name, parent_node in arg_sources.items():
                col_name = parent_node.field_label
                kwargs[arg_name] = adapter.select_field(ds, col_name)

            # Apply the map
            base_fields_subset = base_fields.get(node.dataset_label, None)
            assert base_fields_subset is not None
            out = adapter.apply_map(ds, map_fn, node.field_label, output_dtype, base_fields_subset, **kwargs)
            base_fields_subset.append(node.field_label)

            return out

        return _apply


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
        # TODO: improve map name, referes to kwarg represented by the parent
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

    def compile(self, adapter: OutDataOpsInterface) -> ExecutionPlan:
        sorted_nodes = self.topological_sort()
        steps: list[ExecutionStep] = []

        for node in sorted_nodes:
            delayed = self.delayed_fns.get(node)
            if delayed is None:
                continue

            compute_fn = delayed.build_callable(adapter)
            steps.append(ExecutionStep(node=node, compute=compute_fn))

        ret = ExecutionPlan(steps)
        self.execution_plan = ret

        return ret

    def compute(self, datasets: dict, adapter: OutDataOpsInterface):
        """
        datasets are updated in place
        """
        if self.execution_plan is None:
            raise AssertionError("The graph needs to be compiled first to set up an execution plan: `Graph.compile()`")

        base_fields = {label: adapter.get_element_labels(ds) for label, ds in datasets.items()}

        self.execution_plan.run(datasets, base_fields)
        adapter.collect(datasets)

    @staticmethod
    def _extract_callable(path: str) -> Callable:
        assert "." in path, "Could not split path into module and func_name"
        module_name, func_name = path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    @classmethod
    def from_observations(
        cls, observations: list[peh.Observation], cache_view: CacheContainerView, join_spec_mapping: dict | None = None
    ) -> "Graph":
        g = cls()

        nested_entity_paths = [
            ["observation_design", "identifying_observable_property_id_list"],
            ["observation_design", "required_observable_property_id_list"],
            ["observation_design", "optional_observable_property_id_list"],
        ]

        for observation in observations:
            observation_id = observation.id
            for path in nested_entity_paths:
                for observable_property in cache_view.walk_entity(
                    entity_id=observation_id, nested_entity_path=path, entity_type="Observation"
                ):
                    assert isinstance(observable_property, peh.ObservableProperty)
                    calculation_designs = observable_property.calculation_designs
                    if calculation_designs:
                        child = Node(dataset_label=observation_id, field_label=observable_property.id)
                        if len(calculation_designs) > 1:
                            raise NotImplementedError(
                                "No current implementation for multiple calculation designs linked to a single observable property"
                            )

                        calculation_design = calculation_designs[0]
                        assert isinstance(calculation_design, peh.CalculationDesign)
                        calculation_implementation = calculation_design.calculation_implementation
                        assert isinstance(calculation_implementation, peh.CalculationImplementation)
                        # EXTRACT FUNCTION NAME
                        function_name = calculation_implementation.function_name
                        assert isinstance(function_name, str)
                        map_fn = cls._extract_callable(function_name)
                        output_dtype = observable_property.value_type
                        assert output_dtype is not None
                        g.add_node(child, node_fn=map_fn, output_dtype=output_dtype)
                        assert isinstance(calculation_implementation, peh.CalculationImplementation)
                        function_kwargs = calculation_implementation.function_kwargs
                        assert function_kwargs is not None
                        # MAKE PARENTS FOR ALL KWARGS
                        for function_kwarg in function_kwargs:
                            assert isinstance(function_kwarg, peh.CalculationKeywordArgument)
                            field_ref = function_kwarg.contextual_field_reference
                            assert isinstance(field_ref, peh.ContextualFieldReference)
                            dataset_label = field_ref.dataset_label
                            assert dataset_label is not None
                            field_label = field_ref.field_label
                            assert field_label is not None
                            parent = Node(dataset_label=dataset_label, field_label=field_label)
                            map_name = function_kwarg.mapping_name
                            join_spec = None
                            if join_spec_mapping is not None:
                                join_spec = join_spec_mapping.get(frozenset([observation_id, dataset_label]), None)
                            g.add_edge(parent, child, map_name=map_name, join_spec=join_spec)

        return g
