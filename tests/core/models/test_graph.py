import pytest

from pypeh.adapters.outbound.persistence.hosts import DirectoryIO
from pypeh.core.cache.containers import (
    CacheContainerFactory,
    CacheContainerView,
)
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.core.models.graph import Graph, Node
from pypeh.core.interfaces.outbound.dataops import DataEnrichmentInterface
from pypeh.core.models.internal_data_layout import ContextIndexProtocol
from tests.test_utils.dirutils import get_absolute_path


@pytest.mark.core
class TestGraph:
    def test_add_node(self):
        g = Graph()
        g._add_node(Node("A", "A"))
        assert Node("A", "A") in g.nodes
        assert Node("A", "A") in g.graph
        assert g.graph[Node("A", "A")] == set()

    def test_add_edge(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        assert Node("A", "A") in g.nodes
        assert Node("B", "B") in g.nodes
        assert Node("B", "B") in g.graph[Node("A", "A")]
        assert g.graph[Node("B", "B")] == set()

    def test_get_edges(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("A", "A"), Node("C", "C"))
        g.add_edge(Node("B", "B"), Node("C", "C"))
        edges = g.edges
        expected_edges = [
            (Node("A", "A"), Node("B", "B")),
            (Node("A", "A"), Node("C", "C")),
            (Node("B", "B"), Node("C", "C")),
        ]
        assert set(edges) == set(expected_edges)

    def test_get_children(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("A", "A"), Node("C", "C"))
        children = g.get_children(Node("A", "A"))
        assert children == {Node("B", "B"), Node("C", "C")}
        assert g.get_children(Node("B", "B")) == set()

    def test_get_parents(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("C", "C"), Node("B", "B"))
        parents = g.get_parents(Node("B", "B"))
        assert parents == {Node("A", "A"), Node("C", "C")}
        assert g.get_parents(Node("A", "A")) == set()

    def test_topological_sort(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("B", "B"), Node("C", "C"))
        g.add_edge(Node("A", "A"), Node("C", "C"))
        g.add_edge(Node("C", "C"), Node("D", "D"))
        g.add_edge(Node("B", "B"), Node("D", "D"))
        g.add_edge(Node("E", "E"), Node("D", "D"))
        sorted_nodes = g.topological_sort()
        assert (
            sorted_nodes.index(Node("A", "A"))
            < sorted_nodes.index(Node("B", "B"))
            < sorted_nodes.index(Node("C", "C"))
            < sorted_nodes.index(Node("D", "D"))
        )

    def test_topological_sort_with_single_node(self):
        g = Graph()
        g._add_node(Node("A", "A"))
        sorted_nodes = g.topological_sort()
        assert sorted_nodes == [Node("A", "A")]

    def test_topological_sort_with_independent_nodes(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("C", "C"), Node("D", "D"))
        g.add_edge(Node("E", "E"), Node("F", "F"))
        sorted_nodes = g.topological_sort()
        assert sorted_nodes.index(Node("A", "A")) < sorted_nodes.index(
            Node("B", "B")
        )
        assert sorted_nodes.index(Node("C", "C")) < sorted_nodes.index(
            Node("D", "D")
        )
        assert sorted_nodes.index(Node("E", "E")) < sorted_nodes.index(
            Node("F", "F")
        )

    def test_topological_sort_with_cycle(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("B", "B"), Node("C", "C"))
        g.add_edge(Node("C", "C"), Node("A", "A"))  # Creates a cycle
        g.add_edge(Node("D", "D"), Node("E", "E"))

        with pytest.raises(ValueError) as excinfo:
            g.topological_sort()

        assert "Circular dependency detected" in str(excinfo.value)


class MockIndex(ContextIndexProtocol):
    def context_lookup(
        self, observation_id: str, observable_property_id: str
    ) -> tuple[str, str]:
        return (observation_id, observable_property_id)


@pytest.mark.core
class TestEnrichmentInterfaceCore:
    def container(self, path: str) -> CacheContainerView:
        source = get_absolute_path(path)
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml", maxdepth=3)
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return CacheContainerView(container)

    def test_building_dependency_graph(self):
        interface = DataEnrichmentInterface()
        container = self.container(
            "./input/dependency_graph/Enrichment_01_SINGLE_SOURCE"
        )
        observations = list(container.get_all("Observation"))
        g = interface.build_dependency_graph(
            observations,  # type: ignore
            context_index=MockIndex(),
            cache_view=container,
        )

        # Simple check to see if the dependency graph is built
        assert isinstance(g, Graph)

    def test_topological_sort_single_source(self):
        interface = DataEnrichmentInterface()
        src_path = "./input/dependency_graph/Enrichment_01_SINGLE_SOURCE"
        container = self.container(src_path)
        observations = list(container.get_all("Observation"))
        g = interface.build_dependency_graph(
            observations,  # type: ignore
            context_index=MockIndex(),
            cache_view=container,
        )
        sorted_nodes = g.topological_sort()
        # Simple check to see if the sorted variables list is correct
        assert isinstance(g, Graph)
        assert all(isinstance(var, Node) for var in sorted_nodes)
        assert len(sorted_nodes) == len(g.nodes)
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "N1Birthdate",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "Todaysdate",
            )
        )

    def test_topological_sort_linked_source(self):
        interface = DataEnrichmentInterface()
        src_path = "./input/dependency_graph/Enrichment_02_LINKED_SOURCE"
        container = self.container(src_path)
        observations = list(container.get_all("Observation"))
        g = interface.build_dependency_graph(
            observations,  # type: ignore
            context_index=MockIndex(),
            cache_view=container,
        )
        sorted_nodes = g.topological_sort()
        # Simple check to see if the sorted variables list is correct
        assert isinstance(g, Graph)
        assert all(isinstance(var, Node) for var in sorted_nodes)
        assert len(sorted_nodes) == len(g.nodes)
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "N1Birthdate",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_HOUSEHOLD_INGESTED",
                "Todaysdate",
            )
        )

    def test_topological_sort_multi_steps(self):
        interface = DataEnrichmentInterface()
        src_path = "./input/dependency_graph/Enrichment_03_MULTI_STEP"
        container = self.container(src_path)
        observations = list(container.get_all("Observation"))
        g = interface.build_dependency_graph(
            observations,  # type: ignore
            context_index=MockIndex(),
            cache_view=container,
        )
        sorted_nodes = g.topological_sort()
        # Simple check to see if the sorted variables list is correct
        assert isinstance(g, Graph)
        assert all(isinstance(var, Node) for var in sorted_nodes)
        assert len(sorted_nodes) == len(g.nodes)
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "peh:N1Birthdate",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:agemonths",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:Todaysdate",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:Todaysdate",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "peh:current_day",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:Todaysdate",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "peh:current_month",
            )
        )
        assert sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                "peh:Todaysdate",
            )
        ) > sorted_nodes.index(
            Node(
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                "peh:current_year",
            )
        )
