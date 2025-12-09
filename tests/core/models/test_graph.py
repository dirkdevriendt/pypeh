import pytest

from pypeh.core.models.graph import Graph, Node


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
        assert sorted_nodes.index(Node("A", "A")) < sorted_nodes.index(Node("B", "B"))
        assert sorted_nodes.index(Node("C", "C")) < sorted_nodes.index(Node("D", "D"))
        assert sorted_nodes.index(Node("E", "E")) < sorted_nodes.index(Node("F", "F"))

    def test_topological_sort_with_cycle(self):
        g = Graph()
        g.add_edge(Node("A", "A"), Node("B", "B"))
        g.add_edge(Node("B", "B"), Node("C", "C"))
        g.add_edge(Node("C", "C"), Node("A", "A"))  # Creates a cycle
        g.add_edge(Node("D", "D"), Node("E", "E"))

        with pytest.raises(ValueError) as excinfo:
            g.topological_sort()

        assert "Circular dependency detected" in str(excinfo.value)
