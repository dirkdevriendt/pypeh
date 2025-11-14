import pytest

from pypeh.core.models.graph import Graph


@pytest.mark.core
class TestGraph:
    def test_add_node(self):
        g = Graph()
        g._add_node("A")
        assert "A" in g.nodes
        assert "A" in g.graph
        assert g.graph["A"] == set()

    def test_add_edge(self):
        g = Graph()
        g.add_edge("A", "B")
        assert "A" in g.nodes
        assert "B" in g.nodes
        assert "B" in g.graph["A"]
        assert g.graph["B"] == set()

    def test_get_edges(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("A", "C")
        g.add_edge("B", "C")
        edges = g.edges
        expected_edges = [("A", "B"), ("A", "C"), ("B", "C")]
        assert set(edges) == set(expected_edges)

    def test_get_children(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("A", "C")
        children = g.get_children("A")
        assert children == {"B", "C"}
        assert g.get_children("B") == set()

    def test_get_parents(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("C", "B")
        parents = g.get_parents("B")
        assert parents == {"A", "C"}
        assert g.get_parents("A") == set()

    def test_topological_sort(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        g.add_edge("A", "C")
        g.add_edge("C", "D")
        g.add_edge("B", "D")
        g.add_edge("E", "D")
        sorted_nodes = g.topological_sort()
        assert sorted_nodes.index("A") < sorted_nodes.index("B") < sorted_nodes.index("C") < sorted_nodes.index("D")

    def test_topological_sort_with_single_node(self):
        g = Graph()
        g._add_node("A")
        sorted_nodes = g.topological_sort()
        assert sorted_nodes == ["A"]

    def test_topological_sort_with_independent_nodes(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("C", "D")
        g.add_edge("E", "F")
        sorted_nodes = g.topological_sort()
        assert sorted_nodes.index("A") < sorted_nodes.index("B")
        assert sorted_nodes.index("C") < sorted_nodes.index("D")
        assert sorted_nodes.index("E") < sorted_nodes.index("F")

    def test_topological_sort_with_cycle(self):
        g = Graph()
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        g.add_edge("C", "A")  # Creates a cycle
        g.add_edge("D", "E")

        with pytest.raises(ValueError) as excinfo:
            g.topological_sort()

        assert "['A', 'B', 'C']" in str(excinfo.value)
