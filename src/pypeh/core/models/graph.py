from collections import defaultdict, deque


class Graph:
    # NOTE: This graph can only be traversed root to leaves
    def __init__(self) -> None:
        self.graph = defaultdict(set)
        self.nodes = set()

    def _add_node(self, node: str) -> None:
        self.nodes.add(node)
        if node not in self.graph:
            self.graph[node]

    def add_edge(self, parent: str, child: str) -> None:
        self._add_node(parent)
        self._add_node(child)
        self.graph[parent].add(child)

    @property
    def edges(self):
        edges = []
        for parent, children in self.graph.items():
            for child in children:
                edges.append((parent, child))
        return edges

    def get_children(self, node: str) -> set[str]:
        return self.graph.get(node, set())

    def get_parents(self, node: str) -> set[str]:
        parents = set()
        for parent, children in self.graph.items():
            if node in children:
                parents.add(parent)
        return parents

    def topological_sort(self) -> list[str]:
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
