from rdflib import Graph
from rdflib.namespace import DCTERMS, PROV, XSD
from typing import Type

from pypeh.core.models.semantic_profile import (
    PEH,
    CSVW,
    DCAT,
    SemanticDataset,
    SemanticDatasetSchema,
    SemanticDatasetSeries,
)
from pypeh.core.models.internal_data_layout import Dataset, DatasetSeries


class GraphBuilder:
    PREFIXES = {
        "dcat": DCAT,
        "dcterms": DCTERMS,
        "prov": PROV,
        "xsd": XSD,
        "peh": PEH,
        "csvw": CSVW,
    }

    def build_dataset(self, dataset: SemanticDataset) -> Graph:
        g = self._base_graph()
        self._add_dataset(g, dataset)
        return g

    def build_series(
        self, series: DatasetSeries, provenance_info: dict, schema_profile: Type[SemanticDatasetSchema]
    ) -> Graph:
        semantic_series = SemanticDatasetSeries.from_dataset_series(series, **provenance_info)
        g = self._base_graph()

        for triple in semantic_series.to_rdf():
            g.add(triple)

        for dataset in series.parts.values():
            assert isinstance(dataset, Dataset)
            semantic_dataset = SemanticDataset.from_dataset(dataset, schema_profile=schema_profile)
            self._add_dataset(g, semantic_dataset)

        return g

    def _add_dataset(self, g: Graph, dataset: SemanticDataset) -> None:
        for triple in dataset.to_rdf():
            g.add(triple)
        for triple in dataset.schema.to_rdf(dataset):
            g.add(triple)

    def _base_graph(self) -> Graph:
        g = Graph()
        for prefix, namespace in self.PREFIXES.items():
            g.bind(prefix, namespace)
        return g
