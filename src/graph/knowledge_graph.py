"""
Knowledge Graph: central data store using NetworkX.
Stores ModuleNodes, DatasetNodes, FunctionNodes, TransformationNodes
and their relationships as a directed graph.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import networkx as nx

from src.models.nodes import (
    ModuleNode, DatasetNode, FunctionNode, TransformationNode,
    EdgeType, Language, StorageType
)


class KnowledgeGraph:
    """
    Central knowledge graph for the Brownfield Cartographer.
    Two graphs:
      - module_graph: DiGraph of modules and their import relationships
      - lineage_graph: DiGraph of datasets and transformations
    """

    def __init__(self):
        self.module_graph: nx.DiGraph = nx.DiGraph()
        self.lineage_graph: nx.DiGraph = nx.DiGraph()
        self.function_index: Dict[str, FunctionNode] = {}
        self.modules: Dict[str, ModuleNode] = {}
        self.datasets: Dict[str, DatasetNode] = {}
        self.transformations: Dict[str, TransformationNode] = {}

    # ── Module Graph ────────────────────────────────────────────────────────

    def add_module(self, node: ModuleNode) -> None:
        self.modules[node.path] = node
        self.module_graph.add_node(node.path, **node.to_dict(), node_type="module")

    def add_import_edge(self, source: str, target: str, weight: int = 1) -> None:
        """source imports target."""
        if source in self.module_graph and target in self.module_graph:
            self.module_graph.add_edge(
                source, target,
                edge_type=EdgeType.IMPORTS.value,
                weight=weight
            )

    def add_function(self, fn: FunctionNode) -> None:
        self.function_index[fn.qualified_name] = fn

    # ── Lineage Graph ────────────────────────────────────────────────────────

    def add_dataset(self, node: DatasetNode) -> None:
        self.datasets[node.name] = node
        self.lineage_graph.add_node(node.name, **node.to_dict(), node_type="dataset")

    def add_transformation(self, txn: TransformationNode) -> None:
        self.transformations[txn.id] = txn
        self.lineage_graph.add_node(txn.id, **txn.to_dict(), node_type="transformation")
        for src in txn.source_datasets:
            if src not in self.lineage_graph:
                self.lineage_graph.add_node(src, node_type="dataset", name=src,
                                            storage_type=StorageType.UNKNOWN.value)
            self.lineage_graph.add_edge(
                src, txn.id, edge_type=EdgeType.CONSUMES.value
            )
        for tgt in txn.target_datasets:
            if tgt not in self.lineage_graph:
                self.lineage_graph.add_node(tgt, node_type="dataset", name=tgt,
                                            storage_type=StorageType.UNKNOWN.value)
            self.lineage_graph.add_edge(
                txn.id, tgt, edge_type=EdgeType.PRODUCES.value
            )

    # ── Graph Analysis ───────────────────────────────────────────────────────

    def pagerank_modules(self) -> Dict[str, float]:
        """Identify architectural hubs by PageRank."""
        if len(self.module_graph) == 0:
            return {}
        try:
            return nx.pagerank(self.module_graph, weight="weight")
        except nx.PowerIterationFailedConvergence:
            return {n: 1.0 / len(self.module_graph) for n in self.module_graph}

    def find_circular_deps(self) -> List[List[str]]:
        """Find strongly connected components (potential circular deps)."""
        sccs = list(nx.strongly_connected_components(self.module_graph))
        return [list(s) for s in sccs if len(s) > 1]

    def blast_radius(self, node_id: str, graph: str = "module") -> List[str]:
        """
        BFS from node_id to find all downstream dependents.
        graph: 'module' or 'lineage'
        """
        g = self.module_graph if graph == "module" else self.lineage_graph
        if node_id not in g:
            return []
        return list(nx.descendants(g, node_id))

    def find_lineage_sources(self) -> List[str]:
        """Nodes with in_degree=0 in the lineage graph (data sources)."""
        return [n for n, d in self.lineage_graph.in_degree() if d == 0
                and self.lineage_graph.nodes[n].get("node_type") == "dataset"]

    def find_lineage_sinks(self) -> List[str]:
        """Nodes with out_degree=0 in the lineage graph (output datasets)."""
        return [n for n, d in self.lineage_graph.out_degree() if d == 0
                and self.lineage_graph.nodes[n].get("node_type") == "dataset"]

    def upstream_of(self, dataset_name: str) -> List[Tuple[str, str]]:
        """Return all (node_id, node_type) ancestors of a dataset in lineage graph."""
        if dataset_name not in self.lineage_graph:
            return []
        ancestors = nx.ancestors(self.lineage_graph, dataset_name)
        return [(n, self.lineage_graph.nodes[n].get("node_type", "unknown"))
                for n in ancestors]

    def get_critical_path_modules(self, top_n: int = 10) -> List[Tuple[str, float]]:
        """Return top N modules by PageRank score."""
        scores = self.pagerank_modules()
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # ── Serialization ────────────────────────────────────────────────────────

    def save(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)

        # Module graph
        module_data = nx.node_link_data(self.module_graph)
        (output_dir / "module_graph.json").write_text(
            json.dumps(module_data, indent=2, default=str)
        )

        # Lineage graph
        lineage_data = nx.node_link_data(self.lineage_graph)
        (output_dir / "lineage_graph.json").write_text(
            json.dumps(lineage_data, indent=2, default=str)
        )

        # Function index
        fn_data = {k: v.to_dict() for k, v in self.function_index.items()}
        (output_dir / "function_index.json").write_text(
            json.dumps(fn_data, indent=2, default=str)
        )

        print(f"  [graph] Saved to {output_dir}/")

    @classmethod
    def load(cls, output_dir: Path) -> "KnowledgeGraph":
        kg = cls()
        mg_path = output_dir / "module_graph.json"
        lg_path = output_dir / "lineage_graph.json"

        if mg_path.exists():
            data = json.loads(mg_path.read_text())
            kg.module_graph = nx.node_link_graph(data, directed=True)

        if lg_path.exists():
            data = json.loads(lg_path.read_text())
            kg.lineage_graph = nx.node_link_graph(data, directed=True)

        return kg

    # ── Stats ────────────────────────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        return {
            "modules": len(self.modules),
            "datasets": len(self.datasets),
            "transformations": len(self.transformations),
            "functions": len(self.function_index),
            "module_edges": self.module_graph.number_of_edges(),
            "lineage_edges": self.lineage_graph.number_of_edges(),
            "circular_deps": len(self.find_circular_deps()),
        }
