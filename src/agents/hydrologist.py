"""
The Hydrologist Agent: Data Flow & Lineage Analyst.

Responsibilities:
- Build the DataLineageGraph from Python, SQL, YAML, and notebook files
- Detect pandas/SQLAlchemy/PySpark data read/write operations in Python
- Parse SQL lineage from .sql files and dbt models
- Parse Airflow/dbt config for pipeline topology
- Populate KnowledgeGraph with DatasetNodes and TransformationNodes
- Provide blast_radius, find_sources, find_sinks queries
"""
from __future__ import annotations
import ast
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import (
    DatasetNode, TransformationNode, StorageType, TransformationType, Language
)
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.analyzers.dag_config_parser import DAGConfigParser, AirflowDAGParser


# ── Python Data Flow Patterns ─────────────────────────────────────────────────

# Regex patterns for common pandas/SQLAlchemy/PySpark read/write
PD_READ_RE = re.compile(
    r'pd\.read_(\w+)\s*\(([^)]*)\)|pandas\.read_(\w+)\s*\(([^)]*)\)',
    re.IGNORECASE
)
PD_WRITE_RE = re.compile(
    r'\.to_(\w+)\s*\(([^)]*)\)',
    re.IGNORECASE
)
SPARK_READ_RE = re.compile(
    r'spark\.read\.\w+\s*\(([^)]*)\)|\.load\s*\(([^)]*)\)',
    re.IGNORECASE
)
SPARK_WRITE_RE = re.compile(
    r'\.write\.\w+\s*\(([^)]*)\)|\.save\s*\(([^)]*)\)',
    re.IGNORECASE
)
SQLALCHEMY_RE = re.compile(
    r'\.execute\s*\(\s*["\']([^"\']*)["\']',
    re.IGNORECASE
)
# String literal paths/table names in common patterns
STRING_ARG_RE = re.compile(r"""['"]([\w./\-:]+)['"]""")


def _extract_string_arg(args_str: str) -> Optional[str]:
    """Extract the first string literal from a function arguments string."""
    m = STRING_ARG_RE.search(args_str)
    return m.group(1) if m else None


def _is_dynamic(args_str: str) -> bool:
    """Return True if the argument appears to be a variable (not a string literal)."""
    stripped = args_str.strip()
    if not stripped:
        return True
    return not (stripped.startswith('"') or stripped.startswith("'"))


class PythonDataFlowAnalyzer:
    """
    Finds pandas/SQLAlchemy/PySpark data source and sink references in Python files.
    Uses both regex and AST analysis.
    """

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "reads": [],   # list of {dataset, method, lineno, is_dynamic}
            "writes": [],  # list of {dataset, method, lineno, is_dynamic}
        }

        lines = source.splitlines()
        for lineno, line in enumerate(lines, 1):
            # pandas reads
            for m in PD_READ_RE.finditer(line):
                method = m.group(1) or m.group(3)
                args = m.group(2) or m.group(4) or ""
                dataset = _extract_string_arg(args)
                result["reads"].append({
                    "dataset": dataset or f"<dynamic:{lineno}>",
                    "method": f"pd.read_{method}",
                    "lineno": lineno,
                    "is_dynamic": dataset is None,
                })

            # pandas writes
            for m in PD_WRITE_RE.finditer(line):
                method = m.group(1)
                args = m.group(2) or ""
                if method in ("csv", "parquet", "sql", "json", "excel", "hdf"):
                    dataset = _extract_string_arg(args)
                    result["writes"].append({
                        "dataset": dataset or f"<dynamic:{lineno}>",
                        "method": f"df.to_{method}",
                        "lineno": lineno,
                        "is_dynamic": dataset is None,
                    })

            # Spark reads
            for m in SPARK_READ_RE.finditer(line):
                args = m.group(1) or m.group(2) or ""
                dataset = _extract_string_arg(args)
                result["reads"].append({
                    "dataset": dataset or f"<spark_dynamic:{lineno}>",
                    "method": "spark.read",
                    "lineno": lineno,
                    "is_dynamic": dataset is None,
                })

            # Spark writes
            for m in SPARK_WRITE_RE.finditer(line):
                args = m.group(1) or m.group(2) or ""
                dataset = _extract_string_arg(args)
                result["writes"].append({
                    "dataset": dataset or f"<spark_write_dynamic:{lineno}>",
                    "method": "df.write",
                    "lineno": lineno,
                    "is_dynamic": dataset is None,
                })

        return result


# ── Hydrologist Agent ─────────────────────────────────────────────────────────

class Hydrologist:
    """
    Agent 2: The Hydrologist.
    Builds the DataLineageGraph from all supported file types in the repo.
    """

    def __init__(self, kg: KnowledgeGraph, repo_path: Path, verbose: bool = True):
        self.kg = kg
        self.repo_path = repo_path
        self.verbose = verbose
        self._py_analyzer = PythonDataFlowAnalyzer()
        self._sql_analyzer = SQLLineageAnalyzer()
        self._dag_parser = DAGConfigParser()
        self._airflow_parser = AirflowDAGParser()

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [hydrologist] {msg}")

    def run(self) -> Dict[str, Any]:
        start = time.time()
        self._log("Building data lineage graph...")

        txn_count = 0
        dataset_count = 0

        for fpath in sorted(self.repo_path.rglob("*")):
            # Skip non-files and noise directories
            if not fpath.is_file():
                continue
            parts = set(fpath.parts)
            if any(p.startswith(".") and p != "." for p in fpath.relative_to(self.repo_path).parts):
                continue
            if any(p in {"__pycache__", "node_modules", ".venv", "venv", "env"} for p in fpath.parts):
                continue

            try:
                rel_path = str(fpath.relative_to(self.repo_path))
            except ValueError:
                rel_path = str(fpath)

            suffix = fpath.suffix.lower()

            if suffix == ".py":
                n = self._analyze_python_file(fpath, rel_path)
                txn_count += n
            elif suffix == ".sql":
                n = self._analyze_sql_file(fpath, rel_path)
                txn_count += n
            elif suffix in (".yml", ".yaml"):
                n = self._analyze_yaml_file(fpath, rel_path)
                txn_count += n

        self._link_yaml_to_sql()

        elapsed = time.time() - start
        self._log(
            f"Done in {elapsed:.1f}s — "
            f"{len(self.kg.datasets)} datasets, {len(self.kg.transformations)} transformations"
        )
        return {
            "elapsed_seconds": round(elapsed, 2),
            "datasets": len(self.kg.datasets),
            "transformations": len(self.kg.transformations),
            "lineage_edges": self.kg.lineage_graph.number_of_edges(),
        }

    def _analyze_python_file(self, fpath: Path, rel_path: str) -> int:
        """Analyze a Python file for data reads/writes."""
        try:
            source = fpath.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return 0

        # Check for Airflow DAG
        if "from airflow" in source or "DAG(" in source:
            result = self._airflow_parser.analyze(fpath, source)
            dag_id = result.get("dag_id", rel_path)
            # Add tasks as transformations
            for dep in result.get("dependencies", []):
                txn_id = f"{rel_path}::airflow::{dep[0]}>>{dep[1]}"
                txn = TransformationNode(
                    id=txn_id,
                    source_datasets=[dep[0]],
                    target_datasets=[dep[1]],
                    transformation_type=TransformationType.TRANSFORM,
                    source_file=rel_path,
                )
                self._ensure_datasets(txn.source_datasets + txn.target_datasets, StorageType.TABLE)
                self.kg.add_transformation(txn)
            return len(result.get("dependencies", []))

        # Regular Python data flow
        result = self._py_analyzer.analyze(fpath, source)
        count = 0

        reads = result.get("reads", [])
        writes = result.get("writes", [])

        if reads or writes:
            # Group all reads/writes in a file into a single transformation per file
            source_datasets = list({r["dataset"] for r in reads})
            target_datasets = list({w["dataset"] for w in writes})

            if source_datasets or target_datasets:
                txn_id = f"{rel_path}::py_dataflow"
                txn = TransformationNode(
                    id=txn_id,
                    source_datasets=source_datasets,
                    target_datasets=target_datasets,
                    transformation_type=TransformationType.TRANSFORM,
                    source_file=rel_path,
                    line_range=(
                        reads[0]["lineno"] if reads else 0,
                        writes[-1]["lineno"] if writes else 0,
                    ),
                )
                self._ensure_datasets(
                    source_datasets, StorageType.FILE
                )
                self._ensure_datasets(
                    target_datasets, StorageType.FILE
                )
                self.kg.add_transformation(txn)
                count += 1

        return count

    def _analyze_sql_file(self, fpath: Path, rel_path: str) -> int:
        """Analyze a SQL file for table dependencies."""
        entries = self._sql_analyzer.analyze_sql_file(fpath, self.repo_path)
        count = 0
        for i, entry in enumerate(entries):
            src = entry.get("source_tables", [])
            tgt = entry.get("target_tables", [])
            if not src and not tgt:
                continue

            txn_id = f"{rel_path}::sql::{i}"
            txn = TransformationNode(
                id=txn_id,
                source_datasets=src,
                target_datasets=tgt,
                transformation_type=TransformationType.SQL_SELECT,
                source_file=rel_path,
                sql_query=entry.get("raw_sql", "")[:500],
            )
            self._ensure_datasets(src, StorageType.TABLE)
            self._ensure_datasets(tgt, StorageType.TABLE)
            self.kg.add_transformation(txn)
            count += 1

        return count

    def _analyze_yaml_file(self, fpath: Path, rel_path: str) -> int:
        """Analyze YAML for dbt sources/models or Airflow DAGs."""
        try:
            import yaml
            data = yaml.safe_load(fpath.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            return 0

        if not isinstance(data, dict):
            return 0

        count = 0

        # dbt sources: add as DatasetNodes
        for source in data.get("sources", []) or []:
            if not isinstance(source, dict):
                continue
            source_name = source.get("name", "")
            for table in source.get("tables", []) or []:
                if isinstance(table, dict) and "name" in table:
                    table_name = table["name"]
                    ds = DatasetNode(
                        name=table_name,
                        storage_type=StorageType.TABLE,
                        is_source_of_truth=True,
                        owner=source_name,
                    )
                    self.kg.add_dataset(ds)
                    count += 1

        return count

    def _normalize_rel(self, path: Path | str) -> str:
        return str(path).replace("\\", "/")

    def _ensure_lineage_file_node(self, rel_path: str, node_kind: str) -> None:
        """Ensure config/file node exists in lineage graph for visualization links."""
        rel_path = self._normalize_rel(rel_path)
        if rel_path in self.kg.lineage_graph:
            return

        self.kg.lineage_graph.add_node(
            rel_path,
            node_type="transformation",
            source_file=rel_path,
            transformation_type=node_kind,
            line_range=[0, 0],
        )

    def _add_lineage_link(self, source: str, target: str, edge_type: str) -> None:
        source = self._normalize_rel(source)
        target = self._normalize_rel(target)
        self._ensure_lineage_file_node(source, "config")
        self._ensure_lineage_file_node(target, "config")
        self.kg.lineage_graph.add_edge(source, target, edge_type=edge_type)

    def _add_module_link(self, source: str, target: str, edge_type: str) -> None:
        source = self._normalize_rel(source)
        target = self._normalize_rel(target)
        if source in self.kg.module_graph and target in self.kg.module_graph:
            self.kg.module_graph.add_edge(source, target, edge_type=edge_type, weight=1)

    def _find_sql_transform_ids(self, sql_rel_path: str) -> List[str]:
        sql_rel_path = self._normalize_rel(sql_rel_path)
        return [
            txn_id for txn_id, txn in self.kg.transformations.items()
            if self._normalize_rel(getattr(txn, "source_file", "")) == sql_rel_path
        ]

    def _extract_source_names(self, sources_yml: Path) -> List[tuple[str, str]]:
        """Extract (source_name, table_name) pairs from a dbt sources.yml file."""
        import yaml

        try:
            content = yaml.safe_load(sources_yml.read_text(encoding="utf-8", errors="replace"))
            names: List[tuple[str, str]] = []
            for source in content.get("sources", []) or []:
                if not isinstance(source, dict):
                    continue
                source_name = str(source.get("name", "")).strip()
                for table in source.get("tables", []) or []:
                    if not isinstance(table, dict):
                        continue
                    table_name = str(table.get("name", "")).strip()
                    if source_name and table_name:
                        names.append((source_name, table_name))
            return names
        except Exception:
            return []

    def _link_yaml_to_sql(self) -> None:
        """
        Creates graph edges between dbt YAML config files and their related SQL models.
        Implements three linking rules:
        1. schema.yml -> sibling .sql files (documented_by)
        2. sources.yml -> consuming staging .sql files (source_for)
        3. dbt_project.yml -> all schema/sources YAML files (governs)
        """
        model_yaml_files = [
            p for p in (self.repo_path / "models").rglob("*.yml")
            if p.is_file()
        ] if (self.repo_path / "models").exists() else []

        # RULE 1: model YAML to sibling SQL models.
        # Supports both schema.yml-style files and model-specific YAML like customers.yml.
        for schema_yml in model_yaml_files:
            if schema_yml.name == "dbt_project.yml":
                continue

            sibling_sqls = list(schema_yml.parent.glob("*.sql"))
            if not sibling_sqls:
                continue

            # Prefer exact stem match for model-specific YAML, otherwise fall back to all siblings.
            matched_sqls = [
                sql_file for sql_file in sibling_sqls
                if sql_file.stem == schema_yml.stem
            ]
            if not matched_sqls and schema_yml.stem in {"schema", "models"}:
                matched_sqls = sibling_sqls

            yml_rel = self._normalize_rel(schema_yml.relative_to(self.repo_path))

            for sql_file in matched_sqls:
                sql_rel = self._normalize_rel(sql_file.relative_to(self.repo_path))

                # Module graph link for module visualization
                self._add_module_link(sql_rel, yml_rel, edge_type="documented_by")

                # Lineage graph link to existing SQL transformations where possible
                transform_ids = self._find_sql_transform_ids(sql_rel)
                if transform_ids:
                    for txn_id in transform_ids:
                        self._add_lineage_link(txn_id, yml_rel, edge_type="documented_by")
                else:
                    self._add_lineage_link(sql_rel, yml_rel, edge_type="documented_by")

        # RULE 2: source-declaring YAML to consuming staging SQL models.
        # Supports sources.yml as well as dbt patterns like __sources.yml.
        for sources_yml in model_yaml_files:
            sources_rel = self._normalize_rel(sources_yml.relative_to(self.repo_path))
            source_pairs = self._extract_source_names(sources_yml)
            if not source_pairs:
                continue
            sibling_sqls = list(sources_yml.parent.glob("*.sql"))

            for sql_file in sibling_sqls:
                try:
                    content = sql_file.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue

                sql_rel = self._normalize_rel(sql_file.relative_to(self.repo_path))
                matched = False
                for source_name, table_name in source_pairs:
                    if (
                        f"source('{source_name}', '{table_name}')" in content
                        or f'source("{source_name}", "{table_name}")' in content
                        or ("source(" in content and table_name in content)
                    ):
                        matched = True
                        break

                if not matched:
                    continue

                self._add_module_link(sources_rel, sql_rel, edge_type="source_for")

                transform_ids = self._find_sql_transform_ids(sql_rel)
                if transform_ids:
                    for txn_id in transform_ids:
                        self._add_lineage_link(sources_rel, txn_id, edge_type="source_for")
                else:
                    self._add_lineage_link(sources_rel, sql_rel, edge_type="source_for")

        # RULE 3: dbt_project.yml governs all model YAML files
        dbt_project = self.repo_path / "dbt_project.yml"
        if dbt_project.exists():
            dbt_rel = self._normalize_rel(dbt_project.relative_to(self.repo_path))
            for yaml_file in model_yaml_files:
                yml_rel = self._normalize_rel(yaml_file.relative_to(self.repo_path))
                self._add_module_link(dbt_rel, yml_rel, edge_type="governs")
                self._add_lineage_link(dbt_rel, yml_rel, edge_type="governs")

    def _ensure_datasets(self, names: List[str], storage_type: StorageType) -> None:
        """Add DatasetNodes for any names not already in the graph."""
        for name in names:
            if name and name not in self.kg.datasets:
                ds = DatasetNode(name=name, storage_type=storage_type)
                self.kg.add_dataset(ds)

    # ── Query Interface ───────────────────────────────────────────────────────

    def blast_radius(self, node_id: str) -> Dict[str, Any]:
        """Find all downstream nodes affected if node_id changes."""
        affected = self.kg.blast_radius(node_id, graph="lineage")
        return {
            "node": node_id,
            "affected_count": len(affected),
            "affected_nodes": affected,
        }

    def trace_upstream(self, dataset_name: str) -> Dict[str, Any]:
        """Find all upstream sources of a dataset."""
        upstream = self.kg.upstream_of(dataset_name)
        return {
            "dataset": dataset_name,
            "upstream_count": len(upstream),
            "upstream_nodes": [
                {"id": n, "type": t} for n, t in upstream
            ],
        }

    def find_sources(self) -> List[str]:
        return self.kg.find_lineage_sources()

    def find_sinks(self) -> List[str]:
        return self.kg.find_lineage_sinks()
