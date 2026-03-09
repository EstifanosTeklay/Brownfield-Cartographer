"""
DAG Config Parser: extracts pipeline topology from config files.
Handles:
- Airflow DAG Python files (operator definitions, task dependencies)
- dbt schema.yml / sources.yml
- dbt_project.yml
- Generic YAML pipeline configs
"""
from __future__ import annotations
import ast
import re
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple


# ── Airflow DAG Parser ────────────────────────────────────────────────────────

# Common Airflow operator patterns and their dataset implications
AIRFLOW_READ_OPERATORS = {
    "BigQueryOperator", "SnowflakeOperator", "PostgresOperator",
    "MySqlOperator", "SparkSubmitOperator", "PythonOperator",
    "BashOperator", "S3FileTransformOperator",
}
AIRFLOW_WRITE_OPERATORS = {
    "BigQueryInsertJobOperator", "S3CopyObjectOperator",
    "GCSToGCSOperator", "BigQueryToGCSOperator",
}

# Airflow task dependency pattern: task_a >> task_b or task_a.set_downstream(task_b)
AIRFLOW_DEP_RSHIFT = re.compile(r"(\w+)\s*>>\s*(\w+)")
AIRFLOW_DEP_LIST = re.compile(r"\[([^\]]+)\]\s*>>\s*(\w+)")


class AirflowDAGParser:
    """
    Parses Python Airflow DAG files using ast module.
    Extracts: task IDs, operator types, dependencies, dataset references.
    """

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "dag_id": None,
            "tasks": [],
            "dependencies": [],   # list of (upstream_task, downstream_task)
            "datasets_read": [],
            "datasets_written": [],
            "schedule_interval": None,
        }

        # Extract dag_id via regex (handles various DAG() construction styles)
        dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', source)
        if dag_id_match:
            result["dag_id"] = dag_id_match.group(1)

        schedule_match = re.search(
            r'schedule_interval\s*=\s*["\']([^"\']+)["\']', source
        )
        if schedule_match:
            result["schedule_interval"] = schedule_match.group(1)

        # Extract task >> task dependencies
        for m in AIRFLOW_DEP_RSHIFT.finditer(source):
            result["dependencies"].append((m.group(1), m.group(2)))

        # Try AST-level analysis for richer task info
        try:
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    # Look for task_id= in operator calls
                    if isinstance(node.value, ast.Call):
                        task_info = self._extract_task_from_call(node.value)
                        if task_info:
                            result["tasks"].append(task_info)
        except SyntaxError:
            pass

        # Extract SQL references from sql= parameters in operators
        sql_refs = re.findall(r'sql\s*=\s*["\']([^"\']+)["\']', source)
        for sql in sql_refs:
            # Simple table reference extraction
            tables = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
            result["datasets_read"].extend(tables)

        return result

    def _extract_task_from_call(self, node: ast.Call) -> Optional[Dict[str, Any]]:
        """Extract task metadata from an operator instantiation."""
        # Get operator class name
        if isinstance(node.func, ast.Name):
            op_class = node.func.id
        elif isinstance(node.func, ast.Attribute):
            op_class = node.func.attr
        else:
            return None

        task_info = {"operator": op_class, "task_id": None, "params": {}}

        for kw in node.keywords:
            if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                task_info["task_id"] = kw.value.value
            elif kw.arg == "sql" and isinstance(kw.value, ast.Constant):
                task_info["params"]["sql"] = str(kw.value.value)[:200]  # truncate

        return task_info if task_info["task_id"] else None


# ── dbt Project Parser ────────────────────────────────────────────────────────

class DBTProjectParser:
    """
    Parses dbt project structure: dbt_project.yml, schema.yml, sources.yml.
    Builds a model dependency graph from ref() and source() calls.
    """

    def analyze_project(self, repo_path: Path) -> Dict[str, Any]:
        result = {
            "project_name": None,
            "models": {},      # name → {path, depends_on, description}
            "sources": {},     # source_name.table_name → {schema, description}
            "exposures": [],
        }

        # dbt_project.yml
        project_file = repo_path / "dbt_project.yml"
        if project_file.exists():
            try:
                data = yaml.safe_load(project_file.read_text())
                if isinstance(data, dict):
                    result["project_name"] = data.get("name", "unknown")
            except Exception:
                pass

        # schema.yml files (model metadata)
        for yml_path in repo_path.rglob("*.yml"):
            self._parse_dbt_yml(yml_path, result)

        return result

    def _parse_dbt_yml(self, path: Path, result: Dict[str, Any]) -> None:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            return

        if not isinstance(data, dict):
            return

        # models section
        for model in data.get("models", []) or []:
            if not isinstance(model, dict):
                continue
            name = model.get("name")
            if name:
                result["models"][name] = {
                    "description": model.get("description", ""),
                    "columns": [
                        col.get("name") for col in model.get("columns", []) or []
                        if isinstance(col, dict)
                    ],
                    "yml_path": str(path),
                }

        # sources section
        for source in data.get("sources", []) or []:
            if not isinstance(source, dict):
                continue
            source_name = source.get("name", "")
            schema = source.get("schema", source_name)
            for table in source.get("tables", []) or []:
                if isinstance(table, dict) and "name" in table:
                    key = f"{source_name}.{table['name']}"
                    result["sources"][key] = {
                        "schema": schema,
                        "description": table.get("description", ""),
                        "source_name": source_name,
                        "table_name": table["name"],
                    }

        # exposures section
        for exp in data.get("exposures", []) or []:
            if isinstance(exp, dict) and "name" in exp:
                result["exposures"].append(exp["name"])


# ── Config Parser Facade ──────────────────────────────────────────────────────

class DAGConfigParser:
    """
    Facade: tries to identify config type and parse appropriately.
    Returns unified pipeline topology.
    """

    def __init__(self):
        self._airflow = AirflowDAGParser()
        self._dbt = DBTProjectParser()

    def analyze_file(self, path: Path, source: str) -> Dict[str, Any]:
        """Analyze a single config or DAG file."""
        result = {"config_type": "unknown", "path": str(path)}

        # Airflow DAG detection
        if path.suffix == ".py" and (
            "from airflow" in source or "import airflow" in source
            or "DAG(" in source or "dag = DAG" in source
        ):
            result["config_type"] = "airflow_dag"
            result.update(self._airflow.analyze(path, source))
            return result

        # dbt YAML
        if path.suffix in (".yml", ".yaml"):
            try:
                data = yaml.safe_load(source)
            except Exception:
                return result

            if isinstance(data, dict):
                if "models" in data or "sources" in data:
                    result["config_type"] = "dbt_schema"
                    return result
                if "name" in data and "version" in data:
                    result["config_type"] = "dbt_project"
                    result["project_name"] = data.get("name")
                    return result

        return result

    def analyze_dbt_project(self, repo_path: Path) -> Dict[str, Any]:
        """Full dbt project analysis."""
        return self._dbt.analyze_project(repo_path)
