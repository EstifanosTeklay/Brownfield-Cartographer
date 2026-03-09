"""
Multi-language AST analyzer.
- Python: uses stdlib `ast` module for full AST parsing
- SQL: regex + basic parse for table references
- YAML: stdlib yaml for config parsing
- Notebooks: json parsing of .ipynb cells
Falls back gracefully on parse errors.
"""
from __future__ import annotations
import ast
import re
import json
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any

from src.models.nodes import Language, ModuleNode, FunctionNode


# ── Language Detection ────────────────────────────────────────────────────────

EXTENSION_MAP: Dict[str, Language] = {
    ".py": Language.PYTHON,
    ".sql": Language.SQL,
    ".yaml": Language.YAML,
    ".yml": Language.YAML,
    ".js": Language.JAVASCRIPT,
    ".ts": Language.TYPESCRIPT,
    ".ipynb": Language.NOTEBOOK,
}

SKIP_DIRS = {
    ".git", "__pycache__", "node_modules", ".venv", "venv", "env",
    ".mypy_cache", ".pytest_cache", "dist", "build", ".tox", ".eggs",
}


def detect_language(path: Path) -> Language:
    return EXTENSION_MAP.get(path.suffix.lower(), Language.OTHER)


# ── Python Analyzer ───────────────────────────────────────────────────────────

class PythonAnalyzer:
    """Full AST-based analysis of Python files."""

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "imports": [],
            "exports": [],      # public functions + classes
            "functions": [],    # FunctionNode-like dicts
            "classes": [],
            "docstring": None,
            "complexity": 0,
            "loc": len(source.splitlines()),
        }

        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as e:
            result["parse_error"] = str(e)
            return result

        # Module docstring
        result["docstring"] = ast.get_docstring(tree)

        # Traverse top-level nodes
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                result["imports"].extend(self._extract_import(node))

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                fn = self._extract_function(node, str(path))
                result["functions"].append(fn)
                if fn["is_public"]:
                    result["exports"].append(fn["name"])
                result["complexity"] += self._cyclomatic_complexity(node)

            elif isinstance(node, ast.ClassDef):
                cls = self._extract_class(node, str(path))
                result["classes"].append(cls)
                if not node.name.startswith("_"):
                    result["exports"].append(node.name)

        return result

    def _extract_import(self, node) -> List[str]:
        if isinstance(node, ast.Import):
            return [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level > 0:
                # relative import — prefix with dots
                prefix = "." * node.level
                return [f"{prefix}{module}"]
            return [module] if module else []
        return []

    def _extract_function(self, node, filepath: str) -> Dict[str, Any]:
        args = []
        for arg in node.args.args:
            args.append(arg.arg)
        signature = f"def {node.name}({', '.join(args)})"
        return {
            "name": node.name,
            "signature": signature,
            "lineno": node.lineno,
            "is_public": not node.name.startswith("_"),
            "docstring": ast.get_docstring(node),
            "filepath": filepath,
        }

    def _extract_class(self, node, filepath: str) -> Dict[str, Any]:
        bases = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                bases.append(base.id)
            elif isinstance(base, ast.Attribute):
                bases.append(f"{ast.unparse(base)}")
        methods = []
        for item in ast.iter_child_nodes(node):
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                methods.append(item.name)
        return {
            "name": node.name,
            "bases": bases,
            "methods": methods,
            "lineno": node.lineno,
            "docstring": ast.get_docstring(node),
            "filepath": filepath,
        }

    def _cyclomatic_complexity(self, node) -> int:
        """Simple cyclomatic complexity: count branches."""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.ExceptHandler,
                                  ast.With, ast.Assert, ast.comprehension)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += len(child.values) - 1
        return complexity


# ── SQL Analyzer ──────────────────────────────────────────────────────────────

# Patterns to extract table names from SQL
SQL_FROM_RE = re.compile(
    r'\bFROM\s+([\w.`"\[\]]+)', re.IGNORECASE
)
SQL_JOIN_RE = re.compile(
    r'\bJOIN\s+([\w.`"\[\]]+)', re.IGNORECASE
)
SQL_INTO_RE = re.compile(
    r'\bINSERT\s+INTO\s+([\w.`"\[\]]+)', re.IGNORECASE
)
SQL_CREATE_RE = re.compile(
    r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.`"\[\]]+)',
    re.IGNORECASE
)
SQL_CTE_RE = re.compile(
    r'\bWITH\s+([\w]+)\s+AS\s*\(', re.IGNORECASE
)
# dbt ref() pattern
DBT_REF_RE = re.compile(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}")
DBT_SOURCE_RE = re.compile(r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}")


def _clean_table_name(raw: str) -> str:
    return raw.strip('`"[]').split(".")[-1].lower()


class SQLAnalyzer:
    """Regex-based SQL dependency extractor. Works without sqlglot."""

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "source_tables": [],
            "target_tables": [],
            "cte_names": [],
            "dbt_refs": [],
            "dbt_sources": [],
            "raw_sql": source,
        }

        # CTEs (these are inline, not real tables)
        ctes = {m.group(1).lower() for m in SQL_CTE_RE.finditer(source)}
        result["cte_names"] = list(ctes)

        # dbt-specific
        result["dbt_refs"] = [m.group(1) for m in DBT_REF_RE.finditer(source)]
        result["dbt_sources"] = [
            f"{m.group(1)}.{m.group(2)}" for m in DBT_SOURCE_RE.finditer(source)
        ]

        # Tables from FROM / JOIN
        from_tables = {
            _clean_table_name(m.group(1))
            for m in SQL_FROM_RE.finditer(source)
        }
        join_tables = {
            _clean_table_name(m.group(1))
            for m in SQL_JOIN_RE.finditer(source)
        }
        source_tables = (from_tables | join_tables) - ctes
        # Add dbt refs as sources
        source_tables.update(r.lower() for r in result["dbt_refs"])
        source_tables.update(s.split(".")[-1].lower() for s in result["dbt_sources"])

        result["source_tables"] = sorted(source_tables)

        # Target tables (INSERT INTO / CREATE TABLE)
        into_tables = {
            _clean_table_name(m.group(1))
            for m in SQL_INTO_RE.finditer(source)
        }
        create_tables = {
            _clean_table_name(m.group(1))
            for m in SQL_CREATE_RE.finditer(source)
        }
        result["target_tables"] = sorted(into_tables | create_tables)

        return result


# ── YAML Analyzer ─────────────────────────────────────────────────────────────

class YAMLAnalyzer:
    """Parses YAML configs: dbt schema.yml, Airflow DAGs, Prefect flows."""

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "type": "unknown",
            "models": [],
            "sources": [],
            "dag_tasks": [],
            "dependencies": [],
        }
        try:
            data = yaml.safe_load(source)
        except yaml.YAMLError:
            result["parse_error"] = "invalid yaml"
            return result

        if not isinstance(data, dict):
            return result

        # dbt schema.yml
        if "models" in data:
            result["type"] = "dbt_schema"
            for model in data.get("models", []) or []:
                if isinstance(model, dict) and "name" in model:
                    result["models"].append(model["name"])

        if "sources" in data:
            result["type"] = "dbt_sources"
            for src in data.get("sources", []) or []:
                if isinstance(src, dict):
                    schema = src.get("name", "")
                    for tbl in src.get("tables", []) or []:
                        if isinstance(tbl, dict) and "name" in tbl:
                            result["sources"].append(f"{schema}.{tbl['name']}")

        # Airflow-style: look for 'dag_id', 'tasks', 'default_args'
        if "dag_id" in data or "tasks" in data:
            result["type"] = "airflow_dag"
            tasks = data.get("tasks", {}) or {}
            if isinstance(tasks, dict):
                for task_id, task_cfg in tasks.items():
                    dep = {"id": task_id, "depends_on": []}
                    if isinstance(task_cfg, dict):
                        dep["depends_on"] = task_cfg.get("depends_on_past", [])
                    result["dag_tasks"].append(dep)

        return result


# ── Notebook Analyzer ─────────────────────────────────────────────────────────

class NotebookAnalyzer:
    """Extracts code cells from Jupyter notebooks and re-analyzes as Python."""

    def __init__(self):
        self.python_analyzer = PythonAnalyzer()

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "cells": 0,
            "code_cells": 0,
            "imports": [],
            "exports": [],
            "functions": [],
        }
        try:
            nb = json.loads(source)
        except json.JSONDecodeError:
            result["parse_error"] = "invalid json"
            return result

        cells = nb.get("cells", [])
        result["cells"] = len(cells)
        combined_code = []

        for cell in cells:
            if cell.get("cell_type") == "code":
                result["code_cells"] += 1
                src = cell.get("source", [])
                if isinstance(src, list):
                    combined_code.extend(src)
                elif isinstance(src, str):
                    combined_code.append(src)

        full_source = "\n".join(combined_code)
        py_result = self.python_analyzer.analyze(path, full_source)
        result.update({
            "imports": py_result["imports"],
            "exports": py_result["exports"],
            "functions": py_result["functions"],
        })
        return result


# ── Language Router ───────────────────────────────────────────────────────────

class LanguageRouter:
    """
    Routes files to the appropriate analyzer based on extension.
    Returns a unified analysis dict.
    """

    def __init__(self):
        self._python = PythonAnalyzer()
        self._sql = SQLAnalyzer()
        self._yaml = YAMLAnalyzer()
        self._notebook = NotebookAnalyzer()

    def analyze_file(self, path: Path) -> Optional[Dict[str, Any]]:
        """
        Analyze a single file. Returns None if file should be skipped.
        """
        lang = detect_language(path)
        if lang == Language.OTHER:
            return None

        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except (OSError, PermissionError) as e:
            return {"error": str(e), "language": lang.value, "path": str(path)}

        result = {"language": lang.value, "path": str(path)}

        if lang == Language.PYTHON:
            result.update(self._python.analyze(path, source))
        elif lang == Language.SQL:
            result.update(self._sql.analyze(path, source))
        elif lang == Language.YAML:
            result.update(self._yaml.analyze(path, source))
        elif lang == Language.NOTEBOOK:
            result.update(self._notebook.analyze(path, source))
        else:
            result["loc"] = len(source.splitlines())

        return result

    def walk_repo(self, repo_path: Path) -> List[Dict[str, Any]]:
        """
        Walk a repository and analyze all supported files.
        Skips common non-code directories.
        """
        results = []
        for fpath in sorted(repo_path.rglob("*")):
            # Skip hidden dirs and known noise dirs
            parts = set(fpath.parts)
            if any(p.startswith(".") or p in SKIP_DIRS for p in fpath.parts[:-1]):
                continue
            if not fpath.is_file():
                continue

            analysis = self.analyze_file(fpath)
            if analysis:
                # Make path relative to repo root
                try:
                    analysis["rel_path"] = str(fpath.relative_to(repo_path))
                except ValueError:
                    analysis["rel_path"] = str(fpath)
                results.append(analysis)

        return results
# ── YAML Analyzer ─────────────────────────────────────────────────────────────

class YAMLAnalyzer:
    """Parses YAML configs: dbt schema.yml, Airflow DAGs, Prefect flows."""

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "type": "unknown",
            "models": [],
            "sources": [],
            "dag_tasks": [],
        }
        try:
            data = yaml.safe_load(source)
        except yaml.YAMLError:
            result["parse_error"] = "invalid yaml"
            return result

        if not isinstance(data, dict):
            return result

        if "models" in data:
            result["type"] = "dbt_schema"
            for model in data.get("models", []) or []:
                if isinstance(model, dict) and "name" in model:
                    result["models"].append(model["name"])

        if "sources" in data:
            result["type"] = "dbt_sources"
            for src in data.get("sources", []) or []:
                if isinstance(src, dict):
                    schema = src.get("name", "")
                    for tbl in src.get("tables", []) or []:
                        if isinstance(tbl, dict) and "name" in tbl:
                            result["sources"].append(f"{schema}.{tbl['name']}")

        return result


# ── Notebook Analyzer ─────────────────────────────────────────────────────────

class NotebookAnalyzer:
    """Extracts code cells from Jupyter notebooks and re-analyzes as Python."""

    def __init__(self):
        self.python_analyzer = PythonAnalyzer()

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "cells": 0,
            "code_cells": 0,
            "imports": [],
            "exports": [],
            "functions": [],
        }
        try:
            nb = json.loads(source)
        except json.JSONDecodeError:
            result["parse_error"] = "invalid json"
            return result

        cells = nb.get("cells", [])
        result["cells"] = len(cells)
        combined_code = []

        for cell in cells:
            if cell.get("cell_type") == "code":
                result["code_cells"] += 1
                src = cell.get("source", [])
                if isinstance(src, list):
                    combined_code.extend(src)
                elif isinstance(src, str):
                    combined_code.append(src)

        full_source = "\n".join(combined_code)
        py_result = self.python_analyzer.analyze(path, full_source)
        result.update({
            "imports": py_result["imports"],
            "exports": py_result["exports"],
            "functions": py_result["functions"],
        })
        return result
    # ── SQL Analyzer ──────────────────────────────────────────────────────────────

SQL_FROM_RE = re.compile(r'\bFROM\s+([\w.`"\[\]]+)', re.IGNORECASE)
SQL_JOIN_RE = re.compile(r'\bJOIN\s+([\w.`"\[\]]+)', re.IGNORECASE)
SQL_INTO_RE = re.compile(r'\bINSERT\s+INTO\s+([\w.`"\[\]]+)', re.IGNORECASE)
SQL_CREATE_RE = re.compile(
    r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.`"\[\]]+)',
    re.IGNORECASE
)
SQL_CTE_RE = re.compile(r'\bWITH\s+([\w]+)\s+AS\s*\(', re.IGNORECASE)
DBT_REF_RE = re.compile(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}")
DBT_SOURCE_RE = re.compile(r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}")


def _clean_table_name(raw: str) -> str:
    return raw.strip('`"[]').split(".")[-1].lower()


class SQLAnalyzer:
    """Regex-based SQL dependency extractor."""

    def analyze(self, path: Path, source: str) -> Dict[str, Any]:
        result = {
            "source_tables": [],
            "target_tables": [],
            "cte_names": [],
            "dbt_refs": [],
            "dbt_sources": [],
            "raw_sql": source,
        }

        ctes = {m.group(1).lower() for m in SQL_CTE_RE.finditer(source)}
        result["cte_names"] = list(ctes)

        result["dbt_refs"] = [m.group(1) for m in DBT_REF_RE.finditer(source)]
        result["dbt_sources"] = [
            f"{m.group(1)}.{m.group(2)}" for m in DBT_SOURCE_RE.finditer(source)
        ]

        from_tables = {_clean_table_name(m.group(1)) for m in SQL_FROM_RE.finditer(source)}
        join_tables = {_clean_table_name(m.group(1)) for m in SQL_JOIN_RE.finditer(source)}
        source_tables = (from_tables | join_tables) - ctes
        source_tables.update(r.lower() for r in result["dbt_refs"])
        source_tables.update(s.split(".")[-1].lower() for s in result["dbt_sources"])
        result["source_tables"] = sorted(source_tables)

        into_tables = {_clean_table_name(m.group(1)) for m in SQL_INTO_RE.finditer(source)}
        create_tables = {_clean_table_name(m.group(1)) for m in SQL_CREATE_RE.finditer(source)}
        result["target_tables"] = sorted(into_tables | create_tables)

        return result