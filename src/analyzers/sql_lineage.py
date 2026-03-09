"""
SQL Lineage Analyzer: extracts table dependency graphs from SQL files.
Handles: raw .sql files, dbt models (.sql with Jinja), dbt schema.yml.
Uses the SQLAnalyzer from tree_sitter_analyzer as its parsing engine.
Builds lineage entries (source_tables -> target_tables) for the Hydrologist.
"""
from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from src.analyzers.tree_sitter_analyzer import SQLAnalyzer, YAMLAnalyzer


class SQLLineageAnalyzer:
    """
    Parses SQL files (including dbt Jinja SQL) and produces
    a list of LineageEntry dicts ready for the Hydrologist.
    """

    def __init__(self):
        self._sql = SQLAnalyzer()

    def analyze_sql_file(self, path: Path, repo_path: Path) -> List[Dict[str, Any]]:
        """
        Analyze a single .sql file and return lineage entries.
        A lineage entry is: {source_tables, target_tables, source_file, sql_type}
        """
        entries = []
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return entries

        rel_path = str(path.relative_to(repo_path)) if repo_path else str(path)
        result = self._sql.analyze(path, source)

        # Determine SQL type: dbt model or raw SQL
        is_dbt = bool(result.get("dbt_refs") or result.get("dbt_sources"))
        is_dbt_model = self._is_dbt_model(path)

        source_tables = result.get("source_tables", [])
        target_tables = result.get("target_tables", [])

        # For dbt models: the model name IS the target table (filename without extension)
        if is_dbt_model and not target_tables:
            target_tables = [path.stem.lower()]

        if source_tables or target_tables:
            entries.append({
                "source_tables": source_tables,
                "target_tables": target_tables,
                "source_file": rel_path,
                "sql_type": "dbt_model" if is_dbt_model else "raw_sql",
                "cte_names": result.get("cte_names", []),
                "dbt_refs": result.get("dbt_refs", []),
                "dbt_sources": result.get("dbt_sources", []),
            })

        return entries

    def _is_dbt_model(self, path: Path) -> bool:
        """Heuristic: dbt models live in a 'models/' directory."""
        return "models" in path.parts or bool(
            re.search(r'\{\{', path.read_text(encoding="utf-8", errors="replace")
                      if path.exists() else "")
        )

    def analyze_dbt_project(self, repo_path: Path) -> Dict[str, Any]:
        """
        Analyze a full dbt project structure:
        - models/**/*.sql → SQL lineage
        - models/**/schema.yml → metadata
        - dbt_project.yml → project config
        """
        result = {
            "project_name": None,
            "models": [],
            "sources": [],
            "lineage_entries": [],
        }

        # dbt_project.yml
        dbt_project = repo_path / "dbt_project.yml"
        if dbt_project.exists():
            yaml_analyzer = YAMLAnalyzer()
            try:
                import yaml
                data = yaml.safe_load(dbt_project.read_text())
                if isinstance(data, dict):
                    result["project_name"] = data.get("name")
            except Exception:
                pass

        # Walk models directory
        models_dir = repo_path / "models"
        if not models_dir.exists():
            # Try repo root for .sql files
            models_dir = repo_path

        sql_files = list(models_dir.rglob("*.sql")) if models_dir.exists() else []
        for sql_file in sql_files:
            entries = self.analyze_sql_file(sql_file, repo_path)
            result["lineage_entries"].extend(entries)
            for e in entries:
                result["models"].append({
                    "name": sql_file.stem,
                    "path": e["source_file"],
                    "depends_on": e["source_tables"],
                    "sql_type": e["sql_type"],
                })

        # Walk schema.yml files
        yaml_analyzer = YAMLAnalyzer()
        for yml_file in repo_path.rglob("schema.yml"):
            try:
                yml_result = yaml_analyzer.analyze(
                    yml_file, yml_file.read_text(encoding="utf-8", errors="replace")
                )
                result["sources"].extend(yml_result.get("sources", []))
            except Exception:
                pass

        return result
