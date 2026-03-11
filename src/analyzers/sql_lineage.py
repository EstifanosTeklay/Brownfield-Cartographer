"""
SQL Lineage Analyzer: extracts table dependency graphs from SQL files.
Uses sqlglot as primary parser (AST-level) with regex as fallback.
Supports: PostgreSQL, BigQuery, Snowflake, DuckDB, SparkSQL dialects.
Handles dbt Jinja templates (ref(), source()) before parsing.
"""
from __future__ import annotations
import re
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)

# ── dbt Jinja patterns ────────────────────────────────────────────────────────
DBT_REF_RE = re.compile(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}")
DBT_SOURCE_RE = re.compile(
    r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}"
)
DBT_VAR_RE = re.compile(r"\{\{[^}]+\}\}")

# Supported dialects to try in order
DIALECTS = ["duckdb", "bigquery", "snowflake", "spark", "postgres"]


def _strip_jinja(sql: str) -> Tuple[str, List[str], List[str]]:
    """
    Strip dbt Jinja expressions and extract ref() and source() targets.
    Returns (clean_sql, dbt_refs, dbt_sources).
    """
    dbt_refs = [m.group(1) for m in DBT_REF_RE.finditer(sql)]
    dbt_sources = [
        f"{m.group(1)}.{m.group(2)}" for m in DBT_SOURCE_RE.finditer(sql)
    ]

    # Replace ref() with a plain table name for sqlglot
    clean = DBT_REF_RE.sub(lambda m: m.group(1), sql)
    # Replace source() with table name
    clean = DBT_SOURCE_RE.sub(lambda m: m.group(2), clean)
    # Strip remaining Jinja expressions
    clean = DBT_VAR_RE.sub("'__jinja__'", clean)

    return clean, dbt_refs, dbt_sources


def _parse_sql(sql: str) -> Optional[List[sqlglot.Expression]]:
    """
    Try parsing SQL with multiple dialects.
    Returns list of parsed statements or None if all dialects fail.
    """
    for dialect in DIALECTS:
        try:
            statements = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IGNORE)
            if statements:
                return statements
        except Exception:
            continue
    return None


def _extract_tables_from_ast(
    statements: List[sqlglot.Expression],
) -> Dict[str, Any]:
    """
    Extract source tables, target tables, and CTEs from sqlglot AST.
    Returns structured result with read/write distinction.
    """
    source_tables = set()
    target_tables = set()
    cte_names = set()
    line_ranges = []

    for stmt in statements:
        if stmt is None:
            continue

        # Extract CTEs first so we can exclude them from sources
        for cte in stmt.find_all(exp.CTE):
            if hasattr(cte, 'alias'):
                cte_names.add(cte.alias.lower())

        # SELECT sources — FROM and JOIN clauses
        for table in stmt.find_all(exp.Table):
            name = table.name
            if not name:
                continue
            name_lower = name.lower()
            # Skip CTEs and Jinja placeholders
            if name_lower in cte_names or name_lower == "__jinja__":
                continue
            # Determine if this is a read or write context
            parent = table.parent
            parent_type = type(parent).__name__ if parent else ""
            if parent_type in ("Into", "Create", "Insert"):
                target_tables.add(name_lower)
            else:
                source_tables.add(name_lower)

        # INSERT INTO targets
        for insert in stmt.find_all(exp.Insert):
            if insert.this and hasattr(insert.this, 'name'):
                target_tables.add(insert.this.name.lower())

        # CREATE TABLE / VIEW targets
        for create in stmt.find_all(exp.Create):
            if create.this and hasattr(create.this, 'name'):
                target_tables.add(create.this.name.lower())

        # Try to get line range from AST
        if hasattr(stmt, 'meta') and stmt.meta:
            start = stmt.meta.get('line', 0)
            line_ranges.append(start)

    # Remove targets from sources (a table written to is not a source)
    source_tables -= target_tables
    # Remove CTEs from sources
    source_tables -= cte_names

    line_range = (
        min(line_ranges) if line_ranges else 0,
        max(line_ranges) if line_ranges else 0,
    )

    return {
        "source_tables": sorted(source_tables),
        "target_tables": sorted(target_tables),
        "cte_names": sorted(cte_names),
        "line_range": line_range,
    }


class SQLLineageAnalyzer:
    """
    Production-grade SQL lineage analyzer using sqlglot AST parsing.
    Falls back to regex on parse failure.
    Supports dbt Jinja templates.
    """

    def analyze_sql_file(
        self, path: Path, repo_path: Path
    ) -> List[Dict[str, Any]]:
        """
        Analyze a single SQL file and return lineage entries.
        Each entry: {source_tables, target_tables, source_file,
                     sql_type, cte_names, dbt_refs, dbt_sources,
                     line_range, dialect_used}
        """
        entries = []
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            logger.warning(f"Could not read {path}: {e}")
            return entries

        try:
            rel_path = str(path.relative_to(repo_path))
        except ValueError:
            rel_path = str(path)

        is_dbt_model = self._is_dbt_model(path, source)

        # Strip Jinja and extract dbt refs/sources
        clean_sql, dbt_refs, dbt_sources = _strip_jinja(source)

        # Try sqlglot AST parsing
        statements = _parse_sql(clean_sql)
        dialect_used = "sqlglot"

        if statements:
            result = _extract_tables_from_ast(statements)
        else:
            # Fallback to regex
            logger.warning(f"sqlglot failed on {rel_path} — using regex fallback")
            result = self._regex_fallback(source)
            dialect_used = "regex_fallback"

        # Merge dbt refs into source tables
        source_tables = list(set(
            result["source_tables"] +
            [r.lower() for r in dbt_refs] +
            [s.split(".")[-1].lower() for s in dbt_sources]
        ))

        target_tables = result["target_tables"]

        # For dbt models filename IS the target if no explicit target found
        if is_dbt_model and not target_tables:
            target_tables = [path.stem.lower()]

        if source_tables or target_tables:
            entries.append({
                "source_tables": sorted(source_tables),
                "target_tables": sorted(target_tables),
                "source_file": rel_path,
                "sql_type": "dbt_model" if is_dbt_model else "raw_sql",
                "cte_names": result["cte_names"],
                "dbt_refs": dbt_refs,
                "dbt_sources": dbt_sources,
                "line_range": result["line_range"],
                "dialect_used": dialect_used,
            })

        return entries

    def _is_dbt_model(self, path: Path, source: str) -> bool:
        return "models" in path.parts or bool(re.search(r'\{\{', source))

    def _regex_fallback(self, source: str) -> Dict[str, Any]:
        """Simple regex fallback for when sqlglot cannot parse."""
        from src.analyzers.tree_sitter_analyzer import SQLAnalyzer
        analyzer = SQLAnalyzer()
        from pathlib import Path
        result = analyzer.analyze(Path("fallback.sql"), source)
        return {
            "source_tables": result.get("source_tables", []),
            "target_tables": result.get("target_tables", []),
            "cte_names": result.get("cte_names", []),
            "line_range": (0, 0),
        }

    def analyze_dbt_project(self, repo_path: Path) -> Dict[str, Any]:
        """
        Analyze a full dbt project.
        Returns project metadata and all lineage entries.
        """
        result = {
            "project_name": None,
            "models": [],
            "sources": [],
            "lineage_entries": [],
            "parse_errors": [],
        }

        # dbt_project.yml
        dbt_project = repo_path / "dbt_project.yml"
        if dbt_project.exists():
            try:
                import yaml
                data = yaml.safe_load(dbt_project.read_text())
                if isinstance(data, dict):
                    result["project_name"] = data.get("name")
            except Exception:
                pass

        # Walk all SQL files
        models_dir = repo_path / "models"
        search_dir = models_dir if models_dir.exists() else repo_path

        for sql_file in sorted(search_dir.rglob("*.sql")):
            try:
                entries = self.analyze_sql_file(sql_file, repo_path)
                result["lineage_entries"].extend(entries)
                for e in entries:
                    result["models"].append({
                        "name": sql_file.stem,
                        "path": e["source_file"],
                        "depends_on": e["source_tables"],
                        "sql_type": e["sql_type"],
                        "dialect_used": e["dialect_used"],
                        "line_range": e["line_range"],
                    })
            except Exception as e:
                logger.warning(f"Failed to analyze {sql_file}: {e}")
                result["parse_errors"].append({
                    "file": str(sql_file),
                    "error": str(e),
                })

        # Walk schema.yml for sources
        for yml_file in repo_path.rglob("*.yml"):
            try:
                import yaml
                data = yaml.safe_load(
                    yml_file.read_text(encoding="utf-8", errors="replace")
                )
                if isinstance(data, dict):
                    for src in data.get("sources", []) or []:
                        if isinstance(src, dict):
                            source_name = src.get("name", "")
                            for tbl in src.get("tables", []) or []:
                                if isinstance(tbl, dict) and "name" in tbl:
                                    result["sources"].append(
                                        f"{source_name}.{tbl['name']}"
                                    )
            except Exception:
                pass

        return result