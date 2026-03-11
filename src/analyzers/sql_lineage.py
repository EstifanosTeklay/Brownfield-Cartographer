"""
SQL Lineage Analyzer: extracts table dependency graphs from SQL files.
Uses sqlglot as primary parser (supports 20+ dialects).
Falls back to regex for unparseable SQL.
Handles: raw .sql files, dbt models (.sql with Jinja), dbt schema.yml.
"""
from __future__ import annotations
import re
import yaml
import sqlglot
import sqlglot.expressions as exp
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from src.analyzers.tree_sitter_analyzer import YAMLAnalyzer


# ── Jinja stripping for dbt models ───────────────────────────────────────────

JINJA_REF_RE = re.compile(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}")
JINJA_SOURCE_RE = re.compile(
    r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}"
)
JINJA_BLOCK_RE = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)

# Supported dialects to try in order
DIALECTS = ["duckdb", "bigquery", "snowflake", "spark", "postgres", None]


def _strip_jinja(sql: str) -> Tuple[str, List[str], List[str]]:
    """
    Strip Jinja templating from dbt SQL.
    Extract ref() and source() targets before stripping.
    Replace {{ ref('model') }} with just the model name as a table reference.
    """
    dbt_refs = [m.group(1) for m in JINJA_REF_RE.finditer(sql)]
    dbt_sources = [
        f"{m.group(1)}.{m.group(2)}" for m in JINJA_SOURCE_RE.finditer(sql)
    ]

    # Replace ref() and source() with plain table names so sqlglot can parse
    sql = JINJA_REF_RE.sub(lambda m: m.group(1), sql)
    sql = JINJA_SOURCE_RE.sub(lambda m: m.group(2), sql)

    # Strip remaining Jinja blocks
    sql = JINJA_BLOCK_RE.sub("'__jinja__'", sql)

    return sql, dbt_refs, dbt_sources


def _parse_with_dialects(sql: str) -> Optional[List[sqlglot.Expression]]:
    """Try parsing SQL with multiple dialects. Return first success."""
    for dialect in DIALECTS:
        try:
            result = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IGNORE)
            if result:
                return result
        except Exception:
            continue
    return None


def _extract_tables_from_ast(
    statements: List[sqlglot.Expression],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Extract source tables, target tables, and CTE names from parsed AST.
    Returns (source_tables, target_tables, cte_names).
    """
    source_tables = set()
    target_tables = set()
    cte_names = set()

    for stmt in statements:
        if stmt is None:
            continue

        # Extract CTE names first so we can exclude them from sources
        for cte in stmt.find_all(exp.CTE):
            if hasattr(cte, 'alias'):
                cte_names.add(cte.alias.lower())

        # Find target tables (INSERT INTO, CREATE TABLE/VIEW)
        for node in stmt.find_all(exp.Insert):
            if hasattr(node, 'this') and isinstance(node.this, exp.Table):
                target_tables.add(node.this.name.lower())

        for node in stmt.find_all(exp.Create):
            if hasattr(node, 'this') and isinstance(node.this, exp.Table):
                target_tables.add(node.this.name.lower())

        # Find all table references
        for table in stmt.find_all(exp.Table):
            name = table.name.lower()
            if name and name != '__jinja__' and name not in cte_names:
                # Check if this table is in a FROM or JOIN context
                source_tables.add(name)

    # Remove targets from sources (a table created in this file is not a source)
    source_tables -= target_tables
    source_tables -= cte_names

    return (
        sorted(source_tables),
        sorted(target_tables),
        sorted(cte_names),
    )


class SQLLineageAnalyzer:
    """
    sqlglot-based SQL dependency extractor.
    Supports 20+ SQL dialects.
    Falls back to regex for unparseable files.
    """

    def analyze_sql_file(self, path: Path, repo_path: Path) -> List[Dict[str, Any]]:
        entries = []
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            print(f"  [sql_lineage] Cannot read {path}: {e}")
            return entries

        try:
            rel_path = str(path.relative_to(repo_path))
        except ValueError:
            rel_path = str(path)

        # Extract dbt Jinja refs before stripping
        dbt_refs = [m.group(1) for m in JINJA_REF_RE.finditer(source)]
        dbt_sources = [
            f"{m.group(1)}.{m.group(2)}"
            for m in JINJA_SOURCE_RE.finditer(source)
        ]

        # Strip Jinja for parsing
        clean_sql, _, _ = _strip_jinja(source)

        # Try sqlglot parsing
        statements = _parse_with_dialects(clean_sql)

        if statements:
            source_tables, target_tables, cte_names = \
                _extract_tables_from_ast(statements)
        else:
            # Fallback to regex
            print(f"  [sql_lineage] sqlglot failed for {rel_path} — using regex fallback")
            from src.analyzers.tree_sitter_analyzer import SQLAnalyzer
            regex_result = SQLAnalyzer().analyze(path, clean_sql)
            source_tables = regex_result.get("source_tables", [])
            target_tables = regex_result.get("target_tables", [])
            cte_names = regex_result.get("cte_names", [])

        # For dbt models filename IS the target if no explicit target found
        is_dbt_model = self._is_dbt_model(path, source)
        if is_dbt_model and not target_tables:
            target_tables = [path.stem.lower()]

        # Add dbt ref() targets as additional sources
        for ref in dbt_refs:
            ref_lower = ref.lower()
            if ref_lower not in source_tables:
                source_tables.append(ref_lower)

        # Get line range from first and last non-empty line
        lines = [i+1 for i, l in enumerate(source.splitlines()) if l.strip()]
        line_range = (lines[0] if lines else 0, lines[-1] if lines else 0)

        if source_tables or target_tables:
            entries.append({
                "source_tables": sorted(set(source_tables)),
                "target_tables": sorted(set(target_tables)),
                "source_file": rel_path,
                "line_range": line_range,
                "sql_type": "dbt_model" if is_dbt_model else "raw_sql",
                "cte_names": cte_names,
                "dbt_refs": dbt_refs,
                "dbt_sources": dbt_sources,
                "dialect_used": self._detect_dialect(source),
                "parsed_by": "sqlglot" if statements else "regex_fallback",
            })

        return entries

    def _is_dbt_model(self, path: Path, source: str) -> bool:
        return "models" in path.parts or bool(
            re.search(r'\{\{', source)
        )

    def _detect_dialect(self, source: str) -> str:
        """Heuristic dialect detection from SQL content."""
        source_lower = source.lower()
        if "qualify" in source_lower or "flatten(" in source_lower:
            return "snowflake"
        if "bignumeric" in source_lower or "struct<" in source_lower:
            return "bigquery"
        if "rlike" in source_lower or "array_contains" in source_lower:
            return "spark"
        return "generic"

    def analyze_dbt_project(self, repo_path: Path) -> Dict[str, Any]:
        result = {
            "project_name": None,
            "models": [],
            "sources": [],
            "lineage_entries": [],
        }

        dbt_project = repo_path / "dbt_project.yml"
        if dbt_project.exists():
            try:
                import yaml
                data = yaml.safe_load(dbt_project.read_text())
                if isinstance(data, dict):
                    result["project_name"] = data.get("name")
            except Exception:
                pass

        models_dir = repo_path / "models"
        if not models_dir.exists():
            models_dir = repo_path

        for sql_file in sorted(models_dir.rglob("*.sql")):
            try:
                entries = self.analyze_sql_file(sql_file, repo_path)
                result["lineage_entries"].extend(entries)
                for e in entries:
                    result["models"].append({
                        "name": sql_file.stem,
                        "path": e["source_file"],
                        "depends_on": e["source_tables"],
                        "sql_type": e["sql_type"],
                        "parsed_by": e.get("parsed_by", "unknown"),
                        "line_range": e.get("line_range", (0, 0)),
                    })
            except Exception as ex:
                print(f"  [sql_lineage] Skipping {sql_file}: {ex}")

        return result