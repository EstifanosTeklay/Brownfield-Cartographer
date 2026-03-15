"""
The Surveyor Agent: Static Structure Analyst.

Responsibilities:
- Walk the repo and analyze all supported files
- Build the module import graph (IMPORTS edges)
- Build CALLS edges from function call analysis
- Build CONFIGURES edges from dbt/Airflow config files
- Compute git velocity, PageRank, circular dependencies
- Identify dead code candidates
- Populate KnowledgeGraph with ModuleNodes and FunctionNodes
"""
from __future__ import annotations
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict

from src.analyzers.tree_sitter_analyzer import LanguageRouter, detect_language, SKIP_DIRS
from src.analyzers.git_analyzer import (
    get_file_velocity, get_git_log_summary, get_high_velocity_files,
    get_last_modified, get_file_contributor_signals, is_git_repo
)
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode, FunctionNode, Language


# ── Import resolution helpers ──────────────────────────────────────────────────

def _resolve_relative_import(import_str: str, module_path: str) -> str:
    """Convert a relative import like '.utils' from 'src/agents/surveyor.py'."""
    dots = len(import_str) - len(import_str.lstrip("."))
    suffix = import_str.lstrip(".")
    parts = module_path.rstrip("/").split("/")
    parent = parts[: max(1, len(parts) - dots)]
    if suffix:
        return "/".join(parent) + "/" + suffix.replace(".", "/")
    return "/".join(parent)


def _import_to_path_candidates(import_str: str, repo_path: Path) -> List[Path]:
    """
    Convert 'src.utils.helpers' -> candidate file paths in repo.
    e.g. src/utils/helpers.py or src/utils/helpers/__init__.py
    """
    clean = import_str.lstrip(".")
    as_path = clean.replace(".", "/")
    return [
        repo_path / f"{as_path}.py",
        repo_path / as_path / "__init__.py",
    ]


class Surveyor:
    """
    Agent 1: The Surveyor
    Performs deep static analysis and builds the structural layer of the knowledge graph.
    """

    def __init__(self, kg: KnowledgeGraph, repo_path: Path, verbose: bool = True):
        self.kg = kg
        self.repo_path = repo_path
        self.verbose = verbose
        self.router = LanguageRouter()
        self._analyses: List[Dict[str, Any]] = []

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [surveyor] {msg}")

    def run(self) -> Dict[str, Any]:
        """Run full static analysis. Returns a summary dict."""
        start = time.time()
        self._log(f"Analyzing {self.repo_path} ...")

        self._analyses = self.router.walk_repo(self.repo_path)
        self._log(f"Found {len(self._analyses)} analyzable files")

        self._build_module_nodes()
        self._build_import_graph()
        self._apply_git_velocity()
        self._detect_dead_code()
        self._build_configures_edges()
        self._build_call_edges()

        elapsed = time.time() - start
        stats = self.kg.stats()
        self._log(
            f"Done in {elapsed:.1f}s — {stats['modules']} modules, "
            f"{stats['module_edges']} import edges"
        )
        return {
            "elapsed_seconds": round(elapsed, 2),
            "files_analyzed": len(self._analyses),
            **stats,
        }

    def _build_module_nodes(self) -> None:
        for analysis in self._analyses:
            rel_path = analysis.get("rel_path", analysis.get("path", ""))
            lang_str = analysis.get("language", "other")
            try:
                lang = Language(lang_str)
            except ValueError:
                lang = Language.OTHER

            complexity = 0.0
            if lang == Language.PYTHON:
                fns = analysis.get("functions", [])
                complexity = float(len(fns))

            node = ModuleNode(
                path=rel_path,
                language=lang,
                complexity_score=complexity,
                loc=analysis.get("loc", 0),
                imports=analysis.get("imports", []),
                exports=analysis.get("exports", []),
                docstring=analysis.get("docstring"),
                decorators=analysis.get("decorators", []),
            )
            self.kg.add_module(node)

            if lang == Language.PYTHON:
                for fn_dict in analysis.get("functions", []):
                    fn = FunctionNode(
                        qualified_name=f"{rel_path}::{fn_dict['name']}",
                        parent_module=rel_path,
                        signature=fn_dict["signature"],
                        is_public_api=fn_dict.get("is_public", True),
                        lineno=fn_dict.get("lineno", 0),
                        decorators=fn_dict.get("decorators", []),
                    )
                    self.kg.add_function(fn)

    def _build_import_graph(self) -> None:
        """
        Resolve imports to actual file paths in the repo and add IMPORTS edges.
        Normalizes Windows backslashes before comparison.
        """
        all_module_paths = set(self.kg.modules.keys())

        # Build dotted-module index for package-style imports.
        # Example: mage_ai/server/api.py -> mage_ai.server.api
        dotted_index: Dict[str, str] = {}
        for module_path in all_module_paths:
            if not module_path.endswith(".py"):
                continue
            dotted = module_path[:-3].replace("/", ".")
            dotted_index[dotted] = module_path
            if dotted.endswith(".__init__"):
                dotted_index[dotted[:-9]] = module_path

        for analysis in self._analyses:
            source_path = analysis.get("rel_path", "")
            if analysis.get("language") != Language.PYTHON.value:
                continue

            for imp in analysis.get("imports", []):
                imp = imp.strip()
                if not imp:
                    continue

                if imp.startswith("."):
                    resolved = _resolve_relative_import(imp, source_path)
                    candidates = [
                        Path(resolved + ".py"),
                        Path(resolved + "/__init__.py"),
                    ]
                else:
                    candidates = [
                        Path(imp.replace(".", "/") + ".py"),
                        Path(imp.replace(".", "/") + "/__init__.py"),
                    ]

                    # Direct dotted lookup is often the most reliable for large packages.
                    direct_target = dotted_index.get(imp)
                    if direct_target and direct_target in all_module_paths:
                        self.kg.add_import_edge(source_path, direct_target)
                        continue

                for candidate in candidates:
                    # Normalize separators before lookup
                    cstr = str(candidate).replace("\\", "/")
                    if cstr in all_module_paths:
                        self.kg.add_import_edge(source_path, cstr)
                        break

    def _apply_git_velocity(self) -> None:
        """Attach git change velocity data to ModuleNodes."""
        if not is_git_repo(self.repo_path):
            self._log("Not a git repo — skipping velocity analysis")
            return

        velocity = get_file_velocity(self.repo_path, days=30)
        git_summary = get_git_log_summary(self.repo_path, days=90)
        self._log(
            f"Git: {git_summary['commits_last_90d']} commits in 90d, "
            f"{len(git_summary['contributors'])} contributors"
        )

        for path_str, node in self.kg.modules.items():
            v = velocity.get(path_str, 0)
            last_modified = get_last_modified(self.repo_path, path_str)
            contrib = get_file_contributor_signals(self.repo_path, path_str, days=90, top_n=3)

            node.change_velocity_30d = v
            node.last_modified = last_modified
            node.last_author = contrib.get("last_author")
            node.last_author_email = contrib.get("last_author_email")
            node.likely_contacts = contrib.get("likely_contacts", [])
            if path_str in self.kg.module_graph:
                self.kg.module_graph.nodes[path_str]["change_velocity_30d"] = v
                self.kg.module_graph.nodes[path_str]["last_modified"] = last_modified
                self.kg.module_graph.nodes[path_str]["last_author"] = node.last_author
                self.kg.module_graph.nodes[path_str]["last_author_email"] = node.last_author_email
                self.kg.module_graph.nodes[path_str]["likely_contacts"] = node.likely_contacts

    def _detect_dead_code(self) -> None:
        """
        Mark isolated executable modules as dead code candidates.
        Avoid flagging SQL/YAML/notebooks/configuration files and lineage-active files.
        """
        lineage_active_files = {
            str(txn.source_file).replace("\\", "/")
            for txn in self.kg.transformations.values()
            if getattr(txn, "source_file", None)
        }

        candidate_languages = {
            Language.PYTHON.value,
            Language.JAVASCRIPT.value,
            Language.TYPESCRIPT.value,
        }

        for node_id in self.kg.module_graph.nodes():
            in_deg = self.kg.module_graph.in_degree(node_id)
            out_deg = self.kg.module_graph.out_degree(node_id)
            if node_id not in self.kg.modules:
                continue

            module = self.kg.modules[node_id]
            language = getattr(module.language, "value", module.language)
            language = str(language) if language is not None else Language.OTHER.value

            # Non-executable artifacts should not be dead-code candidates.
            if language not in candidate_languages:
                module.is_dead_code_candidate = False
                self.kg.module_graph.nodes[node_id]["is_dead_code_candidate"] = False
                continue

            # Files participating in lineage transformations are active by definition.
            if node_id in lineage_active_files:
                module.is_dead_code_candidate = False
                self.kg.module_graph.nodes[node_id]["is_dead_code_candidate"] = False
                continue

            is_entrypoint = any(
                node_id.endswith(ep)
                for ep in ["__main__.py", "cli.py", "main.py", "app.py", "run.py"]
            )

            # High-confidence dead code only:
            # isolated + non-entrypoint + no imports + no exports + no recent changes + small file
            has_no_surface = len(getattr(module, "imports", []) or []) == 0 and len(getattr(module, "exports", []) or []) == 0
            low_velocity = int(getattr(module, "change_velocity_30d", 0) or 0) == 0
            small_file = int(getattr(module, "loc", 0) or 0) <= 120

            is_candidate = (
                in_deg == 0
                and out_deg == 0
                and not is_entrypoint
                and has_no_surface
                and low_velocity
                and small_file
            )
            module.is_dead_code_candidate = is_candidate
            self.kg.module_graph.nodes[node_id]["is_dead_code_candidate"] = is_candidate

    def _build_configures_edges(self) -> None:
        """
        Add CONFIGURES edges from config files to the modules they govern.
        - dbt_project.yml  → all .sql model files
        - schema.yml files → .sql files in same directory
        - Airflow dag YAML → .py dag files in same directory
        """
        all_paths = set(self.kg.modules.keys())

        for path_str in list(all_paths):
            fname = Path(path_str).name.lower()

            # dbt_project.yml configures all SQL models
            if fname == "dbt_project.yml":
                for target in all_paths:
                    if target.endswith(".sql"):
                        self.kg.add_configures_edge(path_str, target)

            # schema.yml files configure SQL files in same directory
            elif fname in ("schema.yml", "schema.yaml", "sources.yml", "sources.yaml"):
                same_dir = str(Path(path_str).parent).replace("\\", "/")
                for target in all_paths:
                    t_dir = str(Path(target).parent).replace("\\", "/")
                    if t_dir == same_dir and target.endswith(".sql"):
                        self.kg.add_configures_edge(path_str, target)

            # Airflow-style YAML DAGs configure Python DAG files
            elif fname.endswith((".yml", ".yaml")) and "dag" in fname:
                same_dir = str(Path(path_str).parent).replace("\\", "/")
                for target in all_paths:
                    t_dir = str(Path(target).parent).replace("\\", "/")
                    if t_dir == same_dir and target.endswith(".py") and "dag" in target.lower():
                        self.kg.add_configures_edge(path_str, target)

    def _build_call_edges(self) -> None:
        """
        Add CALLS edges between modules based on function call analysis.
        Requires tree_sitter_analyzer.extract_function_calls().
        """
        try:
            from src.analyzers.tree_sitter_analyzer import extract_function_calls
        except ImportError:
            return

        fn_index = self.kg.function_index  # qualified_name -> FunctionNode

        for analysis in self._analyses:
            if analysis.get("language") != Language.PYTHON.value:
                continue

            source_path = analysis.get("rel_path", "")
            full_path = self.repo_path / source_path

            try:
                source = full_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

            calls = extract_function_calls(source)
            for call in calls:
                callee_name = call.get("callee", "")
                # Find a FunctionNode whose qualified_name ends with the callee
                for qname, fn_node in fn_index.items():
                    # Match if callee matches last segment of qualified name
                    if qname.endswith(f"::{callee_name}"):
                        target_module = fn_node.parent_module
                        if (
                            source_path != target_module
                            and source_path in self.kg.module_graph
                            and target_module in self.kg.module_graph
                        ):
                            self.kg.add_call_edge(
                                source=source_path,
                                target=target_module,
                                caller=call.get("caller", ""),
                                callee=callee_name,
                                lineno=call.get("lineno", 0),
                            )

    # ── Query helpers ──────────────────────────────────────────────────────────

    def get_high_velocity_report(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Return top N files by change velocity with their metadata."""
        velocity_map = {
            path: node.change_velocity_30d
            for path, node in self.kg.modules.items()
        }
        top = sorted(velocity_map.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "path": path,
                "commits_30d": count,
                "loc": self.kg.modules[path].loc,
                "complexity": self.kg.modules[path].complexity_score,
            }
            for path, count in top
            if count > 0
        ]

    def get_hub_modules(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Return top N architectural hubs by PageRank."""
        scores = self.kg.pagerank_modules()
        top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "path": path,
                "pagerank": round(score, 6),
                "in_degree": self.kg.module_graph.in_degree(path),
                "language": self.kg.modules[path].language
                if isinstance(self.kg.modules[path].language, str)
                else self.kg.modules[path].language.value
                if hasattr(self.kg.modules[path].language, "value")
                else str(self.kg.modules[path].language),
            }
            for path, score in top
            if path in self.kg.modules
        ]

    def get_dead_code_candidates(self) -> List[str]:
        return [
            path for path, node in self.kg.modules.items()
            if node.is_dead_code_candidate
        ]
