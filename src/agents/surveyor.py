"""
The Surveyor Agent: Static Structure Analyst.

Responsibilities:
- Walk the repo and analyze all supported files
- Build the module import graph
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
    get_last_modified, is_git_repo
)
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode, FunctionNode, Language


# Resolve a relative import like ".utils" from "src/agents/surveyor.py"
def _resolve_relative_import(import_str: str, module_path: str) -> str:
    """Convert a relative import to a module path string."""
    dots = len(import_str) - len(import_str.lstrip("."))
    suffix = import_str.lstrip(".")
    parts = module_path.rstrip("/").split("/")
    parent = parts[: max(1, len(parts) - dots)]
    if suffix:
        return "/".join(parent) + "/" + suffix.replace(".", "/")
    return "/".join(parent)


def _import_to_path_candidates(import_str: str, repo_path: Path) -> List[Path]:
    """
    Convert a Python import string like 'src.utils.helpers' to candidate file paths
    in the repo, e.g. src/utils/helpers.py or src/utils/helpers/__init__.py
    """
    clean = import_str.lstrip(".")
    as_path = clean.replace(".", "/")
    candidates = [
        repo_path / f"{as_path}.py",
        repo_path / as_path / "__init__.py",
    ]
    return candidates


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
        self._analyses: List[Dict[str, Any]] = []  # raw analysis results

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [surveyor] {msg}")

    def run(self) -> Dict[str, Any]:
        """Run full static analysis. Returns a summary dict."""
        start = time.time()
        self._log(f"Analyzing {self.repo_path} ...")

        # Step 1: Walk and analyze all files
        self._analyses = self.router.walk_repo(self.repo_path)
        self._log(f"Found {len(self._analyses)} analyzable files")

        # Step 2: Build ModuleNodes
        self._build_module_nodes()

        # Step 3: Resolve imports and build import graph
        self._build_import_graph()

        # Step 4: Git velocity
        self._apply_git_velocity()

        # Step 5: Dead code detection
        self._detect_dead_code()

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

            # Compute basic complexity
            complexity = 0.0
            if lang == Language.PYTHON:
                fns = analysis.get("functions", [])
                if fns:
                    complexity = sum(1 for _ in fns)  # simplified: fn count

            node = ModuleNode(
                path=rel_path,
                language=lang,
                complexity_score=float(complexity),
                loc=analysis.get("loc", 0),
                imports=analysis.get("imports", []),
                exports=analysis.get("exports", []),
                docstring=analysis.get("docstring"),
            )
            self.kg.add_module(node)

            # Register FunctionNodes
            if lang == Language.PYTHON:
                for fn_dict in analysis.get("functions", []):
                    fn = FunctionNode(
                        qualified_name=f"{rel_path}::{fn_dict['name']}",
                        parent_module=rel_path,
                        signature=fn_dict["signature"],
                        is_public_api=fn_dict.get("is_public", True),
                        lineno=fn_dict.get("lineno", 0),
                    )
                    self.kg.add_function(fn)

    def _build_import_graph(self) -> None:
        """
        Resolve imports to actual file paths in the repo and add import edges.
        We try direct path matching: 'from src.models.nodes import X' → src/models/nodes.py
        """
        all_module_paths = set(self.kg.modules.keys())

        for analysis in self._analyses:
            source_path = analysis.get("rel_path", "")
            if analysis.get("language") != Language.PYTHON.value:
                continue

            for imp in analysis.get("imports", []):
                # Handle relative imports
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

                for candidate in candidates:
                    cstr = str(candidate)
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
            node.change_velocity_30d = v
            # Update graph node attribute
            if path_str in self.kg.module_graph:
                self.kg.module_graph.nodes[path_str]["change_velocity_30d"] = v

    def _detect_dead_code(self) -> None:
        """
        Mark modules with no imports INTO them and no exports referenced elsewhere
        as dead code candidates.
        """
        import networkx as nx
        # Nodes with in_degree == 0 and out_degree == 0 in module graph
        # are isolated — likely dead code (or entry points)
        for node_id in self.kg.module_graph.nodes():
            in_deg = self.kg.module_graph.in_degree(node_id)
            out_deg = self.kg.module_graph.out_degree(node_id)
            if in_deg == 0 and out_deg == 0 and node_id in self.kg.modules:
                # Only flag if it's not an obvious entry point
                if not any(
                    node_id.endswith(ep)
                    for ep in ["__main__.py", "cli.py", "main.py", "app.py", "run.py"]
                ):
                    self.kg.modules[node_id].is_dead_code_candidate = True
                    self.kg.module_graph.nodes[node_id]["is_dead_code_candidate"] = True

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
                "language": self.kg.modules.get(path, ModuleNode(path)).language.value,
            }
            for path, score in top
        ]

    def get_dead_code_candidates(self) -> List[str]:
        return [
            path for path, node in self.kg.modules.items()
            if node.is_dead_code_candidate
        ]
