"""
Orchestrator V2: wires all four agents in sequence.
Surveyor → Hydrologist → Semanticist → Archivist
Serializes all outputs to .cartography/
Supports incremental mode: re-analyzes only changed files since last run.
"""
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

from src.graph.knowledge_graph import KnowledgeGraph
from src.agents.surveyor import Surveyor
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.archivist import Archivist
from src.analyzers.git_analyzer import (
    get_changed_files_since_last_run,
    get_last_run_timestamp,
)


CARTOGRAPHY_DIR = ".cartography"


class Orchestrator:
    """
    Full pipeline: Surveyor + Hydrologist + Semanticist + Archivist.
    Incremental mode: loads existing KG and re-analyzes only changed files.
    """

    def __init__(
        self,
        repo_path: Path,
        output_dir: Optional[Path] = None,
        verbose: bool = True,
    ):
        self.repo_path = repo_path.resolve()
        self.output_dir = output_dir or (self.repo_path / CARTOGRAPHY_DIR)
        self.verbose = verbose
        self.kg = KnowledgeGraph()

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"[orchestrator] {msg}")

    def run_analysis(self) -> Dict[str, Any]:
        """
        Run the full analysis pipeline.
        Detects incremental mode automatically if .cartography/ already exists.
        """
        start = time.time()
        self._log(f"Starting analysis of {self.repo_path}")
        self._log(f"Output dir: {self.output_dir}")
        print()

        report: Dict[str, Any] = {
            "repo_path": str(self.repo_path),
            "output_dir": str(self.output_dir),
            "agents": {},
        }

        # ── Incremental mode check ─────────────────────────────────────────────
        incremental = False
        changed_files = []
        if self.output_dir.exists():
            last_run = get_last_run_timestamp(self.output_dir)
            if last_run:
                changed_files = get_changed_files_since_last_run(
                    self.repo_path, last_run
                )
                if changed_files:
                    self._log(
                        f"Incremental mode: {len(changed_files)} files changed "
                        f"since last run"
                    )
                    self.kg = KnowledgeGraph.load(self.output_dir)
                    incremental = True
                else:
                    self._log("Incremental mode: no changes since last run")
                    self.kg = KnowledgeGraph.load(self.output_dir)
                    report["kg_stats"] = self.kg.stats()
                    report["incremental"] = True
                    report["changed_files"] = []
                    return report

        report["incremental"] = incremental
        report["changed_files"] = changed_files

        try:
            # ── Agent 1: Surveyor ──────────────────────────────────────────────
            self._log("Running Agent 1: Surveyor (static structure analysis)...")
            try:
                surveyor = Surveyor(self.kg, self.repo_path, verbose=self.verbose)
                surveyor_stats = surveyor.run()
                report["agents"]["surveyor"] = surveyor_stats
    
                hubs = surveyor.get_hub_modules(5)
                dead = surveyor.get_dead_code_candidates()
                circulars = self.kg.find_circular_deps()
    
                self._log(f"  Top hubs: {[h['path'] for h in hubs[:3]]}")
                self._log(f"  Circular deps: {len(circulars)} found")
                self._log(f"  Dead code candidates: {len(dead)}")
            except Exception as e:
                self._log(f"  [Error] Surveyor failed: {e}")
                report["agents"]["surveyor"] = {"status": "error", "error": str(e)}
            print()

            # ── Agent 2: Hydrologist ───────────────────────────────────────────
            self._log("Running Agent 2: Hydrologist (data lineage analysis)...")
            try:
                hydrologist = Hydrologist(self.kg, self.repo_path, verbose=self.verbose)
                hydro_stats = hydrologist.run()
                report["agents"]["hydrologist"] = hydro_stats
    
                sources = hydrologist.find_sources()
                sinks = hydrologist.find_sinks()
                self._log(f"  Data sources: {sources[:5]}")
                self._log(f"  Data sinks: {sinks[:5]}")
            except Exception as e:
                self._log(f"  [Error] Hydrologist failed: {e}")
                report["agents"]["hydrologist"] = {"status": "error", "error": str(e)}
            print()

            # ── Agent 3: Semanticist ───────────────────────────────────────────
            self._log("Running Agent 3: Semanticist (LLM analysis)...")
            try:
                semanticist = Semanticist(self.kg, self.repo_path, verbose=self.verbose)
                sem_stats = semanticist.run()
                report["agents"]["semanticist"] = sem_stats
                day_one_answers = sem_stats.get("day_one_answers")
                domain_map = sem_stats.get("domain_map")
            except Exception as e:
                self._log(f"  [Error] Semanticist failed: {e}")
                report["agents"]["semanticist"] = {"status": "error", "error": str(e)}
                day_one_answers = None
                domain_map = None
            print()

            # ── Agent 4: Archivist ─────────────────────────────────────────────
            self._log("Running Agent 4: Archivist (generating artifacts)...")
            try:
                archivist = Archivist(
                    self.kg, self.repo_path, self.output_dir, verbose=self.verbose
                )
                archivist_stats = archivist.run(
                    day_one_answers=day_one_answers,
                    domain_map=domain_map,
                )
                report["agents"]["archivist"] = archivist_stats
            except Exception as e:
                self._log(f"  [Error] Archivist failed: {e}")
                report["agents"]["archivist"] = {"status": "error", "error": str(e)}
            print()

        finally:
            # Always save the KG even if an agent fails mid-run
            self._log(f"Saving knowledge graph to {self.output_dir}/")
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.kg.save(self.output_dir)

        elapsed = time.time() - start
        report["elapsed_seconds"] = round(elapsed, 2)
        report["kg_stats"] = self.kg.stats()
        report["top_hubs"] = hubs if "hubs" in dir() else []
        report["dead_code_candidates"] = dead[:20] if "dead" in dir() else []
        report["circular_deps"] = circulars if "circulars" in dir() else []
        report["data_sources"] = sources if "sources" in dir() else []
        report["data_sinks"] = sinks if "sinks" in dir() else []

        summary_path = self.output_dir / "analysis_summary.json"
        summary_path.write_text(json.dumps(report, indent=2, default=str))

        self._log(f"Analysis complete in {elapsed:.1f}s")
        self._log(f"Summary: {self.kg.stats()}")

        return report

    def get_knowledge_graph(self) -> KnowledgeGraph:
        return self.kg

    @classmethod
    def load_existing(cls, repo_path: Path) -> "Orchestrator":
        """Load a previously saved analysis."""
        orch = cls(repo_path, verbose=False)
        output_dir = repo_path.resolve() / CARTOGRAPHY_DIR
        if output_dir.exists():
            orch.kg = KnowledgeGraph.load(output_dir)
        return orch
