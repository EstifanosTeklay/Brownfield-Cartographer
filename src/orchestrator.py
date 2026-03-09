"""
Orchestrator: wires Surveyor → Hydrologist in sequence,
serializes outputs to .cartography/.
This is the core of the interim submission pipeline.
"""
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

from src.graph.knowledge_graph import KnowledgeGraph
from src.agents.surveyor import Surveyor
from src.agents.hydrologist import Hydrologist


CARTOGRAPHY_DIR = ".cartography"


class Orchestrator:
    """
    Runs the full analysis pipeline for a repository.
    Phase 1 (interim): Surveyor + Hydrologist
    Phase 2 (final): + Semanticist + Archivist
    """

    def __init__(self, repo_path: Path, output_dir: Optional[Path] = None, verbose: bool = True):
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
        Returns a summary dict with stats from each agent.
        """
        start = time.time()
        self._log(f"Starting analysis of {self.repo_path}")
        self._log(f"Output dir: {self.output_dir}")
        print()

        report = {
            "repo_path": str(self.repo_path),
            "output_dir": str(self.output_dir),
            "agents": {},
        }

        # ── Agent 1: Surveyor ─────────────────────────────────────────────
        self._log("Running Agent 1: Surveyor (static structure analysis)...")
        surveyor = Surveyor(self.kg, self.repo_path, verbose=self.verbose)
        surveyor_stats = surveyor.run()
        report["agents"]["surveyor"] = surveyor_stats

        hubs = surveyor.get_hub_modules(5)
        dead = surveyor.get_dead_code_candidates()
        circulars = self.kg.find_circular_deps()

        self._log(f"  Top hubs: {[h['path'] for h in hubs[:3]]}")
        self._log(f"  Circular deps: {len(circulars)} found")
        self._log(f"  Dead code candidates: {len(dead)}")
        print()

        # ── Agent 2: Hydrologist ──────────────────────────────────────────
        self._log("Running Agent 2: Hydrologist (data lineage analysis)...")
        hydrologist = Hydrologist(self.kg, self.repo_path, verbose=self.verbose)
        hydro_stats = hydrologist.run()
        report["agents"]["hydrologist"] = hydro_stats

        sources = hydrologist.find_sources()
        sinks = hydrologist.find_sinks()
        self._log(f"  Data sources: {sources[:5]}")
        self._log(f"  Data sinks: {sinks[:5]}")
        print()

        # ── Serialize outputs ─────────────────────────────────────────────
        self._log(f"Saving knowledge graph to {self.output_dir}/")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.kg.save(self.output_dir)

        # Save analysis summary
        elapsed = time.time() - start
        report["elapsed_seconds"] = round(elapsed, 2)
        report["kg_stats"] = self.kg.stats()
        report["top_hubs"] = hubs
        report["dead_code_candidates"] = dead[:20]
        report["circular_deps"] = circulars
        report["data_sources"] = sources
        report["data_sinks"] = sinks

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
