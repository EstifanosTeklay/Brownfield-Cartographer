"""
CLI v2: entry point for the Brownfield Cartographer.
Adds query command that launches the Navigator interactive mode.

Usage:
  python -m src.cli_v2 analyze <repo_path>
  python -m src.cli_v2 analyze <repo_path> --llm
  python -m src.cli_v2 query   <repo_path>
  python -m src.cli_v2 info    <repo_path>
"""
from __future__ import annotations
import json
import sys
from pathlib import Path


def _ensure_repo(repo_path_str: str) -> Path:
    if repo_path_str.startswith("https://github.com/"):
        from src.repo_manager import clone_github_repo
        print(f"Detected GitHub URL. Cloning repository...")
        repo_path, _ = clone_github_repo(repo_path_str)
        return repo_path

    p = Path(repo_path_str).resolve()
    if not p.exists():
        print(f"Error: path does not exist: {p}")
        sys.exit(1)
    return p


def cmd_analyze(repo_path_str: str, use_llm: bool = False) -> None:
    """Run full analysis pipeline on a repository."""
    repo_path = _ensure_repo(repo_path_str)

    if use_llm:
        from src.orchestrator_v2 import Orchestrator
        print("Running full pipeline (Surveyor + Hydrologist + Semanticist + Archivist)...")
    else:
        from src.orchestrator import Orchestrator
        print("Running fast pipeline (Surveyor + Hydrologist + Archivist)...")
        print("Tip: use --llm flag to enable Semanticist for LLM-powered analysis")

    print()
    orch = Orchestrator(repo_path, verbose=True)
    report = orch.run_analysis()

    print()
    print("=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    stats = report["kg_stats"]
    print(f"  Modules:         {stats['modules']}")
    print(f"  Functions:       {stats['functions']}")
    print(f"  Datasets:        {stats['datasets']}")
    print(f"  Transformations: {stats['transformations']}")
    print(f"  Import edges:    {stats['module_edges']}")
    print(f"  Lineage edges:   {stats['lineage_edges']}")
    print(f"  Circular deps:   {stats['circular_deps']}")
    print(f"  Elapsed:         {report['elapsed_seconds']}s")
    print()
    print(f"Output saved to: {report['output_dir']}/")


def cmd_info(repo_path_str: str) -> None:
    """Show summary of a previously analyzed repository."""
    repo_path = _ensure_repo(repo_path_str)
    summary_path = repo_path / ".cartography" / "analysis_summary.json"

    if not summary_path.exists():
        print(f"No analysis found. Run: python -m src.cli_v2 analyze {repo_path_str}")
        sys.exit(1)

    data = json.loads(summary_path.read_text())
    stats = data.get("kg_stats", {})

    print(f"\nRepository: {data['repo_path']}")
    print(f"Analyzed in: {data.get('elapsed_seconds', '?')}s")
    print()
    print("Knowledge Graph:")
    for k, v in stats.items():
        print(f"  {k:25s}: {v}")
    print()

    hubs = data.get("top_hubs", [])
    if hubs:
        print("Top Hub Modules (by PageRank):")
        for h in hubs[:5]:
            print(f"  {h['path']:50s}  PageRank={h['pagerank']:.6f}")
    print()

    circulars = data.get("circular_deps", [])
    if circulars:
        print(f"Circular Dependencies ({len(circulars)}):")
        for cycle in circulars[:3]:
            print(f"  {' -> '.join(cycle)}")
    print()

    sources = data.get("data_sources", [])
    sinks = data.get("data_sinks", [])
    if sources:
        print(f"Data Sources: {sources[:10]}")
    if sinks:
        print(f"Data Sinks:   {sinks[:10]}")


def cmd_query(repo_path_str: str) -> None:
    """Launch Navigator interactive query mode."""
    repo_path = _ensure_repo(repo_path_str)

    # Check analysis exists
    cartography_dir = repo_path / ".cartography"
    if not cartography_dir.exists():
        print(f"No analysis found. Run first:")
        print(f"  python -m src.cli_v2 analyze {repo_path_str}")
        sys.exit(1)

    # Load existing knowledge graph
    from src.orchestrator import Orchestrator
    from src.agents.navigator import run_navigator

    print("Loading knowledge graph...")
    orch = Orchestrator.load_existing(repo_path)
    kg = orch.get_knowledge_graph()

    print(f"Loaded: {kg.stats()}")
    run_navigator(kg, repo_path)


def main():
    args = sys.argv[1:]

    if not args:
        print("Brownfield Cartographer v2")
        print()
        print("Usage:")
        print("  python -m src.cli_v2 analyze <repo_path>        # fast, no LLM")
        print("  python -m src.cli_v2 analyze <repo_path> --llm  # full, with LLM")
        print("  python -m src.cli_v2 query   <repo_path>        # interactive AI query")
        print("  python -m src.cli_v2 info    <repo_path>        # show analysis summary")
        sys.exit(0)

    cmd = args[0]
    rest = args[1:]

    if cmd == "analyze":
        if not rest:
            print("Usage: python -m src.cli_v2 analyze <repo_path> [--llm]")
            sys.exit(1)
        use_llm = "--llm" in rest
        repo_path = rest[0]
        cmd_analyze(repo_path, use_llm=use_llm)

    elif cmd == "query":
        if not rest:
            print("Usage: python -m src.cli_v2 query <repo_path>")
            sys.exit(1)
        cmd_query(rest[0])

    elif cmd == "info":
        if not rest:
            print("Usage: python -m src.cli_v2 info <repo_path>")
            sys.exit(1)
        cmd_info(rest[0])

    else:
        print(f"Unknown command: {cmd}")
        print("Commands: analyze, query, info")
        sys.exit(1)


if __name__ == "__main__":
    main()