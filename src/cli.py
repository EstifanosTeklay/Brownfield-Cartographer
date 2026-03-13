"""
CLI entry point for the Brownfield Cartographer.

Usage:
    python -m cli analyze <repo_path>
    python -m cli analyze <repo_path> --fast
    python -m cli query   <repo_path>
    python -m cli info    <repo_path>
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


def cmd_analyze(repo_path_str: str, use_llm: bool = True) -> None:
    """Run full analysis pipeline on a repository."""
    repo_path = _ensure_repo(repo_path_str)

    from src.orchestrator import Orchestrator

    if use_llm:
        print("Running full pipeline (Surveyor + Hydrologist + Semanticist + Archivist)...")
        print("Semanticist is forced to run (no incremental short-circuit).")
        orch = Orchestrator(repo_path, verbose=True, enable_semanticist=True, force_full=True)
    else:
        print("Running fast pipeline (Surveyor + Hydrologist + Archivist)...")
        print("Tip: omit --fast to run Semanticist and the full pipeline")
        orch = Orchestrator(repo_path, verbose=True, enable_semanticist=False, force_full=False)

    print()
    report = orch.run_analysis()

    print()
    print("=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    stats = report.get("kg_stats", {})
    print(f"  Modules:         {stats.get('modules', 'n/a')}")
    print(f"  Functions:       {stats.get('functions', 'n/a')}")
    print(f"  Datasets:        {stats.get('datasets', 'n/a')}")
    print(f"  Transformations: {stats.get('transformations', 'n/a')}")
    print(f"  Import edges:    {stats.get('module_edges', 'n/a')}")
    print(f"  Lineage edges:   {stats.get('lineage_edges', 'n/a')}")
    print(f"  Circular deps:   {stats.get('circular_deps', 'n/a')}")
    print(f"  Elapsed:         {report.get('elapsed_seconds', 'n/a')}s")
    print()
    print(f"Output saved to: {report.get('output_dir', 'n/a')}/")


def cmd_info(repo_path_str: str) -> None:
    """Show summary of a previously analyzed repository."""
    repo_path = _ensure_repo(repo_path_str)
    summary_path = repo_path / ".cartography" / "analysis_summary.json"

    if not summary_path.exists():
        print(f"No analysis found. Run: python -m cli analyze {repo_path_str}")
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
        print(f"  python -m cli analyze {repo_path_str}")
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
        print("Brownfield Cartographer")
        print()
        print("Usage:")
        print("  python -m cli analyze <repo_path>        # full pipeline (default)")
        print("  python -m cli analyze <repo_path> --fast # fast pipeline, no Semanticist")
        print("  python -m cli query   <repo_path>        # interactive AI query")
        print("  python -m cli info    <repo_path>        # show analysis summary")
        sys.exit(0)

    cmd = args[0]
    rest = args[1:]

    if cmd == "analyze":
        if not rest:
            print("Usage: python -m cli analyze <repo_path> [--fast]")
            sys.exit(1)

        flags = {x for x in rest if x.startswith("--")}
        positional = [x for x in rest if not x.startswith("--")]
        if not positional:
            print("Error: missing <repo_path>")
            print("Usage: python -m cli analyze <repo_path> [--fast]")
            sys.exit(1)

        unsupported_flags = flags - {"--fast", "--no-llm", "--llm"}
        if unsupported_flags:
            print(f"Error: unknown flags: {', '.join(sorted(unsupported_flags))}")
            print("Supported flags: --fast, --no-llm, --llm")
            sys.exit(1)

        use_llm = "--fast" not in flags and "--no-llm" not in flags
        if "--llm" in flags:
            use_llm = True

        repo_path = positional[0]
        cmd_analyze(repo_path, use_llm=use_llm)

    elif cmd == "query":
        if not rest:
            print("Usage: python -m cli query <repo_path>")
            sys.exit(1)
        cmd_query(rest[0])

    elif cmd == "info":
        if not rest:
            print("Usage: python -m cli info <repo_path>")
            sys.exit(1)
        cmd_info(rest[0])

    else:
        print(f"Unknown command: {cmd}")
        print("Commands: analyze, query, info")
        sys.exit(1)


if __name__ == "__main__":
    main()