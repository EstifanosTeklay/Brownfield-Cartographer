"""
CLI entry point for the Brownfield Cartographer.
Usage:
  cartographer analyze <repo_path>
  cartographer query <repo_path>
  cartographer info <repo_path>
"""
from __future__ import annotations
import json
import sys
from pathlib import Path


def _ensure_repo(repo_path_str: str) -> Path:
    p = Path(repo_path_str).resolve()
    if not p.exists():
        print(f"Error: path does not exist: {p}")
        sys.exit(1)
    return p


def cmd_analyze(repo_path_str: str, output_dir: str = None, verbose: bool = True) -> None:
    """Run full analysis pipeline on a repository."""
    repo_path = _ensure_repo(repo_path_str)
    out = Path(output_dir) if output_dir else None

    from src.orchestrator import Orchestrator
    orch = Orchestrator(repo_path, output_dir=out, verbose=verbose)
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
        print(f"No analysis found. Run: cartographer analyze {repo_path_str}")
        sys.exit(1)

    data = json.loads(summary_path.read_text())
    stats = data.get("kg_stats", {})

    print(f"\nRepository: {data['repo_path']}")
    print(f"Analyzed:   {data.get('elapsed_seconds', '?')}s")
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
    """Interactive query mode for a previously analyzed repository."""
    repo_path = _ensure_repo(repo_path_str)

    from src.orchestrator import Orchestrator
    orch = Orchestrator.load_existing(repo_path)
    kg = orch.get_knowledge_graph()

    print(f"\nBrownfield Cartographer — Query Mode")
    print(f"Repository: {repo_path}")
    print(f"Loaded: {kg.stats()}")
    print()
    print("Commands:")
    print("  blast <module_path>   — Show blast radius of a module")
    print("  upstream <dataset>    — Show upstream sources of a dataset")
    print("  hubs                  — Show top hub modules by PageRank")
    print("  sources               — Show data source nodes")
    print("  sinks                 — Show data sink nodes")
    print("  circulars             — Show circular dependencies")
    print("  modules               — List all modules")
    print("  quit                  — Exit")
    print()

    while True:
        try:
            line = input("cartographer> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        parts = line.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd in ("quit", "exit", "q"):
            break

        elif cmd == "blast":
            if not arg:
                print("Usage: blast <module_or_dataset_path>")
                continue
            affected = kg.blast_radius(arg, graph="module")
            if not affected:
                affected = kg.blast_radius(arg, graph="lineage")
                label = "lineage"
            else:
                label = "module"
            print(f"Blast radius of '{arg}' ({label} graph): {len(affected)} nodes affected")
            for n in sorted(affected)[:20]:
                print(f"  {n}")

        elif cmd == "upstream":
            if not arg:
                print("Usage: upstream <dataset_name>")
                continue
            upstream = kg.upstream_of(arg)
            print(f"Upstream of '{arg}': {len(upstream)} nodes")
            for node_id, node_type in upstream:
                print(f"  [{node_type}] {node_id}")

        elif cmd == "hubs":
            hubs = kg.get_critical_path_modules(10)
            print("Top Hub Modules (PageRank):")
            for path, score in hubs:
                in_deg = kg.module_graph.in_degree(path)
                print(f"  {score:.6f}  in={in_deg:2d}  {path}")

        elif cmd == "sources":
            sources = kg.find_lineage_sources()
            print(f"Data Sources ({len(sources)}):")
            for s in sorted(sources):
                print(f"  {s}")

        elif cmd == "sinks":
            sinks = kg.find_lineage_sinks()
            print(f"Data Sinks ({len(sinks)}):")
            for s in sorted(sinks):
                print(f"  {s}")

        elif cmd == "circulars":
            circulars = kg.find_circular_deps()
            if circulars:
                print(f"Circular Dependencies ({len(circulars)}):")
                for cycle in circulars:
                    print(f"  {' <-> '.join(cycle)}")
            else:
                print("No circular dependencies found.")

        elif cmd == "modules":
            print(f"All Modules ({len(kg.modules)}):")
            for path in sorted(kg.modules.keys()):
                mod = kg.modules[path]
                print(f"  [{mod.language.value:6s}] {path}")

        else:
            print(f"Unknown command: {cmd}")


def main():
    args = sys.argv[1:]

    if not args:
        print("Brownfield Cartographer")
        print()
        print("Usage:")
        print("  python -m src.cli analyze <repo_path>")
        print("  python -m src.cli query   <repo_path>")
        print("  python -m src.cli info    <repo_path>")
        sys.exit(0)

    cmd = args[0]
    rest = args[1:]

    if cmd == "analyze":
        if not rest:
            print("Usage: cartographer analyze <repo_path> [--output-dir <dir>]")
            sys.exit(1)
        output_dir = None
        if "--output-dir" in rest:
            idx = rest.index("--output-dir")
            output_dir = rest[idx + 1] if idx + 1 < len(rest) else None
        cmd_analyze(rest[0], output_dir=output_dir)

    elif cmd == "query":
        if not rest:
            print("Usage: cartographer query <repo_path>")
            sys.exit(1)
        cmd_query(rest[0])

    elif cmd == "info":
        if not rest:
            print("Usage: cartographer info <repo_path>")
            sys.exit(1)
        cmd_info(rest[0])

    else:
        print(f"Unknown command: {cmd}")
        print("Commands: analyze, query, info")
        sys.exit(1)


if __name__ == "__main__":
    main()
