"""
The Navigator Agent: LangGraph-powered query interface.

Four tools:
- find_implementation(concept): semantic search across module purpose statements
- trace_lineage(dataset, direction): upstream/downstream lineage traversal
- blast_radius(module_path): find all downstream dependents
- explain_module(path): LLM-generated explanation of a specific module
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Annotated, Any
from dotenv import load_dotenv

from langchain_anthropic import ChatAnthropic
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

from src.graph.knowledge_graph import KnowledgeGraph

load_dotenv()


def create_navigator(kg: KnowledgeGraph, repo_path: Path) -> Any:
    """
    Create a LangGraph ReAct agent with four codebase query tools.
    The agent has access to the knowledge graph and can answer
    architectural questions about the codebase.
    """

    # ── Tool 1: find_implementation ───────────────────────────────────────────

    @tool
    def find_implementation(concept: str) -> str:
        """
        Find where a concept or business logic is implemented in the codebase.
        Use this for questions like 'where is the revenue calculation?'
        or 'where is customer data processed?'
        Input: a natural language description of what you are looking for.
        """
        concept_lower = concept.lower()
        matches = []

        for path, mod in kg.modules.items():
            score = 0
            # Check purpose statement
            if mod.purpose_statement:
                purpose_lower = mod.purpose_statement.lower()
                # Count keyword matches
                for word in concept_lower.split():
                    if len(word) > 3 and word in purpose_lower:
                        score += 2
            # Check path name
            for word in concept_lower.split():
                if len(word) > 3 and word in path.lower():
                    score += 1
            # Check domain cluster
            if mod.domain_cluster:
                for word in concept_lower.split():
                    if len(word) > 3 and word in mod.domain_cluster.lower():
                        score += 1

            if score > 0:
                matches.append((score, path, mod))

        if not matches:
            return f"No modules found matching '{concept}'. Try different keywords."

        matches.sort(key=lambda x: x[0], reverse=True)
        top = matches[:5]

        lines = [f"Found {len(matches)} modules matching '{concept}'. Top results:"]
        for score, path, mod in top:
            purpose = mod.purpose_statement[:100] if mod.purpose_statement else "no purpose statement"
            domain = mod.domain_cluster or "unknown domain"
            lines.append(f"\n- `{path}` (domain: {domain}, relevance: {score})")
            lines.append(f"  Purpose: {purpose}")
            lines.append(f"  Evidence source: static_analysis + llm_inference")

        return "\n".join(lines)

    # ── Tool 2: trace_lineage ─────────────────────────────────────────────────

    @tool
    def trace_lineage(dataset: str, direction: str = "upstream") -> str:
        """
        Trace the data lineage of a dataset.
        direction: 'upstream' to find what produces this dataset,
                   'downstream' to find what this dataset feeds into.
        Input: dataset name and direction (upstream or downstream).
        Example: trace_lineage('customers', 'upstream')
        """
        direction = direction.lower().strip()

        if dataset not in kg.lineage_graph:
            # Try case-insensitive match
            all_nodes = list(kg.lineage_graph.nodes())
            matches = [n for n in all_nodes if dataset.lower() in n.lower()]
            if matches:
                dataset = matches[0]
            else:
                return (
                    f"Dataset '{dataset}' not found in lineage graph. "
                    f"Available datasets: {sorted(kg.datasets.keys())[:10]}"
                )

        if direction == "upstream":
            nodes = kg.upstream_of(dataset)
            if not nodes:
                return f"'{dataset}' has no upstream dependencies — it is a source node."

            lines = [f"Upstream lineage of '{dataset}' ({len(nodes)} nodes):"]
            datasets = [(n, t) for n, t in nodes if t == "dataset"]
            transformations = [(n, t) for n, t in nodes if t == "transformation"]

            lines.append(f"\nSource datasets ({len(datasets)}):")
            for n, _ in sorted(datasets):
                lines.append(f"  - `{n}`")

            lines.append(f"\nTransformations ({len(transformations)}):")
            for n, _ in sorted(transformations):
                txn = kg.transformations.get(n)
                if txn:
                    lines.append(f"  - `{txn.source_file}` (line {txn.line_range[0]})")
                else:
                    lines.append(f"  - `{n}`")

            lines.append("\nEvidence source: lineage_graph (static_analysis)")
            return "\n".join(lines)

        elif direction == "downstream":
            affected = kg.blast_radius(dataset, graph="lineage")
            if not affected:
                return f"'{dataset}' has no downstream dependents — it is a sink node."

            lines = [f"Downstream dependents of '{dataset}' ({len(affected)} nodes):"]
            for n in sorted(affected):
                node_type = kg.lineage_graph.nodes[n].get("node_type", "unknown")
                lines.append(f"  - [{node_type}] `{n}`")
            lines.append("\nEvidence source: lineage_graph (static_analysis)")
            return "\n".join(lines)

        else:
            return "direction must be 'upstream' or 'downstream'"

    # ── Tool 3: blast_radius ──────────────────────────────────────────────────

    @tool
    def blast_radius(module_path: str) -> str:
        """
        Find everything that would break if this module changed its interface.
        Works on both module paths and dataset names.
        Input: a file path like 'models/staging/stg_orders.sql'
        or a dataset name like 'stg_orders'.
        """
        # Try module graph first
        module_affected = kg.blast_radius(module_path, graph="module")

        # Try lineage graph
        lineage_affected = kg.blast_radius(module_path, graph="lineage")

        if not module_affected and not lineage_affected:
            # Try partial match
            all_modules = list(kg.modules.keys())
            matches = [m for m in all_modules if module_path.lower() in m.lower()]
            if matches:
                module_path = matches[0]
                module_affected = kg.blast_radius(module_path, graph="module")
                lineage_affected = kg.blast_radius(module_path, graph="lineage")

        if not module_affected and not lineage_affected:
            return (
                f"'{module_path}' not found or has no dependents. "
                f"Try using the exact file path."
            )

        lines = [f"Blast radius of `{module_path}`:"]

        if module_affected:
            lines.append(f"\nModule dependents ({len(module_affected)} files would break):")
            for m in sorted(module_affected):
                lines.append(f"  - `{m}`")

        if lineage_affected:
            lines.append(f"\nLineage dependents ({len(lineage_affected)} datasets/transforms affected):")
            for m in sorted(lineage_affected):
                node_type = kg.lineage_graph.nodes[m].get("node_type", "unknown") \
                    if m in kg.lineage_graph else "unknown"
                lines.append(f"  - [{node_type}] `{m}`")

        lines.append("\nEvidence source: module_graph + lineage_graph (static_analysis)")
        return "\n".join(lines)

    # ── Tool 4: explain_module ────────────────────────────────────────────────

    @tool
    def explain_module(path: str) -> str:
        """
        Get a detailed explanation of what a specific module does.
        Input: the file path of the module.
        Example: explain_module('models/marts/customers.sql')
        """
        # Try exact match first
        mod = kg.modules.get(path)

        # Try partial match
        if not mod:
            matches = [p for p in kg.modules if path.lower() in p.lower()]
            if matches:
                path = matches[0]
                mod = kg.modules[path]

        if not mod:
            return f"Module '{path}' not found. Available modules: {list(kg.modules.keys())[:10]}"

        lines = [f"## Module: `{path}`"]
        lines.append(f"**Language:** {mod.language.value}")
        lines.append(f"**Domain:** {mod.domain_cluster or 'unknown'}")
        lines.append(f"**Lines of code:** {mod.loc}")
        lines.append(f"**Complexity score:** {mod.complexity_score}")
        lines.append(f"**Change velocity (30d):** {mod.change_velocity_30d} commits")
        lines.append(f"**Dead code candidate:** {mod.is_dead_code_candidate}")

        if mod.purpose_statement:
            lines.append(f"\n**Purpose:**\n{mod.purpose_statement}")
            lines.append("\nEvidence source: llm_inference (based on code analysis)")
        else:
            lines.append("\n**Purpose:** Not analyzed yet.")

        if mod.doc_drift_flag:
            lines.append("\n⚠️ **Documentation Drift Detected:** docstring contradicts implementation")

        if mod.imports:
            lines.append(f"\n**Imports:** {', '.join(mod.imports[:10])}")

        if mod.exports:
            lines.append(f"\n**Exports:** {', '.join(mod.exports[:10])}")

        # Show lineage connections
        if path in kg.lineage_graph:
            upstream = kg.upstream_of(path)
            downstream = kg.blast_radius(path, graph="lineage")
            if upstream:
                lines.append(f"\n**Upstream datasets:** {[n for n, t in upstream if t == 'dataset'][:5]}")
            if downstream:
                lines.append(f"\n**Downstream dependents:** {sorted(downstream)[:5]}")

        lines.append("\nEvidence source: static_analysis + llm_inference")
        return "\n".join(lines)

    # ── Build LangGraph Agent ─────────────────────────────────────────────────

    model = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.getenv("ANTHROPIC_API_KEY"),
    )

    tools = [find_implementation, trace_lineage, blast_radius, explain_module]
    agent = create_react_agent(model, tools)

    return agent


def run_navigator(kg: KnowledgeGraph, repo_path: Path) -> None:
    """
    Run the Navigator in interactive mode.
    Loads the knowledge graph and starts a query loop.
    """
    print("\nBrownfield Cartographer — Navigator (AI Query Mode)")
    print(f"Repository: {repo_path}")
    print(f"Loaded: {kg.stats()}")
    print()
    print("Ask any question about the codebase architecture.")
    print("Examples:")
    print("  - Where is the customer data processed?")
    print("  - What upstream sources feed the orders table?")
    print("  - What breaks if stg_orders changes?")
    print("  - Explain what models/marts/customers.sql does")
    print("  - Type 'quit' to exit")
    print()

    agent = create_navigator(kg, repo_path)

    while True:
        try:
            question = input("navigator> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            break

        print()
        try:
            result = agent.invoke({
                "messages": [HumanMessage(content=question)]
            })
            # Extract the final answer
            final_message = result["messages"][-1]
            print(final_message.content)
        except Exception as e:
            print(f"Error: {e}")
        print()