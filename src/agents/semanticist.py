"""
The Semanticist Agent: LLM-Powered Purpose Analyst.

Responsibilities:
- Generate purpose statements for each module based on code (not docstrings)
- Detect documentation drift (docstring contradicts implementation)
- Cluster modules into business domains
- Answer the Five FDE Day-One Questions
- Track token usage and cost (ContextWindowBudget)
"""
from __future__ import annotations
import os
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import anthropic

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode, Language

load_dotenv()

# ── Model Config ──────────────────────────────────────────────────────────────

BULK_MODEL = "claude-haiku-4-5-20251001"      # cheap, for per-module analysis
SYNTHESIS_MODEL = "claude-sonnet-4-6"          # powerful, for Day-One questions

# Approximate cost per 1000 tokens (input/output)
MODEL_COSTS = {
    BULK_MODEL: {"input": 0.00025, "output": 0.00125},
    SYNTHESIS_MODEL: {"input": 0.003, "output": 0.015},
}


# ── Context Window Budget ─────────────────────────────────────────────────────

class ContextWindowBudget:
    """Tracks token usage and estimated cost across all LLM calls."""

    def __init__(self, max_budget_usd: float = 1.0):
        self.max_budget_usd = max_budget_usd
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost_usd = 0.0
        self.calls = 0

    def record(self, model: str, input_tokens: int, output_tokens: int) -> None:
        self.calls += 1
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        costs = MODEL_COSTS.get(model, {"input": 0.003, "output": 0.015})
        cost = (input_tokens / 1000 * costs["input"]) + \
               (output_tokens / 1000 * costs["output"])
        self.total_cost_usd += cost

    def over_budget(self) -> bool:
        return self.total_cost_usd >= self.max_budget_usd

    def summary(self) -> Dict[str, Any]:
        return {
            "calls": self.calls,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "estimated_cost_usd": round(self.total_cost_usd, 4),
        }


# ── Semanticist Agent ─────────────────────────────────────────────────────────

class Semanticist:
    """
    Agent 3: The Semanticist.
    Uses LLMs to generate semantic understanding of code.
    """

    def __init__(
        self,
        kg: KnowledgeGraph,
        repo_path: Path,
        verbose: bool = True,
        max_budget_usd: float = 1.0,
    ):
        self.kg = kg
        self.repo_path = repo_path
        self.verbose = verbose
        self.budget = ContextWindowBudget(max_budget_usd)
        self.client = anthropic.Anthropic(
            api_key=os.getenv("ANTHROPIC_API_KEY")
        )

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [semanticist] {msg}")

    def _call_llm(self, model: str, prompt: str, max_tokens: int = 500) -> str:
        """Make a single LLM call and record token usage."""
        if self.budget.over_budget():
            self._log("Budget exceeded — skipping LLM call")
            return ""
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}]
            )
            self.budget.record(
                model,
                response.usage.input_tokens,
                response.usage.output_tokens,
            )
            return response.content[0].text.strip()
        except Exception as e:
            self._log(f"LLM call failed: {e}")
            return ""

    @staticmethod
    def _language_name(language: Any) -> str:
        """Return a safe language string from enum-or-string values."""
        return getattr(language, "value", language) or "unknown"

    # ── Purpose Statements ────────────────────────────────────────────────────

    def generate_purpose_statement(self, module: ModuleNode) -> str:
        """
        Generate a 2-3 sentence purpose statement for a module.
        Based on actual code, not the docstring.
        """
        # Read the actual source code
        source = ""
        full_path = self.repo_path / module.path
        if full_path.exists():
            try:
                source = full_path.read_text(encoding="utf-8", errors="replace")
                # Truncate to keep costs low
                source = source[:3000]
            except OSError:
                pass

        if not source:
            return "Could not read source file."

        prompt = f"""You are analyzing a file in a data engineering codebase.

File path: {module.path}
    Language: {self._language_name(module.language)}

Source code:
```
{source}
```

Write a 2-3 sentence purpose statement that explains:
1. What this file DOES (its business function, not implementation details)
2. What role it plays in the data pipeline

Be specific and concise. Do not mention variable names or syntax.
Do not repeat the filename. Just write the purpose statement directly."""

        return self._call_llm(BULK_MODEL, prompt, max_tokens=200)

    def check_doc_drift(self, module: ModuleNode) -> Optional[str]:
        """
        Compare the docstring against the implementation.
        Returns a drift description if they contradict, None if aligned.
        """
        if not module.docstring:
            return None

        full_path = self.repo_path / module.path
        if not full_path.exists():
            return None

        try:
            source = full_path.read_text(encoding="utf-8", errors="replace")[:2000]
        except OSError:
            return None

        prompt = f"""Compare this docstring against the actual code implementation.

Docstring:
{module.docstring}

Code:
```
{source}
```

Does the docstring accurately describe what the code actually does?
Reply with one of:
- ALIGNED: (one sentence why)
- DRIFT: (one sentence describing the contradiction)

Start your reply with exactly ALIGNED or DRIFT."""

        result = self._call_llm(BULK_MODEL, prompt, max_tokens=100)
        if result.startswith("DRIFT"):
            return result
        return None

    # ── Domain Clustering ─────────────────────────────────────────────────────

    def cluster_into_domains(self) -> Dict[str, str]:
        """
        Assign each module to a business domain based on its purpose statement.
        Returns {module_path: domain_name}
        """
        # Build a summary of all modules with purpose statements
        modules_with_purpose = [
            (path, mod) for path, mod in self.kg.modules.items()
            if mod.purpose_statement
        ]

        if not modules_with_purpose:
            return {}

        # Build a compact listing for the LLM
        listing = "\n".join([
            f"- {path}: {mod.purpose_statement[:100]}"
            for path, mod in modules_with_purpose[:30]  # cap at 30
        ])

        prompt = f"""You are analyzing a data engineering codebase.
Here are the files and their purposes:

{listing}

Group these files into 4-6 business domains (e.g. ingestion, staging, transformation, serving, config, testing).

Reply with a JSON object where keys are file paths and values are domain names.
Use short domain names like: ingestion, staging, transformation, serving, config, testing, utilities.
Reply with ONLY the JSON object, no explanation."""

        result = self._call_llm(SYNTHESIS_MODEL, prompt, max_tokens=1000)

        # Parse JSON response
        try:
            # Strip markdown code fences if present
            clean = result.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            self._log("Failed to parse domain clustering response")
            return {}

    # ── Five FDE Day-One Questions ────────────────────────────────────────────

    def answer_day_one_questions(self) -> Dict[str, str]:
        """
        Answer the Five FDE Day-One Questions by synthesizing
        Surveyor + Hydrologist output with LLM reasoning.
        """
        # Build context from knowledge graph
        hubs = self.kg.get_critical_path_modules(5)
        sources = self.kg.find_lineage_sources()
        sinks = self.kg.find_lineage_sinks()
        circulars = self.kg.find_circular_deps()

        # Build module purpose index
        purpose_index = "\n".join([
            f"- {path}: {mod.purpose_statement or 'no purpose statement'}"
            for path, mod in list(self.kg.modules.items())[:20]
        ])

        # Build lineage summary
        lineage_summary = f"""
Data Sources (in-degree=0): {sources[:10]}
Data Sinks (out-degree=0): {sinks[:10]}
Total datasets: {len(self.kg.datasets)}
Total transformations: {len(self.kg.transformations)}
"""

        prompt = f"""You are an expert data engineer doing a Day-One assessment of a codebase.

ARCHITECTURAL CONTEXT:
Top hub modules (by PageRank): {[h[0] for h in hubs]}
Circular dependencies: {circulars}

MODULE PURPOSE INDEX:
{purpose_index}

DATA LINEAGE SUMMARY:
{lineage_summary}

Answer these five questions based on the evidence above.
For each answer cite specific file paths or dataset names as evidence.

Q1: What is the primary data ingestion path?
Q2: What are the 3-5 most critical output datasets/endpoints?
Q3: What is the blast radius if the most critical module fails?
Q4: Where is the business logic concentrated vs distributed?
Q5: What files are changing most frequently (high velocity = likely pain points)?

Format your response as JSON with keys: q1, q2, q3, q4, q5.
Each value should be 2-3 sentences with specific evidence.
Reply with ONLY the JSON object."""

        result = self._call_llm(SYNTHESIS_MODEL, prompt, max_tokens=1000)

        try:
            clean = result.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            self._log("Failed to parse Day-One answers")
            return {"error": result}

    # ── Main Run ──────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Run full semantic analysis pipeline."""
        start = time.time()
        self._log("Starting semantic analysis...")

        # Step 1: Generate purpose statements for all modules
        modules = list(self.kg.modules.values())
        self._log(f"Generating purpose statements for {len(modules)} modules...")

        for i, module in enumerate(modules):
            if self.budget.over_budget():
                self._log("Budget limit reached — stopping early")
                break

            purpose = self.generate_purpose_statement(module)
            module.purpose_statement = purpose
            if module.path in self.kg.module_graph:
                self.kg.module_graph.nodes[module.path]["purpose_statement"] = purpose

            # Check doc drift for Python files with docstrings
            if self._language_name(module.language) == Language.PYTHON.value and module.docstring:
                drift = self.check_doc_drift(module)
                if drift:
                    module.doc_drift_flag = True
                    if module.path in self.kg.module_graph:
                        self.kg.module_graph.nodes[module.path]["doc_drift_flag"] = True
                    self._log(f"Doc drift detected in {module.path}")

            if self.verbose and (i + 1) % 5 == 0:
                self._log(
                    f"  Progress: {i+1}/{len(modules)} "
                    f"(${self.budget.total_cost_usd:.4f} spent)"
                )

        # Step 2: Domain clustering
        self._log("Clustering modules into domains...")
        domain_map = self.cluster_into_domains()
        for path, domain in domain_map.items():
            if path in self.kg.modules:
                self.kg.modules[path].domain_cluster = domain
                if path in self.kg.module_graph:
                    self.kg.module_graph.nodes[path]["domain_cluster"] = domain

        # Step 3: Answer Day-One questions
        self._log("Answering Five FDE Day-One Questions...")
        day_one_answers = self.answer_day_one_questions()

        elapsed = time.time() - start
        budget_summary = self.budget.summary()
        self._log(
            f"Done in {elapsed:.1f}s — "
            f"{budget_summary['calls']} LLM calls, "
            f"${budget_summary['estimated_cost_usd']:.4f} spent"
        )

        return {
            "elapsed_seconds": round(elapsed, 2),
            "modules_analyzed": len(modules),
            "domain_map": domain_map,
            "day_one_answers": day_one_answers,
            "budget": budget_summary,
        }