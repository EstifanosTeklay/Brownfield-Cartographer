"""
Flask web app for the Brownfield Cartographer UI.
Supports:
- Landing page with GitHub URL input and zip upload
- Full analysis pipeline triggered from browser
- Interactive dashboard with D3 lineage graph
- Live Claude API chat
"""
from __future__ import annotations
import os
import sys
import json
import socket
import threading
import networkx as nx
from pathlib import Path
from flask import (
    Flask, render_template, jsonify, request,
    redirect, url_for, session
)
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage
from src.agents.navigator import create_navigator

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent
TEMPLATE_DIR = PROJECT_ROOT / "src" / "templates"
CARTOGRAPHY_ROOT = PROJECT_ROOT / "cartography"
LAST_REPO_STATE = CARTOGRAPHY_ROOT / ".last_loaded_repo.json"

app = Flask(__name__, template_folder=str(TEMPLATE_DIR))
app.secret_key = os.urandom(24)


def get_cartography_dir_for_repo(repo_path: Path) -> Path:
    """
    Central location for analysis artifacts for a given repo.
    Matches orchestrator default:
      <project_root>/cartography/<repo_name>/
    """
    return CARTOGRAPHY_ROOT / repo_path.name


def infer_module_visual_group(path: str, language: str, domain: str) -> str:
    """Return a frontend-friendly visual grouping for module graph coloring."""
    if domain and domain != "unknown":
        return domain

    norm = path.replace("\\", "/").lower()
    language = (language or "unknown").lower()

    if language == "sql":
        if "/staging/" in norm or "/stg_" in norm or norm.endswith("stg.sql"):
            return "sql_staging"
        if "/marts/" in norm or "/mart/" in norm:
            return "sql_mart"
        if "/macros/" in norm:
            return "sql_macro"
        return "lang_sql"

    if language == "yaml":
        name = norm.split("/")[-1]
        if "source" in name:
            return "yaml_source"
        if name == "dbt_project.yml":
            return "yaml_project"
        return "yaml_model"

    return f"lang_{language}"


def save_last_loaded_repo(repo_path: Path) -> None:
    """Persist last loaded repo so app can restore dashboard on next startup."""
    try:
        CARTOGRAPHY_ROOT.mkdir(parents=True, exist_ok=True)
        LAST_REPO_STATE.write_text(
            json.dumps(
                {
                    "repo_path": str(repo_path),
                    "repo_name": repo_path.name,
                },
                indent=2,
            )
        )
    except OSError:
        pass


def get_last_loaded_repo_path() -> Path | None:
    """Return last loaded repo path from persisted state if available."""
    if not LAST_REPO_STATE.exists():
        return None
    try:
        data = json.loads(LAST_REPO_STATE.read_text())
        repo_path = data.get("repo_path")
        if repo_path:
            return Path(repo_path)
    except (OSError, json.JSONDecodeError):
        return None
    return None


def get_latest_analyzed_repo_path() -> Path | None:
    """Fallback: pick most recently updated analyzed repo in cartography/."""
    if not CARTOGRAPHY_ROOT.exists():
        return None

    latest_dir = None
    latest_mtime = -1.0
    for child in CARTOGRAPHY_ROOT.iterdir():
        if not child.is_dir():
            continue
        summary = child / "analysis_summary.json"
        module_graph = child / "module_graph.json"
        if not summary.exists() or not module_graph.exists():
            continue
        mtime = summary.stat().st_mtime
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_dir = child

    if latest_dir is None:
        return None

    # Path only needs correct .name for artifact lookup via get_cartography_dir_for_repo
    return Path(latest_dir.name)


def has_existing_analysis(repo_path: Path) -> bool:
    """Return True if analysis artifacts already exist for this repo."""
    analysis_dir = get_cartography_dir_for_repo(repo_path)
    return (
        (analysis_dir / "module_graph.json").exists()
        and (analysis_dir / "analysis_summary.json").exists()
    )


def resolve_existing_repo_for_github_url(url: str) -> Path | None:
    """
    Map a GitHub URL to an existing analyzed repo artifact directory if present.
    Prefers local/cartography names (e.g. jaffle-shop) before temp clone names.
    """
    try:
        normalized_url = url.strip().rstrip("/")
        if normalized_url.endswith(".git"):
            normalized_url = normalized_url[:-4]
        parts = normalized_url.split("/")
        if len(parts) < 5 or parts[2] != "github.com":
            return None
        owner = parts[-2]
        repo = parts[-1]
    except Exception:
        return None

    candidates = [
        repo,
        repo.replace("_", "-"),
        repo.replace("-", "_"),
        f"{owner}__{repo}",
        f"{owner}__{repo.replace('_', '-')}",
        f"{owner}__{repo.replace('-', '_')}",
    ]

    seen = set()
    for name in candidates:
        if not name or name in seen:
            continue
        seen.add(name)
        p = Path(name)
        if has_existing_analysis(p):
            return p

    return None


def try_restore_previous_repo() -> bool:
    """Try restoring the last loaded repo only (deterministic startup behavior)."""
    last_repo = get_last_loaded_repo_path()
    if last_repo is None:
        return False

    load_knowledge_graph(last_repo)
    return KG is not None


# ── Global State ──────────────────────────────────────────────────────────────
KG = None
REPO_PATH = None
ANALYSIS_SUMMARY = None
ANALYSIS_STATUS = {
    "running": False,
    "step": "",
    "progress": 0,
    "error": None,
    "done": False,
}


def load_knowledge_graph(repo_path: Path):
    global KG, REPO_PATH, ANALYSIS_SUMMARY
    REPO_PATH = repo_path

    analysis_dir = get_cartography_dir_for_repo(repo_path)
    module_graph_path = analysis_dir / "module_graph.json"
    if not module_graph_path.exists():
        KG = None
        ANALYSIS_SUMMARY = None
        app.config["CODEBASE_MD"] = ""
        print(
            f"No existing analysis artifacts found for {repo_path.name} at {analysis_dir}. "
            f"Run `python -m src.cli analyze {repo_path}` first."
        )
        return

    from src.orchestrator import Orchestrator
    orch = Orchestrator.load_existing(repo_path)
    KG = orch.get_knowledge_graph()

    summary_path = analysis_dir / "analysis_summary.json"
    if summary_path.exists():
        ANALYSIS_SUMMARY = json.loads(summary_path.read_text())

    codebase_md_path = analysis_dir / "CODEBASE.md"
    if codebase_md_path.exists():
        app.config["CODEBASE_MD"] = codebase_md_path.read_text()
    else:
        app.config["CODEBASE_MD"] = ""

    print(f"Loaded knowledge graph: {KG.stats()}")
    save_last_loaded_repo(repo_path)


def run_analysis_pipeline(repo_path: Path, use_llm: bool = True):
    """Run analysis in a background thread and update ANALYSIS_STATUS."""
    global ANALYSIS_STATUS, KG, REPO_PATH, ANALYSIS_SUMMARY

    ANALYSIS_STATUS = {
        "running": True,
        "step": "Starting analysis...",
        "progress": 5,
        "error": None,
        "done": False,
    }

    try:
        ANALYSIS_STATUS["step"] = "Running Surveyor (static analysis)..."
        ANALYSIS_STATUS["progress"] = 20

        from src.orchestrator import Orchestrator

        orch = Orchestrator(
            repo_path,
            verbose=True,
            enable_semanticist=use_llm,
            force_full=use_llm,
        )

        ANALYSIS_STATUS["step"] = "Running Hydrologist (data lineage)..."
        ANALYSIS_STATUS["progress"] = 50

        if use_llm:
            ANALYSIS_STATUS["step"] = "Running Semanticist (LLM analysis)..."
            ANALYSIS_STATUS["progress"] = 70

        report = orch.run_analysis()

        ANALYSIS_STATUS["step"] = "Finalizing..."
        ANALYSIS_STATUS["progress"] = 90

        # Load the results into global state
        load_knowledge_graph(repo_path)

        ANALYSIS_STATUS = {
            "running": False,
            "step": "Analysis complete",
            "progress": 100,
            "error": None,
            "done": True,
        }

    except Exception as e:
        ANALYSIS_STATUS = {
            "running": False,
            "step": "Error",
            "progress": 0,
            "error": str(e),
            "done": False,
        }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Landing page — show repo input or redirect to dashboard."""
    if KG is not None:
        return render_template("index.html")
    return render_template("landing.html")


@app.route("/dashboard")
def dashboard():
    if KG is None:
        return redirect(url_for("index"))
    return render_template("index.html")


@app.route("/api/clone", methods=["POST"])
def api_clone():
    """Clone a GitHub repo and start analysis."""
    data = request.get_json()
    url = data.get("url", "").strip()
    use_llm = data.get("use_llm", True)

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    try:
        # Prefer existing analyzed artifacts for this GitHub URL.
        existing_repo = resolve_existing_repo_for_github_url(url)
        if existing_repo is not None:
            load_knowledge_graph(existing_repo)
            ANALYSIS_STATUS.update({
                "running": False,
                "step": "Loaded existing analysis",
                "progress": 100,
                "error": None,
                "done": True,
            })
            return jsonify({
                "status": "loaded_existing",
                "reused": True,
                "repo_name": existing_repo.name,
                "repo_path": str(existing_repo),
            })

        from src.repo_manager import clone_github_repo
        repo_path, repo_name = clone_github_repo(url)

        # Reuse previous artifacts if already analyzed
        if has_existing_analysis(repo_path):
            load_knowledge_graph(repo_path)
            ANALYSIS_STATUS.update({
                "running": False,
                "step": "Loaded existing analysis",
                "progress": 100,
                "error": None,
                "done": True,
            })
            return jsonify({
                "status": "loaded_existing",
                "reused": True,
                "repo_name": repo_name,
                "repo_path": str(repo_path),
            })

        # Start analysis in background thread
        thread = threading.Thread(
            target=run_analysis_pipeline,
            args=(repo_path, use_llm),
            daemon=True,
        )
        thread.start()

        return jsonify({
            "status": "started",
            "repo_name": repo_name,
            "repo_path": str(repo_path),
        })
    except (ValueError, RuntimeError) as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Upload a zip file and start analysis."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f.filename.endswith(".zip"):
        return jsonify({"error": "Only .zip files are supported"}), 400

    use_llm = request.form.get("use_llm", "true").lower() == "true"

    try:
        from src.repo_manager import extract_zip_repo
        zip_bytes = f.read()
        repo_path, repo_name = extract_zip_repo(zip_bytes, f.filename)

        # Reuse previous artifacts if already analyzed
        if has_existing_analysis(repo_path):
            load_knowledge_graph(repo_path)
            ANALYSIS_STATUS.update({
                "running": False,
                "step": "Loaded existing analysis",
                "progress": 100,
                "error": None,
                "done": True,
            })
            return jsonify({
                "status": "loaded_existing",
                "reused": True,
                "repo_name": repo_name,
                "repo_path": str(repo_path),
            })

        thread = threading.Thread(
            target=run_analysis_pipeline,
            args=(repo_path, use_llm),
            daemon=True,
        )
        thread.start()

        return jsonify({
            "status": "started",
            "repo_name": repo_name,
            "repo_path": str(repo_path),
        })
    except (ValueError, RuntimeError) as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/analysis_status")
def api_analysis_status():
    """Poll this endpoint to track analysis progress."""
    return jsonify(ANALYSIS_STATUS)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    """Reset state so user can analyze a new repo."""
    global KG, REPO_PATH, ANALYSIS_SUMMARY, ANALYSIS_STATUS
    KG = None
    REPO_PATH = None
    ANALYSIS_SUMMARY = None
    ANALYSIS_STATUS = {
        "running": False,
        "step": "",
        "progress": 0,
        "error": None,
        "done": False,
    }
    return jsonify({"status": "reset"})


# ── Existing API endpoints (unchanged) ───────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    # Graph-backed counts are the most reliable source for UI stats, especially
    # when loading older artifacts where dict reconstruction may be partial.
    module_nodes = [
        n for n, attrs in KG.module_graph.nodes(data=True)
        if attrs.get("node_type") == "module"
    ]
    if not module_nodes:
        module_nodes = list(KG.module_graph.nodes())

    dataset_nodes = [
        n for n, attrs in KG.lineage_graph.nodes(data=True)
        if attrs.get("node_type") == "dataset"
    ]
    transformation_nodes = [
        n for n, attrs in KG.lineage_graph.nodes(data=True)
        if attrs.get("node_type") == "transformation"
        and attrs.get("transformation_type") != "config"
    ]

    stats = {
        "modules": len(module_nodes),
        "datasets": len(dataset_nodes),
        "transformations": len(transformation_nodes),
        "functions": len(KG.function_index),
        "module_edges": KG.module_graph.number_of_edges(),
        "lineage_edges": KG.lineage_graph.number_of_edges(),
        "circular_deps": len(KG.find_circular_deps()),
    }

    stats["repo_path"] = str(REPO_PATH)
    stats["repo_name"] = REPO_PATH.name if REPO_PATH else ""
    if ANALYSIS_SUMMARY:
        stats["elapsed_seconds"] = ANALYSIS_SUMMARY.get("elapsed_seconds")
    return jsonify(stats)


@app.route("/api/graph")
def api_graph():
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    nodes = []
    edges = []
    node_ids = set()

    # Precompute structural roles for easier FDE interpretation
    source_datasets = set(KG.find_lineage_sources())
    sink_datasets = set(KG.find_lineage_sinks())

    # Build nodes directly from lineage graph (robust after graph reload)
    for raw_id, attrs in KG.lineage_graph.nodes(data=True):
        node_id = str(raw_id).replace("\\", "/")
        node_type = attrs.get("node_type", "dataset")

        # Keep lineage graph focused on true dataflow. YAML/config governance
        # relationships stay in the module graph where they are easier to read.
        if node_type == "transformation":
            source_file = str(attrs.get("source_file", ""))
            transformation_type = str(attrs.get("transformation_type", "unknown"))
            if transformation_type == "config" or source_file.endswith((".yml", ".yaml")):
                continue

        node_ids.add(node_id)
        in_deg = KG.lineage_graph.in_degree(raw_id)
        out_deg = KG.lineage_graph.out_degree(raw_id)

        if node_type == "transformation":
            source_file = str(attrs.get("source_file", ""))
            label = source_file.split("/")[-1].split("\\")[-1] if source_file else node_id
            nodes.append({
                "id": node_id,
                "type": "transformation",
                "source_file": source_file,
                "label": label,
                "group": "transformation",
                "transformation_type": str(attrs.get("transformation_type", "unknown")),
                "in_degree": in_deg,
                "out_degree": out_deg,
            })
        else:
            label = str(attrs.get("name", node_id)).replace("\\", "/")
            nodes.append({
                "id": node_id,
                "label": label,
                "type": "dataset",
                "storage_type": str(attrs.get("storage_type", "unknown")),
                "is_source_of_truth": bool(attrs.get("is_source_of_truth", False)),
                "group": _get_node_group(label),
                "is_source": str(raw_id) in source_datasets,
                "is_sink": str(raw_id) in sink_datasets,
                "in_degree": in_deg,
                "out_degree": out_deg,
            })

    # Edges
    for u, v, data in KG.lineage_graph.edges(data=True):
        source = str(u).replace("\\", "/")
        target = str(v).replace("\\", "/")
        if source in node_ids and target in node_ids:
            edges.append({
                "source": source,
                "target": target,
                "edge_type": data.get("edge_type", ""),
            })

    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/api/module_graph")
def api_module_graph():
    """Return the module dependency graph (import/call/configures) for visualization."""
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    nodes = []
    edges = []

    # PageRank and degree are critical for understanding architectural hubs
    pr_scores = KG.pagerank_modules()
    executable_languages = {"python", "javascript", "typescript"}

    for path, mod in KG.modules.items():
        node_id = str(path).replace("\\", "/")
        language = getattr(mod.language, "value", mod.language)
        if not isinstance(language, str):
            language = str(language)
        language = language.lower()
        domain = (mod.domain_cluster or "unknown").strip().lower() or "unknown"
        visual_group = infer_module_visual_group(node_id, language, domain)

        is_dead_code = bool(getattr(mod, "is_dead_code_candidate", False))
        if language not in executable_languages:
            is_dead_code = False

        in_deg = KG.module_graph.in_degree(path) if path in KG.module_graph else 0
        out_deg = KG.module_graph.out_degree(path) if path in KG.module_graph else 0

        nodes.append({
            "id": node_id,
            "label": node_id.split("/")[-1].split("\\")[-1],
            "type": "module",
            "language": language,
            "domain": domain,
            "group": visual_group,
            "pagerank": round(pr_scores.get(path, 0.0), 6),
            "change_velocity_30d": getattr(mod, "change_velocity_30d", 0),
            "last_modified": getattr(mod, "last_modified", None),
            "last_author": getattr(mod, "last_author", None),
            "last_author_email": getattr(mod, "last_author_email", None),
            "likely_contacts": getattr(mod, "likely_contacts", []),
            "is_dead_code": is_dead_code,
            "in_degree": in_deg,
            "out_degree": out_deg,
        })

    for u, v, data in KG.module_graph.edges(data=True):
        if data.get("edge_type") == "governs":
            continue
        source = str(u).replace("\\", "/")
        target = str(v).replace("\\", "/")
        edges.append({
            "source": source,
            "target": target,
            "edge_type": data.get("edge_type", ""),
        })

    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/api/modules")
def api_modules():
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    modules = []
    scores = KG.pagerank_modules()
    executable_languages = {"python", "javascript", "typescript"}

    for path, mod in KG.modules.items():
        language = getattr(mod.language, "value", mod.language)
        if not isinstance(language, str):
            language = str(language)
        language = language.lower()

        is_dead_code = bool(getattr(mod, "is_dead_code_candidate", False))
        if language not in executable_languages:
            is_dead_code = False

        modules.append({
            "path": path,
            "language": language,
            "domain": mod.domain_cluster or "unknown",
            "purpose": mod.purpose_statement or "",
            "loc": mod.loc,
            "complexity": mod.complexity_score,
            "velocity": mod.change_velocity_30d,
            "last_modified": getattr(mod, "last_modified", None),
            "last_author": getattr(mod, "last_author", None),
            "last_author_email": getattr(mod, "last_author_email", None),
            "likely_contacts": getattr(mod, "likely_contacts", []),
            "is_dead_code": is_dead_code,
            "pagerank": round(scores.get(path, 0), 6),
            "in_degree": KG.module_graph.in_degree(path),
        })

    modules.sort(key=lambda x: x["pagerank"], reverse=True)
    return jsonify(modules)


@app.route("/api/blast_radius/<path:node_id>")
def api_blast_radius(node_id):
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    module_downstream = KG.blast_radius(node_id, graph="module")
    lineage_downstream = KG.blast_radius(node_id, graph="lineage")

    module_upstream = []
    lineage_upstream = []
    if node_id in KG.module_graph:
        module_upstream = list(nx.ancestors(KG.module_graph, node_id))
    if node_id in KG.lineage_graph:
        lineage_upstream = list(nx.ancestors(KG.lineage_graph, node_id))

    module_related = sorted(set(module_downstream + module_upstream))
    lineage_related = sorted(set(lineage_downstream + lineage_upstream))

    return jsonify({
        "node": node_id,
        "module_affected": module_downstream,
        "lineage_affected": lineage_downstream,
        "module_upstream": module_upstream,
        "lineage_upstream": lineage_upstream,
        "module_related": module_related,
        "lineage_related": lineage_related,
        "total": len(set(module_related + lineage_related)),
    })


@app.route("/api/upstream/<path:dataset_name>")
def api_upstream(dataset_name):
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    upstream = KG.upstream_of(dataset_name)
    return jsonify({
        "dataset": dataset_name,
        "upstream": [{"id": n, "type": t} for n, t in upstream],
    })


@app.route("/api/day_one_answers")
def api_day_one_answers():
    if not ANALYSIS_SUMMARY:
        return jsonify({"error": "No analysis summary found"}), 404

    semanticist = ANALYSIS_SUMMARY.get("agents", {}).get("semanticist", {})
    answers = semanticist.get("day_one_answers", {})
    return jsonify(answers)


@app.route("/api/newcomer_tasks")
def api_newcomer_tasks():
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    pr_scores = KG.pagerank_modules()
    ranked_modules = sorted(
        KG.modules.items(),
        key=lambda item: pr_scores.get(item[0], 0.0),
        reverse=True,
    )

    hot_files = sorted(
        KG.modules.items(),
        key=lambda item: getattr(item[1], "change_velocity_30d", 0),
        reverse=True,
    )

    top_hub = ranked_modules[0][0] if ranked_modules else ""
    top_hub_neighbors = KG.blast_radius(top_hub, graph="module")[:5] if top_hub else []

    low_risk_docs = [
        path for path, mod in KG.modules.items()
        if str(getattr(mod, "language", "")).lower().endswith("yaml") or path.endswith((".yml", ".yaml"))
    ][:4]

    staging_candidates = [
        path for path, _ in ranked_modules
        if "/staging/" in path.replace("\\", "/") and path.endswith(".sql")
    ]
    mart_candidates = [
        path for path, _ in ranked_modules
        if "/marts/" in path.replace("\\", "/") and path.endswith(".sql")
    ]

    source_datasets = KG.find_lineage_sources()[:5]
    sink_datasets = KG.find_lineage_sinks()[:5]

    def contacts_for_files(file_list: list[str]) -> list[str]:
        contacts = []
        seen = set()
        for file_path in file_list:
            mod = KG.modules.get(file_path)
            if not mod:
                continue
            for contact in getattr(mod, "likely_contacts", [])[:2]:
                if contact and contact not in seen:
                    contacts.append(contact)
                    seen.add(contact)
        return contacts[:3]

    tasks = [
        {
            "id": "task-1",
            "risk": "low",
            "title": "Map model docs to SQL files",
            "goal": "Understand how YAML model documentation aligns with SQL transformations.",
            "files": low_risk_docs,
            "suggested_contacts": contacts_for_files(low_risk_docs),
            "steps": [
                "Open a YAML file and its related SQL model.",
                "Check whether column/test metadata matches actual SQL output.",
                "Record one mismatch or confirm alignment.",
            ],
        },
        {
            "id": "task-2",
            "risk": "medium",
            "title": "Trace one staging-to-mart path",
            "goal": "Build confidence navigating lineage and blast radius for one business flow.",
            "files": (staging_candidates[:2] + mart_candidates[:2])[:4],
            "datasets": (source_datasets[:2] + sink_datasets[:2])[:4],
            "suggested_contacts": contacts_for_files((staging_candidates[:2] + mart_candidates[:2])[:4]),
            "steps": [
                "Pick one staging SQL model and one downstream mart model.",
                "Use lineage graph to verify upstream sources and downstream outputs.",
                "Document the flow in 3 bullet points.",
            ],
        },
        {
            "id": "task-3",
            "risk": "high",
            "title": "Assess central hub blast radius",
            "goal": "Understand failure impact and safe-change strategy for critical modules.",
            "files": [top_hub] + top_hub_neighbors,
            "suggested_contacts": contacts_for_files([top_hub] + top_hub_neighbors),
            "steps": [
                "Open the highest-centrality module.",
                "Inspect its immediate dependents shown in blast radius.",
                "Propose a safe rollout/checklist before changing this module.",
            ],
        },
        {
            "id": "task-4",
            "risk": "medium",
            "title": "Inspect high-velocity files",
            "goal": "Identify change hotspots and likely operational pain points.",
            "files": [path for path, _ in hot_files[:5]],
            "suggested_contacts": contacts_for_files([path for path, _ in hot_files[:5]]),
            "steps": [
                "Review top frequently changed files in module index.",
                "Correlate with pagerank and downstream dependencies.",
                "Flag one hotspot that needs tests or refactor guardrails.",
            ],
        },
    ]

    return jsonify({
        "repo": REPO_PATH.name if REPO_PATH else "",
        "tasks": tasks,
    })


@app.route("/api/support_navigator")
def api_support_navigator():
    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 500

    pr_scores = KG.pagerank_modules()
    contact_map = {}

    for path, mod in KG.modules.items():
        contacts = list(getattr(mod, "likely_contacts", []) or [])
        if not contacts and getattr(mod, "last_author", None):
            contacts = [mod.last_author]

        velocity = int(getattr(mod, "change_velocity_30d", 0) or 0)
        pagerank = float(pr_scores.get(path, 0.0))
        language = str(getattr(getattr(mod, "language", None), "value", getattr(mod, "language", "other")))

        for rank, contact in enumerate(contacts[:3], start=1):
            if not contact:
                continue

            entry = contact_map.setdefault(contact, {
                "name": contact,
                "email": None,
                "support_score": 0.0,
                "files": [],
                "focus": set(),
            })

            if getattr(mod, "last_author", None) == contact and getattr(mod, "last_author_email", None):
                entry["email"] = mod.last_author_email

            norm_path = path.replace("\\", "/")
            if "/staging/" in norm_path:
                entry["focus"].add("staging flow")
            if "/marts/" in norm_path:
                entry["focus"].add("business outputs")
            if norm_path.endswith((".yml", ".yaml")):
                entry["focus"].add("dbt config/docs")
            if velocity >= 3:
                entry["focus"].add("hotspot changes")
            if pagerank >= 0.05:
                entry["focus"].add("architectural hubs")

            entry["support_score"] += max(1, 4 - rank) + velocity + (pagerank * 100)
            entry["files"].append({
                "path": path,
                "velocity": velocity,
                "pagerank": round(pagerank, 6),
                "language": language,
                "last_modified": getattr(mod, "last_modified", None),
            })

    contacts = []
    for entry in contact_map.values():
        ranked_files = sorted(
            entry["files"],
            key=lambda file_info: (file_info["velocity"], file_info["pagerank"]),
            reverse=True,
        )
        contacts.append({
            "name": entry["name"],
            "email": entry["email"],
            "support_score": round(entry["support_score"], 2),
            "focus": sorted(entry["focus"]),
            "top_files": ranked_files[:5],
            "file_count": len(entry["files"]),
        })

    contacts.sort(key=lambda item: item["support_score"], reverse=True)

    return jsonify({
        "repo": REPO_PATH.name if REPO_PATH else "",
        "contacts": contacts[:12],
    })


@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json()
    question = data.get("question", "").strip()
    history = data.get("history", [])

    if not question:
        return jsonify({"error": "No question provided"}), 400

    if not KG:
        return jsonify({"error": "Knowledge graph not loaded"}), 400

    try:
        agent = create_navigator(KG, REPO_PATH)

        messages = []
        for h in history[-6:]:
            if h["role"] == "user":
                messages.append(HumanMessage(content=h["content"]))
            elif h["role"] == "assistant":
                messages.append(AIMessage(content=h["content"]))
        messages.append(HumanMessage(content=question))

        result = agent.invoke({"messages": messages})
        answer = result["messages"][-1].content
        return jsonify({"answer": answer})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_node_group(name: str) -> str:
    name_lower = name.lower()
    if name_lower.startswith("raw_"):
        return "raw"
    elif name_lower.startswith("stg_"):
        return "staging"
    elif any(x in name_lower for x in ["<dynamic", "<spark"]):
        return "dynamic"
    else:
        return "mart"


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Fail fast if an older server is already bound to 5000
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 5000))
    except OSError as exc:
        print(f"Port 5000 is already in use: {exc}")
        print("Stop existing Python/Flask process and start app again.")
        sys.exit(1)
    finally:
        sock.close()

    # Optional: pre-load a repo passed as argument
    if len(sys.argv) >= 2:
        repo_path = Path(sys.argv[1]).resolve()
        if repo_path.exists():
            load_knowledge_graph(repo_path)
            print(f"Pre-loaded: {repo_path}")
    else:
        if try_restore_previous_repo():
            print(f"Restored previous repo context: {REPO_PATH}")

    print("Starting Brownfield Cartographer UI...")
    print("Open http://localhost:5000 in your browser")
    try:
        app.run(debug=False, port=5000)
    except OSError as exc:
        print(f"Failed to start server on port 5000: {exc}")
        print("A previous server process is likely still running.")
        print("Stop existing Python/Flask process and run again.")
        sys.exit(1)