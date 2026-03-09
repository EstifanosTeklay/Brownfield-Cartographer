# Brownfield Cartographer

A multi-agent codebase intelligence system for rapid FDE onboarding in production environments. Ingests any local repository and produces a living, queryable knowledge graph of the system's architecture, data flows, and semantic structure.

## Installation

```bash
# Clone the repo
git clone <repo_url>
cd brownfield-cartographer

# Install dependencies (Python 3.11+)
pip install networkx pydantic PyYAML numpy scikit-learn gitpython

# Or with uv:
uv sync
```

**Core dependencies:** `networkx`, `PyYAML`, `numpy`, `scikit-learn`, `gitpython`
**Optional (for Semanticist/Navigator):** `anthropic`, `langchain`, `langgraph`

## Quick Start

```bash
# Analyze a repository
python -m src.cli analyze /path/to/your/repo

# View summary of analysis
python -m src.cli info /path/to/your/repo

# Interactive query mode
python -m src.cli query /path/to/your/repo
```

## Example: Analyzing a dbt project

```bash
# Clone the jaffle_shop example
git clone https://github.com/dbt-labs/jaffle_shop /tmp/jaffle_shop

# Analyze it
python -m src.cli analyze /tmp/jaffle_shop

# Query the lineage
python -m src.cli query /tmp/jaffle_shop
cartographer> upstream customers
cartographer> blast stg_orders
cartographer> sources
cartographer> sinks
```

## Architecture

The system is composed of four specialized agents:

```
Repository
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  Agent 1: Surveyor (Static Structure Analysis)       │
│  - Python AST parsing (stdlib ast module)            │
│  - Module import graph (NetworkX DiGraph)            │
│  - PageRank to identify architectural hubs           │
│  - Git velocity (change frequency per file)          │
│  - Dead code candidate detection                     │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  Agent 2: Hydrologist (Data Lineage Analysis)        │
│  - Python: pandas/SQLAlchemy/PySpark read/write      │
│  - SQL: table dependency extraction (regex+AST)      │
│  - dbt: ref(), source() → lineage edges              │
│  - Airflow: DAG task dependencies                    │
│  - blast_radius() and upstream tracing               │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  KnowledgeGraph (central data store)                 │
│  - module_graph: NetworkX DiGraph (modules + imports)│
│  - lineage_graph: NetworkX DiGraph (datasets + txns) │
│  - function_index: qualified_name → FunctionNode     │
└────────────────────────┬────────────────────────────┘
                         │
              ┌──────────┴───────────┐
              ▼                      ▼
     .cartography/              Query CLI
     module_graph.json          blast <module>
     lineage_graph.json         upstream <dataset>
     function_index.json        hubs / sources / sinks
     analysis_summary.json
```

## Output Files (`.cartography/`)

| File | Description |
|------|-------------|
| `module_graph.json` | Full module import graph (NetworkX node_link format) |
| `lineage_graph.json` | Data lineage DAG (datasets + transformations) |
| `function_index.json` | Index of all public functions and their signatures |
| `analysis_summary.json` | Summary stats, top hubs, circular deps, sources/sinks |

## Query Interface

```
cartographer> blast src/transforms/revenue.py
  Blast radius of 'src/transforms/revenue.py' (module graph): 3 nodes affected
    src/reports/daily_report.py
    src/api/endpoints.py
    src/exports/csv_exporter.py

cartographer> upstream customers
  Upstream of 'customers': 11 nodes
    [transformation] models/marts/customers.sql::sql::0
    [dataset] stg_customers
    [transformation] models/staging/stg_customers.sql::sql::0
    ...

cartographer> hubs
  Top Hub Modules (PageRank):
    0.253558  in= 3  src/models/nodes.py
    0.093910  in= 1  src/graph/knowledge_graph.py
    ...
```

## Supported Languages & Patterns

| Language | What's Extracted |
|----------|------------------|
| Python | Imports, public functions/classes, AST complexity, pandas/PySpark/SQLAlchemy data flows, Airflow DAGs |
| SQL | Table dependencies from SELECT/FROM/JOIN/WITH (CTEs), INSERT INTO, CREATE TABLE/VIEW, dbt `ref()` and `source()` |
| YAML | dbt schema.yml (models, sources), dbt_project.yml, Airflow YAML DAGs |
| Jupyter | Code cells extracted and analyzed as Python |

## Knowledge Graph Schema

**Node Types:**
- `ModuleNode`: path, language, complexity_score, change_velocity_30d, is_dead_code_candidate, imports, exports
- `DatasetNode`: name, storage_type, is_source_of_truth, owner
- `FunctionNode`: qualified_name, signature, is_public_api, lineno
- `TransformationNode`: source_datasets, target_datasets, transformation_type, source_file, line_range

**Edge Types:**
- `IMPORTS`: module → module
- `PRODUCES`: transformation → dataset
- `CONSUMES`: dataset → transformation
- `CALLS`: function → function (planned)

## Project Structure

```
brownfield-cartographer/
├── src/
│   ├── cli.py                          # Entry point: analyze, query, info
│   ├── orchestrator.py                 # Wires agents in sequence
│   ├── models/
│   │   └── nodes.py                    # Pydantic-style dataclass schemas
│   ├── analyzers/
│   │   ├── tree_sitter_analyzer.py     # Multi-language parser (Python ast + regex)
│   │   ├── sql_lineage.py              # SQL dependency extractor
│   │   ├── dag_config_parser.py        # Airflow/dbt YAML config parser
│   │   └── git_analyzer.py             # Git velocity analysis
│   ├── agents/
│   │   ├── surveyor.py                 # Agent 1: static structure
│   │   └── hydrologist.py              # Agent 2: data lineage
│   └── graph/
│       └── knowledge_graph.py          # NetworkX wrapper + serialization
├── pyproject.toml
└── README.md
```

## Day-One FDE Workflow

1. **Arrive at client**: `git clone <client_repo> /tmp/client_repo`
2. **Run analysis**: `python -m src.cli analyze /tmp/client_repo`
3. **Orient yourself**: `python -m src.cli info /tmp/client_repo`
4. **Explore**: `python -m src.cli query /tmp/client_repo`
   - `hubs` — what are the most critical modules?
   - `sources` — where does data come from?
   - `sinks` — what are the output datasets?
   - `blast <critical_file>` — what breaks if this changes?
5. **Inject CODEBASE.md** (generated by Archivist in final submission) into your AI coding agent for instant architectural context
