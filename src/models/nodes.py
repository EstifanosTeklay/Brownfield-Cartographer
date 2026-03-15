"""
Data models for the Brownfield Cartographer knowledge graph.
Uses Pydantic v2 for validation, serialization, and schema enforcement.
All node and edge types align with the knowledge graph specification.
"""
from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

class Language(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    YAML = "yaml"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    NOTEBOOK = "notebook"
    OTHER = "other"


class StorageType(str, Enum):
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    UNKNOWN = "unknown"


class TransformationType(str, Enum):
    READ = "read"
    WRITE = "write"
    SQL_SELECT = "sql_select"
    SQL_INSERT = "sql_insert"
    SQL_CREATE = "sql_create"
    TRANSFORM = "transform"
    UNKNOWN = "unknown"


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


# ── Node Models ───────────────────────────────────────────────────────────────

class ModuleNode(BaseModel):
    """
    Represents a source file (module) in the codebase.
    Populated by the Surveyor agent.
    """
    path: str = Field(..., description="Relative path in repo")
    language: Language = Field(default=Language.OTHER)
    purpose_statement: Optional[str] = Field(
        default=None,
        description="LLM-generated purpose statement based on code"
    )
    domain_cluster: Optional[str] = Field(
        default=None,
        description="Inferred business domain e.g. ingestion, staging, serving"
    )
    complexity_score: float = Field(
        default=0.0,
        ge=0.0,
        description="Cyclomatic complexity score"
    )
    change_velocity_30d: int = Field(
        default=0,
        ge=0,
        description="Number of commits touching this file in last 30 days"
    )
    is_dead_code_candidate: bool = Field(
        default=False,
        description="True if no other module imports this file"
    )
    last_modified: Optional[str] = Field(
        default=None,
        description="ISO date of last git commit touching this file"
    )
    last_author: Optional[str] = Field(
        default=None,
        description="Most recent contributor to touch this file"
    )
    last_author_email: Optional[str] = Field(
        default=None,
        description="Most recent contributor email if available from git history"
    )
    likely_contacts: List[str] = Field(
        default_factory=list,
        description="Top recent contributors likely to help with this file"
    )
    loc: int = Field(default=0, ge=0, description="Lines of code")
    imports: List[str] = Field(
        default_factory=list,
        description="List of imported module paths"
    )
    exports: List[str] = Field(
        default_factory=list,
        description="List of public functions and class names"
    )
    docstring: Optional[str] = Field(
        default=None,
        description="Module-level docstring if present"
    )
    doc_drift_flag: bool = Field(
        default=False,
        description="True if docstring contradicts implementation"
    )
    decorators: List[str] = Field(
        default_factory=list,
        description="Decorators found on functions in this module"
    )

    @field_validator("path")
    @classmethod
    def normalize_path(cls, v: str) -> str:
        """Normalize Windows backslashes to forward slashes."""
        return v.replace("\\", "/")

    @field_validator("complexity_score")
    @classmethod
    def round_complexity(cls, v: float) -> float:
        return round(v, 2)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    class Config:
        use_enum_values = True


class DatasetNode(BaseModel):
    """
    Represents a dataset, table, file, stream, or API endpoint.
    Populated by the Hydrologist agent.
    """
    name: str = Field(..., description="Dataset or table name")
    storage_type: StorageType = Field(default=StorageType.UNKNOWN)
    schema_snapshot: Optional[Dict[str, str]] = Field(
        default=None,
        description="Column name to type mapping if available"
    )
    freshness_sla: Optional[str] = Field(
        default=None,
        description="Expected freshness e.g. daily, hourly"
    )
    owner: Optional[str] = Field(
        default=None,
        description="Team or system that owns this dataset"
    )
    is_source_of_truth: bool = Field(
        default=False,
        description="True if this is a raw source table"
    )
    path_or_table: Optional[str] = Field(
        default=None,
        description="Full path or qualified table name"
    )

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Dataset name must not be empty")
        return v.strip().lower()

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    class Config:
        use_enum_values = True


class FunctionNode(BaseModel):
    """
    Represents a function or method in the codebase.
    Populated by the Surveyor agent.
    """
    qualified_name: str = Field(
        ...,
        description="Fully qualified name e.g. src/utils.py::my_function"
    )
    parent_module: str = Field(
        ...,
        description="Relative path of the containing module"
    )
    signature: str = Field(
        ...,
        description="Function signature string"
    )
    purpose_statement: Optional[str] = Field(
        default=None,
        description="LLM-generated purpose statement"
    )
    call_count_within_repo: int = Field(
        default=0,
        ge=0,
        description="Number of times this function is called within the repo"
    )
    is_public_api: bool = Field(
        default=True,
        description="False if function name starts with underscore"
    )
    lineno: int = Field(
        default=0,
        ge=0,
        description="Line number where function is defined"
    )
    decorators: List[str] = Field(
        default_factory=list,
        description="List of decorator names on this function"
    )

    @field_validator("qualified_name")
    @classmethod
    def normalize_qualified_name(cls, v: str) -> str:
        return v.replace("\\", "/")

    @field_validator("parent_module")
    @classmethod
    def normalize_parent_module(cls, v: str) -> str:
        return v.replace("\\", "/")

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    class Config:
        use_enum_values = True


class TransformationNode(BaseModel):
    """
    Represents a data transformation operation.
    Populated by the Hydrologist agent.
    Links source datasets to target datasets.
    """
    id: str = Field(
        ...,
        description="Unique identifier: source_file:lineno"
    )
    source_datasets: List[str] = Field(
        default_factory=list,
        description="Input dataset names"
    )
    target_datasets: List[str] = Field(
        default_factory=list,
        description="Output dataset names"
    )
    transformation_type: TransformationType = Field(
        default=TransformationType.UNKNOWN
    )
    source_file: str = Field(
        default="",
        description="Relative path of file containing this transformation"
    )
    line_range: Tuple[int, int] = Field(
        default=(0, 0),
        description="Start and end line numbers of the transformation"
    )
    sql_query: Optional[str] = Field(
        default=None,
        description="SQL query text if applicable, truncated to 500 chars"
    )

    @field_validator("source_file")
    @classmethod
    def normalize_source_file(cls, v: str) -> str:
        return v.replace("\\", "/")

    @field_validator("id")
    @classmethod
    def normalize_id(cls, v: str) -> str:
        return v.replace("\\", "/")

    @field_validator("sql_query")
    @classmethod
    def truncate_sql(cls, v: Optional[str]) -> Optional[str]:
        if v and len(v) > 500:
            return v[:500] + "..."
        return v

    @model_validator(mode="after")
    def check_has_datasets(self) -> "TransformationNode":
        if not self.source_datasets and not self.target_datasets:
            raise ValueError(
                "TransformationNode must have at least one source or target dataset"
            )
        return self

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["line_range"] = list(self.line_range)
        return data

    class Config:
        use_enum_values = True


# ── Edge Models ───────────────────────────────────────────────────────────────

class ImportEdge(BaseModel):
    """IMPORTS: source_module imports target_module."""
    source: str
    target: str
    edge_type: EdgeType = Field(default=EdgeType.IMPORTS)
    weight: int = Field(default=1, ge=1)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class ProducesEdge(BaseModel):
    """PRODUCES: transformation produces dataset."""
    source: str
    target: str
    edge_type: EdgeType = Field(default=EdgeType.PRODUCES)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class ConsumesEdge(BaseModel):
    """CONSUMES: dataset is consumed by transformation."""
    source: str
    target: str
    edge_type: EdgeType = Field(default=EdgeType.CONSUMES)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class CallsEdge(BaseModel):
    """CALLS: function calls another function."""
    source: str
    target: str
    edge_type: EdgeType = Field(default=EdgeType.CALLS)
    caller: str = Field(default="")
    callee: str = Field(default="")
    lineno: int = Field(default=0, ge=0)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class ConfiguresEdge(BaseModel):
    """CONFIGURES: config file configures a module or pipeline."""
    source: str
    target: str
    edge_type: EdgeType = Field(default=EdgeType.CONFIGURES)
    weight: int = Field(default=1, ge=1)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()