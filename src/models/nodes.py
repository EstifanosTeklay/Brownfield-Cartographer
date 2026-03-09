"""
Data models for the Brownfield Cartographer knowledge graph.
Using dataclasses for zero external dependencies.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum


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


@dataclass
class ModuleNode:
    path: str                                  # relative path in repo
    language: Language = Language.OTHER
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: float = 0.0
    change_velocity_30d: int = 0              # commit count in last 30d
    is_dead_code_candidate: bool = False
    last_modified: Optional[str] = None
    loc: int = 0                              # lines of code
    imports: List[str] = field(default_factory=list)
    exports: List[str] = field(default_factory=list)   # public functions/classes
    docstring: Optional[str] = None
    doc_drift_flag: bool = False              # docstring contradicts implementation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "language": self.language.value,
            "purpose_statement": self.purpose_statement,
            "domain_cluster": self.domain_cluster,
            "complexity_score": self.complexity_score,
            "change_velocity_30d": self.change_velocity_30d,
            "is_dead_code_candidate": self.is_dead_code_candidate,
            "last_modified": self.last_modified,
            "loc": self.loc,
            "imports": self.imports,
            "exports": self.exports,
            "docstring": self.docstring,
            "doc_drift_flag": self.doc_drift_flag,
        }


@dataclass
class DatasetNode:
    name: str
    storage_type: StorageType = StorageType.UNKNOWN
    schema_snapshot: Optional[Dict[str, str]] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    path_or_table: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "storage_type": self.storage_type.value,
            "schema_snapshot": self.schema_snapshot,
            "freshness_sla": self.freshness_sla,
            "owner": self.owner,
            "is_source_of_truth": self.is_source_of_truth,
            "path_or_table": self.path_or_table,
        }


@dataclass
class FunctionNode:
    qualified_name: str                       # module.ClassName.method_name
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = True
    lineno: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "qualified_name": self.qualified_name,
            "parent_module": self.parent_module,
            "signature": self.signature,
            "purpose_statement": self.purpose_statement,
            "call_count_within_repo": self.call_count_within_repo,
            "is_public_api": self.is_public_api,
            "lineno": self.lineno,
        }


@dataclass
class TransformationNode:
    id: str                                   # unique: source_file:lineno
    source_datasets: List[str] = field(default_factory=list)
    target_datasets: List[str] = field(default_factory=list)
    transformation_type: TransformationType = TransformationType.UNKNOWN
    source_file: str = ""
    line_range: tuple = (0, 0)
    sql_query: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_datasets": self.source_datasets,
            "target_datasets": self.target_datasets,
            "transformation_type": self.transformation_type.value,
            "source_file": self.source_file,
            "line_range": list(self.line_range),
            "sql_query": self.sql_query,
        }


# Edge type constants
class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"
