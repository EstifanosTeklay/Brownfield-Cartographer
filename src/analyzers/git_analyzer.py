"""
Git velocity analyzer: extracts change frequency per file using git log.
Used by the Surveyor to identify high-churn files (pain points) and
compute change_velocity_30d for each module.
"""
from __future__ import annotations
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


def _run_git(args: List[str], cwd: Path) -> Optional[str]:
    """Run a git command and return stdout, or None on failure."""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def is_git_repo(path: Path) -> bool:
    return _run_git(["rev-parse", "--is-inside-work-tree"], path) == "true"


def get_file_velocity(
    repo_path: Path,
    days: int = 30,
) -> Dict[str, int]:
    """
    Returns {relative_file_path: commit_count} for the last `days` days.
    Files not touched in that period have 0 commits.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    output = _run_git(
        ["log", f"--since={since}", "--name-only", "--pretty=format:", "--no-merges"],
        repo_path,
    )
    if not output:
        return {}

    counts: Dict[str, int] = defaultdict(int)
    for line in output.splitlines():
        line = line.strip()
        if line:
            counts[line] += 1

    return dict(counts)


def get_last_modified(repo_path: Path, file_path: str) -> Optional[str]:
    """Return the ISO date of the last commit touching file_path."""
    output = _run_git(
        ["log", "-1", "--format=%ci", "--", file_path],
        repo_path,
    )
    return output[:10] if output else None  # YYYY-MM-DD


def get_high_velocity_files(
    velocity: Dict[str, int],
    top_n: int = 20,
) -> List[Tuple[str, int]]:
    """
    Return the top N files by change velocity (80/20 rule: 20% of files = 80% changes).
    """
    sorted_files = sorted(velocity.items(), key=lambda x: x[1], reverse=True)
    return sorted_files[:top_n]


def get_git_log_summary(repo_path: Path, days: int = 90) -> Dict[str, any]:
    """Return a summary of recent git activity."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Count commits
    commit_count = _run_git(
        ["rev-list", "--count", f"--since={since}", "HEAD"],
        repo_path,
    )

    # Get contributors
    contributors_raw = _run_git(
        ["log", f"--since={since}", "--format=%an", "--no-merges"],
        repo_path,
    )
    contributors = set()
    if contributors_raw:
        contributors = {c.strip() for c in contributors_raw.splitlines() if c.strip()}

    # Get first and last commit dates
    first = _run_git(["log", "--reverse", "--format=%ci", "-1"], repo_path)
    last = _run_git(["log", "--format=%ci", "-1"], repo_path)

    return {
        "commits_last_90d": int(commit_count) if commit_count and commit_count.isdigit() else 0,
        "contributors": sorted(contributors),
        "first_commit": first[:10] if first else None,
        "last_commit": last[:10] if last else None,
        "is_git_repo": is_git_repo(repo_path),
    }
def get_changed_files_since_last_run(
    repo_path: Path,
    last_run_timestamp: str,
) -> List[str]:
    """
    Return list of files changed since last_run_timestamp.
    last_run_timestamp: ISO date string e.g. '2024-01-15'
    """
    output = _run_git(
        ["log", f"--since={last_run_timestamp}", "--name-only", "--pretty=format:", "--no-merges"],
        repo_path,
    )
    if not output:
        return []

    changed = set()
    for line in output.splitlines():
        line = line.strip()
        if line:
            changed.add(line)

    return sorted(changed)


def get_last_run_timestamp(output_dir: Path) -> Optional[str]:
    """
    Read the timestamp of the last analysis run from analysis_summary.json.
    Returns None if no previous run exists.
    """
    summary_path = output_dir / "analysis_summary.json"
    if not summary_path.exists():
        return None

    try:
        import json
        data = json.loads(summary_path.read_text())
        # Get the timestamp from the trace log if available
        trace_path = output_dir / "cartography_trace.jsonl"
        if trace_path.exists():
            first_line = trace_path.read_text().splitlines()[0]
            entry = json.loads(first_line)
            return entry.get("timestamp", "")[:10]  # YYYY-MM-DD
    except Exception:
        pass

    return None