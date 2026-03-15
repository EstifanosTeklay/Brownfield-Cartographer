"""
Repo Manager: handles cloning GitHub URLs and extracting zip files.
Stores cloned repos in a temp directory for analysis.
"""
from __future__ import annotations
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Tuple
import git


# Where all cloned/extracted repos are stored
REPOS_DIR = Path(tempfile.gettempdir()) / "brownfield_cartographer_repos"

# Where analysis artifacts are stored (matches orchestrator/app defaults)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CARTOGRAPHY_ROOT = PROJECT_ROOT / "cartography"


def ensure_repos_dir() -> Path:
    REPOS_DIR.mkdir(parents=True, exist_ok=True)
    return REPOS_DIR


def clone_github_repo(url: str) -> Tuple[Path, str]:
    """
    Clone a GitHub URL to a local temp directory.
    Returns (repo_path, repo_name).
    Raises ValueError on invalid URL.
    Raises RuntimeError on clone failure.
    """
    # Validate URL
    url = url.strip()
    if not url.startswith("https://github.com/"):
        raise ValueError(
            f"Only GitHub URLs are supported (https://github.com/...). Got: {url}"
        )

    # Extract repo name from URL
    # e.g. https://github.com/dbt-labs/jaffle_shop → jaffle_shop
    normalized_url = url.rstrip("/")
    if normalized_url.endswith(".git"):
        normalized_url = normalized_url[:-4]
    parts = normalized_url.split("/")
    if len(parts) < 5:
        raise ValueError(f"Invalid GitHub URL format: {url}")

    repo_name = parts[-1]
    owner = parts[-2]
    clone_name = f"{owner}__{repo_name}"

    repos_dir = ensure_repos_dir()
    target_path = repos_dir / clone_name

    # If already cloned pull latest instead of re-cloning
    if target_path.exists():
        try:
            print(f"  [repo_manager] Repo already exists — pulling latest...")
            repo = git.Repo(target_path)
            repo.remotes.origin.pull()
            return target_path, repo_name
        except Exception:
            # If pull fails delete and re-clone
            shutil.rmtree(target_path, ignore_errors=True)

    try:
        print(f"  [repo_manager] Cloning {url} → {target_path}")
        git.Repo.clone_from(url, target_path, depth=50)
        return target_path, repo_name
    except git.exc.GitCommandError as e:
        raise RuntimeError(f"Failed to clone repository: {e}")


def extract_zip_repo(zip_bytes: bytes, filename: str) -> Tuple[Path, str]:
    """
    Extract a zip file containing a repository.
    Returns (repo_path, repo_name).
    """
    repos_dir = ensure_repos_dir()

    # Use filename without extension as repo name
    repo_name = Path(filename).stem
    target_path = repos_dir / repo_name

    # Clean up existing
    if target_path.exists():
        shutil.rmtree(target_path, ignore_errors=True)
    target_path.mkdir(parents=True)

    try:
        import io
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(target_path)

        # If zip contains a single top-level folder unwrap it
        contents = list(target_path.iterdir())
        if len(contents) == 1 and contents[0].is_dir():
            inner = contents[0]
            # Move contents up one level
            for item in inner.iterdir():
                shutil.move(str(item), str(target_path))
            inner.rmdir()

        return target_path, repo_name
    except zipfile.BadZipFile:
        raise ValueError("Invalid zip file")


def list_analyzed_repos() -> list:
    """
    Return list of repos that have been analyzed.
    A repo is considered analyzed if there is an analysis_summary.json
    under: <project_root>/cartography/<repo_dir_name>/.
    """
    if not REPOS_DIR.exists():
        return []
    repos = []
    for path in sorted(REPOS_DIR.iterdir()):
        if path.is_dir():
            analysis_dir = CARTOGRAPHY_ROOT / path.name
            has_analysis = (analysis_dir / "analysis_summary.json").exists()
            repos.append({
                "name": path.name,
                "path": str(path),
                "analyzed": has_analysis,
            })
    return repos