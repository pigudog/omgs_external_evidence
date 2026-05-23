from __future__ import annotations

from pathlib import Path


def prepare_dir() -> Path:
    return Path(__file__).resolve().parent


def repo_root() -> Path:
    return prepare_dir().parent


def resolve_from_prepare(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (prepare_dir() / path).resolve()

