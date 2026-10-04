"""Access to the versions pinned in versions.toml at the repository root."""

import tomllib
from pathlib import Path

VERSIONS_FILE = Path(__file__).resolve().parents[3] / "versions.toml"


def pinned(key: str, versions_file: Path = VERSIONS_FILE) -> str:
    """Return the pinned version for a dotted key such as "shared.xgboost"."""
    with versions_file.open("rb") as handle:
        node: object = tomllib.load(handle)
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"versions.toml has no entry for '{key}'")
        node = node[part]
    if not isinstance(node, str):
        raise KeyError(f"versions.toml entry '{key}' is not a version string")
    return node
