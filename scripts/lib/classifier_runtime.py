from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ClassifierRuntimePaths:
    repo_root: Path
    script_dir: Path
    classifier_dir: Path
    export_dir: Path
    legacy_classifier_dir: Path
    legacy_tools_dir: Path


def resolve_repo_root(anchor: str | Path) -> Path:
    env_root = os.environ.get("AIOSTREAMS_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(anchor).resolve().parent.parent


def bootstrap_classifier_runtime(
    anchor: str | Path,
    *,
    include_script_dir: bool = True,
    include_export_dir: bool = False,
) -> ClassifierRuntimePaths:
    anchor_path = Path(anchor).resolve()
    repo_root = resolve_repo_root(anchor_path)
    paths = ClassifierRuntimePaths(
        repo_root=repo_root,
        script_dir=anchor_path.parent,
        classifier_dir=repo_root / "bitmagnet-media" / "classifier",
        export_dir=repo_root / "bitmagnet-media" / "classifier" / "export",
        legacy_classifier_dir=repo_root / "classifier",
        legacy_tools_dir=repo_root / "bitmagnet" / "classifier-tools",
    )

    candidates: list[Path] = []
    if include_script_dir:
        candidates.append(paths.script_dir)
    candidates.append(paths.classifier_dir)
    if include_export_dir:
        candidates.append(paths.export_dir)

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.exists() and str(resolved) not in sys.path:
            sys.path.insert(0, str(resolved))

    return paths
