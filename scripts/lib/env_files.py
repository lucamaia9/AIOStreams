#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line or line.strip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def dump_env(path: Path, values: dict[str, str]) -> None:
    lines: list[str] = []
    original = (
        path.read_text(encoding="utf-8", errors="replace").splitlines()
        if path.exists()
        else []
    )
    touched: set[str] = set()
    for line in original:
        if "=" not in line or line.strip().startswith("#"):
            lines.append(line)
            continue
        key, _ = line.split("=", 1)
        normalized_key = key.strip()
        if normalized_key in values:
            lines.append(f"{normalized_key}={values[normalized_key]}")
            touched.add(normalized_key)
        else:
            lines.append(line)
    for key, value in values.items():
        if key not in touched:
            lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def resolve_project_path(project_root: Path, raw_path: str) -> Path:
    value = str(raw_path or "").strip()
    if not value:
        raise ValueError("path is empty")
    path = Path(value)
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


def resolve_comet_data_path(project_root: Path, raw_path: str) -> Path:
    value = str(raw_path or "").strip()
    if not value:
        raise ValueError("path is empty")

    comet_data_root = (project_root / "data" / "comet-fresh").resolve()
    path = Path(value)

    if path.is_absolute():
        if value == "/app/data":
            return comet_data_root
        if value.startswith("/app/data/"):
            relative = Path(value.removeprefix("/app/data/"))
            return (comet_data_root / relative).resolve()
        return path

    if value == "data":
        return comet_data_root
    if value.startswith("data/"):
        relative = Path(value).relative_to("data")
        return (comet_data_root / relative).resolve()

    return (project_root / path).resolve()
