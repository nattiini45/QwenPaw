# -*- coding: utf-8 -*-
"""Locate the MiniMax Code CLI (``mcode``) executable."""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

MCODE_BINARY_ENV = "MCODE_BINARY"
MCODE_COMMAND = "mcode"


@dataclass(frozen=True)
class McodeBinaryResolution:
    """One resolved ``mcode`` executable and how it was discovered."""

    path: Path
    source: str


def default_install_candidates() -> tuple[tuple[Path, str], ...]:
    """Return known MiniMax Code install locations on this host."""
    return ((Path("/opt/node24/bin/mcode"), "standalone"),)


def resolve_mcode_binary(
    binary: str | None = None,
    *,
    install_candidates: tuple[tuple[Path, str], ...] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path | None:
    """Resolve the configured or installed ``mcode`` executable path."""
    resolution = resolve_mcode_binary_info(
        binary,
        install_candidates=install_candidates,
        environ=environ,
    )
    return resolution.path if resolution is not None else None


def resolve_mcode_binary_info(
    binary: str | None = None,
    *,
    install_candidates: tuple[tuple[Path, str], ...] | None = None,
    environ: Mapping[str, str] | None = None,
) -> McodeBinaryResolution | None:
    """Resolve ``mcode`` and retain the discovery source for diagnostics.

    Resolution order: adapter-configured binary, ``MCODE_BINARY`` environment
    variable, ``PATH`` lookup, then the server-wide Node 24 install under
    ``/opt/node24``.
    """
    environment = environ if environ is not None else os.environ
    configured = str(binary or "").strip()
    if configured:
        resolved = _resolve_configured(configured)
        if resolved is not None or configured != MCODE_COMMAND:
            return (
                McodeBinaryResolution(resolved, "configured")
                if resolved is not None
                else None
            )

    environment_binary = str(environment.get(MCODE_BINARY_ENV) or "").strip()
    if environment_binary:
        resolved = _resolve_configured(environment_binary)
        if resolved is not None or environment_binary != MCODE_COMMAND:
            return (
                McodeBinaryResolution(resolved, "environment")
                if resolved is not None
                else None
            )

    on_path = shutil.which(
        MCODE_COMMAND,
        path=environment.get("PATH") or "",
    )
    if on_path:
        resolved = Path(on_path).resolve()
        if _is_executable(resolved):
            return McodeBinaryResolution(resolved, "path")

    candidates = (
        install_candidates
        if install_candidates is not None
        else default_install_candidates()
    )
    for path, source in candidates:
        if _is_executable(path):
            return McodeBinaryResolution(Path(path).resolve(), source)
    return None


def _resolve_configured(binary: str) -> Path | None:
    path = Path(binary).expanduser()
    if path.is_absolute() or path.parent != Path("."):
        resolved = path.resolve()
        if not _is_executable(path):
            return None
        return resolved
    on_path = shutil.which(binary)
    if not on_path:
        return None
    return Path(on_path).resolve()


def _is_executable(
    path: Path,
    *,
    platform_name: str | None = None,
) -> bool:
    if not path.is_file():
        return False
    return (platform_name or sys.platform) == "win32" or os.access(
        path,
        os.X_OK,
    )


__all__ = [
    "MCODE_BINARY_ENV",
    "McodeBinaryResolution",
    "default_install_candidates",
    "resolve_mcode_binary",
    "resolve_mcode_binary_info",
]
