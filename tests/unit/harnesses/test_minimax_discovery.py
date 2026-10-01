# -*- coding: utf-8 -*-
"""Tests for discovering the MiniMax Code CLI executable."""

from __future__ import annotations

from pathlib import Path

import pytest

from qwenpaw.harnesses.minimax.discovery import (
    McodeBinaryResolution,
    default_install_candidates,
    resolve_mcode_binary,
    resolve_mcode_binary_info,
)


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True)
    path.touch()
    path.chmod(path.stat().st_mode | 0o111)
    return path


def test_prefers_configured_binary(tmp_path: Path) -> None:
    binary = _executable(tmp_path / "custom" / "mcode")

    resolution = resolve_mcode_binary_info(
        str(binary),
        install_candidates=[],
        environ={},
    )

    assert resolution == McodeBinaryResolution(binary, "configured")


def test_reads_mcode_environment_binary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = _executable(tmp_path / "environment" / "mcode")

    resolution = resolve_mcode_binary_info(
        install_candidates=[],
        environ={"MCODE_BINARY": str(binary)},
    )

    assert resolution is not None
    assert resolution.path == binary
    assert resolution.source == "environment"


def test_bare_configured_name_falls_through_to_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = _executable(tmp_path / "environment" / "mcode")
    monkeypatch.setenv("PATH", "")

    resolution = resolve_mcode_binary_info(
        "mcode",
        install_candidates=[],
        environ={"MCODE_BINARY": str(binary)},
    )

    assert resolution is not None
    assert resolution.path == binary
    assert resolution.source == "environment"


def test_resolves_binary_from_path(
    tmp_path: Path,
) -> None:
    binary = _executable(tmp_path / "bin" / "mcode")

    resolution = resolve_mcode_binary_info(
        install_candidates=[],
        environ={"PATH": str(binary.parent)},
    )

    assert resolution is not None
    assert resolution.path == binary
    assert resolution.source == "path"


def test_falls_back_to_standalone_install(tmp_path: Path) -> None:
    binary = _executable(tmp_path / "opt" / "node24" / "bin" / "mcode")

    resolution = resolve_mcode_binary_info(
        install_candidates=[(binary, "standalone")],
        environ={},
    )

    assert resolution is not None
    assert resolution.path == binary
    assert resolution.source == "standalone"


def test_default_install_candidates_target_node24() -> None:
    assert default_install_candidates() == (
        (Path("/opt/node24/bin/mcode"), "standalone"),
    )


def test_invalid_configured_binary_does_not_fall_back(
    tmp_path: Path,
) -> None:
    fallback = _executable(tmp_path / "opt" / "node24" / "bin" / "mcode")

    resolution = resolve_mcode_binary_info(
        str(tmp_path / "missing" / "mcode"),
        install_candidates=[(fallback, "standalone")],
        environ={},
    )

    assert resolution is None


def test_invalid_environment_binary_does_not_fall_back(
    tmp_path: Path,
) -> None:
    fallback = _executable(tmp_path / "opt" / "node24" / "bin" / "mcode")

    resolution = resolve_mcode_binary_info(
        install_candidates=[(fallback, "standalone")],
        environ={"MCODE_BINARY": str(tmp_path / "missing" / "mcode")},
    )

    assert resolution is None


def test_resolve_mcode_binary_returns_path_only(tmp_path: Path) -> None:
    binary = _executable(tmp_path / "custom" / "mcode")

    assert (
        resolve_mcode_binary(
            str(binary),
            install_candidates=[],
            environ={},
        )
        == binary
    )


def test_missing_everywhere_returns_none() -> None:
    resolution = resolve_mcode_binary_info(
        install_candidates=[(Path("/nonexistent/mcode"), "standalone")],
        environ={},
    )

    assert resolution is None
