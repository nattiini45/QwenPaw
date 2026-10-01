# -*- coding: utf-8 -*-
"""Tests for the MiniMax Code third-party agent adapter."""

# pylint: disable=protected-access

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

import qwenpaw.harnesses.minimax.adapter as adapter_module
from qwenpaw.harnesses.base import HarnessOperationNotSupportedError
from qwenpaw.harnesses.minimax.adapter import MiniMaxAdapter
from qwenpaw.harnesses.registry import (
    PROVIDER_CATALOG,
    adapter_config_key,
    create_adapter,
    get_provider,
)
from qwenpaw.utils.io_utils import write_json_atomic

_LOGIN_SCRIPT = """#!/bin/sh
echo "Open: https://account.minimax.io/oauth-authorize?user_code=ABCD-EFGH" >&2
echo "Code: ABCD-EFGH" >&2
exec sleep 30
"""

_URL_ONLY_SCRIPT = """#!/bin/sh
echo "Open: https://account.minimax.io/oauth-authorize" >&2
exec sleep 30
"""


class FakeMcodeClient:
    """Minimal mcode ACP client double."""

    installed = True
    running = True

    def __init__(self, binary: str | None = None, **_: Any) -> None:
        self.binary = binary
        self.stopped = False
        self.closed_sessions: list[str] = []
        self.advertised_models: list[dict[str, Any]] = []

    def model_config_options(self) -> list[dict[str, Any]]:
        return self.advertised_models

    async def stop(self) -> None:
        self.stopped = True

    async def close_session(self, session_id: str) -> None:
        self.closed_sessions.append(session_id)


def _executable_script(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _adapter(
    tmp_path: Path,
    client: FakeMcodeClient | None = None,
    binary: str | None = None,
) -> MiniMaxAdapter:
    factory = (lambda **kwargs: client) if client is not None else None  # type: ignore[arg-type,return-value]
    return MiniMaxAdapter(
        tmp_path,
        binary=binary,
        client_factory=factory,  # type: ignore[arg-type]
    )


def _write_auth_state(
    data_dir: Path,
    payload: dict[str, Any],
    region: str = "en",
) -> None:
    path = (
        data_dir
        / "auth"
        / "prod"
        / region
        / "mcode-public"
        / "auth-state.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture(autouse=True)
def _hermetic_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    data_dir = tmp_path / "minimax-data"
    data_dir.mkdir()
    monkeypatch.delenv("MINIMAX_DATA_DIR", raising=False)
    monkeypatch.delenv("MAVIS_DATA_DIR", raising=False)
    monkeypatch.setenv("MINIMAX_DATA_DIR", str(data_dir))
    return data_dir


def test_registry_factory_returns_minimax_adapter(tmp_path: Path) -> None:
    adapter = create_adapter(
        "minimax",
        tmp_path,
        {"binary": "/custom/mcode"},
    )

    assert isinstance(adapter, MiniMaxAdapter)
    assert adapter._binary == "/custom/mcode"


@pytest.mark.asyncio
async def test_missing_acp_sdk_does_not_break_provider_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delitem(
        sys.modules,
        "qwenpaw.harnesses.minimax.adapter",
        raising=False,
    )
    monkeypatch.delitem(
        sys.modules,
        "qwenpaw.harnesses.minimax.acp_client",
        raising=False,
    )
    monkeypatch.setitem(sys.modules, "acp", None)

    adapter = create_adapter("minimax", tmp_path)
    status = await adapter.status()

    from qwenpaw.harnesses.base import MissingDependencyAdapter

    assert isinstance(adapter, MissingDependencyAdapter)
    assert status.available is False
    assert status.installed is False
    assert status.error == "Install qwenpaw[minimax] to enable MiniMax Code."


def test_adapter_config_key_reacts_to_binary_setting() -> None:
    assert adapter_config_key("minimax") == ("",)
    assert adapter_config_key("minimax", {"binary": " /x/mcode "}) == (
        "/x/mcode",
    )


def test_catalog_entry_exposes_approved_presets() -> None:
    provider = get_provider("minimax")

    assert provider.name == "MiniMax Code"
    assert provider.coming_soon is False
    assert [p.id for p in PROVIDER_CATALOG] == [
        "codex",
        "claude",
        "qoder",
        "minimax",
    ]
    capabilities = provider.capabilities
    assert capabilities.authentication is True
    assert capabilities.model_selection is True
    assert capabilities.reasoning_effort is True
    assert capabilities.session_resume is True
    assert capabilities.attachments is True
    assert capabilities.qwenpaw_mcp_projection is True
    assert capabilities.qwenpaw_skills_projection is False
    assert capabilities.provider_mcp_discovery is False
    assert capabilities.mcp_tool_allowlist is False
    assert capabilities.context_usage is True
    assert capabilities.commands == []
    assert [
        (preset.id, preset.settings)
        for preset in capabilities.approval_presets
    ] == [
        ("ask", {"permission_mode": "default"}),
        ("auto", {"permission_mode": "auto"}),
        ("full-access", {"permission_mode": "bypassPermissions"}),
    ]


@pytest.mark.asyncio
async def test_status_reports_missing_install(tmp_path: Path) -> None:
    adapter = MiniMaxAdapter(
        tmp_path,
        binary=str(tmp_path / "missing" / "mcode"),
    )

    status = await adapter.status()

    assert status.installed is False
    assert status.authenticated is False
    assert status.error is not None
    assert adapter.capability_unavailable_message is not None


@pytest.mark.asyncio
async def test_status_tolerates_missing_auth_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = _executable_script(tmp_path / "mcode", "#!/bin/sh\nexit 0\n")
    monkeypatch.setenv("PATH", "")
    adapter = MiniMaxAdapter(tmp_path, binary=str(binary))

    status = await adapter.status()

    assert status.installed is True
    assert status.authenticated is False
    assert status.account is None
    assert status.runtime_path == str(binary)
    assert status.runtime_source == "configured"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auth_status", "authenticated"),
    [
        ("authorized", True),
        ("authenticated", True),
        ("authorizing", False),
        ("error", False),
    ],
)
async def test_status_reads_auth_state_file(
    tmp_path: Path,
    _hermetic_data_dir: Path,
    auth_status: str,
    authenticated: bool,
) -> None:
    binary = _executable_script(tmp_path / "mcode", "#!/bin/sh\nexit 0\n")
    _write_auth_state(
        _hermetic_data_dir,
        {"schemaVersion": 2, "status": auth_status},
    )
    adapter = MiniMaxAdapter(tmp_path, binary=str(binary))

    status = await adapter.status()

    assert status.installed is True
    assert status.authenticated is authenticated
    assert status.account == {"auth_status": auth_status}


@pytest.mark.asyncio
async def test_status_reads_cn_region_fallback(
    tmp_path: Path,
    _hermetic_data_dir: Path,
) -> None:
    binary = _executable_script(tmp_path / "mcode", "#!/bin/sh\nexit 0\n")
    _write_auth_state(
        _hermetic_data_dir,
        {"schemaVersion": 2, "status": "authorized"},
        region="cn",
    )
    adapter = MiniMaxAdapter(tmp_path, binary=str(binary))

    status = await adapter.status()

    assert status.authenticated is True


@pytest.mark.asyncio
async def test_start_login_parses_stderr_lines(tmp_path: Path) -> None:
    binary = _executable_script(tmp_path / "mcode", _LOGIN_SCRIPT)
    adapter = MiniMaxAdapter(tmp_path, binary=str(binary))

    result = await adapter.start_login()

    assert result["type"] == "external"
    assert result["loginId"].startswith("minimax-")
    assert "login --region global --no-browser" in result["command"]
    assert result["url"] == (
        "https://account.minimax.io/oauth-authorize?user_code=ABCD-EFGH"
    )
    assert result["userCode"] == "ABCD-EFGH"

    await adapter.stop()
    assert adapter._login_process is None


@pytest.mark.asyncio
async def test_start_login_returns_partial_details_without_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        adapter_module,
        "_LOGIN_CAPTURE_TIMEOUT_SECONDS",
        0.5,
    )
    binary = _executable_script(tmp_path / "mcode", _URL_ONLY_SCRIPT)
    adapter = MiniMaxAdapter(tmp_path, binary=str(binary))

    result = await adapter.start_login()

    assert result["url"] == "https://account.minimax.io/oauth-authorize"
    assert result["userCode"] == ""

    await adapter.stop()


@pytest.mark.asyncio
async def test_models_fall_back_to_config_yaml(
    tmp_path: Path,
    _hermetic_data_dir: Path,
) -> None:
    (_hermetic_data_dir / "config.yaml").write_text(
        """
defaultModel: "deepseek/deepseek-v4-pro"
custom_provider:
  deepseek:
    authMode: api_key
    options:
      baseURL: https://api.deepseek.com
    models:
      deepseek-v4-pro:
        enabled: true
        thinking:
          effortOptions: [low, high]
      retired-model:
        enabled: false
""",
        encoding="utf-8",
    )
    adapter = _adapter(tmp_path, client=FakeMcodeClient())

    models = await adapter.models()

    assert [model.id for model in models] == ["deepseek/deepseek-v4-pro"]
    assert models[0].is_default is True
    assert models[0].reasoning_efforts == ["low", "high"]


@pytest.mark.asyncio
async def test_models_return_empty_without_config(
    tmp_path: Path,
    _hermetic_data_dir: Path,
) -> None:
    adapter = _adapter(tmp_path, client=FakeMcodeClient())

    assert await adapter.models() == []


@pytest.mark.asyncio
async def test_models_prefer_advertised_config_options(
    tmp_path: Path,
    _hermetic_data_dir: Path,
) -> None:
    client = FakeMcodeClient()
    client.advertised_models = [
        {
            "id": "m:minimax:MiniMax-M2.7:u",
            "name": "MiniMax M2.7",
            "description": "Fast",
            "is_default": False,
        },
        {
            "id": "m:deepseek:deepseek-v4-pro:u",
            "name": "DeepSeek V4 Pro",
            "description": "",
            "is_default": False,
        },
    ]
    adapter = _adapter(tmp_path, client=client)

    models = await adapter.models()

    assert [model.id for model in models] == [
        "m:minimax:MiniMax-M2.7:u",
        "m:deepseek:deepseek-v4-pro:u",
    ]
    assert models[0].is_default is True
    assert models[1].is_default is False


@pytest.mark.asyncio
async def test_history_and_run_turn_are_m2(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, client=FakeMcodeClient())

    assert await adapter.history("chat-1") == []
    with pytest.raises(NotImplementedError, match="M2"):
        adapter.run_turn(
            session_id="chat-1",
            prompt="hello",
            cwd=tmp_path,
            settings={},
        )
    with pytest.raises(HarnessOperationNotSupportedError):
        await adapter.run_command(
            session_id="chat-1",
            command="compact",
            arguments="",
            cwd=tmp_path,
            settings={},
        )


@pytest.mark.asyncio
async def test_reset_session_closes_acp_and_persists(
    tmp_path: Path,
) -> None:
    write_json_atomic(
        tmp_path / "minimax_sessions.json",
        {"chat-1": "acp-1", "chat-2": "acp-2"},
    )
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    await adapter.reset_session("chat-1")

    assert client.closed_sessions == ["acp-1"]
    assert adapter._sessions == {"chat-2": "acp-2"}
    assert json.loads(
        (tmp_path / "minimax_sessions.json").read_text(encoding="utf-8"),
    ) == {"chat-2": "acp-2"}
    assert "chat-1" not in adapter._session_locks


@pytest.mark.asyncio
async def test_reset_session_tolerates_client_errors(tmp_path: Path) -> None:
    write_json_atomic(
        tmp_path / "minimax_sessions.json",
        {"chat-1": "acp-1"},
    )
    client = FakeMcodeClient()

    async def failing_close(session_id: str) -> None:
        raise RuntimeError("process gone")

    client.close_session = failing_close  # type: ignore[method-assign]
    adapter = _adapter(tmp_path, client=client)

    await adapter.reset_session("chat-1")

    assert adapter._sessions == {}


@pytest.mark.asyncio
async def test_stop_stops_client_and_login_process(tmp_path: Path) -> None:
    binary = _executable_script(tmp_path / "mcode", _LOGIN_SCRIPT)
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client, binary=str(binary))
    await adapter.start_login()

    await adapter.stop()

    assert client.stopped is True
    assert adapter._login_process is None


def test_loads_persisted_sessions(tmp_path: Path) -> None:
    write_json_atomic(
        tmp_path / "minimax_sessions.json",
        {"chat-1": "acp-1"},
    )

    adapter = _adapter(tmp_path, client=FakeMcodeClient())

    assert adapter._sessions == {"chat-1": "acp-1"}


def test_session_lock_is_per_session(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, client=FakeMcodeClient())

    first = adapter.session_lock("chat-1")
    second = adapter.session_lock("chat-1")
    other = adapter.session_lock("chat-2")

    assert first is second
    assert first is not other
