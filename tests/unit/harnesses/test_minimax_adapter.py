# -*- coding: utf-8 -*-
"""Tests for the MiniMax Code third-party agent adapter."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import json
import stat
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import qwenpaw.harnesses.minimax.adapter as adapter_module
from qwenpaw.harnesses.base import HarnessOperationNotSupportedError
from qwenpaw.harnesses.capabilities import (
    HarnessMCPServerDefinition,
    HarnessRuntimeCapabilities,
)
from qwenpaw.harnesses.events import (
    HarnessAttachment,
    HarnessAttachmentKind,
    HarnessEventKind,
    HarnessHistoryKind,
)
from qwenpaw.harnesses.minimax.adapter import MiniMaxAdapter
from qwenpaw.harnesses.minimax.acp_client import McodeAcpError
from qwenpaw.harnesses.registry import (
    PROVIDER_CATALOG,
    adapter_config_key,
    create_adapter,
    get_provider,
)
from qwenpaw.security.tool_guard.approval import ApprovalDecision
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
    """Minimal mcode ACP client double with scripted prompt behavior."""

    installed = True
    running = True

    def __init__(self, binary: str | None = None, **_: Any) -> None:
        self.binary = binary
        self.stopped = False
        self.closed_sessions: list[str] = []
        self.advertised_models: list[dict[str, Any]] = []
        self.connection: Any = object()
        self.sessions: dict[str, str] = {}
        self.listeners: dict[str, list[Any]] = {}
        self.permission_handler: Any = None
        self.config_options: list[Any] | None = None
        self.ensure_started_calls = 0
        self.prompts: list[tuple[str, Any]] = []
        self.cancels: list[str] = []
        self.set_options: list[tuple[str, str, Any]] = []
        self.session_modes: list[tuple[str, str]] = []
        self.new_session_calls: list[dict[str, Any]] = []
        self.resumed_sessions: list[str] = []
        self.resumable_sessions: set[str] = set()
        self.unresumable_sessions: set[str] = set()
        self.prompt_results: dict[str, Any] = {}
        self.scripted_updates: dict[str, list[dict[str, Any]]] = {}
        self.gate: asyncio.Event | None = None
        self.prompt_started = asyncio.Event()
        self.resume_attempts: list[str] = []
        self._counter = 0
        self._prompt_in_flight = False

    # -- lifecycle ---------------------------------------------------------

    async def ensure_started(self) -> None:
        self.ensure_started_calls += 1

    async def stop(self) -> None:
        self.stopped = True

    async def close_session(self, session_id: str) -> None:
        self.closed_sessions.append(session_id)

    # -- listeners and handlers --------------------------------------------

    def add_listener(self, session_id: str, listener: Any) -> None:
        self.listeners.setdefault(session_id, []).append(listener)

    def remove_listener(self, session_id: str, listener: Any) -> None:
        remaining = [
            item
            for item in self.listeners.get(session_id, [])
            if item != listener
        ]
        if remaining:
            self.listeners[session_id] = remaining
        else:
            self.listeners.pop(session_id, None)

    def set_permission_handler(self, handler: Any) -> None:
        self.permission_handler = handler

    def model_config_options(self) -> list[dict[str, Any]]:
        return self.advertised_models

    # -- session operations --------------------------------------------------

    async def new_session(
        self,
        cwd: str,
        *,
        mcp_servers: list[Any] | None = None,
    ) -> str:
        self._counter += 1
        session_id = f"mvs-{self._counter}"
        self.sessions[session_id] = cwd
        self.new_session_calls.append(
            {"cwd": cwd, "mcp_servers": mcp_servers},
        )
        self.resumable_sessions.add(session_id)
        return session_id

    async def resume_session(
        self,
        session_id: str,
        *,
        cwd: str | None = None,
    ) -> str:
        self.resume_attempts.append(session_id)
        if session_id in self.unresumable_sessions:
            raise McodeAcpError("session vanished")
        self.resumed_sessions.append(session_id)
        self.sessions[session_id] = str(cwd or "")
        return session_id

    async def set_config_option(
        self,
        session_id: str,
        config_id: str,
        value: Any,
    ) -> None:
        self.set_options.append((session_id, config_id, value))

    async def set_session_mode(
        self,
        session_id: str,
        mode_id: str,
    ) -> None:
        self.session_modes.append((session_id, mode_id))

    async def list_sessions(
        self,
        *,
        cursor: str | None = None,
        cwd: str | None = None,
    ) -> Any:
        return SimpleNamespace(
            sessions=[
                SimpleNamespace(
                    session_id="mvs-existing",
                    cwd="/tmp/work",
                    title="existing session",
                    updated_at="2026-10-01T11:44:32.390Z",
                ),
            ],
        )

    async def prompt(self, session_id: str, prompt: Any) -> Any:
        if self._prompt_in_flight:
            raise McodeAcpError("session already has an active prompt")
        self._prompt_in_flight = True
        self.prompt_started.set()
        try:
            self.prompts.append((session_id, prompt))
            if self.gate is not None:
                await self.gate.wait()
            else:
                await asyncio.sleep(0)
            for payload in self.scripted_updates.get(session_id, []):
                for listener in list(self.listeners.get(session_id, ())):
                    await listener(session_id, payload)
            return self.prompt_results.get(session_id) or SimpleNamespace(
                stop_reason="end_turn",
            )
        finally:
            self._prompt_in_flight = False

    async def cancel(self, session_id: str) -> None:
        self.cancels.append(session_id)


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

    # The fallback synthesizes mcode select values so ids round-trip
    # through setConfigOption("model", ...).
    assert [model.id for model in models] == [
        "m:deepseek:deepseek-v4-pro:u",
    ]
    assert models[0].is_default is True
    assert models[0].reasoning_efforts == ["low", "high"]


@pytest.mark.asyncio
async def test_models_probe_captures_advertised_options(
    tmp_path: Path,
    _hermetic_data_dir: Path,
) -> None:
    client = FakeMcodeClient()
    original_new_session = client.new_session

    async def capturing_new_session(
        cwd: str,
        *,
        mcp_servers: list[Any] | None = None,
    ) -> str:
        # mcode advertises its configOptions inline in session/new.
        client.advertised_models = [
            {
                "id": "m:minimax:MiniMax-M3:v:thinking",
                "name": "M3 · thinking",
                "description": "",
                "is_default": True,
            },
        ]
        return await original_new_session(cwd, mcp_servers=mcp_servers)

    client.new_session = capturing_new_session  # type: ignore[method-assign]
    adapter = _adapter(tmp_path, client=client)

    models = await adapter.models()

    assert [model.id for model in models] == [
        "m:minimax:MiniMax-M3:v:thinking",
    ]
    assert models[0].is_default is True
    # The probe session is closed again; nothing stays mapped.
    assert client.closed_sessions == ["mvs-1"]
    assert adapter._sessions == {}


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
async def test_run_command_unsupported_and_history_lists(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    history = await adapter.history("chat-1")

    assert [item.kind for item in history] == [HarnessHistoryKind.MESSAGE]
    assert history[0].item_id == "mvs-existing"
    assert history[0].text == "existing session"
    assert history[0].data["cwd"] == "/tmp/work"
    with pytest.raises(HarnessOperationNotSupportedError):
        await adapter.run_command(
            session_id="chat-1",
            command="compact",
            arguments="",
            cwd=tmp_path,
            settings={},
        )


@pytest.mark.asyncio
async def test_history_tolerates_client_failures(tmp_path: Path) -> None:
    client = FakeMcodeClient()

    async def failing_list(**_: Any) -> Any:
        raise McodeAcpError("not running")

    client.list_sessions = failing_list  # type: ignore[method-assign]
    adapter = _adapter(tmp_path, client=client)

    assert await adapter.history("chat-1") == []


# -- run_turn ----------------------------------------------------------------


def _script_pong(client: FakeMcodeClient, session_id: str) -> None:
    client.scripted_updates[session_id] = [
        {"type": "text", "text": "PONG", "is_chunk": False},
        {
            "type": "usage",
            "used": 16847,
            "size": 512000,
            "cost": {"amount": 0.0, "currency": "USD"},
        },
    ]


@pytest.mark.asyncio
async def test_run_turn_streams_pong_and_completes(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    _script_pong(client, "mvs-1")

    events = [
        event
        async for event in adapter.run_turn(
            session_id="chat-1",
            prompt="Reply with exactly: PONG",
            cwd=tmp_path,
            settings={},
        )
    ]

    kinds = [event.kind for event in events]
    assert kinds == [
        HarnessEventKind.TEXT_DELTA,
        HarnessEventKind.COMPLETED,
    ]
    assert events[0].text == "PONG"
    assert events[1].data["used"] == 16847
    assert events[1].data["size"] == 512000
    assert events[1].data["cost_usd"] == 0.0
    # The ACP session mapping is persisted for resume across restarts.
    assert adapter._sessions == {"chat-1": "mvs-1"}
    assert json.loads(
        (tmp_path / "minimax_sessions.json").read_text(encoding="utf-8"),
    ) == {"chat-1": "mvs-1"}
    assert client.prompts == [("mvs-1", "Reply with exactly: PONG")]


@pytest.mark.asyncio
async def test_run_turn_applies_settings_each_turn(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    settings = {
        "permission_mode": "auto",
        "model": "m:minimax:MiniMax-M3:v:thinking",
        "session_mode": "plan",
    }

    events = [
        event
        async for event in adapter.run_turn(
            session_id="chat-1",
            prompt="hi",
            cwd=tmp_path,
            settings=settings,
        )
    ]

    assert events[-1].kind == HarnessEventKind.COMPLETED
    assert ("mvs-1", "permissionMode", "auto") in client.set_options
    assert (
        "mvs-1",
        "model",
        "m:minimax:MiniMax-M3:v:thinking",
    ) in client.set_options
    # permissionMode is re-applied per turn even when unchanged.
    assert client.set_options.count(("mvs-1", "permissionMode", "auto")) == 1
    assert ("mvs-1", "plan") in client.session_modes


@pytest.mark.asyncio
async def test_run_turn_rejects_unknown_permission_mode(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={"permission_mode": "yolo"},
    ):
        pass

    assert ("mvs-1", "permissionMode", "default") in client.set_options


@pytest.mark.asyncio
async def test_run_turn_sets_effort_only_when_advertised(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    settings = {"reasoning_effort": "high"}

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings=dict(settings),
    ):
        pass
    assert not any(
        config_id == "thinkingEffort"
        for _, config_id, _ in client.set_options
    )

    client.config_options = [
        SimpleNamespace(id="thinkingEffort", options=[]),
    ]
    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings=dict(settings),
    ):
        pass
    assert ("mvs-1", "thinkingEffort", "high") in client.set_options


@pytest.mark.asyncio
async def test_run_turn_tolerates_rejected_effort(tmp_path: Path) -> None:
    from acp import RequestError

    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    client.config_options = [SimpleNamespace(id="thinkingEffort")]

    async def failing_option(
        session_id: str,
        config_id: str,
        value: Any,
    ) -> None:
        if config_id == "thinkingEffort":
            raise RequestError(
                code=-32602,
                message="Thinking effort is not advertised",
            )
        client.set_options.append((session_id, config_id, value))

    client.set_config_option = failing_option  # type: ignore[method-assign]

    events = [
        event
        async for event in adapter.run_turn(
            session_id="chat-1",
            prompt="hi",
            cwd=tmp_path,
            settings={"reasoning_effort": "high"},
        )
    ]

    assert events[-1].kind == HarnessEventKind.COMPLETED


@pytest.mark.asyncio
async def test_run_turn_resumes_after_process_restart(
    tmp_path: Path,
) -> None:
    write_json_atomic(
        tmp_path / "minimax_sessions.json",
        {"chat-1": "mvs-persisted"},
    )
    client = FakeMcodeClient()
    client.resumable_sessions.add("mvs-persisted")
    adapter = _adapter(tmp_path, client=client)

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={},
    ):
        pass

    assert client.resumed_sessions == ["mvs-persisted"]
    assert client.prompts == [("mvs-persisted", "hi")]
    assert not client.new_session_calls


@pytest.mark.asyncio
async def test_run_turn_creates_new_session_when_resume_fails(
    tmp_path: Path,
) -> None:
    write_json_atomic(
        tmp_path / "minimax_sessions.json",
        {"chat-1": "mvs-gone"},
    )
    client = FakeMcodeClient()
    client.unresumable_sessions.add("mvs-gone")
    adapter = _adapter(tmp_path, client=client)

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={},
    ):
        pass

    assert client.resume_attempts == ["mvs-gone"]
    assert not client.resumed_sessions
    assert len(client.new_session_calls) == 1
    assert adapter._sessions == {"chat-1": "mvs-1"}


@pytest.mark.asyncio
async def test_run_turn_reuses_attached_session_within_process(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    for _ in range(2):
        async for _ in adapter.run_turn(
            session_id="chat-1",
            prompt="hi",
            cwd=tmp_path,
            settings={},
        ):
            pass

    assert len(client.new_session_calls) == 1
    assert not client.resumed_sessions


@pytest.mark.asyncio
async def test_run_turn_detects_respawned_process(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={},
    ):
        pass
    # The process died and came back with a fresh connection.
    client.connection = object()
    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="again",
        cwd=tmp_path,
        settings={},
    ):
        pass

    assert client.resumed_sessions == ["mvs-1"]
    assert client.prompts[-1] == ("mvs-1", "again")


@pytest.mark.asyncio
async def test_run_turn_projects_mcp_overlay(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    capabilities = HarnessRuntimeCapabilities(
        mcp_servers=[
            HarnessMCPServerDefinition(
                name="stdio-server",
                display_name="Stdio Server",
                transport="stdio",
                command="npx",
                args=["-y", "server-everything"],
                env={"SECRET_TOKEN": "s3cr3t"},
            ),
            HarnessMCPServerDefinition(
                name="http-server",
                display_name="HTTP Server",
                transport="streamable_http",
                url="https://mcp.example.test/mcp",
                headers={"Authorization": "Bearer tok"},
            ),
            HarnessMCPServerDefinition(
                name="denied-server",
                display_name="Denied Server",
                transport="stdio",
                command="denied",
                args=[],
                default_policy="deny",
            ),
        ],
    )

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={"_runtime_capabilities": capabilities},
    ):
        pass

    [call] = client.new_session_calls
    servers = call["mcp_servers"]
    assert [server.name for server in servers] == [
        "stdio-server",
        "http-server",
    ]
    stdio = servers[0]
    assert stdio.command == "npx"
    assert stdio.args == ["-y", "server-everything"]
    # The SDK requires stdio env as a list of typed variables.
    assert [
        {"name": item.name, "value": item.value} for item in stdio.env
    ] == [{"name": "SECRET_TOKEN", "value": "s3cr3t"}]
    http = servers[1]
    assert http.type == "http"
    assert http.url == "https://mcp.example.test/mcp"
    assert [
        {"name": item.name, "value": item.value} for item in http.headers
    ] == [{"name": "Authorization", "value": "Bearer tok"}]


@pytest.mark.asyncio
async def test_run_turn_references_file_attachments(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    attachment = HarnessAttachment(
        kind=HarnessAttachmentKind.FILE,
        path=tmp_path / "notes.txt",
        name="notes.txt",
    )
    (tmp_path / "notes.txt").write_text("notes", encoding="utf-8")

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="summarize",
        cwd=tmp_path,
        settings={},
        attachments=[attachment],
    ):
        pass

    [(_session_id, prompt_text)] = client.prompts
    assert prompt_text == "@notes.txt\nsummarize"


@pytest.mark.asyncio
async def test_run_turn_serializes_turns_per_session(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    client.gate = asyncio.Event()
    adapter = _adapter(tmp_path, client=client)
    _script_pong(client, "mvs-1")

    async def collect(prompt: str) -> list[Any]:
        return [
            event
            async for event in adapter.run_turn(
                session_id="chat-1",
                prompt=prompt,
                cwd=tmp_path,
                settings={},
            )
        ]

    first = asyncio.create_task(collect("one"))
    await asyncio.wait_for(client.prompt_started.wait(), timeout=5)
    assert len(client.prompts) == 1

    second = asyncio.create_task(collect("two"))
    await asyncio.sleep(0.05)
    # The second turn waits on the per-session lock; the fake would
    # reject a concurrent prompt outright.
    assert len(client.prompts) == 1

    client.gate.set()
    first_events, second_events = await asyncio.gather(first, second)

    assert [prompt for _, prompt in client.prompts] == ["one", "two"]
    assert first_events[-1].kind == HarnessEventKind.COMPLETED
    assert second_events[-1].kind == HarnessEventKind.COMPLETED


@pytest.mark.asyncio
async def test_run_turn_emits_error_on_prompt_failure(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()

    async def failing_prompt(session_id: str, prompt: Any) -> Any:
        raise McodeAcpError("assertAuthenticated: not logged in")

    client.prompt = failing_prompt  # type: ignore[method-assign]
    adapter = _adapter(tmp_path, client=client)

    events = [
        event
        async for event in adapter.run_turn(
            session_id="chat-1",
            prompt="hi",
            cwd=tmp_path,
            settings={},
        )
    ]

    assert [event.kind for event in events] == [HarnessEventKind.ERROR]
    assert "assertAuthenticated" in events[0].text


@pytest.mark.asyncio
async def test_run_turn_cancelled_error_sends_session_cancel(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    client.gate = asyncio.Event()
    adapter = _adapter(tmp_path, client=client)

    async def collect() -> list[Any]:
        return [
            event
            async for event in adapter.run_turn(
                session_id="chat-1",
                prompt="slow",
                cwd=tmp_path,
                settings={},
            )
        ]

    task = asyncio.create_task(collect())
    await asyncio.wait_for(client.prompt_started.wait(), timeout=5)
    task.cancel()
    client.gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.cancels == ["mvs-1"]


# -- approval routing ----------------------------------------------------------


def _permission_options() -> list[SimpleNamespace]:
    return [
        SimpleNamespace(
            option_id="allow-once",
            kind="allow_once",
            name="Allow once",
        ),
        SimpleNamespace(
            option_id="allow-always",
            kind="allow_always",
            name="Always allow",
        ),
        SimpleNamespace(option_id="deny", kind="reject_once", name="Deny"),
    ]


def _permission_tool_call() -> SimpleNamespace:
    return SimpleNamespace(
        tool_call_id="perm_404df608",
        title="write",
        kind="edit",
        locations=[SimpleNamespace(path="/tmp/mcode-probe-outside.txt")],
        raw_input={
            "path": "/tmp/mcode-probe-outside.txt",
            "content": "hello",
        },
        status="pending",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [ApprovalDecision.APPROVED, ApprovalDecision.DENIED],
)
async def test_ask_preset_routes_through_approval_service(
    tmp_path: Path,
    decision: ApprovalDecision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock, MagicMock, patch

    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    adapter._permission_contexts["mvs-1"] = {
        "session_id": "chat-1",
        "ask": True,
        "agent_id": "agent-1",
        "user_id": "user-1",
        "channel": "dingtalk",
    }
    pending = MagicMock(request_id="approval-1", timeout_seconds=30)
    service = MagicMock()
    service.create_pending_summary = AsyncMock(return_value=pending)
    service.wait_for_approval = AsyncMock(return_value=decision)

    with patch(
        "qwenpaw.harnesses.minimax.adapter.get_approval_service",
        return_value=service,
    ):
        response = await adapter._handle_permission_request(
            _permission_options(),
            "mvs-1",
            _permission_tool_call(),
        )

    create_call = service.create_pending_summary.await_args.kwargs
    assert create_call["session_id"] == "chat-1"
    assert create_call["agent_id"] == "agent-1"
    assert create_call["channel"] == "dingtalk"
    summary = create_call["summary"]
    assert summary.source_type == "minimax"
    assert summary.payload["provider_item_id"] == "perm_404df608"
    assert summary.payload["locations"] == ["/tmp/mcode-probe-outside.txt"]
    if decision == ApprovalDecision.APPROVED:
        assert response.outcome.option_id == "allow-always"
    else:
        assert response.outcome.option_id == "deny"


@pytest.mark.asyncio
async def test_auto_preset_never_consults_approval_service(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    adapter._permission_contexts["mvs-1"] = {"ask": False}

    response = await adapter._handle_permission_request(
        _permission_options(),
        "mvs-1",
        _permission_tool_call(),
    )

    assert response.outcome.option_id == "allow-always"


@pytest.mark.asyncio
async def test_permission_handler_deny_falls_back_to_cancelled_outcome(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    adapter._permission_contexts["mvs-1"] = {"ask": False}

    response = await adapter._handle_permission_request(
        [SimpleNamespace(option_id="bogus", kind="unknown", name="??")],
        "mvs-1",
        _permission_tool_call(),
    )

    assert response.outcome.outcome == "cancelled"


@pytest.mark.asyncio
async def test_run_turn_wires_permission_handler_and_context(
    tmp_path: Path,
) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)

    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={"permission_mode": "default"},
    ):
        pass

    assert client.permission_handler == adapter._handle_permission_request
    # The context is cleaned up once the turn settles.
    assert adapter._permission_contexts == {}


@pytest.mark.asyncio
async def test_reset_session_drops_attached_state(tmp_path: Path) -> None:
    client = FakeMcodeClient()
    adapter = _adapter(tmp_path, client=client)
    async for _ in adapter.run_turn(
        session_id="chat-1",
        prompt="hi",
        cwd=tmp_path,
        settings={},
    ):
        pass
    assert adapter._attached_sessions == {"mvs-1"}

    await adapter.reset_session("chat-1")

    assert adapter._attached_sessions == set()
    assert adapter._sessions == {}


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
