# -*- coding: utf-8 -*-
"""ACP client for the MiniMax Code CLI (``mcode acp``).

One persistent ``mcode acp`` subprocess per adapter instance. MiniMax Code
accepts exactly one client connection per process, so this client owns the
single connection and multiplexes ACP sessions over it.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from typing import Any
from urllib.parse import quote, unquote

import psutil
from acp import PROTOCOL_VERSION, RequestError, spawn_agent_process, text_block
from acp.schema import (
    ClientCapabilities,
    ConfigOptionUpdate,
    DeniedOutcome,
    Implementation,
    RequestPermissionResponse,
)

from .discovery import (
    McodeBinaryResolution,
    resolve_mcode_binary_info,
)

__all__ = [
    "McodeAcpClient",
    "McodeAcpError",
    "decode_model_value",
    "encode_model_value",
]

_CLIENT_INFO = Implementation(
    name="qwenpaw-minimax-harness",
    title="QwenPaw",
    version="0.1.0",
)
MODEL_CONFIG_ID = "model"
_DOUBLE_INIT_MESSAGE = (
    "The mcode ACP process is already initialized; MiniMax Code allows "
    "exactly one client connection per process."
)

NotificationListener = Callable[[str, Any], Awaitable[None]]
PermissionHandler = Callable[
    [list[Any], str, Any],
    Awaitable[RequestPermissionResponse],
]


class McodeAcpError(RuntimeError):
    """Raised when the mcode ACP process cannot satisfy a request."""


def encode_model_value(
    provider: str,
    model: str,
    variant: str | None = None,
) -> str:
    """Encode provider/model/variant into mcode's select value format.

    Mirrors ``modelConfigValue`` in minimax-code ``control-state.ts``:
    ``m:<provider>:<model>:u`` for models without variants, otherwise
    ``m:<provider>:<model>:v:<variant>``. An empty variant (``m:p:m:v:``
    selects the base model of a variant-capable model, as observed live
    from mcode 0.5.10). Every part is URL-encoded so ``:`` cannot break
    the structure.
    """
    provider_text = str(provider or "").strip()
    model_text = str(model or "").strip()
    if not provider_text or not model_text:
        raise ValueError("provider and model are required")
    encoded = "m:" + ":".join(
        quote(part, safe="") for part in (provider_text, model_text)
    )
    if variant is None:
        return f"{encoded}:u"
    return f"{encoded}:v:{quote(str(variant).strip(), safe='')}"


def decode_model_value(value: str) -> tuple[str, str, str | None]:
    """Decode an mcode model select value into (provider, model, variant).

    The variant is ``None`` for ``:u`` values and a possibly empty string
    for ``:v:`` values (empty = base model without the variant applied).
    """
    if not isinstance(value, str) or not value.startswith("m:"):
        raise ValueError(f"Invalid MiniMax model config value: {value!r}")
    parts = value[2:].split(":")
    if len(parts) == 3 and parts[2] == "u" and parts[0] and parts[1]:
        return unquote(parts[0]), unquote(parts[1]), None
    if len(parts) == 4 and parts[2] == "v" and parts[0] and parts[1]:
        return unquote(parts[0]), unquote(parts[1]), unquote(parts[3])
    raise ValueError(f"Invalid MiniMax model config value: {value!r}")


def _kill_process_tree(pid: int) -> None:
    """Recursively kill a process and all its descendants."""
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    for child in children:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    try:
        parent.kill()
    except psutil.NoSuchProcess:
        pass


class McodeAcpClient:
    """Own one ``mcode acp`` subprocess and multiplex ACP sessions."""

    def __init__(
        self,
        binary: str | None = None,
        *,
        environ: dict[str, str] | None = None,
    ) -> None:
        self._binary = binary
        self._environ = environ
        self._conn: Any = None
        self._process: Any = None
        self._exit_stack: AsyncExitStack | None = None
        self._sessions: dict[str, str] = {}
        self._listeners: dict[str, list[NotificationListener]] = {}
        self._permission_handler: PermissionHandler | None = None
        self._config_options: list[Any] | None = None
        self._start_lock = asyncio.Lock()

    # -- lifecycle ---------------------------------------------------------

    @property
    def installed(self) -> bool:
        """Return whether an mcode executable can be resolved."""
        return self.binary_resolution is not None

    @property
    def running(self) -> bool:
        """Return whether the owned process is still alive."""
        return self._process is not None and self._process.returncode is None

    @property
    def binary_resolution(self) -> McodeBinaryResolution | None:
        """Return the current executable resolution and source."""
        return resolve_mcode_binary_info(
            self._binary,
            environ=self._environ,
        )

    @property
    def connection(self) -> Any:
        """Return the live ClientSideConnection, if any."""
        return self._conn

    @property
    def sessions(self) -> dict[str, str]:
        """Return known ACP session ids mapped to their working directories."""
        return dict(self._sessions)

    async def start(self) -> None:
        """Spawn and initialize the process exactly once.

        MiniMax Code closes any second client connection, so double
        initialization is a hard error rather than a silent respawn.
        """
        async with self._start_lock:
            if self.running:
                raise McodeAcpError(_DOUBLE_INIT_MESSAGE)
            await self._spawn()

    async def ensure_started(self) -> None:
        """Spawn the process if it is not alive yet; respawn after death."""
        async with self._start_lock:
            if self.running:
                return
            await self._cleanup_dead_process()
            await self._spawn()

    async def stop(self) -> None:
        """Close the connection and terminate the whole process tree."""
        async with self._start_lock:
            process = self._process
            await self._teardown()
        if process is not None:
            _kill_process_tree(process.pid)

    async def respawn(self) -> None:
        """Force a fresh process, dropping the previous connection."""
        async with self._start_lock:
            process = self._process
            await self._teardown()
            await self._cleanup_dead_process()
            await self._spawn()
        if process is not None:
            _kill_process_tree(process.pid)

    # -- session operations ------------------------------------------------

    async def new_session(
        self,
        cwd: str,
        *,
        mcp_servers: list[Any] | None = None,
    ) -> str:
        """Create one ACP session and remember its working directory."""
        conn = self._require_connection()
        response = await conn.new_session(cwd=cwd, mcp_servers=mcp_servers)
        session_id = str(getattr(response, "session_id", "") or "")
        if not session_id:
            raise McodeAcpError("mcode did not return a session id")
        self._sessions[session_id] = str(cwd)
        # mcode advertises its config options inline in session/new.
        self._capture_config_options(response)
        return session_id

    async def resume_session(
        self,
        session_id: str,
        *,
        cwd: str | None = None,
    ) -> str:
        """Re-attach a persisted session after a process restart."""
        conn = self._require_connection()
        session_cwd = cwd or self._sessions.get(session_id) or os.getcwd()
        response = await conn.resume_session(
            cwd=session_cwd,
            session_id=session_id,
        )
        self._sessions[session_id] = session_cwd
        self._capture_config_options(response)
        return session_id

    async def close_session(self, session_id: str) -> None:
        """Close one ACP session and forget it."""
        self._sessions.pop(session_id, None)
        self._listeners.pop(session_id, None)
        if not self.running:
            return
        try:
            await self._conn.close_session(session_id=session_id)
        except RequestError:
            # The session may already be gone after a process restart.
            return

    async def prompt(
        self,
        session_id: str,
        prompt: str | list[Any],
    ) -> Any:
        """Send one prompt; stream results via registered listeners."""
        conn = self._require_connection()
        blocks = (
            [text_block(prompt)] if isinstance(prompt, str) else list(prompt)
        )
        return await conn.prompt(prompt=blocks, session_id=session_id)

    async def cancel(self, session_id: str) -> None:
        """Request cancellation of the active prompt in one session."""
        conn = self._require_connection()
        await conn.cancel(session_id=session_id)

    async def set_config_option(
        self,
        session_id: str,
        config_id: str,
        value: str | bool,
    ) -> Any:
        """Apply one session config option (permissionMode/model/...)."""
        conn = self._require_connection()
        return await conn.set_config_option(
            config_id=config_id,
            session_id=session_id,
            value=value,
        )

    async def set_session_mode(self, session_id: str, mode_id: str) -> Any:
        """Switch the session mode (default/plan)."""
        conn = self._require_connection()
        return await conn.set_session_mode(
            mode_id=mode_id,
            session_id=session_id,
        )

    async def list_sessions(
        self,
        *,
        cursor: str | None = None,
        cwd: str | None = None,
    ) -> Any:
        """List persisted mcode sessions for history and recovery."""
        conn = self._require_connection()
        return await conn.list_sessions(cursor=cursor, cwd=cwd)

    # -- raw transport helpers ----------------------------------------------

    async def request(self, method: str, params: Any = None) -> Any:
        """Send one raw JSON-RPC request over the live connection."""
        conn = self._require_connection()
        return await conn._conn.send_request(method, params)  # noqa: SLF001

    async def notify(self, method: str, params: Any = None) -> None:
        """Send one raw JSON-RPC notification over the live connection."""
        conn = self._require_connection()
        await conn._conn.send_notification(method, params)  # noqa: SLF001

    # -- listeners and handlers ----------------------------------------------

    def add_listener(
        self,
        session_id: str,
        listener: NotificationListener,
    ) -> None:
        """Register one update listener for a session."""
        self._listeners.setdefault(session_id, []).append(listener)

    def remove_listener(
        self,
        session_id: str,
        listener: NotificationListener,
    ) -> None:
        """Unregister one update listener for a session."""
        listeners = self._listeners.get(session_id)
        if not listeners:
            return
        self._listeners[session_id] = [
            item for item in listeners if item != listener
        ]
        if not self._listeners[session_id]:
            self._listeners.pop(session_id, None)

    def set_permission_handler(self, handler: PermissionHandler | None) -> None:
        """Route server-initiated permission requests to one handler."""
        self._permission_handler = handler

    @property
    def config_options(self) -> list[Any] | None:
        """Return the latest advertised session config options."""
        return self._config_options

    def model_config_options(self) -> list[dict[str, Any]]:
        """Flatten the advertised model select options, if any."""
        options: list[dict[str, Any]] = []
        for item in self._config_options or []:
            if str(getattr(item, "id", "") or "") != MODEL_CONFIG_ID:
                continue
            current_value = str(getattr(item, "currentValue", "") or "")
            for entry in getattr(item, "options", None) or []:
                for select in self._flatten_select_entry(entry):
                    value = str(getattr(select, "value", "") or "")
                    if not value:
                        continue
                    options.append(
                        {
                            "id": value,
                            "name": str(getattr(select, "name", "") or value),
                            "description": str(
                                getattr(select, "description", "") or "",
                            ),
                            "is_default": value == current_value,
                        },
                    )
        return options

    # -- ACP client callbacks invoked by the SDK ----------------------------

    async def session_update(
        self,
        session_id: str,
        update: Any,
        **_: Any,
    ) -> None:
        """Fan one session/update notification out to registered listeners."""
        if isinstance(update, ConfigOptionUpdate):
            self._config_options = list(update.config_options)
        for listener in list(self._listeners.get(session_id, ())):
            await listener(session_id, update)

    async def request_permission(
        self,
        options: list[Any],
        session_id: str,
        tool_call: Any,
        **_: Any,
    ) -> RequestPermissionResponse:
        """Answer a permission request through the settable handler."""
        handler = self._permission_handler
        if handler is None:
            # Fail closed: no handler means no authorization to proceed.
            return RequestPermissionResponse(
                outcome=DeniedOutcome(outcome="cancelled"),
            )
        return await handler(options, session_id, tool_call)

    async def ext_method(
        self,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Reject unsupported ACP extension requests."""
        del params
        raise RequestError(
            code=-32601,
            message=f"Unsupported ACP extension method: {method}",
        )

    async def ext_notification(
        self,
        method: str,
        params: dict[str, Any],
    ) -> None:
        """Ignore unsupported ACP extension notifications."""
        del method, params

    def on_connect(self, conn: Any) -> None:  # noqa: ARG002
        """Accept the connection handshake hook."""

    # -- internals -----------------------------------------------------------

    async def _spawn(self) -> None:
        resolution = self.binary_resolution
        if resolution is None:
            raise McodeAcpError(
                "MiniMax Code CLI (mcode) was not found. Install mcode or "
                "set the binary setting / MCODE_BINARY.",
            )
        # Imported lazily: pulls the QwenPaw config loader only when a
        # process is actually spawned, mirroring agents/acp/service.py.
        from ...agents.acp.node_runtime import build_acp_process_env

        exit_stack = AsyncExitStack()
        try:
            environment = build_acp_process_env(
                dict(self._environ or os.environ),
            )
            command = self._resolve_command(str(resolution.path), environment)
            conn, process = await exit_stack.enter_async_context(
                spawn_agent_process(
                    self,
                    command,
                    "acp",
                    env=environment,
                    # mcode emits large single-line JSON frames (skills/command
                    # rosters, history replay); the asyncio default 64 KiB
                    # readline limit kills the receive loop (LimitOverrunError,
                    # verified live 2026-10-01). Mirror the delegated-agent
                    # path's stdio_buffer_limit_bytes default (50 MiB).
                    transport_kwargs={"limit": 50 * 1024 * 1024},
                ),
            )
            initialized = await conn.initialize(
                protocol_version=PROTOCOL_VERSION,
                capabilities=ClientCapabilities(),
                client_info=_CLIENT_INFO,
            )
            if initialized.protocol_version != PROTOCOL_VERSION:
                raise McodeAcpError(
                    f"Protocol mismatch: {initialized.protocol_version}",
                )
        except Exception:
            await exit_stack.aclose()
            raise
        self._exit_stack = exit_stack
        self._conn = conn
        self._process = process

    @staticmethod
    def _resolve_command(command: str, env: dict[str, str]) -> str:
        path = next(
            (value for key, value in env.items() if key.lower() == "path"),
            None,
        )
        return shutil.which(command, path=path) or command

    async def _teardown(self) -> None:
        exit_stack = self._exit_stack
        self._exit_stack = None
        self._conn = None
        self._process = None
        if exit_stack is not None:
            await exit_stack.aclose()

    async def _cleanup_dead_process(self) -> None:
        await self._teardown()

    def _require_connection(self) -> Any:
        if not self.running or self._conn is None:
            raise McodeAcpError(
                "The mcode ACP process is not running; call ensure_started() "
                "first.",
            )
        return self._conn

    @staticmethod
    def _flatten_select_entry(entry: Any) -> list[Any]:
        """Return select options, descending into grouped entries."""
        group_options = getattr(entry, "options", None)
        group_name = getattr(entry, "group", None)
        if group_name is not None and group_options is not None:
            return list(group_options)
        return [entry]

    def _capture_config_options(self, response: Any) -> None:
        """Record config options advertised by a session response."""
        options = getattr(response, "config_options", None)
        if options:
            self._config_options = list(options)
