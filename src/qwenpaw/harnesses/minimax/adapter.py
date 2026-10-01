# -*- coding: utf-8 -*-
"""MiniMax Code implementation of the third-party agent adapter."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import yaml
from acp import RequestError
from acp.schema import (
    AllowedOutcome,
    DeniedOutcome,
    EnvVariable,
    HttpHeader,
    HttpMcpServer,
    McpServerStdio,
    RequestPermissionResponse,
    SseMcpServer,
)

from ...app.approvals import ApprovalRequestSummary, get_approval_service
from ...security.tool_guard.approval import ApprovalDecision
from ...utils.io_utils import read_json, write_json_atomic_async
from ..base import HarnessAdapter, HarnessOperationNotSupportedError
from ..capabilities import HarnessRuntimeCapabilities
from ..events import (
    HarnessAttachment,
    HarnessEvent,
    HarnessEventKind,
    HarnessHistoryItem,
    HarnessHistoryKind,
    HarnessModel,
    HarnessProvider,
)
from .acp_client import McodeAcpClient, encode_model_value
from .discovery import (
    McodeBinaryResolution,
    resolve_mcode_binary_info,
)
from .event_mapper import McodeUpdateNormalizer, MiniMaxEventMapper

logger = logging.getLogger(__name__)

_INSTALL_MESSAGE = (
    "MiniMax Code CLI not found. Install mcode (Node.js >= 24.2) or set "
    "the binary setting / MCODE_BINARY environment variable."
)
_LOGIN_REGION = "global"
_LOGIN_CAPTURE_TIMEOUT_SECONDS = 15.0
_CLI_TIMEOUT_SECONDS = 30.0
# Terminal auth states observed from mcode's auth-state.json. The full
# vocabulary is probed in M0; "authorizing" and "error" are confirmed
# non-authenticated states.
_AUTHENTICATED_STATUSES = frozenset({"authorized", "authenticated"})
_AUTH_STATE_RELATIVE = Path("auth") / "prod"
_OPEN_URL_PATTERN = re.compile(r"^\s*Open:\s*(\S+)")
_USER_CODE_PATTERN = re.compile(r"^\s*Code:\s*(\S+)")
# mcode's ACP permissionMode accepts exactly these values.
_PERMISSION_MODES = frozenset({"default", "auto", "bypassPermissions"})
_SESSION_MODES = frozenset({"plan", "default"})
# After the prompt response resolves, mcode notifications may still be
# draining through the SDK's notification worker; collect until quiet.
_PROMPT_SETTLE_SECONDS = 0.25
# How long to wait for the prompt future to settle after session/cancel.
_CANCEL_SETTLE_SECONDS = 2.0

_MCODE_NOT_FOUND_MESSAGE = "MiniMax Code CLI (mcode) executable was not found."


class MiniMaxAdapter(HarnessAdapter):
    """Run MiniMax Code sessions through one workspace-scoped ACP process."""

    def __init__(
        self,
        state_dir: Path,
        binary: str | None = None,
        client_factory: Callable[..., McodeAcpClient] | None = None,
    ) -> None:
        self._state_dir = state_dir
        self._session_path = state_dir / "minimax_sessions.json"
        self._binary = binary
        self._client_factory = client_factory or McodeAcpClient
        self._client = self._client_factory(binary=binary)
        self._sessions = self._load_sessions()
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._session_registry_lock = asyncio.Lock()
        self._login_process: asyncio.subprocess.Process | None = None
        # ACP sessions attached to the current process generation: mcode
        # allows one client connection per process, so a respawned process
        # must re-attach persisted sessions through session/resume.
        self._attached_sessions: set[str] = set()
        self._attached_connection: Any = None
        # Per-ACP-session approval routing context for the single
        # process-wide permission handler slot.
        self._permission_contexts: dict[str, dict[str, Any]] = {}

    @property
    def capability_unavailable_message(self) -> str | None:
        """Explain why MiniMax capability discovery is unavailable."""
        if not self._client.installed:
            return _INSTALL_MESSAGE
        return None

    async def status(self) -> HarnessProvider:
        """Return mcode installation and authentication status."""
        resolution = self._resolution()
        if resolution is None:
            return HarnessProvider(
                id="minimax",
                name="MiniMax Code",
                available=True,
                installed=False,
                error=_INSTALL_MESSAGE,
            )
        auth_state = self._read_auth_state()
        status_value = str((auth_state or {}).get("status") or "")
        authenticated = status_value in _AUTHENTICATED_STATUSES
        account: dict[str, Any] | None = None
        if auth_state is not None:
            account = {"auth_status": status_value or "unknown"}
        return HarnessProvider(
            id="minimax",
            name="MiniMax Code",
            available=True,
            installed=True,
            authenticated=authenticated,
            account=account,
            runtime_path=str(resolution.path),
            runtime_source=resolution.source,
        )

    async def start_login(self, device_code: bool = False) -> dict[str, Any]:
        """Start mcode's external browser login flow in the background."""
        del device_code
        resolution = self._require_resolution()
        command = (
            f"{resolution.path} login --region {_LOGIN_REGION} --no-browser"
        )
        if (
            self._login_process is None
            or self._login_process.returncode is not None
        ):
            self._login_process = await asyncio.create_subprocess_exec(
                str(resolution.path),
                "login",
                "--region",
                _LOGIN_REGION,
                "--no-browser",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        url, user_code = await self._capture_login_details(
            self._login_process,
        )
        return {
            "type": "external",
            "loginId": f"minimax-{uuid.uuid4().hex}",
            "command": command,
            "url": url,
            "userCode": user_code,
        }

    async def logout(self) -> None:
        """Run mcode logout and capture its output."""
        resolution = self._require_resolution()
        process = await asyncio.create_subprocess_exec(
            str(resolution.path),
            "logout",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            async with asyncio.timeout(_CLI_TIMEOUT_SECONDS):
                stdout, _ = await process.communicate()
        except BaseException:
            if process.returncode is None:
                process.kill()
            await asyncio.shield(process.wait())
            raise
        output = stdout.decode(errors="replace")
        if process.returncode != 0:
            raise RuntimeError(
                f"mcode logout failed ({process.returncode}): "
                f"{output.strip()}",
            )

    async def models(self) -> list[HarnessModel]:
        """Return models advertised by a live session or the local config.

        Advertised ``model`` select values from the mcode session control
        state are canonical (ids round-trip through setConfigOption); when
        none have been captured yet, one ephemeral ACP session probes
        them before falling back to ``config.yaml`` BYOK entries.
        """
        models = self._advertised_models()
        if models:
            return models
        models = await self._probe_advertised_models()
        if models:
            return models
        return self._models_from_config()

    def _advertised_models(self) -> list[HarnessModel]:
        advertised = self._client.model_config_options()
        models = [
            HarnessModel(
                id=str(item["id"]),
                name=str(item["name"]),
                description=str(item.get("description") or ""),
                is_default=bool(item.get("is_default")),
            )
            for item in advertised
            if item.get("id")
        ]
        if models and not any(model.is_default for model in models):
            models[0].is_default = True
        return models

    async def _probe_advertised_models(self) -> list[HarnessModel]:
        """Capture configOptions through one throwaway ACP session."""
        client = self._client
        if not getattr(client, "installed", False):
            return []
        try:
            await client.ensure_started()
            acp_session_id = await client.new_session(str(self._state_dir))
            try:
                return self._advertised_models()
            finally:
                await client.close_session(acp_session_id)
        except Exception as exc:  # noqa: BLE001 - fall back to config.yaml
            logger.debug("mcode model probe failed: %s", exc)
            return []

    async def history(self, session_id: str) -> list[HarnessHistoryItem]:
        """List persisted mcode sessions (v1: list only, no load/replay)."""
        del session_id
        client = self._client
        if not getattr(client, "installed", False):
            return []
        try:
            await client.ensure_started()
            response = await client.list_sessions()
        except Exception as exc:  # noqa: BLE001 - best-effort recovery API
            logger.debug("mcode session/list failed: %s", exc)
            return []
        items: list[HarnessHistoryItem] = []
        for entry in getattr(response, "sessions", None) or []:
            acp_session_id = str(getattr(entry, "session_id", "") or "")
            if not acp_session_id:
                continue
            items.append(
                HarnessHistoryItem(
                    kind=HarnessHistoryKind.MESSAGE,
                    item_id=acp_session_id,
                    text=str(getattr(entry, "title", "") or ""),
                    data={
                        "cwd": str(getattr(entry, "cwd", "") or ""),
                        "updated_at": str(
                            getattr(entry, "updated_at", "") or "",
                        ),
                    },
                ),
            )
        return items

    async def run_command(
        self,
        *,
        session_id: str,
        command: str,
        arguments: str,
        cwd: Path,
        settings: dict[str, Any],
    ) -> list[HarnessEvent]:
        """Reject provider commands until the ACP command surface lands."""
        del session_id, command, arguments, cwd, settings
        raise HarnessOperationNotSupportedError(
            "MiniMax Code commands are not supported yet.",
        )

    async def reset_session(self, session_id: str) -> None:
        """Close the ACP session and forget the QwenPaw mapping."""
        async with self._session_registry_lock:
            acp_session_id = self._sessions.pop(session_id, None)
            self._session_locks.pop(session_id, None)
            await write_json_atomic_async(
                self._session_path,
                self._sessions,
            )
        if acp_session_id:
            self._attached_sessions.discard(acp_session_id)
            self._permission_contexts.pop(acp_session_id, None)
            with contextlib.suppress(Exception):
                # The ACP process may already be gone; the mapping is dropped
                # either way.
                await self._client.close_session(acp_session_id)

    async def run_turn(  # pylint: disable=invalid-overridden-method
        self,
        *,
        session_id: str,
        prompt: str,
        cwd: Path,
        settings: dict[str, Any],
        attachments: list[HarnessAttachment] | None = None,
    ) -> AsyncIterator[HarnessEvent]:
        """Stream one MiniMax Code turn through the owned ACP process."""
        client = self._client
        async with self._lock_for(session_id):
            # mcode rejects a second concurrent prompt on the same session,
            # so each QwenPaw session serializes its own turns here while
            # unrelated sessions keep multiplexing over the one process.
            await client.ensure_started()
            self._track_connection_generation()
            acp_session_id = await self._ensure_acp_session(
                session_id,
                cwd,
                settings,
            )
            await self._apply_settings(acp_session_id, settings)
            prompt_text = self._attachment_text(prompt, cwd, attachments)

            events: asyncio.Queue[HarnessEvent] = asyncio.Queue()
            mapper = MiniMaxEventMapper()
            normalizer = McodeUpdateNormalizer()

            async def listener(acp_sid: str, update: Any) -> None:
                del acp_sid
                for payload in normalizer.normalize(acp_session_id, update):
                    for event in mapper.convert(payload):
                        await events.put(event)

            client.add_listener(acp_session_id, listener)
            request_context = dict(settings.get("_request_context") or {})
            self._permission_contexts[acp_session_id] = {
                "session_id": session_id,
                "ask": self._permission_mode(settings) == "default",
                "agent_id": str(request_context.get("agent_id") or "default"),
                "user_id": str(request_context.get("user_id") or "default"),
                "channel": str(request_context.get("channel") or "console"),
            }
            with contextlib.suppress(AttributeError):
                client.set_permission_handler(self._handle_permission_request)

            prompt_task = asyncio.create_task(
                client.prompt(acp_session_id, prompt_text),
            )
            pending_get: asyncio.Task[HarnessEvent] | None = None
            try:
                pending_get = asyncio.create_task(events.get())
                while True:
                    await asyncio.wait(
                        {pending_get, prompt_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if pending_get.done():
                        yield pending_get.result()
                        pending_get = asyncio.create_task(events.get())
                    if prompt_task.done():
                        break
                # The prompt response resolves before the SDK's
                # notification worker may have finished dispatching the
                # turn's final updates; collect until the queue is quiet.
                while True:
                    try:
                        event = await asyncio.wait_for(
                            pending_get,
                            timeout=_PROMPT_SETTLE_SECONDS,
                        )
                    except (TimeoutError, asyncio.TimeoutError):
                        break
                    yield event
                    pending_get = asyncio.create_task(events.get())
                response = prompt_task.result()
            except asyncio.CancelledError:
                await asyncio.shield(
                    self._cancel_prompt(acp_session_id, prompt_task),
                )
                raise
            except Exception as exc:
                logger.exception("MiniMax Code turn failed")
                yield HarnessEvent(
                    kind=HarnessEventKind.ERROR,
                    text=str(exc) or type(exc).__name__,
                )
                return
            finally:
                if pending_get is not None and not pending_get.done():
                    pending_get.cancel()
                client.remove_listener(acp_session_id, listener)
                self._permission_contexts.pop(acp_session_id, None)

            for event in mapper.finish(
                getattr(response, "stop_reason", None),
            ):
                yield event

    async def stop(self) -> None:
        """Stop the ACP process tree and any pending login process."""
        if self._login_process is not None:
            if self._login_process.returncode is None:
                self._login_process.terminate()
                with contextlib.suppress(ProcessLookupError):
                    await self._login_process.wait()
            self._login_process = None
        self._attached_sessions.clear()
        self._attached_connection = None
        self._permission_contexts.clear()
        await self._client.stop()

    def session_lock(self, session_id: str) -> asyncio.Lock:
        """Return the per-session turn lock (one active prompt per session)."""
        return self._lock_for(session_id)

    def _lock_for(self, session_id: str) -> asyncio.Lock:
        lock = self._session_locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._session_locks[session_id] = lock
        return lock

    # -- run_turn helpers -----------------------------------------------------

    def _track_connection_generation(self) -> None:
        """Forget attached sessions whenever the process was respawned."""
        connection = getattr(self._client, "connection", None)
        if connection is not self._attached_connection:
            self._attached_connection = connection
            self._attached_sessions.clear()

    async def _ensure_acp_session(
        self,
        session_id: str,
        cwd: Path,
        settings: dict[str, Any],
    ) -> str:
        """Return an attached ACP session, resuming or creating as needed."""
        client = self._client
        cwd_text = str(cwd)
        existing = self._sessions.get(session_id)
        if existing:
            if existing in self._attached_sessions:
                return existing
            try:
                await client.resume_session(existing, cwd=cwd_text)
            except Exception as exc:  # noqa: BLE001 - fall back to a new one
                logger.debug(
                    "mcode session/resume %s failed (%s); creating a new "
                    "session",
                    existing,
                    exc,
                )
            else:
                self._attached_sessions.add(existing)
                return existing
        acp_session_id = await client.new_session(
            cwd_text,
            mcp_servers=self._mcp_servers(settings),
        )
        self._sessions[session_id] = acp_session_id
        self._attached_sessions.add(acp_session_id)
        await write_json_atomic_async(self._session_path, self._sessions)
        return acp_session_id

    @staticmethod
    def _mcp_servers(settings: dict[str, Any]) -> list[Any] | None:
        """Project resolved QwenPaw MCP servers onto session/new.

        Mirrors the server filtering of ``qoder/projection.py:mcp_servers``
        (deny-only servers without an explicit tool list are skipped; stdio
        plus streamable-http and sse transports are supported). The ACP
        overlay has no per-tool policy surface, so QwenPaw tool policies
        are not forwarded.
        """
        capabilities = settings.get("_runtime_capabilities")
        if not isinstance(capabilities, HarnessRuntimeCapabilities):
            return None
        servers: list[Any] = []
        for server in capabilities.mcp_servers:
            if server.tools is None and server.default_policy == "deny":
                continue
            if server.transport == "stdio":
                servers.append(
                    McpServerStdio(
                        name=server.name,
                        command=server.command,
                        args=list(server.args),
                        env=[
                            EnvVariable(name=key, value=value)
                            for key, value in sorted(
                                server.revealed_env().items(),
                            )
                        ],
                    ),
                )
            else:
                server_type = (
                    "http"
                    if server.transport == "streamable_http"
                    else "sse"
                )
                model = (
                    HttpMcpServer
                    if server_type == "http"
                    else SseMcpServer
                )
                servers.append(
                    model(
                        type=server_type,
                        name=server.name,
                        url=server.url,
                        headers=[
                            HttpHeader(name=key, value=value)
                            for key, value in sorted(
                                server.revealed_headers().items(),
                            )
                        ],
                    ),
                )
        return servers or None

    async def _apply_settings(
        self,
        acp_session_id: str,
        settings: dict[str, Any],
    ) -> None:
        """Apply per-turn settings through ACP config options.

        ``permissionMode`` is process-scoped in mcode, so it is re-applied
        before every turn; only interleaved concurrent turns on different
        presets could bleed into each other.
        """
        client = self._client
        await client.set_config_option(
            acp_session_id,
            "permissionMode",
            self._permission_mode(settings),
        )
        model = str(settings.get("model") or "").strip()
        if model:
            # settings["model"] already carries the advertised "m:..."
            # select value; it round-trips through setConfigOption as-is.
            await client.set_config_option(acp_session_id, "model", model)
        effort = str(settings.get("reasoning_effort") or "").strip()
        if effort and self._effort_advertised():
            try:
                await client.set_config_option(
                    acp_session_id,
                    "thinkingEffort",
                    effort,
                )
            except RequestError as exc:
                # Effort options are model-specific; mcode answers
                # invalidParams when the selected model lacks them.
                logger.debug(
                    "mcode rejected thinkingEffort %r: %s",
                    effort,
                    exc,
                )
        session_mode = str(settings.get("session_mode") or "").strip()
        if session_mode in _SESSION_MODES:
            # mcode applies mode transitions on the NEXT prompt.
            try:
                await client.set_session_mode(
                    acp_session_id,
                    session_mode,
                )
            except RequestError as exc:
                logger.debug(
                    "mcode rejected session mode %r: %s",
                    session_mode,
                    exc,
                )

    @staticmethod
    def _permission_mode(settings: dict[str, Any]) -> str:
        mode = str(settings.get("permission_mode") or "").strip()
        return mode if mode in _PERMISSION_MODES else "default"

    def _effort_advertised(self) -> bool:
        for option in self._client.config_options or []:
            if str(getattr(option, "id", "") or "") == "thinkingEffort":
                return True
        return False

    @staticmethod
    def _attachment_text(
        prompt: str,
        cwd: Path,
        attachments: list[HarnessAttachment] | None,
    ) -> str:
        """Reference attachments in the prompt (Qoder file-reference path).

        mcode's ACP promptCapabilities.image is false, so every attachment
        is materialized as a workspace-relative @path reference in the
        prompt text instead of inline content blocks.
        """
        if not attachments:
            return prompt
        references = [
            MiniMaxAdapter._file_reference(attachment.path, cwd)
            for attachment in attachments
        ]
        return "\n".join(
            part for part in (" ".join(references), prompt) if part
        )

    @staticmethod
    def _file_reference(path: Path, cwd: Path) -> str:
        """Return a portable @file reference (mirrors the Qoder adapter)."""
        resolved_path = path.expanduser().resolve(strict=False)
        resolved_cwd = cwd.expanduser().resolve(strict=False)
        try:
            display_path = resolved_path.relative_to(resolved_cwd)
        except ValueError:
            display_path = resolved_path
        path_text = display_path.as_posix()
        if any(character.isspace() for character in path_text):
            escaped_path = path_text.replace("\\", "\\\\").replace(
                '"',
                '\\"',
            )
            return f'@"{escaped_path}"'
        return f"@{path_text}"

    async def _cancel_prompt(
        self,
        acp_session_id: str,
        prompt_task: "asyncio.Task[Any]",
    ) -> None:
        """Send session/cancel and wait briefly for the prompt to settle."""
        with contextlib.suppress(Exception):
            await self._client.cancel(acp_session_id)
        if not prompt_task.done():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    asyncio.shield(prompt_task),
                    _CANCEL_SETTLE_SECONDS,
                )

    # -- approval routing ------------------------------------------------------

    async def _handle_permission_request(
        self,
        options: list[Any],
        session_id: str,
        tool_call: Any,
        **_: Any,
    ) -> RequestPermissionResponse:
        """Answer session/request_permission through the preset policy.

        The ``ask`` preset routes eligible permission prompts to the
        QwenPaw approval queue; the auto and full-access presets never
        consult the queue and answer with the most permissive option the
        agent offered.
        """
        context = self._permission_contexts.get(session_id) or {}
        if bool(context.get("ask")):
            decision = await self._route_approval(
                options,
                tool_call,
                context,
            )
        else:
            decision = ApprovalDecision.APPROVED
        if decision == ApprovalDecision.APPROVED:
            selected = _pick_option(options, allow=True)
            if selected is not None:
                return RequestPermissionResponse(
                    outcome=AllowedOutcome(
                        outcome="selected",
                        option_id=selected,
                    ),
                )
        denied = _pick_option(options, allow=False)
        if denied is not None:
            return RequestPermissionResponse(
                outcome=AllowedOutcome(outcome="selected", option_id=denied),
            )
        return RequestPermissionResponse(
            outcome=DeniedOutcome(outcome="cancelled"),
        )

    async def _route_approval(
        self,
        options: list[Any],
        tool_call: Any,
        context: dict[str, Any],
    ) -> ApprovalDecision:
        """Create one approval request and wait for the user's decision."""
        title = str(
            getattr(tool_call, "title", None)
            or getattr(tool_call, "name", None)
            or "tool",
        )
        kind = str(getattr(tool_call, "kind", None) or "")
        tool_call_id = str(
            getattr(tool_call, "tool_call_id", None)
            or getattr(tool_call, "toolCallId", None)
            or "",
        )
        locations = [
            str(getattr(location, "path", "") or "")
            for location in (getattr(tool_call, "locations", None) or [])
        ]
        raw_input = getattr(tool_call, "raw_input", None)
        target = next((path for path in locations if path), "")
        summary = ApprovalRequestSummary(
            source_type="minimax",
            name=f"MiniMax Code {title} call",
            severity="high" if kind == "execute" else "medium",
            result_summary=target or title,
            payload={
                "provider": "minimax",
                "provider_item_id": tool_call_id,
                "title": title,
                "kind": kind,
                "command": (
                    raw_input.get("command")
                    if isinstance(raw_input, dict)
                    else None
                ),
                "locations": locations,
                "raw_input": raw_input,
                "options": [
                    {
                        "optionId": str(
                            getattr(option, "option_id", "") or "",
                        ),
                        "kind": str(getattr(option, "kind", "") or ""),
                        "name": str(getattr(option, "name", "") or ""),
                    }
                    for option in options
                ],
            },
        )
        service = get_approval_service()
        try:
            pending = await service.create_pending_summary(
                session_id=str(context.get("session_id") or "default"),
                root_session_id=str(context.get("session_id") or "default"),
                owner_agent_id=str(context.get("agent_id") or "default"),
                user_id=str(context.get("user_id") or "default"),
                channel=str(context.get("channel") or "console"),
                agent_id=str(context.get("agent_id") or "default"),
                summary=summary,
            )
        except Exception as exc:  # noqa: BLE001 - fail closed on queue errors
            logger.warning("MiniMax approval queue rejected a ask: %s", exc)
            return ApprovalDecision.DENIED
        return await service.wait_for_approval(
            pending.request_id,
            pending.timeout_seconds,
        )

    async def _capture_login_details(
        self,
        process: asyncio.subprocess.Process,
    ) -> tuple[str, str]:
        """Parse ``Open: <url>`` and ``Code: <code>`` from login stderr."""
        url = ""
        user_code = ""
        if process.stderr is None:
            return url, user_code
        while not (url and user_code):
            try:
                async with asyncio.timeout(_LOGIN_CAPTURE_TIMEOUT_SECONDS):
                    raw = await process.stderr.readline()
            except (TimeoutError, asyncio.TimeoutError):
                break
            if not raw:
                break
            line = raw.decode(errors="replace")
            if not url:
                match = _OPEN_URL_PATTERN.match(line)
                if match:
                    url = match.group(1)
            if not user_code:
                match = _USER_CODE_PATTERN.match(line)
                if match:
                    user_code = match.group(1)
        return url, user_code

    def _resolution(self) -> McodeBinaryResolution | None:
        return resolve_mcode_binary_info(self._binary)

    def _require_resolution(self) -> McodeBinaryResolution:
        resolution = self._resolution()
        if resolution is None:
            raise RuntimeError(_MCODE_NOT_FOUND_MESSAGE)
        return resolution

    @staticmethod
    def _data_dir(environ: dict[str, str] | None = None) -> Path:
        environment = environ if environ is not None else dict(os.environ)
        override = str(
            environment.get("MINIMAX_DATA_DIR")
            or environment.get("MAVIS_DATA_DIR")
            or "",
        ).strip()
        if override:
            return Path(override).expanduser()
        return Path.home() / ".minimax"

    @classmethod
    def _read_auth_state(
        cls,
        environ: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        """Read the freshest mcode auth-state.json across regions."""
        root = cls._data_dir(environ)
        for region in ("en", "cn"):
            path = (
                root
                / _AUTH_STATE_RELATIVE
                / region
                / "mcode-public"
                / "auth-state.json"
            )
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                return payload
        return None

    @classmethod
    def _models_from_config(
        cls,
        environ: dict[str, str] | None = None,
    ) -> list[HarnessModel]:
        """Parse BYOK models from ``config.yaml`` (fallback source)."""
        path = cls._data_dir(environ) / "config.yaml"
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            return []
        if not isinstance(payload, dict):
            return []
        default_model = str(payload.get("defaultModel") or "")
        providers = payload.get("custom_provider")
        if not isinstance(providers, dict):
            return []
        models: list[HarnessModel] = []
        for raw_provider_id, provider_config in providers.items():
            if not isinstance(provider_config, dict):
                continue
            provider_models = provider_config.get("models")
            if not isinstance(provider_models, dict):
                continue
            provider_id = str(raw_provider_id)
            for raw_model_id, model_config in provider_models.items():
                if (
                    isinstance(model_config, dict)
                    and model_config.get("enabled") is False
                ):
                    continue
                model_id = str(raw_model_id)
                # Canonical ids are mcode's own select values so that a
                # fallback-sourced id round-trips through setConfigOption.
                entry_id = encode_model_value(provider_id, model_id)
                description = (
                    str(model_config.get("description") or "")
                    if isinstance(model_config, dict)
                    else ""
                )
                models.append(
                    HarnessModel(
                        id=entry_id,
                        name=model_id,
                        description=description,
                        is_default=f"{provider_id}/{model_id}" == default_model,
                        reasoning_efforts=cls._effort_options(model_config),
                    ),
                )
        return models

    @staticmethod
    def _effort_options(model_config: Any) -> list[str]:
        if not isinstance(model_config, dict):
            return []
        thinking = model_config.get("thinking")
        if not isinstance(thinking, dict):
            return []
        efforts = thinking.get("effortOptions")
        if isinstance(efforts, dict):
            return [str(key) for key in efforts if key]
        if isinstance(efforts, list):
            return [str(item) for item in efforts if item]
        return []

    def _load_sessions(self) -> dict[str, str]:
        try:
            payload = read_json(self._session_path)
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return {
            str(key): str(value)
            for key, value in payload.items()
            if key and value
        }


def _pick_option(options: list[Any], *, allow: bool) -> str | None:
    """Return the preferred optionId from an mcode permission request.

    Option ids are agent-chosen, so the protocol ``kind`` carries the
    stable semantics; kebab-case ids are the mcode fallback.
    """
    if allow:
        kinds = ("allow_always", "allow_once")
        preferred_ids = ("allow-always", "allow-once")
    else:
        kinds = ("reject_once", "reject_always")
        preferred_ids = ("deny", "reject-once")
    for kind in kinds:
        for option in options:
            if str(getattr(option, "kind", "") or "") == kind:
                return str(getattr(option, "option_id", "") or "") or None
    for option_id in preferred_ids:
        for option in options:
            if (
                str(getattr(option, "option_id", "") or "").lower()
                == option_id
            ):
                return str(getattr(option, "option_id", "") or "")
    return None


__all__ = ["MiniMaxAdapter"]
