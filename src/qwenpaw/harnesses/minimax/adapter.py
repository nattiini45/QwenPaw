# -*- coding: utf-8 -*-
"""MiniMax Code implementation of the third-party agent adapter."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import yaml

from ...utils.io_utils import read_json, write_json_atomic_async
from ..base import HarnessAdapter, HarnessOperationNotSupportedError
from ..events import (
    HarnessAttachment,
    HarnessEvent,
    HarnessHistoryItem,
    HarnessModel,
    HarnessProvider,
)
from .acp_client import McodeAcpClient
from .discovery import (
    McodeBinaryResolution,
    resolve_mcode_binary_info,
)

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

        M1 stub: reads the configOptions captured from an mcode ACP session
        control state when one is live, and falls back to parsing
        ``config.yaml`` BYOK ``custom_provider`` entries plus ``defaultModel``.
        """
        advertised = self._client.model_config_options()
        if advertised:
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
        return self._models_from_config()

    async def history(self, session_id: str) -> list[HarnessHistoryItem]:
        """Return nothing yet; session/list recovery lands in M2."""
        del session_id
        return []

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
            with contextlib.suppress(Exception):
                # The ACP process may already be gone; the mapping is dropped
                # either way.
                await self._client.close_session(acp_session_id)

    def run_turn(
        self,
        *,
        session_id: str,
        prompt: str,
        cwd: Path,
        settings: dict[str, Any],
        attachments: list[HarnessAttachment] | None = None,
    ) -> AsyncIterator[HarnessEvent]:
        """Stream one MiniMax Code turn; implemented in milestone M2."""
        del session_id, prompt, cwd, settings, attachments
        raise NotImplementedError(
            "MiniMaxAdapter.run_turn lands in milestone M2.",
        )

    async def stop(self) -> None:
        """Stop the ACP process tree and any pending login process."""
        if self._login_process is not None:
            if self._login_process.returncode is None:
                self._login_process.terminate()
                with contextlib.suppress(ProcessLookupError):
                    await self._login_process.wait()
            self._login_process = None
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
                entry_id = f"{provider_id}/{model_id}"
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
                        is_default=entry_id == default_model,
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


__all__ = ["MiniMaxAdapter"]
