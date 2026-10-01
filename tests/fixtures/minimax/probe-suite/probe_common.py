"""Shared harness for live mcode 0.5.10 ACP contract probes.

Pattern mirrors qwenpaw's own ACP subsystem (qwenpaw/agents/acp/service.py::_open_conversation):
  spawn_agent_process(client, command, *args, cwd=..., env=...) ->
  conn.initialize(protocol_version=acp.PROTOCOL_VERSION,
                  capabilities=ClientCapabilities(),
                  client_info=Implementation(name=..., version=...)) ->
  conn.new_session(cwd=...)

Raw JSON-RPC frames are captured verbatim (both directions) via the SDK's
StreamObserver hook and written as JSONL: one file per probe in fixtures/.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import os
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import acp
from acp import PROTOCOL_VERSION, spawn_agent_process, text_block
from acp.connection import StreamDirection
from acp.exceptions import RequestError
from acp.schema import (
    AllowedOutcome,
    ClientCapabilities,
    DeniedOutcome,
    Implementation,
    ReadTextFileResponse,
    RequestPermissionResponse,
    WriteTextFileResponse,
)

PROBE_DIR = Path("/root/mcode-probes")
FIXTURES = PROBE_DIR / "fixtures"
STATE = PROBE_DIR / "state"
MCODE = "/usr/local/bin/mcode"
NODE_BIN_DIR = "/opt/node24/bin"
RPC_TIMEOUT = 120.0  # seconds, per task spec

FIXTURES.mkdir(parents=True, exist_ok=True)
STATE.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- env


def build_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Env for the mcode process: keep HOME (auth state), prepend node24 for npx."""
    env = {
        "HOME": os.environ.get("HOME", "/root"),
        "PATH": NODE_BIN_DIR + ":" + os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "TERM": os.environ.get("TERM", "xterm-256color"),
        "USER": os.environ.get("USER", "root"),
    }
    if extra:
        env.update(extra)
    return env


# ------------------------------------------------------------------- frame capture


class FrameRecorder:
    """StreamObserver that persists every raw JSON-RPC frame verbatim as JSONL."""

    def __init__(self, fixture_path: Path, append: bool = False) -> None:
        self.path = fixture_path
        self.fh = self.path.open("a" if append else "w", encoding="utf-8")
        self.seq = 0
        self.frames: list[dict[str, Any]] = []

    def observe(self, event) -> None:
        self.seq += 1
        direction = "c2a" if event.direction is StreamDirection.OUTGOING else "a2c"
        rec = {
            "seq": self.seq,
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "dir": direction,
            "frame": event.message,
        }
        self.frames.append(rec)
        self.fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.fh.flush()

    def frames_since(self, seq: int) -> list[dict[str, Any]]:
        return [f for f in self.frames if f["seq"] > seq]

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.fh.close()


# ------------------------------------------------------------------- probe client


@dataclass
class PermissionRequestRecord:
    options: list[dict[str, Any]]
    tool_call: dict[str, Any] | None
    answered_with: dict[str, Any]


class ProbeClient:
    """acp Client implementation: records everything, policy-driven permissions."""

    def __init__(self) -> None:
        self.updates: list[Any] = []  # parsed sessionUpdate payloads
        self.permission_requests: list[PermissionRequestRecord] = []
        # "allow_once" | "allow_always" | "reject_once" | "reject_always" | "cancel"
        self.permission_policy = "reject_once"
        self.fs_events: list[dict[str, Any]] = []
        self.ext_methods: list[dict[str, Any]] = []

    # -- notifications -------------------------------------------------------
    async def session_update(self, session_id: str, update: Any, **kwargs: Any) -> None:
        self.updates.append(update)

    # -- server->client requests ---------------------------------------------
    async def request_permission(
        self, options: Any, session_id: str, tool_call: Any = None, **kwargs: Any
    ) -> RequestPermissionResponse:
        opt_dicts = [o.model_dump(by_alias=True, exclude_none=True) for o in options]
        tc = tool_call.model_dump(by_alias=True, exclude_none=True) if tool_call is not None else None

        policy = self.permission_policy
        outcome: AllowedOutcome | DeniedOutcome
        if policy == "cancel":
            outcome = DeniedOutcome(outcome="cancelled")
        else:
            want_kind = policy
            match = next((o for o in options if getattr(o, "kind", None) == want_kind), None)
            if match is None:  # fall back to any option of the same allow/reject family
                family = "allow" if want_kind.startswith("allow") else "reject"
                match = next(
                    (o for o in options if str(getattr(o, "kind", "")).startswith(family)), None
                )
            if match is None:
                outcome = DeniedOutcome(outcome="cancelled")
            else:
                outcome = AllowedOutcome(outcome="selected", option_id=match.option_id)

        answered = outcome.model_dump(by_alias=True, exclude_none=True)
        self.permission_requests.append(
            PermissionRequestRecord(options=opt_dicts, tool_call=tc, answered_with=answered)
        )
        return RequestPermissionResponse(outcome=outcome)

    async def write_text_file(
        self, content: str, path: str, session_id: str, **kwargs: Any
    ) -> WriteTextFileResponse:
        self.fs_events.append({"op": "fs_write_text_file", "path": path, "content": content})
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return WriteTextFileResponse()

    async def read_text_file(
        self,
        path: str,
        session_id: str,
        limit: int | None = None,
        line: int | None = None,
        **kwargs: Any,
    ) -> ReadTextFileResponse:
        self.fs_events.append({"op": "fs_read_text_file", "path": path, "limit": limit, "line": line})
        p = Path(path)
        content = p.read_text(encoding="utf-8") if p.exists() else ""
        return ReadTextFileResponse(content=content)

    # terminal_* methods are optional routes with defaults; no-ops are fine
    async def create_terminal(self, *a: Any, **k: Any) -> None:
        return None

    async def terminal_output(self, *a: Any, **k: Any) -> None:
        return None

    async def release_terminal(self, *a: Any, **k: Any) -> None:
        return None

    async def wait_for_terminal_exit(self, *a: Any, **k: Any) -> None:
        return None

    async def kill_terminal(self, *a: Any, **k: Any) -> None:
        return None

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.ext_methods.append({"method": method, "params": params})
        raise RequestError.method_not_found(method)

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        self.ext_methods.append({"notification": method, "params": params})

    def on_connect(self, conn: Any) -> None:
        pass


# ------------------------------------------------------------------- probe context


@dataclass
class ProbeContext:
    conn: Any
    process: Any
    client: ProbeClient
    recorder: FrameRecorder

    async def shutdown(self) -> None:
        """Hard-kill the mcode process (mcode allows exactly one client per process)."""
        p = self.process
        if p is not None and p.returncode is None:
            with contextlib.suppress(Exception):
                p.kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(p.wait(), 5)


class _ProbeRun:
    """One spawned mcode process + recorder; guarantees cleanup."""

    def __init__(
        self,
        fixture_name: str,
        extra_env: dict[str, str] | None = None,
        append: bool = False,
    ) -> None:
        self.fixture_path = FIXTURES / fixture_name
        suffix = ".stderr.log" if not append else ".append-%d.stderr.log" % int(__import__("time").time())
        self.stderr_path = FIXTURES / fixture_name.replace(".json", suffix)
        self.recorder = FrameRecorder(self.fixture_path, append=append)
        self.client = ProbeClient()
        self.extra_env = extra_env
        self.process: Any = None
        self._stderr_fh = self.stderr_path.open("wb")
        self._drain_task: asyncio.Task | None = None

    async def __aenter__(self) -> ProbeContext:
        transport_cm = spawn_agent_process(
            self.client,
            MCODE,
            "acp",
            cwd=str(PROBE_DIR),
            env=build_env(self.extra_env),
            transport_kwargs={"limit": 16 * 1024 * 1024},
            observers=[self.recorder.observe],
        )
        self.conn, self.process = await transport_cm.__aenter__()
        self._transport_cm = transport_cm
        if self.process.stderr is not None:
            self._drain_task = asyncio.create_task(self._drain_stderr())
        return ProbeContext(conn=self.conn, process=self.process, client=self.client, recorder=self.recorder)

    async def _drain_stderr(self) -> None:
        try:
            while True:
                chunk = await self.process.stderr.read(4096)
                if not chunk:
                    break
                self._stderr_fh.write(chunk)
                self._stderr_fh.flush()
        except Exception:
            pass

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # 1) explicit kill per task requirement
        if self.process is not None and self.process.returncode is None:
            with contextlib.suppress(Exception):
                self.process.kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.process.wait(), 5)
        # 2) let the SDK transport do its own graceful shutdown
        with contextlib.suppress(Exception):
            await self._transport_cm.__aexit__(exc_type, exc, tb)
        if self._drain_task is not None:
            self._drain_task.cancel()
            with contextlib.suppress(Exception):
                await self._drain_task
        self._stderr_fh.close()
        self.recorder.close()


def open_probe(
    fixture_name: str,
    extra_env: dict[str, str] | None = None,
    append: bool = False,
) -> _ProbeRun:
    return _ProbeRun(fixture_name, extra_env, append=append)


# ------------------------------------------------------------------- helpers


async def initialize(conn: Any, probe_name: str) -> Any:
    return await asyncio.wait_for(
        conn.initialize(
            protocol_version=PROTOCOL_VERSION,
            capabilities=ClientCapabilities(),
            client_info=Implementation(name=f"mcode-probe-{probe_name}", version="0.1.0"),
        ),
        RPC_TIMEOUT,
    )


async def wf(coro: Any, label: str, timeout: float = RPC_TIMEOUT) -> Any:
    """wait_for wrapper that tags timeouts with the RPC label."""
    try:
        return await asyncio.wait_for(coro, timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"{label} timed out after {timeout}s") from None


def error_obj(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, RequestError):
        return {"type": "RequestError", "code": exc.code, "message": str(exc), "data": exc.data}
    return {"type": type(exc).__name__, "message": str(exc)}


def dump(model: Any) -> Any:
    """Pydantic model -> wire-shaped dict (camelCase aliases, no Nones). Handles lists."""
    if model is None:
        return None
    if isinstance(model, (list, tuple)):
        return [dump(m) for m in model]
    if hasattr(model, "model_dump"):
        return model.model_dump(by_alias=True, exclude_none=True)
    return model


def update_payload(update: Any) -> dict:
    """Payload fields of a sessionUpdate model.

    Wire shape (mcode live): {"sessionUpdate": "agent_message_chunk", <fields...>}
    i.e. the discriminator is a flat string tag with payload fields as siblings.
    """
    d = dump(update)
    if not isinstance(d, dict):
        return {}
    tag = d.get("sessionUpdate")
    if isinstance(tag, str):
        return {k: v for k, v in d.items() if k != "sessionUpdate"}
    if isinstance(tag, dict):
        return tag
    return d


_KNOWN = (
    "agent_message_chunk", "user_message_chunk", "agent_thought_chunk", "tool_call",
    "plan", "available_commands_update", "current_mode_update", "config_option_update",
    "session_info_update", "usage_update",
)


def update_type(update: Any) -> str:
    """Discriminator value of a sessionUpdate payload, e.g. 'agent_message_chunk'."""
    d = dump(update)
    if isinstance(d, dict):
        tag = d.get("sessionUpdate")
        if isinstance(tag, str):
            return tag
        if isinstance(tag, dict):
            for k in _KNOWN:
                if k in tag:
                    return k
            return next(iter(tag.keys()), "unknown")
    return "unknown"


def write_summary(name: str, obj: dict[str, Any]) -> None:
    (STATE / name).write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def flatten_select_options(select_dict: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten SessionConfigSelect options (plain or grouped) -> [{value,name,description,_meta}]."""
    out: list[dict[str, Any]] = []
    for opt in select_dict.get("options", []) or []:
        if "group" in opt and "options" in opt:
            for sub in opt.get("options", []):
                out.append(sub)
        else:
            out.append(opt)
    return out


def find_config_option(config_options: list[Any] | None, config_id: str) -> dict[str, Any] | None:
    if not config_options:
        return None
    for opt in config_options:
        d = dump(opt) if hasattr(opt, "model_dump") else opt
        if isinstance(d, dict) and d.get("configId") == config_id or (
            isinstance(d, dict) and d.get("id") == config_id
        ):
            return d
    return None


async def run_probe(main_coro_fn, summary_name: str) -> bool:
    """Execute a probe, always write a summary (even on error), never raise."""
    summary: dict[str, Any] = {"probe": summary_name, "ok": False}
    try:
        result = await main_coro_fn()
        summary.update(result or {})
        summary["ok"] = True
    except Exception as exc:  # noqa: BLE001
        summary["ok"] = False
        summary["error"] = error_obj(exc)
        summary["traceback"] = traceback.format_exc()
    write_summary(summary_name.replace(".json", "") + ".summary.json", summary)
    print(f"[{summary_name}] ok={summary['ok']}" + ("" if summary["ok"] else f" error={summary.get('error')}"))
    return summary["ok"]
