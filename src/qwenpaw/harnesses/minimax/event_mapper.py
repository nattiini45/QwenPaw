# -*- coding: utf-8 -*-
"""Translate MiniMax Code ACP notifications into provider-neutral events.

Two stages, mirroring ``agents/acp/client.py`` (ACPHostedClient) for the
normalization and ``qoder/event_mapper.py`` for the mapping:

1. :class:`McodeUpdateNormalizer` collapses the typed ACP session updates
   that :class:`~qwenpaw.harnesses.minimax.acp_client.McodeAcpClient`
   delivers into the small payload dictionaries an ACPHostedClient-style
   callback emits (the shapes recorded verbatim in
   ``tests/fixtures/minimax/acp_probe_session*.jsonl``)::

       {"type": "text" | "thought", "text": str, "is_chunk": bool}
       {"type": "tool_start" | "tool_update" | "tool_end", "name": str,
        "call_id": str, "title": str, "kind": str, "status": str,
        "detail"?: str, "target"?: str, "summary"?: str}
       {"type": "usage", "used": int, "size": int, "cost"?: dict | None}

2. :class:`MiniMaxEventMapper` turns those payload dictionaries into
   :class:`~qwenpaw.harnesses.events.HarnessEvent` streams, including the
   usage/cost data mcode reports in ``usage_update`` notifications (the
   ``PromptResponse.usage`` field is always ``null`` live).
"""

from __future__ import annotations

from typing import Any

from acp import session_notification
from acp.contrib.session_state import SessionAccumulator
from acp.schema import (
    AgentMessageChunk,
    AgentThoughtChunk,
    ConfigOptionUpdate,
    ToolCallProgress,
    ToolCallStart,
    UsageUpdate,
)

from ..events import HarnessEvent, HarnessEventKind

_TERMINAL_TOOL_STATUSES = frozenset({"completed", "failed"})


class McodeUpdateNormalizer:
    """Collapse typed ACP session updates into mcode payload dictionaries."""

    def __init__(self) -> None:
        self._accumulator = SessionAccumulator()
        self._assistant_text = ""

    def normalize(self, session_id: str, update: Any) -> list[dict[str, Any]]:
        """Return the payload dictionaries produced by one session update."""
        if isinstance(update, dict):
            # Already-normalized payload dictionaries (fixture replay and
            # fakes) pass through untouched.
            return [update] if update.get("type") else []
        if isinstance(update, AgentMessageChunk):
            text = _extract_text(update.content)
            if not text:
                return []
            self._assistant_text = _merge_text(self._assistant_text, text)
            return [
                {
                    "type": "text",
                    "text": self._assistant_text,
                    "is_chunk": False,
                },
            ]
        if isinstance(update, AgentThoughtChunk):
            text = _extract_text(update.content)
            if not text:
                return []
            # Thought chunks are independent pieces of one reasoning
            # stream, so each one is a delta rather than a full snapshot.
            return [{"type": "thought", "text": text, "is_chunk": True}]
        if isinstance(update, (ToolCallStart, ToolCallProgress)):
            snapshot = self._accumulator.apply(
                session_notification(session_id, update),
            )
            state = snapshot.tool_calls.get(
                str(getattr(update, "tool_call_id", "") or ""),
            )
            return [self._tool_payload(update, state)]
        if isinstance(update, UsageUpdate):
            payload: dict[str, Any] = {
                "type": "usage",
                "used": int(update.used or 0),
                "size": int(update.size or 0),
            }
            cost = getattr(update, "cost", None)
            if cost is not None:
                payload["cost"] = {
                    "amount": float(getattr(cost, "amount", 0.0) or 0.0),
                    "currency": str(getattr(cost, "currency", "") or ""),
                }
            else:
                payload["cost"] = None
            return [payload]
        # UserMessageChunk / AvailableCommandsUpdate / CurrentModeUpdate /
        # AgentPlanUpdate / SessionInfoUpdate carry no harness events, and
        # ConfigOptionUpdate is captured by the ACP client itself.
        if isinstance(update, ConfigOptionUpdate):
            return []
        return []

    def _tool_payload(
        self,
        update: ToolCallStart | ToolCallProgress,
        state: Any,
    ) -> dict[str, Any]:
        call_id = str(getattr(update, "tool_call_id", "") or "")
        title = (
            _string_value(getattr(state, "title", None))
            or _string_value(getattr(update, "title", None))
            or "unknown"
        )
        kind = (
            _string_value(getattr(state, "kind", None))
            or _string_value(getattr(update, "kind", None))
            or "other"
        )
        status = str(
            getattr(state, "status", None)
            or getattr(update, "status", None)
            or "pending",
        )
        target = _tool_target(state, update)
        detail = _tool_detail(kind, title, state, update) or title
        summary = _stringify_summary(
            getattr(state, "raw_output", None),
        ) or _stringify_summary(getattr(update, "raw_output", None))
        event_type = "tool_start"
        if isinstance(update, ToolCallProgress):
            event_type = (
                "tool_end"
                if status in _TERMINAL_TOOL_STATUSES
                else "tool_update"
            )
        payload: dict[str, Any] = {
            "type": event_type,
            "name": title,
            "call_id": call_id,
            "title": title,
            "kind": kind,
            "status": status,
        }
        if detail:
            payload["detail"] = detail
        if target:
            payload["target"] = target
        if summary:
            payload["summary"] = summary
        return payload


class MiniMaxEventMapper:
    """Map mcode payload dictionaries onto harness events for one turn."""

    def __init__(self) -> None:
        self._emitted_text = ""
        self._emitted_thought = ""
        self.usage: dict[str, Any] | None = None

    def convert(self, payload: dict[str, Any]) -> list[HarnessEvent]:
        """Convert one normalized mcode payload dictionary."""
        if not isinstance(payload, dict):
            return []
        payload_type = str(payload.get("type") or "")
        if payload_type == "text":
            delta = self._delta(self._emitted_text, payload)
            self._emitted_text += delta
            return _text_events(HarnessEventKind.TEXT_DELTA, delta)
        if payload_type == "thought":
            delta = self._delta(self._emitted_thought, payload)
            self._emitted_thought += delta
            return _text_events(HarnessEventKind.REASONING_DELTA, delta)
        if payload_type == "usage":
            self.usage = _usage_data(payload)
            return []
        if payload_type == "tool_start":
            return [self._tool_event(payload, HarnessEventKind.TOOL_STARTED)]
        if payload_type == "tool_update":
            return [self._tool_event(payload, HarnessEventKind.TOOL_PROGRESS)]
        if payload_type == "tool_end":
            return [
                self._tool_event(payload, HarnessEventKind.TOOL_COMPLETED),
            ]
        return []

    def finish(self, stop_reason: Any = None) -> list[HarnessEvent]:
        """Return the terminal events for one settled prompt."""
        data = dict(self.usage or {})
        reason = str(stop_reason or "")
        if reason == "cancelled":
            return [HarnessEvent(kind=HarnessEventKind.CANCELLED, data=data)]
        if reason in {"refusal", "max_tokens", "max_turn_requests"}:
            data["stop_reason"] = reason
        return [HarnessEvent(kind=HarnessEventKind.COMPLETED, data=data)]

    def _delta(
        self,
        emitted: str,
        payload: dict[str, Any],
    ) -> str:
        text = str(payload.get("text") or "")
        if not text:
            return ""
        if bool(payload.get("is_chunk")):
            return text
        if text.startswith(emitted):
            return text[len(emitted) :]
        # A full-text frame that does not extend what was already emitted
        # is treated as a new segment; genuine replacements cannot be
        # distinguished from appended segments at this layer.
        return text

    def _tool_event(
        self,
        payload: dict[str, Any],
        kind: HarnessEventKind,
    ) -> HarnessEvent:
        call_id = str(payload.get("call_id") or "")
        name = str(payload.get("name") or payload.get("title") or "tool")
        status = str(payload.get("status") or "")
        data: dict[str, Any] = {
            "title": str(payload.get("title") or ""),
            "kind": str(payload.get("kind") or ""),
            "status": status,
        }
        if payload.get("target"):
            data["target"] = str(payload["target"])
        if payload.get("detail"):
            data["detail"] = str(payload["detail"])
        if payload.get("summary"):
            data["summary"] = str(payload["summary"])
        if kind == HarnessEventKind.TOOL_COMPLETED:
            data["is_error"] = status == "failed"
            text = str(payload.get("summary") or "")
        else:
            text = str(payload.get("detail") or payload.get("target") or "")
        return HarnessEvent(
            kind=kind,
            item_id=call_id,
            tool_name=name,
            text=text,
            data=data,
        )


def _usage_data(payload: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    used = payload.get("used")
    if used is not None:
        data["used"] = int(used)
    size = payload.get("size")
    if size is not None:
        data["size"] = int(size)
    cost = payload.get("cost")
    if isinstance(cost, dict) and cost.get("amount") is not None:
        try:
            data["cost_usd"] = float(cost["amount"])
        except (TypeError, ValueError):
            pass
    return data


def _text_events(kind: HarnessEventKind, text: str) -> list[HarnessEvent]:
    return [HarnessEvent(kind=kind, text=text)] if text else []


def _merge_text(current: str, incoming: str) -> str:
    """Merge streamed assistant text like ``agents/acp/client.py``.

    mcode re-sends the accumulated message content in each chunk, so an
    incoming text that extends (or repeats) the current snapshot replaces
    it; overlapping tails append; anything else concatenates.
    """
    if not incoming:
        return current
    if not current:
        return incoming
    if incoming == current:
        return current
    if incoming.startswith(current):
        return incoming
    max_overlap = min(len(current), len(incoming))
    for size in range(max_overlap, 0, -1):
        if current.endswith(incoming[:size]):
            return current + incoming[size:]
    return current + incoming


def _extract_text(content: Any) -> str:
    if hasattr(content, "text") and isinstance(
        getattr(content, "text", None),
        str,
    ):
        return str(content.text)
    if hasattr(content, "name") and hasattr(content, "uri"):
        return str(
            getattr(content, "name", None)
            or getattr(content, "uri", None)
            or "",
        )
    if hasattr(content, "resource"):
        resource = getattr(content, "resource", None)
        if resource is not None:
            text = getattr(resource, "text", None)
            if isinstance(text, str) and text:
                return text
            blob = getattr(resource, "blob", None)
            if isinstance(blob, str) and blob:
                return blob
        return ""
    if isinstance(content, list):
        parts = [_extract_text(item) for item in content]
        return "".join(part for part in parts if part)
    if isinstance(content, dict):
        if content.get("type") == "text" and isinstance(
            content.get("text"),
            str,
        ):
            return str(content["text"])
        return ""
    return ""


def _tool_target(state: Any, update: Any) -> str | None:
    locations = (
        getattr(state, "locations", None)
        or getattr(update, "locations", None)
        or []
    )
    for location in locations:
        path = getattr(location, "path", None)
        if path:
            return str(path)
        if isinstance(location, dict) and location.get("path"):
            return str(location["path"])
    return None


def _tool_detail(
    kind: str,
    title: str,
    state: Any,
    update: Any,
) -> str | None:
    target = _tool_target(state, update)
    if kind == "execute":
        return _tool_input_text(state, update, "command") or title
    if kind == "read":
        return (
            _tool_input_text(state, update, "file_path", "filePath", "path")
            or target
            or title
        )
    if kind == "search":
        return _tool_input_text(state, update, "path", "pattern") or (
            target or title
        )
    if kind == "edit":
        return title or target
    return title


def _tool_input_text(state: Any, update: Any, *keys: str) -> str | None:
    raw_inputs = (
        getattr(state, "raw_input", None),
        getattr(update, "raw_input", None),
    )
    for raw_input in raw_inputs:
        if not isinstance(raw_input, dict):
            continue
        for key in keys:
            value = raw_input.get(key)
            if isinstance(value, list):
                for item in reversed(value):
                    text = _string_value(item)
                    if text:
                        return text
            else:
                text = _string_value(value)
                if text:
                    return text
    return None


def _string_value(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _stringify_summary(value: Any) -> str | None:
    if value is None:
        return None
    return _string_value(value) if isinstance(value, str) else str(value)


__all__ = [
    "McodeUpdateNormalizer",
    "MiniMaxEventMapper",
]
