# -*- coding: utf-8 -*-
"""Tests for the MiniMax Code event mapper and update normalizer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from acp.schema import (
    AgentMessageChunk,
    AgentThoughtChunk,
    TextContentBlock,
    ToolCallProgress,
    ToolCallStart,
    ToolCallLocation,
    UsageUpdate,
)

from qwenpaw.harnesses.events import HarnessEventKind
from qwenpaw.harnesses.minimax.event_mapper import (
    McodeUpdateNormalizer,
    MiniMaxEventMapper,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "minimax"


def _fixture_frames(name: str) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (FIXTURES / name).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _notification_payloads(name: str) -> list[dict[str, Any]]:
    return [
        row["payload"]
        for row in _fixture_frames(name)
        if row.get("dir") == "notification"
    ]


def _prompt_response(name: str) -> dict[str, Any]:
    for row in _fixture_frames(name):
        if row.get("dir") == "response" and "prompt" in str(
            row.get("step") or "",
        ):
            return dict(row.get("result") or {})
    raise AssertionError(f"no prompt response in {name}")


# -- fixture replay ---------------------------------------------------------


@pytest.mark.parametrize(
    ("probe_file", "payload"),
    [
        (probe_file, payload)
        for probe_file in (
            "acp_probe_session1.jsonl",
            "acp_probe_session2.jsonl",
            "acp_probe_session3.jsonl",
        )
        for payload in _notification_payloads(probe_file)
    ],
)
def test_real_payloads_map_without_errors(
    probe_file: str,
    payload: dict[str, Any],
) -> None:
    del probe_file
    events = MiniMaxEventMapper().convert(payload)

    assert all(
        event.kind
        in {
            HarnessEventKind.TEXT_DELTA,
            HarnessEventKind.REASONING_DELTA,
            HarnessEventKind.TOOL_STARTED,
            HarnessEventKind.TOOL_PROGRESS,
            HarnessEventKind.TOOL_COMPLETED,
        }
        for event in events
    )


@pytest.mark.parametrize("probe_file", [
    "acp_probe_session1.jsonl",
    "acp_probe_session2.jsonl",
    "acp_probe_session3.jsonl",
])
def test_full_turn_replay_yields_text_and_completed(
    probe_file: str,
) -> None:
    mapper = MiniMaxEventMapper()
    kinds: list[HarnessEventKind] = []
    text = ""

    for payload in _notification_payloads(probe_file):
        for event in mapper.convert(payload):
            kinds.append(event.kind)
            if event.kind == HarnessEventKind.TEXT_DELTA:
                text += event.text
    terminal = mapper.finish(_prompt_response(probe_file).get("stop_reason"))
    kinds.extend(event.kind for event in terminal)

    assert HarnessEventKind.TEXT_DELTA in kinds
    assert kinds[-1] == HarnessEventKind.COMPLETED
    assert text  # every captured turn produced assistant text


def test_pong_turn_replays_exactly() -> None:
    mapper = MiniMaxEventMapper()

    for payload in _notification_payloads("acp_probe_session1.jsonl"):
        mapper.convert(payload)
    terminal = mapper.finish(
        _prompt_response("acp_probe_session1.jsonl").get("stop_reason"),
    )

    assert [event.kind for event in terminal] == [
        HarnessEventKind.COMPLETED,
    ]
    assert terminal[0].data == {}


def test_tool_turn_replays_all_tool_lifecycle_events() -> None:
    mapper = MiniMaxEventMapper()
    events = [
        event
        for payload in _notification_payloads("acp_probe_session2.jsonl")
        for event in mapper.convert(payload)
    ]

    kinds = [event.kind for event in events]
    assert kinds.count(HarnessEventKind.TOOL_STARTED) == 1
    assert kinds.count(HarnessEventKind.TOOL_PROGRESS) == 3
    assert kinds.count(HarnessEventKind.TOOL_COMPLETED) == 1
    completed = next(
        event for event in events if event.kind == HarnessEventKind.TOOL_COMPLETED
    )
    assert completed.item_id == "call_01a0f747c433756a8e6dbf4a"
    assert completed.tool_name == "read"
    assert completed.data["status"] == "completed"
    assert completed.data["is_error"] is False
    assert "port_patch.py" in completed.text or completed.data.get("target")


def test_incremental_text_chunks_emit_deltas_only() -> None:
    mapper = MiniMaxEventMapper()

    first = mapper.convert({"type": "text", "text": "Hello", "is_chunk": False})
    second = mapper.convert(
        {"type": "text", "text": "Hello, world", "is_chunk": False},
    )
    third = mapper.convert({"type": "text", "text": "!", "is_chunk": True})

    assert [event.text for event in first] == ["Hello"]
    assert [event.text for event in second] == [", world"]
    assert [event.text for event in third] == ["!"]


def test_thought_payloads_map_to_reasoning_deltas() -> None:
    events = MiniMaxEventMapper().convert(
        {"type": "thought", "text": "thinking...", "is_chunk": True},
    )

    assert [event.kind for event in events] == [
        HarnessEventKind.REASONING_DELTA,
    ]
    assert events[0].text == "thinking..."


def test_failed_tool_end_is_error() -> None:
    events = MiniMaxEventMapper().convert(
        {
            "type": "tool_end",
            "name": "bash",
            "call_id": "call-1",
            "title": "bash",
            "kind": "execute",
            "status": "failed",
            "summary": "exit 1",
        },
    )

    assert events[0].kind == HarnessEventKind.TOOL_COMPLETED
    assert events[0].data["is_error"] is True
    assert events[0].text == "exit 1"


# -- usage and terminal mapping ---------------------------------------------


def _usage_wire_frames() -> list[dict[str, Any]]:
    """Extract the real usage_update wire frames from the probe suite."""
    frames = [
        json.loads(line)
        for line in (
            FIXTURES
            / "probe-suite"
            / "fixtures"
            / "07-usage-cost.json"
        )
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    updates = []
    for row in frames:
        frame = row.get("frame", {})
        if frame.get("method") != "session/update":
            continue
        update = (frame.get("params") or {}).get("update") or {}
        if update.get("sessionUpdate") == "usage_update":
            updates.append(update)
    assert updates  # the probe suite captured at least one usage frame
    return updates


def test_real_usage_update_carries_into_completed_event() -> None:
    wire = _usage_wire_frames()[0]
    mapper = MiniMaxEventMapper()

    events = mapper.convert(
        {
            "type": "usage",
            "used": wire.get("used"),
            "size": wire.get("size"),
            "cost": wire.get("cost"),
        },
    )
    terminal = mapper.finish("end_turn")

    assert events == []
    assert terminal[0].kind == HarnessEventKind.COMPLETED
    assert terminal[0].data["used"] == 16847
    assert terminal[0].data["size"] == 512000
    assert terminal[0].data["cost_usd"] == 0.0


@pytest.mark.parametrize(
    ("stop_reason", "expected_kind", "expected_reason"),
    [
        ("end_turn", HarnessEventKind.COMPLETED, None),
        (None, HarnessEventKind.COMPLETED, None),
        ("cancelled", HarnessEventKind.CANCELLED, None),
        (
            "max_turn_requests",
            HarnessEventKind.COMPLETED,
            "max_turn_requests",
        ),
        ("refusal", HarnessEventKind.COMPLETED, "refusal"),
    ],
)
def test_finish_maps_stop_reasons(
    stop_reason: str | None,
    expected_kind: HarnessEventKind,
    expected_reason: str | None,
) -> None:
    terminal = MiniMaxEventMapper().finish(stop_reason)

    assert [event.kind for event in terminal] == [expected_kind]
    if expected_reason is None:
        assert "stop_reason" not in terminal[0].data
    else:
        assert terminal[0].data["stop_reason"] == expected_reason


# -- SDK update normalization -----------------------------------------------


def test_normalizer_merges_agent_message_chunks() -> None:
    normalizer = McodeUpdateNormalizer()

    first = normalizer.normalize(
        "mvs-1",
        AgentMessageChunk(
            sessionUpdate="agent_message_chunk",
            content=TextContentBlock(type="text", text="PO"),
        ),
    )
    second = normalizer.normalize(
        "mvs-1",
        AgentMessageChunk(
            sessionUpdate="agent_message_chunk",
            content=TextContentBlock(type="text", text="PONG"),
        ),
    )

    assert first == [{"type": "text", "text": "PO", "is_chunk": False}]
    assert second == [{"type": "text", "text": "PONG", "is_chunk": False}]
    mapper = MiniMaxEventMapper()
    mapper.convert(first[0])
    events = mapper.convert(second[0])
    assert [event.text for event in events] == ["NG"]


def test_normalizer_forwards_thought_chunks_as_deltas() -> None:
    normalizer = McodeUpdateNormalizer()

    payloads = normalizer.normalize(
        "mvs-1",
        AgentThoughtChunk(
            sessionUpdate="agent_thought_chunk",
            content=TextContentBlock(type="text", text="step 1"),
        ),
    )

    assert payloads == [
        {"type": "thought", "text": "step 1", "is_chunk": True},
    ]


def test_normalizer_tracks_tool_lifecycle_through_accumulator() -> None:
    normalizer = McodeUpdateNormalizer()
    start = normalizer.normalize(
        "mvs-1",
        ToolCallStart(
            sessionUpdate="tool_call",
            toolCallId="call-1",
            title="write",
            kind="edit",
            status="pending",
        ),
    )
    progress = normalizer.normalize(
        "mvs-1",
        ToolCallProgress(
            sessionUpdate="tool_call_update",
            toolCallId="call-1",
            status="in_progress",
            locations=[
                ToolCallLocation(path="/tmp/outside.txt"),
            ],
        ),
    )
    done = normalizer.normalize(
        "mvs-1",
        ToolCallProgress(
            sessionUpdate="tool_call_update",
            toolCallId="call-1",
            status="completed",
            rawOutput={"content": [{"type": "text", "text": "wrote 6 bytes"}]},
        ),
    )

    assert start[0]["type"] == "tool_start"
    assert start[0]["name"] == "write"
    assert progress[0]["type"] == "tool_update"
    assert progress[0]["target"] == "/tmp/outside.txt"
    assert done[0]["type"] == "tool_end"
    assert done[0]["status"] == "completed"
    assert "wrote 6 bytes" in done[0]["summary"]

    events = [MiniMaxEventMapper().convert(item[0]) for item in (start, progress, done)]
    assert [event[0].kind for event in events] == [
        HarnessEventKind.TOOL_STARTED,
        HarnessEventKind.TOOL_PROGRESS,
        HarnessEventKind.TOOL_COMPLETED,
    ]
    assert events[2][0].data["target"] == "/tmp/outside.txt"


def test_normalizer_converts_usage_updates() -> None:
    payloads = McodeUpdateNormalizer().normalize(
        "mvs-1",
        UsageUpdate(
            sessionUpdate="usage_update",
            used=42,
            size=512000,
            cost={"amount": 0.0, "currency": "USD"},
        ),
    )

    assert payloads == [
        {
            "type": "usage",
            "used": 42,
            "size": 512000,
            "cost": {"amount": 0.0, "currency": "USD"},
        },
    ]


def test_normalizer_ignores_non_event_updates() -> None:
    normalizer = McodeUpdateNormalizer()

    assert normalizer.normalize("mvs-1", object()) == []
    assert normalizer.normalize("mvs-1", None) == []
