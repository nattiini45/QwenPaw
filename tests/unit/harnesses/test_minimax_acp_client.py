# -*- coding: utf-8 -*-
"""Tests for the MiniMax Code ACP client and model value codec."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from acp.schema import (
    AllowedOutcome,
    ConfigOptionUpdate,
    DeniedOutcome,
    RequestPermissionResponse,
    SessionConfigOptionSelect,
    SessionConfigSelectGroup,
    SessionConfigSelectOption,
)

from qwenpaw.harnesses.minimax.acp_client import (
    McodeAcpClient,
    McodeAcpError,
    decode_model_value,
    encode_model_value,
)


def _fake_process(pid: int = 424242) -> SimpleNamespace:
    return SimpleNamespace(returncode=None, pid=pid)


def _dead_process(pid: int = 424243) -> SimpleNamespace:
    return SimpleNamespace(returncode=1, pid=pid)


def _fake_spawn(client: McodeAcpClient, process: Any) -> list[str]:
    """Replace the real spawn with a recorder returning a fake connection."""
    calls: list[str] = []

    async def fake_spawn() -> None:
        calls.append("spawn")
        client._conn = SimpleNamespace(closed=[])
        client._process = process

    client._spawn = fake_spawn  # type: ignore[method-assign]
    return calls


# -- model value codec ------------------------------------------------------


def test_encodes_plain_model_value() -> None:
    assert encode_model_value("minimax", "MiniMax-M2.7") == (
        "m:minimax:MiniMax-M2.7:u"
    )


def test_encodes_variant_model_value() -> None:
    assert encode_model_value("minimax", "MiniMax-M2.7", "thinking") == (
        "m:minimax:MiniMax-M2.7:v:thinking"
    )


def test_encodes_empty_variant_for_base_model() -> None:
    # Observed live from mcode 0.5.10: variant-capable models expose a
    # base option with an empty variant ("M3" -> m:minimax:MiniMax-M3:v:).
    assert encode_model_value("minimax", "MiniMax-M3", "") == (
        "m:minimax:MiniMax-M3:v:"
    )
    assert decode_model_value("m:minimax:MiniMax-M3:v:") == (
        "minimax",
        "MiniMax-M3",
        "",
    )


def test_codec_round_trips_url_encoded_parts() -> None:
    provider = "custom/provider:a"
    model = "model with spaces"
    variant = "vär/iant"

    encoded = encode_model_value(provider, model, variant)

    # URL-encoding keeps ':' out of the individual parts, so the value
    # always splits into exactly provider/model/terminator/variant.
    assert encoded.startswith("m:")
    assert encoded[2:].split(":") == [
        "custom%2Fprovider%3Aa",
        "model%20with%20spaces",
        "v",
        "v%C3%A4r%2Fiant",
    ]
    assert decode_model_value(encoded) == (provider, model, variant)
    assert decode_model_value(encode_model_value(provider, model)) == (
        provider,
        model,
        None,
    )


@pytest.mark.parametrize(
    "value",
    [
        "minimax/MiniMax-M2.7",
        "m:minimax",
        "m:minimax:MiniMax-M2.7",
        "m:minimax:MiniMax-M2.7:x",
        "m::MiniMax-M2.7:u",
        "m:minimax::u",
        "m:minimax:MiniMax-M2.7:u:extra",
        "m:minimax:MiniMax-M2.7:v",
        "",
    ],
)
def test_decode_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError, match="Invalid MiniMax model"):
        decode_model_value(value)


def test_decode_rejects_non_string_values() -> None:
    with pytest.raises(ValueError, match="Invalid MiniMax model"):
        decode_model_value(None)  # type: ignore[arg-type]


def test_encode_rejects_empty_parts() -> None:
    with pytest.raises(ValueError, match="provider and model"):
        encode_model_value("", "model")
    with pytest.raises(ValueError, match="provider and model"):
        encode_model_value("provider", " ")


# -- one-connection-per-process enforcement --------------------------------


@pytest.mark.asyncio
async def test_second_initialize_is_rejected() -> None:
    client = McodeAcpClient()
    spawns = _fake_spawn(client, _fake_process())

    await client.start()
    assert spawns == ["spawn"]

    with pytest.raises(McodeAcpError, match="one client connection"):
        await client.start()
    assert spawns == ["spawn"]


@pytest.mark.asyncio
async def test_ensure_started_is_idempotent_and_respawns() -> None:
    client = McodeAcpClient()
    process = _fake_process()
    spawns = _fake_spawn(client, process)

    await client.ensure_started()
    await client.ensure_started()
    assert spawns == ["spawn"]

    process.returncode = 1
    await client.ensure_started()
    assert spawns == ["spawn", "spawn"]


@pytest.mark.asyncio
async def test_start_respawns_after_process_death() -> None:
    client = McodeAcpClient()
    process = _dead_process()
    spawns: list[str] = []

    async def fake_spawn() -> None:
        spawns.append("spawn")
        client._conn = SimpleNamespace(closed=[])
        client._process = process

    client._spawn = fake_spawn  # type: ignore[method-assign]

    await client.start()
    # A dead process is not a live connection: re-start must respawn
    # instead of raising the one-connection error.
    await client.start()

    assert spawns == ["spawn", "spawn"]


@pytest.mark.asyncio
async def test_stop_tears_down_and_kills_tree() -> None:
    client = McodeAcpClient()
    _fake_spawn(client, _fake_process(pid=999999))
    await client.start()

    await client.stop()

    assert client.running is False
    assert client.connection is None


@pytest.mark.asyncio
async def test_session_operations_require_running_process() -> None:
    client = McodeAcpClient()

    with pytest.raises(McodeAcpError, match="not running"):
        await client.prompt("session-1", "hello")
    with pytest.raises(McodeAcpError, match="not running"):
        await client.cancel("session-1")
    with pytest.raises(McodeAcpError, match="not running"):
        await client.set_config_option("session-1", "permissionMode", "auto")


@pytest.mark.asyncio
async def test_close_session_is_tolerated_without_process() -> None:
    client = McodeAcpClient()

    await client.close_session("session-1")

    assert client.sessions == {}


@pytest.mark.asyncio
async def test_new_session_captures_inline_config_options() -> None:
    client = McodeAcpClient()
    select = SessionConfigOptionSelect(
        id="model",
        name="Model",
        type="select",
        currentValue="m:minimax:MiniMax-M3:v:",
        options=[
            SessionConfigSelectOption(
                value="m:minimax:MiniMax-M3:v:",
                name="M3",
            ),
            SessionConfigSelectOption(
                value="m:minimax:MiniMax-M3:v:thinking",
                name="M3 thinking",
            ),
        ],
    )
    response = SimpleNamespace(
        session_id="mvs-1",
        config_options=[select],
    )

    async def fake_spawn() -> None:
        client._conn = SimpleNamespace(
            new_session=lambda **_: _async_result(response),
        )
        client._process = _fake_process()

    client._spawn = fake_spawn  # type: ignore[method-assign]
    await client.start()

    session_id = await client.new_session("/tmp")

    assert session_id == "mvs-1"
    assert client.sessions == {"mvs-1": "/tmp"}
    assert client.model_config_options() == [
        {
            "id": "m:minimax:MiniMax-M3:v:",
            "name": "M3",
            "description": "",
            "is_default": True,
        },
        {
            "id": "m:minimax:MiniMax-M3:v:thinking",
            "name": "M3 thinking",
            "description": "",
            "is_default": False,
        },
    ]


async def _async_result(value: Any) -> Any:
    return value


# -- notification fan-out and permission routing ---------------------------


@pytest.mark.asyncio
async def test_session_update_fans_out_to_listeners() -> None:
    client = McodeAcpClient()
    received: list[tuple[str, Any]] = []

    async def listener(session_id: str, update: Any) -> None:
        received.append((session_id, update))

    client.add_listener("session-1", listener)
    update = SimpleNamespace(sessionUpdate="agent_message_chunk")
    await client.session_update("session-1", update)  # type: ignore[arg-type]
    await client.session_update("session-2", update)  # type: ignore[arg-type]

    assert received == [("session-1", update)]

    client.remove_listener("session-1", listener)
    await client.session_update("session-1", update)  # type: ignore[arg-type]
    assert received == [("session-1", update)]


@pytest.mark.asyncio
async def test_request_permission_denies_without_handler() -> None:
    client = McodeAcpClient()

    response = await client.request_permission(
        options=[],
        session_id="session-1",
        tool_call=SimpleNamespace(toolCallId="call-1"),
    )

    assert response == RequestPermissionResponse(
        outcome=DeniedOutcome(outcome="cancelled"),
    )


@pytest.mark.asyncio
async def test_request_permission_routes_to_handler() -> None:
    client = McodeAcpClient()
    seen: list[tuple[list[Any], str]] = []

    async def handler(
        options: list[Any],
        session_id: str,
        tool_call: Any,
    ) -> RequestPermissionResponse:
        seen.append((options, session_id))
        return RequestPermissionResponse(
            outcome=AllowedOutcome(
                outcome="selected",
                optionId="allow_once",
            ),
        )

    client.set_permission_handler(handler)

    response = await client.request_permission(
        options=["allow_once"],
        session_id="session-1",
        tool_call=SimpleNamespace(toolCallId="call-1"),
    )

    assert response.outcome.optionId == "allow_once"  # type: ignore[attr-defined]
    assert seen == [(["allow_once"], "session-1")]


@pytest.mark.asyncio
async def test_model_config_options_flatten_groups_and_default() -> None:
    update = ConfigOptionUpdate(
        configOptions=[
            # One select may carry either a flat or a grouped option list;
            # grouped entries must flatten to their inner options.
            SessionConfigOptionSelect(
                id="model",
                name="Model",
                type="select",
                currentValue="m:minimax:MiniMax-M2.7:u",
                options=[
                    SessionConfigSelectGroup(
                        group="providers",
                        name="Providers",
                        options=[
                            SessionConfigSelectOption(
                                value="m:minimax:MiniMax-M2.7:u",
                                name="MiniMax M2.7",
                            ),
                            SessionConfigSelectOption(
                                value="m:deepseek:deepseek-v4-pro:u",
                                name="DeepSeek V4 Pro",
                                description="BYOK",
                            ),
                        ],
                    ),
                ],
            ),
            SessionConfigOptionSelect(
                id="thinkingEffort",
                name="Thinking Effort",
                type="select",
                currentValue="high",
                options=[
                    SessionConfigSelectOption(value="low", name="Low"),
                ],
            ),
        ],
        sessionUpdate="config_option_update",
    )
    client = McodeAcpClient()

    await client.session_update("session-1", update)

    assert client.model_config_options() == [
        {
            "id": "m:minimax:MiniMax-M2.7:u",
            "name": "MiniMax M2.7",
            "description": "",
            "is_default": True,
        },
        {
            "id": "m:deepseek:deepseek-v4-pro:u",
            "name": "DeepSeek V4 Pro",
            "description": "BYOK",
            "is_default": False,
        },
    ]
