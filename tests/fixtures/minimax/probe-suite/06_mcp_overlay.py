"""P6: session/new with mcpServers stdio overlay (npx @modelcontextprotocol/server-everything)
— does session/new accept it, do MCP tools surface?"""

import asyncio

from acp import text_block
from acp.schema import McpServerStdio

from probe_common import (
    PROBE_DIR,
    update_payload,
    dump,
    initialize,
    open_probe,
    run_probe,
    update_type,
    wf,
)


async def probe() -> dict:
    out: dict = {}
    overlay = McpServerStdio(
        name="echo-test",
        command="npx",
        args=["-y", "@modelcontextprotocol/server-everything"],
        env=[],
    )
    out["overlay_sent"] = dump(overlay)

    async with open_probe("06-mcp-overlay.json") as pc:
        try:
            await initialize(pc.conn, "p6")
            try:
                new = await asyncio.wait_for(
                    pc.conn.new_session(cwd=str(PROBE_DIR), mcp_servers=[overlay]),
                    120,
                )
                out["session_new_with_overlay_ok"] = True
                out["session_id"] = new.session_id
                out["config_ids"] = [
                    c.get("configId") for c in (dump(new.config_options) or [])
                ]
                sid = new.session_id
            except Exception as e:  # noqa: BLE001
                out["session_new_with_overlay_ok"] = False
                out["session_new_overlay_error"] = f"{type(e).__name__}: {e}"
                raise RuntimeError("session/new with overlay failed — captured") from e

            # probe tool surface: ask the agent to use the echo tool
            pc.client.updates.clear()
            try:
                resp = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[
                            text_block(
                                "Use the echo tool from the echo-test MCP server to echo the text "
                                "'overlay-ok'. Then reply with the tool's exact response."
                            )
                        ],
                        session_id=sid,
                    ),
                    120,
                )
                out["prompt_stop_reason"] = resp.stop_reason
            except Exception as e:  # noqa: BLE001
                out["prompt_error"] = f"{type(e).__name__}: {e}"

            out["update_type_counts"] = {}
            for u in pc.client.updates:
                t = update_type(u)
                out["update_type_counts"][t] = out["update_type_counts"].get(t, 0) + 1

            # tool_call updates: titles (mcp tool naming evidence)
            out["tool_call_updates"] = [
                {
                    "title": update_payload(u).get("title"),
                    "kind": update_payload(u).get("kind"),
                    "status": update_payload(u).get("status"),
                    "locations": update_payload(u).get("locations"),
                }
                for u in pc.client.updates
                if update_type(u) == "tool_call"
            ][:20]
            # available commands update (if mcode advertises mcp tools there)
            out["available_commands_updates"] = [
                dump(u) for u in pc.client.updates if update_type(u) == "available_commands_update"
            ][:3]

            texts = [
                (update_payload(u).get("content") or {}).get("text", "")
                for u in pc.client.updates
                if update_type(u) == "agent_message_chunk"
            ]
            out["assistant_text_last800"] = "".join(texts)[-800:]
        finally:
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "06-mcp-overlay"))
    raise SystemExit(0 if ok else 1)
