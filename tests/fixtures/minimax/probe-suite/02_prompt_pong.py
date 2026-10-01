"""P2: session/new -> session/prompt "Reply with exactly: PONG" — update stream + stopReason."""

import asyncio

from acp import text_block

from probe_common import (
    PROBE_DIR,
    update_payload,
    STATE,
    dump,
    initialize,
    open_probe,
    run_probe,
    update_type,
    wf,
)


async def probe() -> dict:
    out: dict = {}
    async with open_probe("02-prompt-pong.json") as pc:
        try:
            await initialize(pc.conn, "p2")
            new = await wf(pc.conn.new_session(cwd=str(PROBE_DIR)), "session/new")
            out["session_id"] = new.session_id
            (STATE / "p2_session_id.txt").write_text(new.session_id)

            resp = await wf(
                pc.conn.prompt(
                    prompt=[text_block("Reply with exactly: PONG")],
                    session_id=new.session_id,
                ),
                "session/prompt",
            )
            out["stop_reason"] = resp.stop_reason
            out["stop_reason_wire_enum"] = dump(resp).get("stopReason")
            out["user_message_id"] = resp.user_message_id
            out["prompt_response_usage"] = dump(resp.usage)

            # update stream analysis
            types = [update_type(u) for u in pc.client.updates]
            out["update_type_counts"] = {t: types.count(t) for t in sorted(set(types))}
            out["update_type_sequence_first30"] = types[:30]

            # agent_message_chunk content shape (first chunk, verbatim)
            for u in pc.client.updates:
                if update_type(u) == "agent_message_chunk":
                    out["first_agent_message_chunk"] = dump(u)
                    break

            # usage_update notifications, verbatim
            usage_updates = [dump(u) for u in pc.client.updates if update_type(u) == "usage_update"]
            out["usage_update_notifications"] = usage_updates

            # reconstructed assistant text
            text = "".join(
                (update_payload(u).get("content") or {}).get("text", "")
                for u in pc.client.updates
                if update_type(u) == "agent_message_chunk"
            )
            out["assistant_text"] = text
        finally:
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "02-prompt-pong"))
    raise SystemExit(0 if ok else 1)
