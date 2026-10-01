"""P7: usage/cost — capture the usage_update notification verbatim after a prompt."""

import asyncio

from acp import text_block

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
    async with open_probe("07-usage-cost.json") as pc:
        try:
            await initialize(pc.conn, "p7")
            new = await wf(pc.conn.new_session(cwd=str(PROBE_DIR)), "session/new")
            sid = new.session_id
            out["session_id"] = sid

            resp = await wf(
                pc.conn.prompt(
                    prompt=[text_block("Reply with exactly: USAGE")], session_id=sid
                ),
                "session/prompt",
            )
            out["stop_reason"] = resp.stop_reason
            out["prompt_response_usage"] = dump(resp.usage)

            usage_updates = [dump(u) for u in pc.client.updates if update_type(u) == "usage_update"]
            out["usage_update_count"] = len(usage_updates)
            out["usage_updates_verbatim"] = usage_updates
            if usage_updates:
                out["usage_update_field_names"] = sorted(update_payload(pc.client.updates[-1]).keys())
        finally:
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "07-usage-cost"))
    raise SystemExit(0 if ok else 1)
