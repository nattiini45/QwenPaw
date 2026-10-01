"""P4: session/list shape; session/load replay of the P2 session (frames + count);
fresh process initialize -> session/resume same id (attach, no replay)."""

import asyncio

from acp import text_block

from probe_common import (
    PROBE_DIR,
    STATE,
    dump,
    initialize,
    open_probe,
    run_probe,
    wf,
)


def _update_kind(frame: dict) -> str:
    # wire shape: update = {"sessionUpdate": "<type-tag>", <payload fields...>}
    upd = (frame["frame"].get("params") or {}).get("update", {})
    tag = upd.get("sessionUpdate")
    if isinstance(tag, str):
        return tag
    if isinstance(tag, dict):
        return next(iter(tag.keys()), "?")
    return "?"


async def probe() -> dict:
    out: dict = {}
    p2_id = (STATE / "p2_session_id.txt").read_text().strip()
    out["p2_session_id"] = p2_id

    # ---- process 1: session/list + session/load (replay) ------------------------
    async with open_probe("04-sessions.json") as pc:
        try:
            await initialize(pc.conn, "p4a")
            ls = await wf(pc.conn.list_sessions(cwd=str(PROBE_DIR)), "session/list")
            out["session_list_response"] = dump(ls)
            out["session_list_count"] = len(ls.sessions or [])
            out["session_list_first_items"] = [dump(s) for s in (ls.sessions or [])[:5]]
            out["session_list_next_cursor"] = ls.next_cursor

            marker = pc.recorder.seq
            load = await wf(pc.conn.load_session(cwd=str(PROBE_DIR), session_id=p2_id), "session/load")
            ld = dump(load)
            out["session_load_response"] = {k: v for k, v in ld.items() if k != "configOptions"}
            out["session_load_response_keys"] = list(ld.keys())
            cfg = find = None
            if load.config_options:
                out["session_load_config_ids"] = [c.get("configId") or c.get("id") for c in dump(load.config_options)]

            replay = [
                f
                for f in pc.recorder.frames_since(marker)
                if f["dir"] == "a2c" and f["frame"].get("method") == "session/update"
            ]
            out["replay_frame_count"] = len(replay)
            out["replay_first40_types"] = [_update_kind(f) for f in replay[:40]]
            chunks = []
            for f in replay[:200]:
                upd = f["frame"]["params"]["update"]
                kind = upd.get("sessionUpdate")
                if kind in ("user_message_chunk", "agent_message_chunk"):
                    chunks.append({"kind": kind, "content": upd.get("content") or {}})
                if len(chunks) >= 6:
                    break
            out["replay_first_chunks"] = chunks
        finally:
            await pc.shutdown()

    # ---- process 2 (fresh): initialize -> session/resume (attach, expect no replay)
    async with open_probe("04-sessions.json", append=True) as pc2:
        try:
            await initialize(pc2.conn, "p4b")
            marker = pc2.recorder.seq
            res = await wf(pc2.conn.resume_session(cwd=str(PROBE_DIR), session_id=p2_id), "session/resume")
            rd = dump(res)
            out["session_resume_response"] = {k: v for k, v in rd.items() if k != "configOptions"}
            out["session_resume_response_keys"] = list(rd.keys())
            if res.config_options:
                out["session_resume_config_ids"] = [c.get("configId") or c.get("id") for c in dump(res.config_options)]
            # replay frames between resume request and our liveness prompt
            replay_after_resume = [
                f
                for f in pc2.recorder.frames_since(marker)
                if f["dir"] == "a2c" and f["frame"].get("method") == "session/update"
            ]
            out["resume_replay_frame_count"] = len(replay_after_resume)
            out["resume_replay_types"] = [_update_kind(f) for f in replay_after_resume[:40]]

            # liveness proof: attach really works
            prompt_marker = pc2.recorder.seq
            resp = await wf(
                pc2.conn.prompt(prompt=[text_block("Reply with exactly: ATTACHED")], session_id=p2_id),
                "session/prompt-after-resume",
            )
            out["resume_prompt_stop_reason"] = resp.stop_reason
            frames_during_prompt = pc2.recorder.frames_since(prompt_marker)
            out["resume_prompt_update_types"] = [
                _update_kind(f)
                for f in frames_during_prompt
                if f["dir"] == "a2c" and f["frame"].get("method") == "session/update"
            ][:30]
        finally:
            await pc2.shutdown()

    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "04-sessions"))
    raise SystemExit(0 if ok else 1)
