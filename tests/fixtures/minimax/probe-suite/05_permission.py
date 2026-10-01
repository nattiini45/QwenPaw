"""P5: permission flow — deny a file-create, then allow-once; capture the
session/request_permission payload verbatim and how the turn proceeds."""

import asyncio
from pathlib import Path

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

TARGET = Path("/root/mcode-probes/perm-test.txt")
OUTSIDE = Path("/tmp/mcode-probe-outside.txt")


async def probe() -> dict:
    out: dict = {}
    if TARGET.exists():
        TARGET.unlink()

    async with open_probe("05-permission.json") as pc:
        try:
            await initialize(pc.conn, "p5")
            new = await wf(pc.conn.new_session(cwd=str(PROBE_DIR)), "session/new")
            sid = new.session_id
            out["session_id"] = sid

            # make sure permissionMode=default (best effort; capture result)
            try:
                setresp = await asyncio.wait_for(
                    pc.conn.set_config_option(
                        config_id="permissionMode", session_id=sid, value="default"
                    ),
                    30,
                )
                out["permissionMode_set_default_ok"] = True
                out["permissionMode_after"] = next(
                    (
                        c.get("currentValue")
                        for c in (dump(setresp.config_options) or [])
                        if (c.get("configId") or c.get("id")) == "permissionMode"
                    ),
                    None,
                )
            except Exception as e:  # noqa: BLE001
                out["permissionMode_set_default_ok"] = False
                out["permissionMode_set_error"] = str(e)

            # ---- phase 1: DENY -----------------------------------------------------
            pc.client.permission_policy = "reject_once"
            n_perm_before = len(pc.client.permission_requests)
            marker = pc.recorder.seq
            try:
                r1 = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[
                            text_block(
                                "Create a file named perm-test.txt in the current directory "
                                "containing the word hello"
                            )
                        ],
                        session_id=sid,
                    ),
                    120,
                )
                out["phase1_stop_reason"] = r1.stop_reason
            except Exception as e:  # noqa: BLE001
                out["phase1_error"] = f"{type(e).__name__}: {e}"
            out["phase1_permission_requests"] = [
                {
                    "options": pr.options,
                    "toolCall": pr.tool_call,
                    "answered_with": pr.answered_with,
                }
                for pr in pc.client.permission_requests[n_perm_before:]
            ]
            out["phase1_update_types"] = [
                update_type(u) for u in pc.client.updates
            ][:40]
            out["phase1_file_exists_after_deny"] = TARGET.exists()

            # assistant message after deny (how the turn proceeded)
            texts = [
                (update_payload(u).get("content") or {}).get("text", "")
                for u in pc.client.updates
                if update_type(u) == "agent_message_chunk"
            ]
            out["phase1_assistant_text"] = "".join(texts)[-600:]

            # ---- phase 2: ALLOW-ONCE ----------------------------------------------
            pc.client.permission_policy = "allow_once"
            pc.client.updates.clear()
            n_perm_before2 = len(pc.client.permission_requests)
            marker2 = pc.recorder.seq
            try:
                r2 = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[
                            text_block(
                                "Now create the file perm-test.txt in the current directory "
                                "containing the word hello"
                            )
                        ],
                        session_id=sid,
                    ),
                    120,
                )
                out["phase2_stop_reason"] = r2.stop_reason
            except Exception as e:  # noqa: BLE001
                out["phase2_error"] = f"{type(e).__name__}: {e}"
            out["phase2_permission_requests"] = [
                {
                    "options": pr.options,
                    "toolCall": pr.tool_call,
                    "answered_with": pr.answered_with,
                }
                for pr in pc.client.permission_requests[n_perm_before2:]
            ]
            out["phase2_tool_call_titles"] = [
                update_payload(u).get("title")
                for u in pc.client.updates
                if update_type(u) == "tool_call"
            ]
            out["phase2_file_exists_after_allow"] = TARGET.exists()
            out["phase2_file_content"] = TARGET.read_text() if TARGET.exists() else None
            out["phase2_client_fs_events"] = pc.client.fs_events

            # ---- phase 3: write OUTSIDE cwd, DENY ---------------------------------
            # (drift finding: in-cwd writes do NOT trigger session/request_permission)
            if OUTSIDE.exists():
                OUTSIDE.unlink()
            pc.client.permission_policy = "reject_once"
            pc.client.updates.clear()
            n_perm_before3 = len(pc.client.permission_requests)
            try:
                r3 = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[text_block("Create a file at /tmp/mcode-probe-outside.txt containing the word hello")],
                        session_id=sid,
                    ),
                    120,
                )
                out["phase3_stop_reason"] = r3.stop_reason
            except Exception as e:  # noqa: BLE001
                out["phase3_error"] = f"{type(e).__name__}: {e}"
            out["phase3_permission_requests"] = [
                {"options": pr.options, "toolCall": pr.tool_call, "answered_with": pr.answered_with}
                for pr in pc.client.permission_requests[n_perm_before3:]
            ]
            out["phase3_file_exists_after_deny"] = OUTSIDE.exists()
            t3 = "".join(
                (update_payload(u).get("content") or {}).get("text", "")
                for u in pc.client.updates
                if update_type(u) == "agent_message_chunk"
            )
            out["phase3_assistant_text"] = t3[-500:]
            out["phase3_tool_kinds"] = [
                update_payload(u).get("kind") or update_payload(u).get("title")
                for u in pc.client.updates
                if update_type(u) in ("tool_call",)
            ]

            # ---- phase 4: same outside write, ALLOW-ONCE ---------------------------
            pc.client.permission_policy = "allow_once"
            pc.client.updates.clear()
            n_perm_before4 = len(pc.client.permission_requests)
            try:
                r4 = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[text_block("Now create the file at /tmp/mcode-probe-outside.txt containing the word hello")],
                        session_id=sid,
                    ),
                    120,
                )
                out["phase4_stop_reason"] = r4.stop_reason
            except Exception as e:  # noqa: BLE001
                out["phase4_error"] = f"{type(e).__name__}: {e}"
            out["phase4_permission_requests"] = [
                {"options": pr.options, "toolCall": pr.tool_call, "answered_with": pr.answered_with}
                for pr in pc.client.permission_requests[n_perm_before4:]
            ]
            out["phase4_file_exists_after_allow"] = OUTSIDE.exists()
            out["phase4_file_content"] = OUTSIDE.read_text() if OUTSIDE.exists() else None

            # ---- phase 5: shell command under deny (does exec ask?) ----------------
            pc.client.permission_policy = "reject_once"
            pc.client.updates.clear()
            n_perm_before5 = len(pc.client.permission_requests)
            try:
                r5 = await asyncio.wait_for(
                    pc.conn.prompt(
                        prompt=[text_block("Run the shell command: echo shell-marker")],
                        session_id=sid,
                    ),
                    120,
                )
                out["phase5_stop_reason"] = r5.stop_reason
            except Exception as e:  # noqa: BLE001
                out["phase5_error"] = f"{type(e).__name__}: {e}"
            out["phase5_permission_requests"] = [
                {"options": pr.options, "toolCall": pr.tool_call, "answered_with": pr.answered_with}
                for pr in pc.client.permission_requests[n_perm_before5:]
            ]
        finally:
            # cleanup
            if TARGET.exists():
                out["cleanup_removed_perm_test"] = True
                TARGET.unlink()
            if OUTSIDE.exists():
                out["cleanup_removed_outside"] = True
                OUTSIDE.unlink()
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "05-permission"))
    raise SystemExit(0 if ok else 1)
