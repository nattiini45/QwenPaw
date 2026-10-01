"""P3: setConfigOption matrix — permissionMode cycle, model swap, thinkingEffort,
invalid configId + invalid model value (exact error strings)."""

import asyncio

from acp.exceptions import RequestError

from probe_common import (
    PROBE_DIR,
    dump,
    find_config_option,
    flatten_select_options,
    initialize,
    open_probe,
    run_probe,
    update_type,
    wf,
    write_summary,
)

STEP_TIMEOUT = 120.0


async def try_set(pc, out, label: str, config_id: str, value) -> None:
    """set_config_option; success -> response dump; RequestError -> exact error."""
    marker = pc.recorder.seq
    entry: dict = {"configId": config_id, "value": value}
    try:
        resp = await asyncio.wait_for(
            pc.conn.set_config_option(
                config_id=config_id, session_id=pc_session_id_holder["id"], value=value
            ),
            STEP_TIMEOUT,
        )
        entry["ok"] = True
        entry["response_configOptions"] = dump(resp.config_options) if resp.config_options else None
        cfg = find_config_option(resp.config_options, config_id)
        if cfg:
            entry["current_value_after"] = cfg.get("currentValue")
    except RequestError as e:
        entry["ok"] = False
        entry["error"] = {"code": e.code, "message": str(e), "data": e.data}
    except Exception as e:  # noqa: BLE001
        entry["ok"] = False
        entry["error"] = {"type": type(e).__name__, "message": str(e)}
    # any broadcast notifications triggered by this call
    entry["notifications_after"] = [
        dump(u) for u in pc.client.updates if update_type(u) == "config_option_update"
    ][-3:]
    out.setdefault("steps", []).append({label: entry})


pc_session_id_holder: dict = {"id": ""}


async def probe() -> dict:
    out: dict = {"steps": []}
    async with open_probe("03-setconfig.json") as pc:
        try:
            await initialize(pc.conn, "p3")
            new = await wf(pc.conn.new_session(cwd=str(PROBE_DIR)), "session/new")
            pc_session_id_holder["id"] = new.session_id
            out["session_id"] = new.session_id

            # observed config ids + current selections at session start
            co = dump(new.config_options) if new.config_options else []
            out["config_ids_observed"] = [c.get("configId") or c.get("id") for c in co]

            perm = find_config_option(new.config_options, "permissionMode")
            perm_values = [o.get("value") for o in flatten_select_options(perm)] if perm else []
            out["permissionMode_values_observed"] = perm_values
            out["permissionMode_initial"] = perm.get("currentValue") if perm else None

            model = find_config_option(new.config_options, "model")
            model_values = [o.get("value") for o in flatten_select_options(model)] if model else []
            out["model_values_observed_count"] = len(model_values)
            out["model_initial"] = model.get("currentValue") if model else None
            out["model_initial_is_m_format"] = (
                isinstance(out["model_initial"], str) and out["model_initial"].startswith("m:")
            )

            te = find_config_option(new.config_options, "thinkingEffort")
            te_values = [o.get("value") for o in flatten_select_options(te)] if te else []
            out["thinkingEffort_values_observed"] = te_values
            out["thinkingEffort_initial"] = te.get("currentValue") if te else None

            # --- permissionMode cycle: default -> auto -> bypassPermissions -> default
            cycle = ["default", "auto", "bypassPermissions", "default"]
            observed_cycle = [v for v in cycle if not perm_values or v in perm_values]
            if not observed_cycle and perm_values:
                observed_cycle = perm_values
            for v in observed_cycle:
                await try_set(pc, out, f"permissionMode->{v}", "permissionMode", v)

            # --- model: swap to a DIFFERENT advertised value, then back.
            # Live finding (first run): empty-variant values (e.g. ...:v:) fail with
            # -32603 "Invalid model reasoning". Prefer a same-variant switch, but also
            # capture the empty-variant failure explicitly.
            if model and model_values and out["model_initial"] in model_values:
                empty_variant = next(
                    (v for v in model_values if v != out["model_initial"] and v.endswith(":v:")), None
                )
                if empty_variant:
                    await try_set(pc, out, "model->empty-variant-advertised", "model", empty_variant)
                same_variant = next(
                    (
                        v
                        for v in model_values
                        if v != out["model_initial"]
                        and not v.endswith(":v:")
                        and v.rsplit(":", 1)[-1] == out["model_initial"].rsplit(":", 1)[-1]
                    ),
                    None,
                )
                different = same_variant or next(
                    (v for v in model_values if v != out["model_initial"]), None
                )
                if different:
                    await try_set(pc, out, "model->different", "model", different)
                    await try_set(pc, out, "model->restore", "model", out["model_initial"])

            # --- thinkingEffort: one advertised effort value (prefer a different one)
            if te and te_values:
                target = next(
                    (v for v in te_values if v != out["thinkingEffort_initial"]), te_values[0]
                )
                await try_set(pc, out, f"thinkingEffort->{target}", "thinkingEffort", target)
                if out["thinkingEffort_initial"] is not None and target != out["thinkingEffort_initial"]:
                    await try_set(
                        pc, out, "thinkingEffort->restore", "thinkingEffort", out["thinkingEffort_initial"]
                    )

            # --- thinkingEffort NOT advertised: blind-set attempts (error capture)
            for tv in ("low", "medium", "high", "minimal", "none"):
                await try_set(pc, out, f"thinkingEffort-blind->{tv}", "thinkingEffort", tv)

            # --- invalid configId
            await try_set(pc, out, "invalid-configId-bogus", "bogus", "x")

            # --- invalid model value
            await try_set(pc, out, "invalid-model-not-a-model", "model", "not-a-model")
        finally:
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "03-setconfig"))
    raise SystemExit(0 if ok else 1)
