"""P1: initialize + session/new — full capability/configOptions capture."""

import asyncio

from probe_common import (
    PROBE_DIR,
    dump,
    flatten_select_options,
    find_config_option,
    initialize,
    open_probe,
    run_probe,
    wf,
)


async def probe() -> dict:
    out: dict = {}
    async with open_probe("01-initialize.json") as pc:
        try:
            init = await initialize(pc.conn, "p1")
            out["initialize_response"] = dump(init)
            out["initialize_protocol_version"] = init.protocol_version
            new = await wf(pc.conn.new_session(cwd=str(PROBE_DIR)), "session/new")
            out["session_new_response"] = dump(new)
            out["session_id"] = new.session_id

            config_options = dump(new.config_options) if new.config_options else None
            out["config_ids_observed"] = (
                [c.get("configId") or c.get("id") for c in config_options] if config_options else []
            )

            # model option values (expect m:<provider>:<model>:u / m:...:v:<variant>)
            model_opt = find_config_option(new.config_options, "model")
            if model_opt:
                out["model_option_current_value"] = model_opt.get("currentValue")
                flat = flatten_select_options(model_opt)
                out["model_option_values"] = [o.get("value") for o in flat]
                out["model_option_count"] = len(flat)

            # permissionMode option
            perm_opt = find_config_option(new.config_options, "permissionMode")
            if perm_opt:
                out["permissionMode_option"] = perm_opt
                out["permissionMode_option_values"] = [
                    o.get("value") for o in flatten_select_options(perm_opt)
                ]

            # thinkingEffort options
            te_opt = find_config_option(new.config_options, "thinkingEffort")
            if te_opt:
                out["thinkingEffort_option"] = te_opt
                out["thinkingEffort_values"] = [
                    o.get("value") for o in flatten_select_options(te_opt)
                ]

            # modes + models
            if new.modes is not None:
                out["modes"] = dump(new.modes)
            if new.models is not None:
                out["models_current"] = new.models.current_model_id
                out["models_available_ids"] = [
                    getattr(m, "model_id", None) or dump(m).get("modelId")
                    for m in (new.models.available_models or [])
                ]
        finally:
            await pc.shutdown()
    return out


if __name__ == "__main__":
    ok = asyncio.run(run_probe(probe, "01-initialize"))
    raise SystemExit(0 if ok else 1)
