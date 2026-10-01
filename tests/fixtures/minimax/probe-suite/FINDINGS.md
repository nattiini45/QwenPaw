# mcode 0.5.10 — Live ACP Contract Probe Findings

**Date:** 2026-10-01 (Europe/Berlin)
**Target:** `mcode` 0.5.10 (`/usr/local/bin/mcode` wrapper → `/opt/node24/bin/node` v24.21.0), already authenticated
**Client SDK:** `agent-client-protocol` **0.9.0** (`acp`, `PROTOCOL_VERSION = 1`), driven via `spawn_agent_process` + `ClientSideConnection` (same pattern as `qwenpaw/agents/acp/service.py::_open_conversation`)
**Method:** one fresh `mcode acp` process per probe run (one client connection per process), cwd `/root/mcode-probes`, every raw JSON-RPC frame recorded verbatim as JSONL in `fixtures/` (`dir: c2a|a2c`).

## Global wire-format notes (apply everywhere)

- `session/update` params use a **flat string tag**: `{"sessionId": "...", "update": {"sessionUpdate": "agent_message_chunk", "messageId": "...", "content": {...}}}` — payload fields are *siblings* of the `"sessionUpdate"` tag, not nested under it.
- Update tags observed live: `user_message_chunk`, `agent_message_chunk`, `agent_thought_chunk`, `tool_call`, **`tool_call_update`** (⚠️ not a stock ACP discriminator; mcode uses it for progress/completion instead of re-sending `tool_call`), `available_commands_update`, `current_mode_update` (n/a observed), `config_option_update`, `session_info_update`, `usage_update`.
- `initialize` response carries an **undocumented vendor extension block**: `_meta["minimax-code/extensions"] = {"version": 1, "methods": ["session/activate", "mcode/session/activate", "mcode/session/steer", "mcode/session/queue/list", "mcode/session/queue/enqueue", "mcode/session/queue/update", "mcode/session/queue/delete", "mcode/session/queue/steer", "mcode/session/goal/get", "mcode/session/goal/create", "mcode/session/goal/patch", "mcode/session/goal/clear", "mcode/session/delegation/get", "mcode/session/delegation/stop"], "notifications": ["mcode/session/current_session_update", "mcode/session/queue_update", "mcode/session/goal_update", "mcode/session/delegation_update"]}`.
- `available_commands_update` advertises ~180 commands (built-in: `help, new, model, status, doctor, context, skills, mcp, usage, compact, init, ...` plus a large skills catalog). The `model` command carries `input.hint = "[provider/model[#variant]]"`.

---

## P1 — initialize + session/new ✅ (`fixtures/01-initialize.json`)

**Verdict:** Matches expectations, with config-surface drift.

- `initialize` → `protocolVersion: 1` (int echo), `agentInfo = {"name": "minimax-code", "title": "MiniMax Code", "version": "0.5.10"}`, `agentCapabilities = {"loadSession": true, "mcpCapabilities": {"http": true, "sse": true}, "promptCapabilities": {"audio": false, "embeddedContext": false, "image": false}, "sessionCapabilities": {"close": {}, "fork": {}, "list": {}, "resume": {}}}` — all as expected.
- `session/new` response keys: `sessionId`, `modes`, `configOptions`. `modes = {"currentModeId": "default", "availableModes": [{"id": "default", ...}, {"id": "plan", ...}]}`.

**Drift:**
1. **configOptions contain exactly 2 options: `permissionMode` and `model`. `thinkingEffort` is NOT advertised** (for the account's current model set).
2. Config option wire key is **`"id"`** (not `"configId"`), plus non-stock fields `name`, `description`, `category` (`"_permission"`, `"model"`), `type: "select"`.
3. `permissionMode` option carries `_meta = {"minimax-code/scope": "process"}`; values `[{"value": "default", "name": "Ask"}, {"value": "auto", "name": "Auto"}, {"value": "bypassPermissions", "name": "Full access"}]`; **initial `currentValue` is `"auto"`**, not `"default"`.
4. Model values (6, all provider `minimax`): `m:minimax:MiniMax-M3.1-Flash-Preview:v:`, `m:minimax:MiniMax-M3.1-Flash-Preview:v:thinking`, `m:minimax:MiniMax-M3:v:`, `m:minimax:MiniMax-M3:v:thinking`, `m:minimax:MiniMax-M2.7-highspeed:v:thinking`, `m:minimax:MiniMax-M2.7:v:thinking`. Current: `m:minimax:MiniMax-M3:v:thinking`. **Format is `m:<provider>:<model>:v:<variant>` only — the variant may be EMPTY (`:v:`); no `:u` suffix observed.**
5. **No top-level `models` field** in session/new (model is exposed solely as the `model` configOption).

## P2 — prompt PONG ✅ (`fixtures/02-prompt-pong.json`)

**Verdict:** Works; reply exactly `PONG`, `stopReason: "end_turn"`.

- Update stream (1 prompt): `available_commands_update` ×3, `session_info_update` ×2 (`{"updatedAt": "<ISO8601>"}`), `agent_thought_chunk` ×3 (streamed reasoning text), `agent_message_chunk` ×1, `usage_update` ×1.
- `agent_message_chunk.content` is a **single content block**: `{"type": "text", "text": "PONG"}` (not an array); `messageId` = agent-side UUID.

**Drift:** `PromptResponse.userMessageId = null` (absent), `PromptResponse.usage = null` — usage arrives **only** as a `usage_update` notification, duplicated at turn end (2 identical frames).

## P3 — setConfigOption matrix ✅ (`fixtures/03-setconfig.json`)

**Verdict:** All valid sets succeed and broadcast; error taxonomy crisp; two real drifts.

| step | result |
|---|---|
| `permissionMode` → default → auto → bypassPermissions → default | ✅ each returns full `configOptions` with updated `currentValue`, and broadcasts a `config_option_update` notification carrying the full configOptions list |
| `model` → `m:minimax:MiniMax-M3.1-Flash-Preview:v:` (advertised, empty variant) | ❌ `-32603 "Internal error"` `data: {"details": "Invalid model reasoning"}` — **an advertised value is rejected** |
| `model` → `m:minimax:MiniMax-M3.1-Flash-Preview:v:thinking` (thinking→thinking) | ✅ then restore ✅ |
| `thinkingEffort` → low/medium/high/minimal/none | ❌ all `-32602 "Invalid params: Thinking effort is not advertised for the selected model: <value>"` |
| configId `"bogus"` | ❌ `-32602 "Invalid params: Unsupported Session configuration option: bogus"` |
| `model` = `"not-a-model"` | ❌ `-32602 "Invalid params: Invalid model config value: not-a-model"` |

**Drift:** (1) empty-variant (`:v:`) model options are advertised but **cannot be set** ("Invalid model reasoning"); only same-reasoning-family switches observed working. (2) `thinkingEffort` exists as a configId (distinct error text) but is not advertised for the current models, so it's effectively unsettable on this account.

## P4 — session/list / load / resume ✅ (`fixtures/04-sessions.json`)

**Verdict:** All three work; replay confirmed; resume attaches without replay.

- `session/list` → `sessions: [{cwd, sessionId, updatedAt}, ...]` (**no `title` field**), `nextCursor: null` (6 sessions listed, no pagination exercised).
- `session/load` → response keys `[configOptions, modes]` (**sessionId not echoed**); history replayed as 6 `session/update` frames: `user_message_chunk` (the original prompt text), `agent_thought_chunk` ×1, `agent_message_chunk` ("PONG"), `available_commands_update` ×3. No `session_info_update`/`usage_update` replay.
- Fresh process → `initialize` → `session/resume` same id → response keys `[configOptions, modes]`; **zero history replay** (only 3 fresh `available_commands_update`); liveness prompt "Reply with exactly: ATTACHED" → `stopReason: "end_turn"` — attach works.

## P5 — permission flow ✅ (`fixtures/05-permission.json`)

**Verdict:** `session/request_permission` captured verbatim; deny path graceful — but the *trigger conditions* drift hard from expectations.

- **In-cwd write under `permissionMode=default` (Ask): NO permission request.** File `perm-test.txt` created directly (`tool_call {title: "write", name: "write", kind: "edit"}` → completed). Assistant: "Created [perm-test.txt](/root/mcode-probes/perm-test.txt) containing the word `hello` (5 bytes)."
- **Shell exec (`echo shell-marker`) under Ask mode: NO permission request.** `tool_call {title: "bash", kind: "execute"}` ran, `rawOutput.details.execution = {"status": "succeeded", "reason": "exited", "exitCode": 0, "timing": {...}}`.
- **Out-of-workspace write DID ask.** Full request payload:
  - `options`: `[{"kind": "allow_once", "name": "Allow once", "optionId": "allow-once"}, {"kind": "allow_always", "name": "Always allow", "optionId": "allow-always"}, {"kind": "reject_once", "name": "Deny", "optionId": "deny"}]` — **no `reject_always` offered; optionIds are kebab-case** (`allow-once`, `allow-always`, `deny`).
  - `toolCall`: `{"kind": "edit", "locations": [{"path": "/tmp/mcode-probe-outside.txt"}], "rawInput": {"path": "/tmp/mcode-probe-outside.txt", "content": "hello"}, "status": "pending", "title": "write", "toolCallId": "perm_404df6086ebf4f6f9afd0eea00bf9722"}`.
- **DENY** (`{"outcome": "selected", "optionId": "deny"}`) → turn proceeds to normal completion (`end_turn`), file NOT created; agent explains: "path.Permission denied — the runtime blocked the write because `/tmp/mcode-probe-outside.txt` is outside the workspace (`/root/mcode-probes`) and the configured allow paths."
- Repeat prompt with allow-once policy armed: **no second permission request arrived** — the agent did not re-attempt the write (treats the deny/runtime block as sticky within the session). File remained absent. So the allow-once branch could not be exercised against the same path; in-cwd allow path (phase 2) needed no permission at all.

**Drift:** "Ask" mode asks only for out-of-workspace actions; workspace edits and shell commands are auto-allowed. A `reject_once` deny is effectively sticky for the identical follow-up request (no re-ask observed).

## P6 — MCP stdio overlay ✅ (`fixtures/06-mcp-overlay.json`)

**Verdict:** Overlay accepted end-to-end; MCP tool surfaced and executed.

- `session/new` with `mcpServers: [{"name": "echo-test", "type": "stdio", "command": "npx", "args": ["-y", "@modelcontextprotocol/server-everything"], "env": []}]` → success (sessionId returned, no error). Note: SDK 0.9.0 requires `env` as a **list**, `env: {}` is rejected client-side by pydantic.
- Prompt → `tool_call {"title": "mcp__echo-test__echo", "name": "mcp__echo-test__echo", "kind": "other"}` → `tool_call_update status: "completed"` with `rawInput: {"message": "overlay-ok"}` (tool args as-is), `rawOutput: {"content": [{"type": "text", "text": "Echo: overlay-ok"}], "details": {"mcp": {"content": [...], "isError": false}, "server": "echo-test", "tool": "echo", "is_error": false}}`.
- Agent reply "Echo: overlay-ok", `stopReason: "end_turn"`. **MCP tool naming: `mcp__<server>__<tool>`; `kind: "other"`.**

## P7 — usage/cost ✅ (`fixtures/07-usage-cost.json`)

**Verdict:** usage update is exactly `{used, size, cost}` — no token breakdown.

- Verbatim: `{"sessionUpdate": "usage_update", "used": 16847, "size": 512000, "cost": {"amount": 0.0, "currency": "USD"}}` (other runs: used 15927/22639).
- **Drift:** no `inputTokens`/`outputTokens`/`thoughtTokens`/cached fields in the notification; `PromptResponse.usage` is `null`; `cost.amount` reported as `0` USD on this account (subscription-billed); `size` fixed at 512000.

---

## Artifacts

| probe | script | fixture (JSONL frames) |
|---|---|---|
| P1 | `01_initialize.py` | `fixtures/01-initialize.json` (4 frames) |
| P2 | `02_prompt_pong.py` | `fixtures/02-prompt-pong.json` (17) |
| P3 | `03_setconfig.py` | `fixtures/03-setconfig.json` (39) |
| P4 | `04_sessions.py` | `fixtures/04-sessions.json` (34, two processes) |
| P5 | `05_permission.py` | `fixtures/05-permission.json` (510) |
| P6 | `06_mcp_overlay.py` | `fixtures/06-mcp-overlay.json` (41) |
| P7 | `07_usage_cost.py` | `fixtures/07-usage-cost.json` (23) |

Parsed highlights per probe: `state/*.summary.json`. Agent stderr per probe: `fixtures/*.stderr.log`. Harness: `probe_common.py`; runner: `run_probes.sh`.

**Test hygiene:** perm-test.txt deleted after P5; `/tmp/mcode-probe-outside.txt` never existed (write denied); no mcode processes left running; only side effects outside `/root/mcode-probes` were mcode's own session-store updates under its HOME.
