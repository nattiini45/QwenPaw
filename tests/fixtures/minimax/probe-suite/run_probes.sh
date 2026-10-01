#!/bin/sh
# Run all mcode ACP probes sequentially; failures don't block later probes.
PY=/root/.local/share/pipx/venvs/qwenpaw/bin/python
cd /root/mcode-probes || exit 1
for p in 01_initialize 02_prompt_pong 03_setconfig 04_sessions 05_permission 06_mcp_overlay 07_usage_cost; do
  echo "=== $p ==="
  timeout 300 "$PY" "$p.py"
  echo "exit=$?"
done
