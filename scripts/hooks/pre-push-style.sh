#!/usr/bin/env bash
# Claude Code PreToolUse hook for the Bash tool. When the command opens a pull
# request or pushes, run `just style` first and deny the command on failure.
set -uo pipefail
cmd="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("command",""))')"
case "$cmd" in
    *"gh pr create"*|*"jj git push"*|*"git push"*) ;;
    *) exit 0 ;;
esac
root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
log="$(mktemp)"
if (cd "$root" && just style) >"$log" 2>&1; then
    rm -f "$log"
    exit 0
fi
python3 - "$log" <<'PY'
import json, sys
reason = ("just style failed. Fix the style findings before you push or open a "
          f"pull request. Log: {sys.argv[1]}")
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
      "permissionDecision": "deny", "permissionDecisionReason": reason}}))
PY
