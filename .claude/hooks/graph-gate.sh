#!/usr/bin/env bash
# PreToolUse gate for Edit/Write.
#
# Blocks the FIRST Python-source edit of each session until the Graphify
# knowledge graph in graphify-out/ has been consulted. One deny per session;
# every edit after that passes straight through.
#
# Rationale: the CLAUDE.md instruction to read graphify-out/ is advisory, so it
# competes with everything else in context and gets skipped on edits that look
# small. This makes it mechanical.
set -u

payload=$(cat)

# Only gate Python sources. Docs, JSON and config edits don't need the graph.
echo "$payload" | grep -qE '"file_path"[[:space:]]*:[[:space:]]*"[^"]*\.py"' || exit 0

root="${CLAUDE_PROJECT_DIR:-$PWD}"
report="$root/graphify-out/GRAPH_REPORT.md"
graph="$root/graphify-out/graph.json"
[ -f "$report" ] || exit 0

# One gate per session: keyed on session_id so /clear or a new session re-arms it.
session=$(echo "$payload" | grep -oE '"session_id"[[:space:]]*:[[:space:]]*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
marker="${TMPDIR:-/tmp}/claude-graph-gate-${session:-nosession}"
[ -f "$marker" ] && exit 0
: > "$marker"

stale=""
if [ -n "$(find "$root/src" -name '*.py' -newer "$graph" -print -quit 2>/dev/null)" ]; then
  stale=" The graph is OLDER than some source files, so treat it as a map rather than ground truth: confirm anything load-bearing against the code, and tell the user it needs a rebuild."
fi

reason="Read graphify-out/GRAPH_REPORT.md before editing Python sources in this repo, then repeat this edit. If the change touches client-specific behaviour, also compare the KZO / KZP / KZDW / S5 / UMBER branches (graphify-out/graph.json, or the graphify skill) before editing shared logic.${stale} This gate fires once per session."

printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"%s"},"suppressOutput":true}\n' "$reason"
exit 0
