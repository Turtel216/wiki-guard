# Shared helpers for the scripts in this directory. Source it, do not run it.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Print the value pinned in versions.toml for a "section.key" name.
pinned() {
  local section="${1%%.*}" key="${1#*.}" value
  value="$(awk -v section="[$section]" -v key="$key" '
    /^\[/ { inside = ($0 == section) }
    inside && $1 == key && $2 == "=" { gsub(/"/, "", $3); print $3; exit }
  ' "$REPO_ROOT/versions.toml")"
  if [ -z "$value" ]; then
    echo "versions.toml has no entry for '$1'" >&2
    return 1
  fi
  echo "$value"
}

# Stop with a clear message when a command line tool is missing.
need() {
  local tool
  for tool in "$@"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "missing tool: $tool" >&2
      exit 1
    fi
  done
}
