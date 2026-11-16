#!/usr/bin/env bash
# files: back up the whole volume or selected paths. For volumes without an application-level backup.
#   PLUGIN_PATHS    comma/newline separated paths relative to the volume (default: everything)
#   PLUGIN_EXCLUDE  comma/newline separated restic exclude patterns; a leading "/" means the volume root
set -euo pipefail
. /plugins/lib.sh

: > "$OUTPUT_DIR/.backup-paths"
: > "$OUTPUT_DIR/.backup-excludes"
while IFS= read -r p; do
  rel=$(safe_rel "$p")
  abs="$SOURCE_DIR/$rel"
  abs=${abs%/}
  abs=${abs%/.}
  [[ -e $abs ]] || { echo "path not found on the volume: $p" >&2; exit 1; }
  echo "$abs" >> "$OUTPUT_DIR/.backup-paths"
done <<< "$(list_items "${PLUGIN_PATHS:-.}")"
add_excludes "${PLUGIN_EXCLUDE:-}"
