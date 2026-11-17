#!/usr/bin/env bash
# archives: back up archive files an application writes itself (Home Assistant, game servers, ...).
#   PLUGIN_DIR          directory on the volume holding the archives (default: backups)
#   PLUGIN_PATTERN      comma separated file globs (default: *.tar,*.tar.gz,*.tgz,*.zip)
#   PLUGIN_RECURSIVE    true to search subdirectories (default: false)
#   PLUGIN_NEWEST       only the N newest matches (default: all)
#   PLUGIN_ALLOW_EMPTY  true to succeed when nothing matches (default: false, so a broken app backup is noticed)
set -euo pipefail
. /plugins/lib.sh

dir="$SOURCE_DIR/$(safe_rel "${PLUGIN_DIR:-backups}")"
[[ -d $dir ]] || { echo "archive directory not found: ${PLUGIN_DIR:-backups}" >&2; exit 1; }
depth=(-maxdepth 1)
if [[ ${PLUGIN_RECURSIVE:-false} == true ]]; then depth=(); fi
names=()
while IFS= read -r g; do names+=(-o -name "$g"); done <<< "$(list_items "${PLUGIN_PATTERN:-*.tar,*.tar.gz,*.tgz,*.zip}")"
found=$(find "$dir" "${depth[@]}" -type f \( "${names[@]:1}" \) -printf '%T@ %p\n' | sort -rn | cut -d' ' -f2-)
if [[ -n ${PLUGIN_NEWEST:-} && -n $found ]]; then found=$(head -n "$PLUGIN_NEWEST" <<< "$found"); fi
if [[ -z $found ]]; then
  if [[ ${PLUGIN_ALLOW_EMPTY:-false} == true ]]; then echo "$dir" > "$OUTPUT_DIR/.backup-paths"; exit 0; fi
  echo "no archives matched in ${PLUGIN_DIR:-backups}" >&2
  exit 1
fi
printf '%s\n' "$found" > "$OUTPUT_DIR/.backup-paths"
