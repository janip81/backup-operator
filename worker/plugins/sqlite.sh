#!/usr/bin/env bash
# sqlite: consistent copies of SQLite databases (WAL applied), integrity-checked, plus the other files.
#   PLUGIN_PATTERN        comma separated database file globs (default: *.db,*.sqlite,*.sqlite3)
#   PLUGIN_INCLUDE_OTHER  also back up all other files on the volume (default: true)
#   PLUGIN_EXCLUDE        extra exclude patterns for the other files (see the files plugin)
set -euo pipefail
. /plugins/lib.sh

work="${OUTPUT_DIR:?}/.work"
mkdir -p "$work" "$OUTPUT_DIR/volume"
: > "$OUTPUT_DIR/.backup-excludes"
names=()
while IFS= read -r g; do names+=(-o -name "$g"); done <<< "$(list_items "${PLUGIN_PATTERN:-*.db,*.sqlite,*.sqlite3}")"
count=0
while IFS= read -r -d '' db; do
  head -c 16 "$db" 2>/dev/null | grep -q '^SQLite format 3' || continue
  rel=${db#"$SOURCE_DIR"/}
  # The snapshot clone is read-only: copy the database with its WAL so SQLite applies it, then .backup.
  find "${work:?}" -mindepth 1 -delete
  cp --preserve=timestamps "$db" "$work/db"   # no ownership: backups run without CAP_CHOWN
  for ext in -wal -shm -journal; do
    if [[ -f "$db$ext" ]]; then cp --preserve=timestamps "$db$ext" "$work/db$ext"; fi
  done
  # Copies go to $OUTPUT_DIR/volume/<path>: restore overlays them onto the volume at their original place.
  mkdir -p "$OUTPUT_DIR/volume/$(dirname "$rel")"
  sqlite3 "$work/db" ".backup '$OUTPUT_DIR/volume/$rel'"
  check=$(sqlite3 "$OUTPUT_DIR/volume/$rel" 'PRAGMA integrity_check;')
  [[ $check == ok ]] || { echo "integrity check failed for $rel: $check" >&2; exit 1; }
  # Backups can't chown, so remember the original owner and mode; restore re-applies them.
  printf '%s\t%s\n' "$(stat -c '%u %g %a' "$db")" "$rel" >> "$OUTPUT_DIR/volume.owners"
  for f in "$db" "$db-wal" "$db-shm" "$db-journal"; do echo "$f" >> "$OUTPUT_DIR/.backup-excludes"; done
  count=$((count + 1))
done < <(find "$SOURCE_DIR" -type f \( "${names[@]:1}" \) -print0)
find "${work:?}" -delete
[[ $count -gt 0 ]] || { echo "no SQLite databases found" >&2; exit 1; }

: > "$OUTPUT_DIR/.backup-paths"
if [[ ${PLUGIN_INCLUDE_OTHER:-true} == true ]]; then
  echo "$SOURCE_DIR" >> "$OUTPUT_DIR/.backup-paths"
  add_excludes "${PLUGIN_EXCLUDE:-}"
fi
echo "copied $count database(s)" >&2
