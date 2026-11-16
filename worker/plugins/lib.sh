# Shared helpers for the built-in plugins (sourced, not executed).

# list_items "a, b<newline>c" -> one trimmed item per line
list_items() {
  tr ',' '\n' <<< "${1:-}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | grep -v '^$' || true
}

# safe_rel PATH -> the path relative to the volume; refuses ".." traversal
safe_rel() {
  local p=${1#/}
  case "/$p/" in
    */../*) echo "path escapes the volume: $1" >&2; return 1 ;;
  esac
  printf '%s' "$p"
}

# add_excludes LIST -> appends exclude patterns; a leading "/" means the volume root
add_excludes() {
  local e
  while IFS= read -r e; do
    [[ -n $e ]] || continue
    if [[ $e == /* ]]; then echo "$SOURCE_DIR$e"; else echo "$e"; fi >> "$OUTPUT_DIR/.backup-excludes"
  done <<< "$(list_items "${1:-}")"
}
