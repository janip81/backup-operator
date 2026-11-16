#!/usr/bin/env bash
# Worker integration tests (#12, #13): runs the real worker image the way the operator runs it
# (root with only DAC_READ_SEARCH, read-only root filesystem, read-only source) against a local
# restic repository, then restores and compares hashes. Needs docker.
#   WORKER_IMAGE=... tests/worker/test_worker.sh
set -euo pipefail
IMG=${WORKER_IMAGE:-bo-worker:test}
PY_IMG=python:3.13-slim
tmp=$(mktemp -d)
id=$$
repo=bo-test-repo-$id
repo_nonroot=bo-test-repo-nonroot-$id
restore_vol=bo-test-restore-$id
cleanup() {
  docker volume rm -f "$repo" "$repo_nonroot" "$restore_vol" >/dev/null 2>&1 || true
  rm -rf -- "${tmp:?}"
}
trap cleanup EXIT
pass=0
ok() { echo "ok - $*"; pass=$((pass + 1)); }
die() { echo "FAIL - $*" >&2; exit 1; }

mkdir -p "$tmp/creds" "$tmp/term"
chmod 0777 "$tmp/term"
printf 'correct horse battery staple' > "$tmp/creds/RESTIC_PASSWORD"
chmod 0600 "$tmp/creds/RESTIC_PASSWORD"
docker volume create "$repo" >/dev/null

# worker SRC_DIR [ENV=VAL ...] -- [command]
worker() {
  local src=$1; shift
  local envs=()
  while [[ $# -gt 0 && $1 != -- ]]; do envs+=(-e "$1"); shift; done
  [[ ${1:-} == -- ]] && shift
  # backups only read (DAC_READ_SEARCH); restores write into existing trees and restore ownership
  local caps=(--cap-add DAC_READ_SEARCH) extra=()
  if [[ ${NO_READ_CAP:-} == 1 ]]; then caps=(); fi   # simulate files the worker can't read
  [[ ${1:-} == restore ]] && caps+=(--cap-add CHOWN --cap-add FOWNER --cap-add DAC_OVERRIDE)
  rm -f -- "$tmp/term/msg"
  docker run --rm --read-only --tmpfs /tmp --tmpfs /output --user 0 --cap-drop ALL "${caps[@]}" \
    --security-opt no-new-privileges --network none \
    -v "$src:/source:ro" -v "$repo:/repo" -v "$tmp/creds:/etc/backup/creds:ro" -v "$tmp/term:/term" \
    -v "$restore_vol:/restore" \
    -e RESTIC_REPOSITORY=/repo -e TERMINATION_LOG=/term/msg -e BACKUP_SOURCE=test -e BACKUP_HOST=app \
    "${extra[@]}" "${envs[@]}" "$IMG" "$@"
}
# the worker writes the message root-only (umask 077), as in a pod; read it through a container
msg() { docker run --rm --user 0 -v "$tmp/term:/term:ro" --entrypoint sh "$IMG" -c 'cat /term/msg 2>/dev/null || true'; }
# hashes of a host directory, relative paths
host_hashes() { (cd "$1" && find . -type f -print0 | sort -z | xargs -0 -r sha256sum); }
# hashes of the restore volume
restored_hashes() {
  docker run --rm --user 0 -v "$restore_vol:/r:ro" --entrypoint sh "$IMG" -c \
    'cd /r && find . -type f -print0 | sort -z | xargs -0 -r sha256sum'
}
reset_restore() {
  docker volume rm -f "$restore_vol" >/dev/null
  docker volume create "$restore_vol" >/dev/null
}

# ---------------------------------------------------------------- files plugin, versions, retention
src=$tmp/src
mkdir -p "$src/dir"
echo "version one" > "$src/a.txt"
head -c 200000 /dev/urandom > "$src/dir/b.bin"
echo "owner-only" > "$src/private.txt"
chmod 0000 "$src/private.txt"   # readable only through DAC_READ_SEARCH
h1=$(host_hashes "$src" 2>/dev/null || true)
chmod 0600 "$src/private.txt"; h1=$(host_hashes "$src"); chmod 0000 "$src/private.txt"

worker "$src" PLUGIN=files -- backup >/dev/null 2>&1 || { msg; die "backup 1"; }
s1=$(msg | jq -r .snapshotId)
[[ $(msg | jq -r .filesNew) == 3 ]] || die "backup 1 file count: $(msg)"
ok "backup 1 stored snapshot ${s1:0:8} with 3 new files"

echo "version two" > "$src/a.txt"   # overwrite
rm -f -- "$src/dir/b.bin"           # delete
echo "new" > "$src/c.txt"           # add
chmod 0600 "$src/private.txt"; h2=$(host_hashes "$src"); chmod 0000 "$src/private.txt"
worker "$src" PLUGIN=files -- backup >/dev/null 2>&1 || { msg; die "backup 2"; }
s2=$(msg | jq -r .snapshotId)
ok "backup 2 stored snapshot ${s2:0:8}"

reset_restore
worker "$src" RESTORE_TARGET=/restore "RESTORE_SNAPSHOT=$s1" -- restore >/dev/null 2>&1 || { msg; die "restore 1"; }
[[ $(restored_hashes) == "$h1" ]] || die "restore of snapshot 1 differs from the original"
ok "restore of snapshot 1 matches (overwritten and deleted files come back)"
reset_restore
worker "$src" RESTORE_TARGET=/restore "RESTORE_SNAPSHOT=$s2" -- restore >/dev/null 2>&1 || { msg; die "restore 2"; }
[[ $(restored_hashes) == "$h2" ]] || die "restore of snapshot 2 differs"
ok "restore of snapshot 2 matches"

worker "$src" PLUGIN=files KEEP_LAST=2 -- backup >/dev/null 2>&1 || { msg; die "backup 3"; }
[[ $(msg | jq '.retention.kept | length') == 2 && $(msg | jq .retention.removed) == 1 ]] \
  || die "retention: $(msg)"
ok "retention kept 2 snapshots and removed 1"
reset_restore
if worker "$src" RESTORE_TARGET=/restore "RESTORE_SNAPSHOT=$s1" -- restore >/dev/null 2>&1; then
  die "pruned snapshot 1 should be gone"
fi
ok "pruned snapshot is gone"
worker "$src" RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "restore latest"; }
[[ $(restored_hashes) == "$h2" ]] || die "restore of latest differs"
ok "restore of latest matches"

# a partial backup (unreadable file) fails and never becomes "latest" or a retention candidate
chmod 0600 "$src/private.txt"; echo "changed" >> "$src/private.txt"; chmod 0000 "$src/private.txt"  # must be re-read
chmod 0644 "$tmp/creds/RESTIC_PASSWORD"   # without the capability, only the 0000 source file is unreadable
NO_READ_CAP=1 worker "$src" PLUGIN=files -- backup >/dev/null 2>&1 && die "partial backup accepted"
NO_READ_CAP=1 worker "$src" PLUGIN=files ALLOW_PARTIAL=true -- backup >/dev/null 2>&1 && die "partial accepted via ALLOW_PARTIAL"
chmod 0600 "$tmp/creds/RESTIC_PASSWORD"
[[ $(msg | jq -r .error) == backup-incomplete ]] || die "partial error: $(msg)"
reset_restore
worker "$src" RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "restore after partial"; }
[[ $(restored_hashes) == "$h2" ]] || die "latest after a partial backup is not the last complete snapshot"
ok "partial backup fails and is never selected as latest"

# snapshots not made by this worker (no incomplete/complete tags) are never removed
docker run --rm --user 0 -v "$src:/data:ro" -v "$repo:/repo" -v "$tmp/creds:/c:ro" --entrypoint restic "$IMG" \
  -r /repo --password-file /c/RESTIC_PASSWORD backup --host app --tag source=test --tag manual-by-admin /data >/dev/null
worker "$src" PLUGIN=files -- backup >/dev/null 2>&1 || { msg; die "backup after manual snapshot"; }
count=$(docker run --rm --user 0 -v "$repo:/repo" -v "$tmp/creds:/c:ro" --entrypoint restic "$IMG" \
  -r /repo --password-file /c/RESTIC_PASSWORD snapshots --json --tag manual-by-admin | jq length)
[[ $count == 1 ]] || die "a foreign snapshot was removed"
# an abandoned run (tagged incomplete, e.g. the pod was killed) is cleaned up by the next successful run
docker run --rm --user 0 -v "$src:/data:ro" -v "$repo:/repo" -v "$tmp/creds:/c:ro" --entrypoint restic "$IMG" \
  -r /repo --password-file /c/RESTIC_PASSWORD backup --host app --tag source=test --tag incomplete /data >/dev/null 2>&1 || true
worker "$src" PLUGIN=files -- backup >/dev/null 2>&1 || { msg; die "backup after abandoned run"; }
left=$(docker run --rm --user 0 -v "$repo:/repo" -v "$tmp/creds:/c:ro" --entrypoint restic "$IMG" \
  -r /repo --password-file /c/RESTIC_PASSWORD snapshots --json --tag incomplete | jq length)
[[ $left == 0 ]] || die "abandoned incomplete snapshot was not cleaned up"
ok "abandoned incomplete snapshots are cleaned up by the next successful run"
chmod 0600 "$src/private.txt"; h3=$(host_hashes "$src"); chmod 0000 "$src/private.txt"
ok "snapshots made outside the operator are never removed"

# same source name in another namespace (host) in the same repository: restores stay separate
other=$tmp/other; mkdir -p "$other"; echo "other namespace" > "$other/x.txt"
worker "$other" BACKUP_HOST=ns2 -- backup >/dev/null 2>&1 || { msg; die "ns2 backup"; }
reset_restore
worker "$src" RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "restore app"; }
[[ $(restored_hashes) == "$h3" ]] || die "restore of latest picked another namespace's data"
reset_restore
worker "$src" BACKUP_HOST=ns2 RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "restore ns2"; }
[[ $(restored_hashes) == "$(host_hashes "$other")" ]] || die "restore of ns2 differs"
ok "same source name in two namespaces restores separately"

worker "$src" PLUGIN=files PLUGIN_PATHS=dir/../.. -- backup >/dev/null 2>&1 && die "path traversal accepted"
ok "path traversal in plugin config is refused"

# ---------------------------------------------------------------- archives plugin
src2=$tmp/src2
mkdir -p "$src2/backups" "$src2/other"
echo x > "$src2/backups/x.tar"; echo y > "$src2/backups/y.tar.gz"
echo n > "$src2/backups/notes.txt"; echo z > "$src2/other/z.tar"
worker "$src2" PLUGIN=archives BACKUP_SOURCE=arch -- backup >/dev/null 2>&1 || { msg; die "archives backup"; }
reset_restore
worker "$src2" BACKUP_SOURCE=arch RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "archives restore"; }
files=$(restored_hashes | awk '{print $2}' | tr '\n' ' ')
[[ $files == "./backups/x.tar ./backups/y.tar.gz " ]] || die "archives selected: $files"
ok "archives plugin backs up only the app's archives"
mkdir -p "$tmp/src3/backups"
worker "$tmp/src3" PLUGIN=archives BACKUP_SOURCE=empty -- backup >/dev/null 2>&1 && die "empty archive dir accepted"
[[ $(msg | jq -r .error) == plugin-failed ]] || die "empty archives error: $(msg)"
ok "archives plugin fails loudly when the app wrote no archives"

# ---------------------------------------------------------------- sqlite plugin with an uncheckpointed WAL
src4=$tmp/src4
mkdir -p "$src4/data"
echo "config: yes" > "$src4/config.yaml"
docker run --rm --user "$(id -u):$(id -g)" -v "$src4/data:/d" "$PY_IMG" python -c '
import shutil, sqlite3
c = sqlite3.connect("/tmp/app.db")
c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA wal_autocheckpoint=0")
c.execute("CREATE TABLE t (n INTEGER)")
c.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(100)]); c.commit()
for ext in ("", "-wal", "-shm"):   # copy while the connection is open: rows live in the WAL only
    shutil.copy("/tmp/app.db" + ext, "/d/app.db" + ext)
'
[[ -s $src4/data/app.db-wal ]] || die "test setup: no WAL"
worker "$src4" PLUGIN=sqlite BACKUP_SOURCE=sql -- backup >/dev/null 2>&1 || { msg; die "sqlite backup"; }
reset_restore
worker "$src4" BACKUP_SOURCE=sql RESTORE_TARGET=/restore -- restore >/dev/null 2>&1 || { msg; die "sqlite restore"; }
rows=$(docker run --rm --user 0 -v "$restore_vol:/r" --entrypoint sh "$IMG" -c \
  'sqlite3 /r/data/app.db "PRAGMA integrity_check; SELECT count(*) FROM t;"' | tr '\n' ' ')
[[ $rows == "ok 100 " ]] || die "sqlite restore: $rows"
docker run --rm --user 0 -v "$restore_vol:/r" --entrypoint sh "$IMG" -c 'test -f /r/config.yaml && ! test -e /r/data/app.db-wal' \
  || die "sqlite restore layout"
owner=$(docker run --rm --user 0 -v "$restore_vol:/r" --entrypoint stat "$IMG" -c '%u:%g %a' /r/data/app.db)
[[ $owner == "$(stat -c '%u:%g %a' "$src4/data/app.db")" ]] || die "restored db owner/mode: $owner"
ok "sqlite plugin restores a consistent database (WAL-only rows, original owner and mode) plus other files"

# ---------------------------------------------------------------- fail closed, no secrets in output
out=$(worker "$src2" RESTIC_REPOSITORY=s3:http://minio.invalid/bucket -- backup 2>&1) && die "plain http accepted"
[[ $(msg | jq -r .error) == insecure-repository ]] || die "http error: $(msg)"
ok "plain-http repository is refused"
worker "$src2" RESTIC_REPOSITORY=sftp:backup@nas.invalid:/srv/repo -- backup >/dev/null 2>&1 && die "sftp w/o keys accepted"
[[ $(msg | jq -r .error) == credentials-missing ]] || die "sftp error: $(msg)"
ok "sftp without key and known_hosts is refused"
mv "$tmp/creds/RESTIC_PASSWORD" "$tmp/pw"
worker "$src2" -- backup >/dev/null 2>&1 && die "missing password accepted"
mv "$tmp/pw" "$tmp/creds/RESTIC_PASSWORD"
ok "missing repository password is refused"
printf 'Zq9-not-the-password-7Xv' > "$tmp/creds/RESTIC_PASSWORD"
out=$(worker "$src2" -- backup 2>&1) && die "wrong password accepted"
printf 'correct horse battery staple' > "$tmp/creds/RESTIC_PASSWORD"
if grep -q "Zq9-not-the-password-7Xv\|correct horse" <<< "$out$(msg)"; then die "password leaked into output"; fi
ok "wrong password fails without printing the password"

# termination messages carry only fixed error codes, never tool output
[[ $(msg | jq -c 'keys') == '["error"]' && $(msg | jq -r .error) =~ ^[a-z-]+$ ]] || die "termination message: $(msg)"
ok "termination message holds only a fixed error code"

# non-root mode (spec.worker.runAsUser): no capabilities; credentials and data readable via the group
src5=$tmp/src5; mkdir -p "$src5"; echo "group readable" > "$src5/data.txt"
chmod 0750 "$src5"; chmod 0640 "$src5/data.txt" "$tmp/creds/RESTIC_PASSWORD"
rm -f -- "$tmp/term/msg"
docker volume create "$repo_nonroot" >/dev/null   # a local repo needs file permissions for 4242 (S3 has none)
docker run --rm --user 0 -v "$repo_nonroot:/r" --entrypoint chown "$IMG" 4242 /r
docker run --rm --read-only --tmpfs /tmp:mode=1777 --tmpfs /output:mode=1777 --user "4242:$(id -g)" --cap-drop ALL \
  --security-opt no-new-privileges --network none \
  -v "$src5:/source:ro" -v "$repo_nonroot:/repo" -v "$tmp/creds:/etc/backup/creds:ro" -v "$tmp/term:/term" \
  -e RESTIC_REPOSITORY=/repo -e TERMINATION_LOG=/term/msg -e BACKUP_SOURCE=nonroot -e BACKUP_HOST=app \
  "$IMG" backup >/dev/null 2>&1 && nonroot_rc=0 || nonroot_rc=$?
chmod 0600 "$tmp/creds/RESTIC_PASSWORD"
[[ $nonroot_rc == 0 ]] || { msg; die "non-root backup failed"; }
ok "non-root worker without capabilities backs up via group permissions"

echo "# $pass passed"
