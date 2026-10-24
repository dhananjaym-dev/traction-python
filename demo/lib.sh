#!/usr/bin/env bash
# Shared helpers for the scenario scripts. Source it; do not run it. Adapted from the companion
# Java repo's demo/lib.sh: the control plane here is the unmodified Java demo-app, and the tenant
# under test is this repo's Python app.
#
# Everything the demo touches lives inside docker compose: the broker and the store are reached
# only through `docker compose exec` (rabbitmqctl / valkey-cli inside their containers), the apps
# through their published loopback HTTP ports. Needs bash, curl, jq and docker compose.
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/.." && pwd)"
APP="${APP_URL:-http://localhost:8080}"      # the Java control plane
PY="${PY_URL:-http://localhost:8000}"        # the Python tenant
ACME="${ACME_URL:-http://localhost:9090}"
API="$APP/traction/api"

compose() { docker compose --project-directory "$REPO_ROOT" -f "$REPO_ROOT/docker-compose.yml" "$@"; }

# ---------------------------------------------------------------- output

if [[ -t 1 ]]; then BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'; OFF=$'\033[0m'
else BOLD=''; DIM=''; GREEN=''; RED=''; OFF=''; fi

banner() { printf '\n%s== %s ==%s\n' "$BOLD" "$*" "$OFF"; }
say()    { printf '  %s\n' "$*"; }
runs()   { printf '  %s$ %s%s\n' "$DIM" "$1" "$OFF"; OUT=$(eval "$1"); printf '%s' "$OUT" | indent; }
indent() { sed 's/^/  /'; printf '\n'; }

fail() { printf '%sFAIL%s %s\n' "$RED" "$OFF" "$*" >&2; exit 1; }
ok()   { printf '%sOK%s %s\n' "$GREEN" "$OFF" "$*"; }

assert_eq() { # <actual> <expected> <what>
  [[ "$1" == "$2" ]] || fail "$3: expected $2, got $1"
  say "check: $3 = $2"
}

# Polls <cmd...> once a second until it succeeds; fails after <seconds>.
wait_until() { # <seconds> <what> <cmd...>
  local limit=$1 what=$2; shift 2
  local start=$SECONDS
  until "$@"; do
    (( SECONDS - start < limit )) || fail "timed out after ${limit}s waiting for: $what"
    sleep 1
  done
}

# ---------------------------------------------------------------- time and logs

# UTC now with milliseconds (RFC 3339), for `docker compose logs --since`.
now_ts() {
  compose exec -T redis valkey-cli TIME | tr -d '\r' | paste -s -d ' ' - \
    | jq -Rr 'split(" ") | (.[0] | tonumber | strftime("%Y-%m-%dT%H:%M:%S")) + "." + (.[1] | ("00000" + .)[-6:][0:3]) + "Z"'
}

# Count of control-plane log lines containing <text> since <timestamp>.
log_count() { # <text> <since>
  compose logs --no-log-prefix --since "$2" control-plane 2>/dev/null | grep -cF -- "$1" || true
}
# Count of `traction.notify type=<TYPE>` lines (the control plane's LogNotifyHook) since <timestamp>.
notify_count() { log_count "traction.notify type=$1 " "$2"; }
notify_lines() { # <type-prefix> <since>
  compose logs --no-log-prefix --since "$2" control-plane 2>/dev/null \
    | grep -F -- "traction.notify type=$1" | sed 's/^.*traction\.notify/  traction.notify/' || true
}

# ---------------------------------------------------------------- broker (inside the rabbitmq container)

queues() { compose exec -T rabbitmq rabbitmqctl list_queues -q --no-table-headers name messages; }

depth() { # <queue>
  local n
  n=$(queues | awk -v q="$1" '$1 == q { print $2 }')
  [[ -n "$n" ]] || fail "no such queue: $1"
  echo "$n"
}

# runway + holding + parked for one airport, from one broker read.
backlog() { # <airportId>
  queues | awk -v a="tr.$1." 'index($1, a) == 1 && $1 ~ /\.(runway|holding|parked)$/ { s += $2 } END { print s + 0 }'
}

demo_backlog() { queues | awk '$1 ~ /^tr\./ && $1 !~ /^tr\.tower\./ && $1 != "tr.dlq" { s += $2 } END { print s + 0 }'; }

purge_demo_queues() {
  compose exec -T rabbitmq sh -c '
    rabbitmqctl list_queues -q --no-table-headers name </dev/null | while read -r q; do
      case "$q" in tr.tower.queue|tr.dlq) ;; tr.*) rabbitmqadmin -q purge queue name="$q" </dev/null ;; esac
    done; true'
}

# Purges until every airport queue reads 0 twice in a row (a running consumer may still settle a
# prefetched delivery back onto a queue right after a purge).
drain_demo_queues() {
  local quiet=0 start=$SECONDS
  while (( quiet < 2 )); do
    (( SECONDS - start < 120 )) || fail "queues did not go quiet"
    purge_demo_queues
    sleep 1
    if [[ "$(demo_backlog)" == 0 ]]; then quiet=$((quiet + 1)); else quiet=0; fi
  done
}

runway_consumers() { # <airportId>
  compose exec -T rabbitmq rabbitmqctl list_queues -q --no-table-headers name consumers \
    | awk -v q="tr.$1.runway" '$1 == q { print $2 }'
}

# ---------------------------------------------------------------- store (inside the redis container)

redis_cli() { compose exec -T redis valkey-cli "$@"; }

# Deletes every tr:* key (SCAN, not KEYS) except the announcement projection. Slot buckets keep
# their config and are refilled (token fields dropped) rather than deleted.
clear_store() {
  compose exec -T redis sh -c '
    valkey-cli --scan --pattern "tr:*" | while read -r k; do
      case "$k" in
        tr:airports|tr:airport:*) ;;
        tr:slots:*) valkey-cli HDEL "$k" tokens_milli last_refill_ms </dev/null >/dev/null ;;
        *) valkey-cli DEL "$k" </dev/null >/dev/null ;;
      esac
    done'
}

# ---------------------------------------------------------------- vendor and apps

vendor_mode() { # <endpoint|all> <mode>
  curl -fsS -X POST "$ACME/mode" -H 'Content-Type: application/json' \
    -d "{\"endpoint\":\"$1\",\"mode\":\"$2\"}"
}
vendor_stat() { curl -fsS "$ACME/stats" | jq -r ".$1.$2"; } # <endpoint> <ok|failed>

airport_ids()    { curl -fsS "$API/airports" | jq -r '.[].id'; }
airport_status() { curl -fsS "$API/airports/$1" | jq -r '.status'; }
gate_status()    { curl -fsS "$API/airports/$1" | jq -r --arg g "$2" '.gates[] | select(.id == $g) | .status'; }

is_up() {
  curl -fsS -o /dev/null "$ACME/mode" 2>/dev/null \
    && curl -fsS -o /dev/null "$API/airports" 2>/dev/null \
    && curl -fsS -o /dev/null "$PY/healthz" 2>/dev/null
}

wait_healthy() { wait_until 120 "acme-vendor, control-plane and py-tenant to answer" is_up; }

consumers_back() {
  local id
  for id in $(airport_ids); do
    [[ "$(airport_status "$id")" == OPEN ]] || return 1
    [[ "$(runway_consumers "$id")" -ge 1 ]] || return 1
  done
}

# Brings a running compose back to a clean slate from any state: vendor healthy, every airport
# queue empty, Traction's runtime state gone, every airport consuming, vendor counters at zero.
reset() {
  banner "reset"
  wait_healthy
  vendor_mode all healthy >/dev/null
  drain_demo_queues
  clear_store
  # Reopen re-broadcasts OPEN even on an OPEN airport, so a consumer that paused itself resumes.
  local id
  for id in $(airport_ids); do curl -fsS -X POST "$API/airports/$id/reopen" >/dev/null; done
  wait_until 30 "every airport consuming" consumers_back
  drain_demo_queues
  curl -fsS -X POST "$ACME/stats/reset" >/dev/null
  say "vendor healthy, queues empty, traction state cleared, airports consuming"
}

# Flips the vendor's mode for one endpoint (or all), printing the exact curl that runs.
flip() { # <endpoint|all> <mode>
  runs "curl -fsS -X POST '$ACME/mode' -H 'Content-Type: application/json' -d '{\"endpoint\":\"$1\",\"mode\":\"$2\"}'"
}
