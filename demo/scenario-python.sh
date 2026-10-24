#!/usr/bin/env bash
# The cross-language acceptance scenario: the unmodified JAVA control plane governs this repo's
# PYTHON tenant end to end — dispatch, paced landing, AIRPORT_FAULT ground stop, one-curl reopen,
# GATE_FAULT parks, operator replay. Every decision below is made by the Java control plane from
# the Python tenant's tower reports; the two processes share nothing but the broker and the store.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
AIRPORT=crm:py
HEALTHY="${HEALTHY:-40}"
COUNT="${COUNT:-200}"
COMPANIES="${COMPANIES:-30}"

reset
since=$(now_ts)

banner "1. a healthy batch lands, paced by the shared slot bucket"
runs "curl -fsS -X POST '$PY/seed/contacts?count=$HEALTHY'"
landed_healthy() {
  local ok b; ok=$(vendor_stat contacts ok); b=$(backlog "$AIRPORT")
  say "landed $ok/$HEALTHY · runway+holding+parked $b"
  [[ "$b" == 0 && "$ok" -ge "$HEALTHY" ]]
}
wait_until 60 "all $HEALTHY contacts to land" landed_healthy
assert_eq "$(vendor_stat contacts ok)" "$HEALTHY" "contacts landed"

banner "2. the token expires; $COUNT more syncs arrive"
flip contacts token_expired
runs "curl -fsS -X POST '$PY/seed/contacts?count=$COUNT'"

banner "the first AIRPORT_FAULT report ground-stops the airport — decided in the JAVA control plane"
grounded() { [[ "$(airport_status "$AIRPORT")" == GROUND_STOP ]]; }
wait_until 30 "$AIRPORT to ground-stop" grounded
runs "curl -fsS '$API/airports/$AIRPORT' | jq -c '{id, status, reason, detail}'"

waiting() {
  local b; b=$(backlog "$AIRPORT")
  say "waiting on the runway: $b"
  [[ "$b" == "$COUNT" ]]
}
wait_until 30 "all $COUNT flights to wait on the runway" waiting
sleep 3
waiting || fail "the runway did not hold still"
say "vendor calls burned against the dead token: $(vendor_stat contacts failed)"
assert_eq "$(notify_count GROUND_STOP "$since")" 1 "GROUND_STOP notifications"

banner "3. fix the token, then one curl to the Java admin API"
flip contacts healthy
runs "curl -fsS -X POST '$API/airports/$AIRPORT/reopen'"

banner "everything lands (paced at the airport's 20/s slot limit)"
TOTAL=$((HEALTHY + COUNT))
landed_all() {
  local ok b; ok=$(vendor_stat contacts ok); b=$(backlog "$AIRPORT")
  say "landed $ok/$TOTAL · runway+holding+parked $b"
  [[ "$b" == 0 && "$ok" -ge "$TOTAL" ]]
}
wait_until 300 "all $TOTAL contacts to land" landed_all
assert_eq "$(vendor_stat contacts ok)" "$TOTAL" "contacts landed"
assert_eq "$(depth "tr.$AIRPORT.parked")" 0 "parked"
assert_eq "$(depth tr.dlq)" 0 "dead-lettered"

banner "4. one gate breaks: companies are rejected (422), contacts keep flowing"
flip companies contract_break
runs "curl -fsS -X POST '$PY/seed/companies?count=$COMPANIES'"
parked_all() {
  local p; p=$(depth "tr.$AIRPORT.parked")
  say "parked: $p"
  [[ "$p" == "$COMPANIES" ]]
}
wait_until 60 "all $COMPANIES companies to park" parked_all
gate_stopped() { [[ "$(gate_status "$AIRPORT" company-sync)" == GROUND_STOP ]]; }
wait_until 30 "gate company-sync to ground-stop (3 distinct GATE_FAULTs)" gate_stopped
runs "curl -fsS '$API/airports/$AIRPORT' | jq -c '.gates'"
# The airport itself still flies: a quick healthy contact batch lands beside the stopped gate.
runs "curl -fsS -X POST '$PY/seed/contacts?count=5'"
side_landed() { [[ "$(vendor_stat contacts ok)" -ge $((TOTAL + 5)) ]]; }
wait_until 60 "contacts to keep landing beside the stopped gate" side_landed

banner "5. fix the contract, reopen the gate, replay the parked flights"
flip companies healthy
runs "curl -fsS -X POST '$API/airports/$AIRPORT/gates/company-sync/reopen'"
runs "curl -fsS -X POST '$API/airports/$AIRPORT/replayParked'"
replayed() {
  local ok p; ok=$(vendor_stat companies ok); p=$(depth "tr.$AIRPORT.parked")
  say "companies landed $ok/$COMPANIES · parked $p"
  [[ "$p" == 0 && "$ok" -ge "$COMPANIES" ]]
}
wait_until 120 "all $COMPANIES companies to land after replay" replayed
assert_eq "$(vendor_stat companies ok)" "$COMPANIES" "companies landed"
assert_eq "$(backlog "$AIRPORT")" 0 "backlog"
assert_eq "$(depth tr.dlq)" 0 "dead-lettered"

banner "the picture"
printf '  %sone Java control plane, one Python tenant, nothing shared but the broker and the store%s\n\n' "$BOLD" "$OFF"
ok "scenario-python"
