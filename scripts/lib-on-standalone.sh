#!/bin/sh
# Runs the library tests (cargo test --lib, as CI's test job runs them on its
# Pulsar versions) against one standalone Pulsar 5.0.1 broker on 6650 and
# 8080, while the integration suite's two-broker cluster
# (tests/integration/cluster.rs) is up: its brokers hold those ports, so the
# ones running are stopped for the run and started again afterwards, also on
# failure or Ctrl-C.
#
# usage: scripts/lib-on-standalone.sh LOGFILE
set -u
cd "$(dirname "$0")/.."
log=${1:?log file}

# the holder owns the brokers' network namespace, so it goes up first
holder=$(docker ps -q --filter name=rprs-brokers-net)
brokers=$(docker ps -q --filter name=rprs-broker-)

restore() {
  docker rm -f rprs-standalone >/dev/null 2>&1
  [ -n "$holder" ] && docker start $holder >/dev/null
  [ -n "$brokers" ] && docker start $brokers >/dev/null
}
trap restore EXIT
trap 'exit 130' INT TERM

[ -n "$brokers$holder" ] && docker stop $brokers $holder >/dev/null
docker rm -f rprs-standalone >/dev/null 2>&1
docker run -d --name rprs-standalone --label rprs.cluster=standalone \
  -p 6650:6650 -p 8080:8080 \
  -e PULSAR_PREFIX_advertisedAddress=127.0.0.1 \
  -e PULSAR_PREFIX_topicLevelPoliciesEnabled=true \
  apachepulsar/pulsar:5.0.1 bin/pulsar standalone >/dev/null || exit 1
waited=0
until curl -sf http://127.0.0.1:8080/admin/v2/brokers/health >/dev/null; do
  waited=$((waited + 1))
  if [ "$waited" -ge 180 ]; then
    echo "the standalone broker was not healthy within 180 s" >&2
    exit 1
  fi
  sleep 1
done
cargo test --lib --features admin-api -- --test-threads=1 >"$log" 2>&1
status=$?
echo "exit=$status" >>"$log"
grep -E 'test result' "$log"
exit $status
