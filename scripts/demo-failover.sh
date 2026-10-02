#!/usr/bin/env bash
set -euo pipefail

# Deterministic three-node leader-failover demonstration.
#
# The scenario itself lives in the integration test TestThreeNodeFailoverDemo;
# this script only runs that test. The demo watches an initial leader accept a
# write, a replacement leader take over after the first leader stops, the
# stopped node restart against its retained durable state, and the whole cluster
# converge on both writes. Every step is real Raft code with no sleeps.
go test -v -count=1 ./integration -run '^TestThreeNodeFailoverDemo$'