#!/usr/bin/env bash
set -euo pipefail
# Phase 1: Set kibana_system password and create 5 fixed indices with explicit mappings
# (ais-events, ais-traffic-agg, ais-geo-agg, ais-dq-metrics, ais-quarantine).
# Supports explicit manual --reset.
