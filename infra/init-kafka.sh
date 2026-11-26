#!/usr/bin/env bash
set -euo pipefail
# Phase 1: Create Kafka topic ais.locations.raw (4 partitions, RF=1, retention 24h / 1 GB).
