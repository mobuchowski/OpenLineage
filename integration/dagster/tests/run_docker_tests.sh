#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Run end-to-end OpenLineage Dagster integration tests in Docker.
#
# Usage (from repo root):
#   bash integration/dagster/tests/run_docker_tests.sh
#
# What it does:
#   1. Builds the Docker image
#   2. Starts the Dagster container
#   3. Waits for the webserver to be ready
#   4. Starts all OpenLineage sensors
#   5. Runs multiple pipeline scenarios (happy path, partial, failure, op-based)
#   6. Waits for sensor ticks and collects the output file
#   7. Validates events with a Python assertion script
#   8. Cleans up

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
OUTPUT_DIR="$REPO_ROOT/integration/dagster/output"
IMAGE_NAME="dagster-ol-test"
CONTAINER_NAME="dagster-ol-test-run"

GRAPHQL="http://localhost:3000/graphql"
REPO_LOCATION="definitions.py"
REPO_NAME="__repository__"

gql() {
  curl -sf -X POST "$GRAPHQL" -H "Content-Type: application/json" -d "$1"
}

echo "=== Building Docker image ==="
docker build -f "$REPO_ROOT/integration/dagster/Dockerfile.test" -t "$IMAGE_NAME" "$REPO_ROOT"

echo "=== Starting container ==="
rm -rf "$OUTPUT_DIR" && mkdir -p "$OUTPUT_DIR"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$CONTAINER_NAME" \
  -v "$OUTPUT_DIR:/output" \
  -p 3000:3000 \
  "$IMAGE_NAME"

echo "=== Waiting for Dagster webserver ==="
for i in $(seq 1 30); do
  if curl -sf http://localhost:3000/server_info > /dev/null 2>&1; then
    echo "  Ready after ${i}s"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Dagster did not start in 30s"
    docker logs "$CONTAINER_NAME" | tail -20
    exit 1
  fi
done

echo "=== Starting OpenLineage sensors ==="
for sensor in openlineage_sensor_start openlineage_sensor_success openlineage_sensor_failure; do
  gql "{\"query\":\"mutation { startSensor(sensorSelector: { repositoryLocationName: \\\"$REPO_LOCATION\\\", repositoryName: \\\"$REPO_NAME\\\", sensorName: \\\"$sensor\\\" }) { ... on Sensor { name } ... on PythonError { message } } }\"}" > /dev/null
  echo "  Started $sensor"
done

launch_run() {
  local job_name="$1"
  gql "{\"query\":\"mutation { launchRun(executionParams: { selector: { repositoryLocationName: \\\"$REPO_LOCATION\\\", repositoryName: \\\"$REPO_NAME\\\", jobName: \\\"$job_name\\\" }, runConfigData: {} }) { ... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['launchRun'].get('run',{}).get('runId','ERROR'))"
}

wait_for_run() {
  local run_id="$1"
  local expected_status="${2:-SUCCESS}"
  for i in $(seq 1 30); do
    status=$(gql "{\"query\":\"{ runOrError(runId: \\\"$run_id\\\") { ... on Run { status } } }\"}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['runOrError']['status'])")
    if [ "$status" = "$expected_status" ] || [ "$status" = "FAILURE" ] || [ "$status" = "CANCELED" ]; then
      echo "  Run $run_id finished: $status"
      echo "$status"
      return
    fi
    sleep 1
  done
  echo "TIMEOUT"
}

echo "=== Scenario 1: Full orders_pipeline (happy path) ==="
RUN1=$(launch_run orders_pipeline)
echo "  Launched run: $RUN1"
wait_for_run "$RUN1" SUCCESS > /dev/null

echo "=== Scenario 2: Partial pipeline (raw_orders + clean_orders only) ==="
RUN2=$(launch_run partial_pipeline)
echo "  Launched run: $RUN2"
wait_for_run "$RUN2" SUCCESS > /dev/null

echo "=== Scenario 3: Failing pipeline (should produce FAIL event) ==="
RUN3=$(launch_run failing_pipeline)
echo "  Launched run: $RUN3"
wait_for_run "$RUN3" FAILURE > /dev/null

echo "=== Scenario 4: Op-based pipeline (no assets) ==="
RUN4=$(launch_run op_pipeline)
echo "  Launched run: $RUN4"
wait_for_run "$RUN4" SUCCESS > /dev/null

echo "=== Waiting for sensor ticks to process all runs (40s) ==="
sleep 40

echo "=== Validating OpenLineage events ==="
python3 "$REPO_ROOT/integration/dagster/tests/validate_docker_events.py" \
  "$OUTPUT_DIR/ol-events.ndjson" \
  "$RUN1" "$RUN2" "$RUN3" "$RUN4"

echo "=== Cleaning up ==="
docker stop "$CONTAINER_NAME" && docker rm "$CONTAINER_NAME"

echo ""
echo "✓ All Docker integration tests passed!"
