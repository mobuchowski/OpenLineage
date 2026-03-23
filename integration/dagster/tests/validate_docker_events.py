#!/usr/bin/env python3
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""
Validate OpenLineage events produced by the Docker integration test.

Usage:
    python3 validate_docker_events.py <events.ndjson> <run1_id> <run2_id> <run3_id> <run4_id>

Validates:
  run1 (orders_pipeline)  — full happy path: 3 assets, correct inputs/outputs
  run2 (partial_pipeline) — 2 assets: raw_orders, clean_orders
  run3 (failing_pipeline) — FAIL events for job and for clean_orders_fail step
  run4 (op_pipeline)      — op-based job: extract_op → transform_op → load_op
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict


def load_events(path: str) -> list[dict]:
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def events_for_run(events: list[dict], run_id: str) -> list[dict]:
    """Get all events that belong to a given Dagster run (via DagsterRunFacet or ParentRunFacet)."""
    result = []
    for ev in events:
        facets = ev.get("run", {}).get("facets", {})
        dagster_facet = facets.get("dagster", {})
        parent_facet = facets.get("parent", {})
        # Job-level event: DagsterRunFacet.runId matches
        if dagster_facet.get("runId") == run_id:
            result.append(ev)
        # Step-level event: ParentRunFacet.run.runId matches
        elif parent_facet.get("run", {}).get("runId") == run_id:
            result.append(ev)
    return result


def check(condition: bool, msg: str) -> None:
    if not condition:
        print(f"  FAIL: {msg}", file=sys.stderr)
        sys.exit(1)
    print(f"  OK: {msg}")


def validate_orders_pipeline(events: list[dict], run_id: str) -> None:
    """Full pipeline: raw_orders → clean_orders → order_stats."""
    print(f"\n[Scenario 1] orders_pipeline run={run_id[:8]}...")
    run_evs = events_for_run(events, run_id)
    check(len(run_evs) > 0, f"Found events for run {run_id[:8]}")

    by_job: dict[str, list[dict]] = defaultdict(list)
    for ev in run_evs:
        by_job[ev["job"]["name"]].append(ev)

    # Job-level
    check("orders_pipeline" in by_job, "Job-level events present for orders_pipeline")
    job_types = {e["eventType"] for e in by_job["orders_pipeline"]}
    check("START" in job_types, "orders_pipeline has START event")
    check("COMPLETE" in job_types, "orders_pipeline has COMPLETE event")

    # Step-level: all 3 assets
    for asset_name in ["raw_orders", "clean_orders", "order_stats"]:
        step_key = f"orders_pipeline.{asset_name}"
        check(step_key in by_job, f"Step events present for {asset_name}")
        step_types = {e["eventType"] for e in by_job[step_key]}
        check("COMPLETE" in step_types, f"{asset_name} has COMPLETE event")

    # raw_orders: no inputs, has output
    raw_complete = next(e for e in by_job["orders_pipeline.raw_orders"] if e["eventType"] == "COMPLETE")
    check(len(raw_complete.get("outputs", [])) == 1, "raw_orders COMPLETE has 1 output")
    check(raw_complete["outputs"][0]["name"] == "raw_orders", "raw_orders output name correct")
    check(len(raw_complete.get("inputs", [])) == 0, "raw_orders has no inputs")

    # clean_orders: input=raw_orders, output=clean_orders
    clean_complete = next(e for e in by_job["orders_pipeline.clean_orders"] if e["eventType"] == "COMPLETE")
    check(len(clean_complete.get("inputs", [])) == 1, "clean_orders COMPLETE has 1 input")
    check(clean_complete["inputs"][0]["name"] == "raw_orders", "clean_orders input is raw_orders")
    check(len(clean_complete.get("outputs", [])) == 1, "clean_orders COMPLETE has 1 output")
    check(clean_complete["outputs"][0]["name"] == "clean_orders", "clean_orders output name correct")

    # order_stats: input=clean_orders
    stats_complete = next(e for e in by_job["orders_pipeline.order_stats"] if e["eventType"] == "COMPLETE")
    check(stats_complete["inputs"][0]["name"] == "clean_orders", "order_stats input is clean_orders")

    # ParentRunFacet present on step events
    step_ev = next(e for e in by_job["orders_pipeline.raw_orders"] if e["eventType"] == "COMPLETE")
    parent = step_ev["run"]["facets"].get("parent", {})
    check(parent.get("job", {}).get("name") == "orders_pipeline", "ParentRunFacet.job.name correct")
    check(parent.get("run", {}).get("runId") == run_id, "ParentRunFacet.run.runId matches Dagster run")

    # Schema facet on raw_orders output
    raw_schema = raw_complete["outputs"][0].get("facets", {}).get("schema", {})
    check(len(raw_schema.get("fields", [])) == 5, "raw_orders schema has 5 fields")


def validate_partial_pipeline(events: list[dict], run_id: str) -> None:
    """Partial pipeline: only raw_orders + clean_orders."""
    print(f"\n[Scenario 2] partial_pipeline run={run_id[:8]}...")
    run_evs = events_for_run(events, run_id)
    check(len(run_evs) > 0, f"Found events for run {run_id[:8]}")

    by_job: dict[str, list[dict]] = defaultdict(list)
    for ev in run_evs:
        by_job[ev["job"]["name"]].append(ev)

    check("partial_pipeline" in by_job, "Job-level events for partial_pipeline")
    check("partial_pipeline.raw_orders" in by_job, "raw_orders step events present")
    check("partial_pipeline.clean_orders" in by_job, "clean_orders step events present")
    check("partial_pipeline.order_stats" not in by_job, "order_stats NOT included in partial pipeline")

    # clean_orders still sees raw_orders as input
    clean_complete = next(
        (e for e in by_job.get("partial_pipeline.clean_orders", []) if e["eventType"] == "COMPLETE"), None
    )
    if clean_complete:
        check(
            any(i["name"] == "raw_orders" for i in clean_complete.get("inputs", [])),
            "clean_orders input is raw_orders in partial pipeline",
        )


def validate_failing_pipeline(events: list[dict], run_id: str) -> None:
    """Failing pipeline: raw_orders OK, clean_orders_fail → FAIL."""
    print(f"\n[Scenario 3] failing_pipeline run={run_id[:8]}...")
    run_evs = events_for_run(events, run_id)
    check(len(run_evs) > 0, f"Found events for run {run_id[:8]}")

    by_job: dict[str, list[dict]] = defaultdict(list)
    for ev in run_evs:
        by_job[ev["job"]["name"]].append(ev)

    check("failing_pipeline" in by_job, "Job-level events for failing_pipeline")
    job_types = {e["eventType"] for e in by_job["failing_pipeline"]}
    check("FAIL" in job_types, "failing_pipeline has FAIL event")

    # raw_orders step should complete successfully
    if "failing_pipeline.raw_orders" in by_job:
        raw_types = {e["eventType"] for e in by_job["failing_pipeline.raw_orders"]}
        check("COMPLETE" in raw_types, "raw_orders step completed before failure")

    # clean_orders_fail step should have FAIL
    if "failing_pipeline.clean_orders_fail" in by_job:
        fail_types = {e["eventType"] for e in by_job["failing_pipeline.clean_orders_fail"]}
        check("FAIL" in fail_types, "clean_orders_fail step has FAIL event")


def validate_op_pipeline(events: list[dict], run_id: str) -> None:
    """Op-based pipeline (no assets): extract_op → transform_op → load_op."""
    print(f"\n[Scenario 4] op_pipeline run={run_id[:8]}...")
    run_evs = events_for_run(events, run_id)
    check(len(run_evs) > 0, f"Found events for run {run_id[:8]}")

    by_job: dict[str, list[dict]] = defaultdict(list)
    for ev in run_evs:
        by_job[ev["job"]["name"]].append(ev)

    check("op_pipeline" in by_job, "Job-level events for op_pipeline")
    job_types = {e["eventType"] for e in by_job["op_pipeline"]}
    check("START" in job_types, "op_pipeline has START event")
    check("COMPLETE" in job_types, "op_pipeline has COMPLETE event")

    # Step events should exist for each op
    step_names = [k for k in by_job if k.startswith("op_pipeline.")]
    check(len(step_names) >= 3, f"op_pipeline has step events for all 3 ops (found {len(step_names)})")

    # All step events should have ParentRunFacet pointing to op_pipeline
    for step_key in step_names:
        for ev in by_job[step_key]:
            parent = ev["run"]["facets"].get("parent", {})
            check(
                parent.get("job", {}).get("name") == "op_pipeline",
                f"{step_key} has ParentRunFacet.job.name=op_pipeline",
            )


def main() -> None:
    if len(sys.argv) != 6:
        print("Usage: {} <events.ndjson> <run1> <run2> <run3> <run4>".format(sys.argv[0]))
        sys.exit(1)

    events_path = sys.argv[1]
    run1_id, run2_id, run3_id, run4_id = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]

    print(f"Loading events from {events_path}...")
    events = load_events(events_path)
    print(f"Loaded {len(events)} total events")

    validate_orders_pipeline(events, run1_id)
    validate_partial_pipeline(events, run2_id)
    validate_failing_pipeline(events, run3_id)
    validate_op_pipeline(events, run4_id)

    print("\n✓ All validations passed!")


if __name__ == "__main__":
    main()
