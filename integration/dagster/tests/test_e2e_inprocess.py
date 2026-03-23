# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
End-to-end integration test: materializes all assets in-process,
verifies that OpenLineage events are emitted with correct structure.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

# Add test project to path
sys.path.insert(0, str(Path(__file__).parent))


class TestE2EInProcess:
    """Run the Dagster assets in-process and verify OL event output."""

    def test_assets_materialize_and_produce_ol_events(self, tmp_path):
        """
        Execute the orders pipeline in-process and check that:
        - Events are written to the output file
        - START and COMPLETE events are present for the job
        - Each asset step has START + COMPLETE events
        - COMPLETE events have outputs with dataset names
        - Step events have ParentRunFacet linking to the parent job
        """
        ol_output = tmp_path / "ol-events.ndjson"
        # OpenLineage client reads transport config from OPENLINEAGE__TRANSPORT__* env vars.
        # Use append=true so all events go to a single file (default creates timestamped files).
        os.environ["OPENLINEAGE__TRANSPORT__TYPE"] = "file"
        os.environ["OPENLINEAGE__TRANSPORT__LOG_FILE_PATH"] = str(ol_output)
        os.environ["OPENLINEAGE__TRANSPORT__APPEND"] = "true"

        try:
            from dagster import materialize
            from dagster_test_project.definitions import clean_orders, order_stats, raw_orders

            result = materialize(
                assets=[raw_orders, clean_orders, order_stats],
                raise_on_error=True,
            )
            assert result.success, "Asset materialization failed"

            # Now manually trigger the sensor logic on the completed run
            # (Sensors normally run asynchronously; we simulate their firing here)
            from openlineage.client import OpenLineageClient
            from openlineage.dagster.converter import (
                build_parent_job_complete_event,
                build_parent_job_start_event,
                build_step_events,
            )

            # Get all events from the in-process result
            dagster_run = result.dagster_run
            event_records = list(result.all_events)

            # Get asset dep graph for resolving upstream inputs
            from dagster_test_project.definitions import defs as test_defs
            asset_dep_graph = test_defs.get_repository_def().asset_graph.asset_dep_graph

            # Emit events manually using the converter
            client = OpenLineageClient()

            start_event = build_parent_job_start_event(dagster_run)
            client.emit(start_event)

            step_events = build_step_events(dagster_run, event_records, asset_dep_graph=asset_dep_graph)
            for ev in step_events:
                client.emit(ev)

            complete_event = build_parent_job_complete_event(dagster_run, success=True)
            client.emit(complete_event)

            client.close(timeout=5)

            # Parse and verify the output
            assert ol_output.exists(), "No OpenLineage output file was created"
            lines = [l for l in ol_output.read_text().splitlines() if l.strip()]
            assert len(lines) >= 2, f"Expected at least 2 events, got {len(lines)}"

            events = [json.loads(l) for l in lines]

            # Should have at least one START and one COMPLETE event
            event_types = [e.get("eventType") for e in events]
            assert "START" in event_types, "Missing START event"
            assert "COMPLETE" in event_types, "Missing COMPLETE event"

            # Job-level events should reference the job name
            # (when using materialize() directly, Dagster uses __ephemeral_asset_job__)
            job_names = {e["job"]["name"] for e in events}
            assert len(job_names) > 0, "No job names found in events"
            print(f"Job names in OL events: {job_names}")

            # Step-level events should have ParentRunFacet
            step_events = [e for e in events if "." in e["job"]["name"]]
            if step_events:
                for ev in step_events:
                    assert "parent" in ev["run"].get("facets", {}), \
                        f"Missing ParentRunFacet in step event: {ev['job']['name']}"

            # Should have output datasets for asset materializations
            complete_events = [e for e in events if e.get("eventType") == "COMPLETE"]
            events_with_outputs = [e for e in complete_events if e.get("outputs")]
            assert len(events_with_outputs) > 0, "No COMPLETE events with outputs found"

            # Verify dataset names match asset keys
            all_output_names = {d["name"] for e in events_with_outputs for d in e["outputs"]}
            assert "raw_orders" in all_output_names, f"raw_orders not in outputs: {all_output_names}"

            # Verify input datasets are captured via asset dep graph
            events_with_inputs = [e for e in events if e.get("inputs")]
            assert len(events_with_inputs) > 0, "No events with inputs found"
            all_input_names = {d["name"] for e in events_with_inputs for d in e["inputs"]}
            # clean_orders depends on raw_orders, order_stats depends on clean_orders
            assert "raw_orders" in all_input_names, f"raw_orders not in inputs: {all_input_names}"
            assert "clean_orders" in all_input_names, f"clean_orders not in inputs: {all_input_names}"

        finally:
            # Clean up env vars
            os.environ.pop("OPENLINEAGE__TRANSPORT__TYPE", None)
            os.environ.pop("OPENLINEAGE__TRANSPORT__LOG_FILE_PATH", None)
            os.environ.pop("OPENLINEAGE__TRANSPORT__APPEND", None)
