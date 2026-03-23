# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Dagster → OpenLineage converter."""

from __future__ import annotations

from unittest.mock import MagicMock

from openlineage.client.event_v2 import RunState

from openlineage.dagster.converter import (
    DAGSTER_NAMESPACE,
    _asset_key_to_dataset_name,
    _materialization_to_output_dataset,
    build_parent_job_complete_event,
    build_parent_job_start_event,
    build_step_events,
    make_parent_run_facet,
)


def _make_dagster_run(run_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", job_name="test_job", tags=None):
    """Create a minimal mock of a DagsterRun."""
    run = MagicMock()
    run.run_id = run_id
    run.job_name = job_name
    run.tags = tags or {}
    return run


class TestAssetKeyConversions:
    def test_simple_key(self):
        assert _asset_key_to_dataset_name(["my_table"]) == "my_table"

    def test_nested_key(self):
        assert _asset_key_to_dataset_name(["my_schema", "my_table"]) == "my_schema/my_table"

    def test_three_parts(self):
        assert _asset_key_to_dataset_name(["a", "b", "c"]) == "a/b/c"


class TestMaterializationToOutputDataset:
    def test_basic_materialization(self):
        mat = MagicMock()
        mat.asset_key.path = ["raw_orders"]
        mat.metadata = {}

        ds = _materialization_to_output_dataset(mat)
        assert ds.name == "raw_orders"
        assert ds.namespace == DAGSTER_NAMESPACE

    def test_uri_metadata_sets_namespace(self):
        mat = MagicMock()
        mat.asset_key.path = ["s3", "my_table"]
        uri_val = MagicMock()
        uri_val.value = "s3://my-bucket/path"
        mat.metadata = {"dagster/uri": uri_val}

        ds = _materialization_to_output_dataset(mat)
        assert ds.namespace == "s3://my-bucket"
        assert ds.name == "s3/my_table"


class TestBuildParentJobStartEvent:
    def test_basic_start_event(self):
        run = _make_dagster_run()
        event = build_parent_job_start_event(run)

        assert event.eventType == RunState.START
        assert event.job.name == "test_job"
        assert event.job.namespace == DAGSTER_NAMESPACE
        assert event.run.runId == run.run_id

    def test_start_event_has_dagster_facets(self):
        run = _make_dagster_run()
        event = build_parent_job_start_event(run)

        assert "dagster" in event.run.facets
        assert "jobType" in event.job.facets
        assert "dagster" in event.job.facets


class TestBuildParentJobCompleteEvent:
    def test_success_event(self):
        run = _make_dagster_run()
        event = build_parent_job_complete_event(run, success=True)
        assert event.eventType == RunState.COMPLETE

    def test_failure_event(self):
        run = _make_dagster_run()
        event = build_parent_job_complete_event(run, success=False)
        assert event.eventType == RunState.FAIL


class TestMakeParentRunFacet:
    def test_parent_facet(self):
        facet = make_parent_run_facet("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "my_job")
        assert facet.run.runId == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert facet.job.name == "my_job"
        assert facet.job.namespace == DAGSTER_NAMESPACE


class TestBuildStepEvents:
    def _make_event_record(self, event_type: str, step_key: str, timestamp: float = 1700000000.0):
        """Build a mock event record resembling what Dagster returns."""
        dagster_event = MagicMock()
        dagster_event.event_type_value = event_type
        dagster_event.step_key = step_key

        record = MagicMock()
        record.timestamp = timestamp
        record.dagster_event = dagster_event
        # Simulate event_log_entry being the record itself
        del record.event_log_entry  # remove attribute so hasattr returns False
        return record

    def _make_asset_mat_record(self, step_key: str, asset_key_path: list[str]):
        """Build a mock ASSET_MATERIALIZATION event record."""
        asset_key = MagicMock()
        asset_key.path = asset_key_path

        materialization = MagicMock()
        materialization.asset_key = asset_key
        materialization.metadata = {}

        mat_data = MagicMock()
        mat_data.materialization = materialization

        dagster_event = MagicMock()
        dagster_event.event_type_value = "ASSET_MATERIALIZATION"
        dagster_event.step_key = step_key
        dagster_event.event_specific_data = mat_data

        record = MagicMock()
        record.timestamp = 1700000001.0
        record.dagster_event = dagster_event
        del record.event_log_entry
        return record

    def test_empty_records(self):
        run = _make_dagster_run()
        events = build_step_events(run, [])
        assert events == []

    def test_single_step_success_emits_start_and_complete(self):
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "raw_orders", timestamp=1700000000.0),
            self._make_event_record("STEP_SUCCESS", "raw_orders", timestamp=1700000010.0),
        ]
        events = build_step_events(run, records)

        # Should get START + COMPLETE for each step
        assert len(events) == 2
        start_ev, complete_ev = events
        assert start_ev.eventType == RunState.START
        assert complete_ev.eventType == RunState.COMPLETE
        assert "raw_orders" in start_ev.job.name

    def test_step_with_asset_materialization_has_output(self):
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "raw_orders", timestamp=1700000000.0),
            self._make_asset_mat_record("raw_orders", ["raw_orders"]),
            self._make_event_record("STEP_SUCCESS", "raw_orders", timestamp=1700000010.0),
        ]
        events = build_step_events(run, records)
        complete_ev = events[1]
        assert complete_ev.eventType == RunState.COMPLETE
        assert len(complete_ev.outputs) == 1
        assert complete_ev.outputs[0].name == "raw_orders"

    def test_step_events_have_parent_run_facet(self):
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "my_op"),
            self._make_event_record("STEP_SUCCESS", "my_op"),
        ]
        events = build_step_events(run, records)
        for ev in events:
            assert "parent" in ev.run.facets
            assert ev.run.facets["parent"].job.name == "test_job"

    def test_multiple_steps(self):
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "step_a"),
            self._make_event_record("STEP_SUCCESS", "step_a"),
            self._make_event_record("STEP_START", "step_b"),
            self._make_event_record("STEP_FAILURE", "step_b"),
        ]
        events = build_step_events(run, records)
        # 2 events per step × 2 steps = 4
        assert len(events) == 4
        step_jobs = {ev.job.name for ev in events}
        assert any("step_a" in n for n in step_jobs)
        assert any("step_b" in n for n in step_jobs)
        # step_b should have a FAIL event
        fail_events = [e for e in events if e.eventType == RunState.FAIL]
        assert len(fail_events) == 1

    def test_step_run_ids_are_uuid7_and_deterministic(self):
        """Step run IDs must be UUID7 (version 7) and stable across repeated calls."""
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "raw_orders", timestamp=1700000000.0),
            self._make_event_record("STEP_SUCCESS", "raw_orders", timestamp=1700000010.0),
        ]

        events1 = build_step_events(run, records)
        events2 = build_step_events(run, records)

        id1 = events1[0].run.runId
        id2 = events2[0].run.runId

        # Deterministic: same inputs → same ID
        assert id1 == id2, "Step run ID must be deterministic"

        # UUID7: version nibble (14th hex char, index 14) must be '7'
        assert id1[14] == "7", f"Expected UUID version 7, got version at pos 14: {id1[14]} (full: {id1})"

        # START and COMPLETE share the same run ID within a single call
        start_id = events1[0].run.runId
        complete_id = events1[1].run.runId
        assert start_id == complete_id, "START and COMPLETE for same step must share run ID"

    def test_step_run_ids_are_time_ordered(self):
        """Steps that start later should have greater UUID7 values (time-ordered)."""
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "step_a", timestamp=1700000000.0),
            self._make_event_record("STEP_SUCCESS", "step_a", timestamp=1700000010.0),
            self._make_event_record("STEP_START", "step_b", timestamp=1700000020.0),
            self._make_event_record("STEP_SUCCESS", "step_b", timestamp=1700000030.0),
        ]
        events = build_step_events(run, records)

        id_a = next(e.run.runId for e in events if "step_a" in e.job.name and e.eventType == RunState.START)
        id_b = next(e.run.runId for e in events if "step_b" in e.job.name and e.eventType == RunState.START)

        # UUID7 string comparison is time-ordered for events in the same ms range
        assert id_a < id_b, f"step_a UUID7 {id_a} should be < step_b UUID7 {id_b}"

    def test_step_run_ids_are_unique_across_steps(self):
        """Different steps within the same run must get different run IDs."""
        run = _make_dagster_run()
        records = [
            self._make_event_record("STEP_START", "step_a", timestamp=1700000000.0),
            self._make_event_record("STEP_SUCCESS", "step_a"),
            self._make_event_record("STEP_START", "step_b", timestamp=1700000020.0),
            self._make_event_record("STEP_SUCCESS", "step_b"),
        ]
        events = build_step_events(run, records)
        run_ids = {e.run.runId for e in events}
        # 2 steps × 2 events, but each step shares 1 ID → should be exactly 2 unique IDs
        assert len(run_ids) == 2, f"Expected 2 unique step run IDs, got {len(run_ids)}: {run_ids}"
