# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Convert Dagster run/event data into OpenLineage RunEvents."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import job_type_job, parent_run, schema_dataset
from openlineage.client.uuid import generate_static_uuid

from openlineage.dagster.facets import PRODUCER, DagsterJobFacet, DagsterRunFacet

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

DAGSTER_NAMESPACE = "dagster"

JOB_TYPE_FACET = job_type_job.JobTypeJobFacet(
    jobType="JOB",
    integration="DAGSTER",
    processingType="BATCH",
    producer=PRODUCER,
)

STEP_JOB_TYPE_FACET = job_type_job.JobTypeJobFacet(
    jobType="MODEL",
    integration="DAGSTER",
    processingType="BATCH",
    producer=PRODUCER,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _asset_key_to_dataset_name(asset_key_path: list[str]) -> str:
    """Convert Dagster AssetKey path list to an OL dataset name."""
    return "/".join(asset_key_path)


def _asset_key_to_namespace(metadata: dict) -> str:
    """Extract namespace from asset metadata URI if present, else use default."""
    uri = metadata.get("dagster/uri")
    if uri and hasattr(uri, "value"):
        uri = uri.value
    if isinstance(uri, str) and "://" in uri:
        # Use scheme + authority as namespace
        from urllib.parse import urlparse

        parsed = urlparse(uri)
        return f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else DAGSTER_NAMESPACE
    return DAGSTER_NAMESPACE


def _extract_schema_facet(metadata: dict) -> schema_dataset.SchemaDatasetFacet | None:
    """Extract OpenLineage schema facet from Dagster column schema metadata."""
    col_schema = metadata.get("dagster/column_schema")
    if col_schema is None:
        return None
    # col_schema is a TableSchema MetadataValue; access via .value
    table_schema = getattr(col_schema, "value", col_schema)
    columns = getattr(table_schema, "columns", None)
    if not columns:
        return None
    fields = [
        schema_dataset.SchemaDatasetFacetFields(
            name=col.name,
            type=getattr(col, "type", None),
            description=getattr(col, "description", None),
        )
        for col in columns
    ]
    return schema_dataset.SchemaDatasetFacet(fields=fields, producer=PRODUCER)


def _materialization_to_output_dataset(materialization) -> OutputDataset:
    """Convert a Dagster AssetMaterialization to an OpenLineage OutputDataset."""
    asset_key = materialization.asset_key
    metadata = dict(materialization.metadata or {})

    namespace = _asset_key_to_namespace(metadata)
    name = _asset_key_to_dataset_name(list(asset_key.path))

    facets: dict = {}
    schema_facet = _extract_schema_facet(metadata)
    if schema_facet:
        facets["schema"] = schema_facet

    return OutputDataset(
        namespace=namespace,
        name=name,
        facets=facets,
    )


def make_parent_run_facet(dagster_run_id: str, job_name: str) -> parent_run.ParentRunFacet:
    """Build a ParentRunFacet linking a step event back to the parent Dagster run."""
    return parent_run.ParentRunFacet(
        run=parent_run.Run(runId=dagster_run_id),
        job=parent_run.Job(namespace=DAGSTER_NAMESPACE, name=job_name),
    )


def dagster_run_to_ol_run_id(dagster_run_id: str) -> str:
    """
    Dagster run IDs are UUID4 — reuse them as OpenLineage run IDs.
    This keeps START and COMPLETE events correlated across separate sensor firings.
    """
    return dagster_run_id


def build_parent_job_start_event(dagster_run) -> RunEvent:
    """
    Emit a START RunEvent for the top-level Dagster job run.
    Called when the sensor sees DagsterRunStatus.STARTED.
    """
    run_id = dagster_run_to_ol_run_id(dagster_run.run_id)
    return RunEvent(
        eventType=RunState.START,
        eventTime=_now(),
        run=Run(
            runId=run_id,
            facets={
                "dagster": DagsterRunFacet(
                    jobName=dagster_run.job_name,
                    runId=dagster_run.run_id,
                    runTags=dict(dagster_run.tags or {}),
                )
            },
        ),
        job=Job(
            namespace=DAGSTER_NAMESPACE,
            name=dagster_run.job_name,
            facets={
                "jobType": JOB_TYPE_FACET,
                "dagster": DagsterJobFacet(),
            },
        ),
        producer=PRODUCER,
    )


def build_parent_job_complete_event(dagster_run, success: bool) -> RunEvent:
    """
    Emit COMPLETE or FAIL for the top-level Dagster job run.
    Called after processing all step events.
    """
    run_id = dagster_run_to_ol_run_id(dagster_run.run_id)
    state = RunState.COMPLETE if success else RunState.FAIL
    return RunEvent(
        eventType=state,
        eventTime=_now(),
        run=Run(
            runId=run_id,
            facets={
                "dagster": DagsterRunFacet(
                    jobName=dagster_run.job_name,
                    runId=dagster_run.run_id,
                    runTags=dict(dagster_run.tags or {}),
                )
            },
        ),
        job=Job(
            namespace=DAGSTER_NAMESPACE,
            name=dagster_run.job_name,
            facets={
                "jobType": JOB_TYPE_FACET,
                "dagster": DagsterJobFacet(),
            },
        ),
        producer=PRODUCER,
    )


def _asset_key_to_input_dataset(asset_key) -> InputDataset:
    """Convert an upstream AssetKey to an OpenLineage InputDataset."""
    return InputDataset(
        namespace=DAGSTER_NAMESPACE,
        name=_asset_key_to_dataset_name(list(asset_key.path)),
    )


def _build_upstream_inputs(step_key: str, asset_dep_graph: dict | None) -> list[InputDataset]:
    """
    Resolve upstream asset dependencies for a step from the asset dep graph.

    asset_dep_graph is the dict returned by AssetGraph.asset_dep_graph, which has
    the form: {"upstream": {AssetKey: {AssetKey, ...}, ...}, "downstream": {...}}
    """
    if not asset_dep_graph:
        return []
    upstream = asset_dep_graph.get("upstream", {})
    for asset_key, parent_keys in upstream.items():
        if asset_key.to_user_string() == step_key or "/".join(asset_key.path) == step_key:
            return [_asset_key_to_input_dataset(pk) for pk in parent_keys]
    return []


def build_step_events(dagster_run, event_records, asset_dep_graph: dict | None = None) -> list[RunEvent]:
    """
    For each step (op/asset) in the run, emit START + COMPLETE/FAIL RunEvents
    with inputs/outputs extracted from ASSET_MATERIALIZATION and STEP_INPUT events.

    Each step event carries a ParentRunFacet pointing to the parent Dagster job run.

    Args:
        dagster_run: The DagsterRun object.
        event_records: Iterable of EventLogRecord or DagsterEvent objects.
        asset_dep_graph: Optional dict from AssetGraph.asset_dep_graph used to resolve
            upstream asset dependencies as InputDatasets. Pass
            ``context.repository_def.asset_graph.asset_dep_graph`` from the sensor.
    """
    # Group events by step key
    steps: dict[str, dict] = {}  # step_key → {start_time, end_time, success, outputs, inputs}

    for record in event_records:
        # Support both EventLogRecord wrappers (from instance.get_records_for_run)
        # and DagsterEvent objects directly (from ExecuteInProcessResult.events_for_node).
        if hasattr(record, "event_log_entry"):
            # EventLogRecord path: the actual DagsterEvent is under .event_log_entry.dagster_event
            log_entry = record.event_log_entry
            dagster_event = getattr(log_entry, "dagster_event", None)
            timestamp = getattr(log_entry, "timestamp", None)
        elif hasattr(record, "dagster_event"):
            # Record has a dagster_event attribute (e.g. EventLogEntry)
            dagster_event = record.dagster_event
            timestamp = getattr(record, "timestamp", None)
        elif hasattr(record, "event_type_value"):
            # DagsterEvent itself (from events_for_node or similar APIs)
            dagster_event = record
            timestamp = None
        else:
            continue

        if dagster_event is None:
            continue

        step_key = getattr(dagster_event, "step_key", None)
        if not step_key:
            continue

        if step_key not in steps:
            steps[step_key] = {
                "start_time": None,
                "end_time": None,
                "success": True,
                "outputs": [],
                "inputs": [],
            }
        step = steps[step_key]
        event_type = dagster_event.event_type_value

        if event_type == "STEP_START":
            step["start_time"] = timestamp
        elif event_type == "STEP_SUCCESS":
            step["end_time"] = timestamp
            step["success"] = True
        elif event_type == "STEP_FAILURE":
            step["end_time"] = timestamp
            step["success"] = False
        elif event_type == "ASSET_MATERIALIZATION":
            mat = dagster_event.event_specific_data
            # event_specific_data for ASSET_MATERIALIZATION is StepMaterializationData
            materialization = getattr(mat, "materialization", mat)
            if hasattr(materialization, "asset_key"):
                step["outputs"].append(_materialization_to_output_dataset(materialization))

    parent_facet = make_parent_run_facet(dagster_run.run_id, dagster_run.job_name)
    result_events: list[RunEvent] = []

    for step_key, step_data in steps.items():
        # Resolve inputs from the asset dep graph if available and inputs not already populated
        if not step_data["inputs"]:
            step_data["inputs"] = _build_upstream_inputs(step_key, asset_dep_graph)

        # Use a stable deterministic run ID for the step (UUIDv5 would be ideal;
        # for now generate fresh UUIDs — START + COMPLETE will be correlated via job name)
        # Deterministic UUID7: seeded from (step_start_time, dagster_run_id + step_key bytes).
        # UUID7 is time-ordered (monotonically increasing), so events sort naturally.
        # generate_static_uuid produces the same value for the same (instant, data) pair,
        # ensuring START and COMPLETE for the same step always share the same run ID.
        start_dt = _parse_ts(step_data["start_time"]) if step_data["start_time"] else datetime.now(timezone.utc)
        step_run_id = str(
            generate_static_uuid(
                instant=start_dt,
                data=(dagster_run.run_id + ":" + step_key).encode(),
            )
        )
        step_job_name = f"{dagster_run.job_name}.{step_key}"
        start_time = step_data["start_time"] or start_dt
        end_time = step_data["end_time"] or _now()

        # START
        result_events.append(
            RunEvent(
                eventType=RunState.START,
                eventTime=_format_ts(start_time),
                run=Run(
                    runId=step_run_id,
                    facets={"parent": parent_facet},
                ),
                job=Job(
                    namespace=DAGSTER_NAMESPACE,
                    name=step_job_name,
                    facets={
                        "jobType": STEP_JOB_TYPE_FACET,
                        "dagster": DagsterJobFacet(stepKey=step_key),
                    },
                ),
                inputs=step_data["inputs"],
                outputs=[],
                producer=PRODUCER,
            )
        )

        # COMPLETE or FAIL
        state = RunState.COMPLETE if step_data["success"] else RunState.FAIL
        result_events.append(
            RunEvent(
                eventType=state,
                eventTime=_format_ts(end_time),
                run=Run(
                    runId=step_run_id,
                    facets={"parent": parent_facet},
                ),
                job=Job(
                    namespace=DAGSTER_NAMESPACE,
                    name=step_job_name,
                    facets={
                        "jobType": STEP_JOB_TYPE_FACET,
                        "dagster": DagsterJobFacet(stepKey=step_key),
                    },
                ),
                inputs=step_data["inputs"],
                outputs=step_data["outputs"],
                producer=PRODUCER,
            )
        )

    return result_events


def _parse_ts(ts) -> datetime:
    """Convert a Dagster timestamp (float epoch seconds or datetime) to a timezone-aware datetime."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)
    # Fallback to now if unparsable (e.g. None that slipped through)
    return datetime.now(timezone.utc)


def _format_ts(ts) -> str:
    """Convert a Dagster timestamp to ISO 8601 string."""
    return _parse_ts(ts).isoformat()
