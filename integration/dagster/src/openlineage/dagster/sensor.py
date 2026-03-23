# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Dagster run_status_sensor that emits OpenLineage events."""

from __future__ import annotations

import logging

from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from openlineage.client.client import OpenLineageClient
from openlineage.dagster.converter import (
    build_parent_job_complete_event,
    build_parent_job_start_event,
    build_step_events,
)

log = logging.getLogger(__name__)

_RECORD_LIMIT = 1000


def _get_asset_dep_graph(context: RunStatusSensorContext) -> dict | None:
    """
    Extract the asset dependency graph from the repository definition.
    Returns the dict form: {"upstream": {AssetKey: {AssetKey,...}}, "downstream": {...}}
    or None if unavailable.
    """
    try:
        repo_def = context.repository_def
        if repo_def is None:
            return None
        asset_graph = getattr(repo_def, "asset_graph", None)
        if asset_graph is None:
            return None
        return getattr(asset_graph, "asset_dep_graph", None)
    except Exception:
        log.debug("OpenLineage: could not retrieve asset dep graph", exc_info=True)
        return None


def _get_run_event_records(context: RunStatusSensorContext, run_id: str) -> list:
    """Fetch all event log records for a given run from the Dagster instance."""
    try:
        # get_records_for_run returns EventLogRecord list
        records = context.instance.get_records_for_run(run_id=run_id, limit=_RECORD_LIMIT).records
        return list(records)
    except Exception:
        log.exception("Failed to fetch event records for run %s", run_id)
        return []


def make_openlineage_sensor(
    name: str = "openlineage_sensor",
    minimum_interval_seconds: int = 30,
):
    """
    Factory that returns a Dagster run_status_sensor emitting OpenLineage events.

    Usage::

        from openlineage.dagster import openlineage_sensor

        defs = Definitions(
            assets=[...],
            sensors=[openlineage_sensor()],
        )

    Configure transport via the standard OpenLineage environment variables:
      - OPENLINEAGE_URL            HTTP backend
      - OPENLINEAGE_CONFIG         JSON transport config (file, console, kafka, …)
    """

    @run_status_sensor(
        run_status=DagsterRunStatus.STARTED,
        name=f"{name}_start",
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def _on_start(context: RunStatusSensorContext):
        dagster_run = context.dagster_run
        log.info("OpenLineage: emitting START for run %s (%s)", dagster_run.run_id, dagster_run.job_name)
        client = OpenLineageClient()
        try:
            client.emit(build_parent_job_start_event(dagster_run))
        except Exception:
            log.exception("OpenLineage: failed to emit START event for run %s", dagster_run.run_id)
        finally:
            client.close(timeout=5)

    @run_status_sensor(
        run_status=DagsterRunStatus.SUCCESS,
        name=f"{name}_success",
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def _on_success(context: RunStatusSensorContext):
        dagster_run = context.dagster_run
        log.info("OpenLineage: emitting COMPLETE for run %s (%s)", dagster_run.run_id, dagster_run.job_name)
        client = OpenLineageClient()
        try:
            records = _get_run_event_records(context, dagster_run.run_id)
            asset_dep_graph = _get_asset_dep_graph(context)
            for event in build_step_events(dagster_run, records, asset_dep_graph=asset_dep_graph):
                client.emit(event)
            client.emit(build_parent_job_complete_event(dagster_run, success=True))
        except Exception:
            log.exception("OpenLineage: failed to emit COMPLETE events for run %s", dagster_run.run_id)
        finally:
            client.close(timeout=10)

    @run_status_sensor(
        run_status=DagsterRunStatus.FAILURE,
        name=f"{name}_failure",
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def _on_failure(context: RunStatusSensorContext):
        dagster_run = context.dagster_run
        log.info("OpenLineage: emitting FAIL for run %s (%s)", dagster_run.run_id, dagster_run.job_name)
        client = OpenLineageClient()
        try:
            records = _get_run_event_records(context, dagster_run.run_id)
            asset_dep_graph = _get_asset_dep_graph(context)
            for event in build_step_events(dagster_run, records, asset_dep_graph=asset_dep_graph):
                client.emit(event)
            client.emit(build_parent_job_complete_event(dagster_run, success=False))
        except Exception:
            log.exception("OpenLineage: failed to emit FAIL events for run %s", dagster_run.run_id)
        finally:
            client.close(timeout=10)

    return [_on_start, _on_success, _on_failure]
