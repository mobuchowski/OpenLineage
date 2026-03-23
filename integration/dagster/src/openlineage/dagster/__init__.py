# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OpenLineage integration for Dagster."""

from openlineage.dagster.sensor import make_openlineage_sensor

__all__ = ["make_openlineage_sensor"]


def openlineage_sensor(**kwargs):
    """Convenience wrapper — returns the list of run_status_sensors."""
    return make_openlineage_sensor(**kwargs)
