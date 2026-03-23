# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.facet_v2 import JobFacet, RunFacet

PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/main/integration/dagster"


@attr.define
class DagsterRunFacet(RunFacet):
    """Dagster-specific run metadata attached to every OpenLineage run."""

    jobName: str = attr.field()  # noqa: N815
    """The Dagster job name."""

    runId: str = attr.field()  # noqa: N815
    """The Dagster run ID (not the OpenLineage run ID)."""

    runTags: dict[str, str] = attr.field(factory=dict)  # noqa: N815
    """Dagster run tags."""

    _additional_skip_redact: ClassVar[list[str]] = ["jobName", "runId"]

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://github.com/OpenLineage/OpenLineage/tree/main"
            "/integration/dagster/dagster-run-facet.json"
        )


@attr.define
class DagsterJobFacet(JobFacet):
    """Dagster-specific job metadata."""

    repositoryName: str | None = attr.field(default=None)  # noqa: N815
    """Dagster code location / repository name."""

    stepKey: str | None = attr.field(default=None)  # noqa: N815
    """For step-level events: the Dagster step key (op name)."""

    _additional_skip_redact: ClassVar[list[str]] = ["repositoryName", "stepKey"]

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://github.com/OpenLineage/OpenLineage/tree/main"
            "/integration/dagster/dagster-job-facet.json"
        )
