# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.client import set_producer
from openlineage.client.facet_v2 import tags_run
from openlineage.common.provider.dbt.processor import (
    DbtArtifactProcessor,
    DbtRunContext,
)


@pytest.fixture(scope="session", autouse=True)
def setup_producer():
    set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt")


class TestParseDbtTag:
    def test_simple_tag(self):
        result = DbtArtifactProcessor._parse_dbt_tag("sometag")
        assert result == tags_run.TagsRunFacetFields(key="sometag", value="true", source="DBT")

    def test_key_value_tag(self):
        result = DbtArtifactProcessor._parse_dbt_tag("team:analytics")
        assert result == tags_run.TagsRunFacetFields(key="team", value="analytics", source="DBT")

    def test_key_value_source_tag(self):
        result = DbtArtifactProcessor._parse_dbt_tag("env:production:INFRA")
        assert result == tags_run.TagsRunFacetFields(key="env", value="production", source="INFRA")

    def test_empty_value_tag(self):
        result = DbtArtifactProcessor._parse_dbt_tag("pii:")
        assert result == tags_run.TagsRunFacetFields(key="pii", value="", source="DBT")


def test_seed_snapshot_nodes_do_not_throw():
    processor = DbtArtifactProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="job-namespace",
    )

    # Should just skip processing
    processor.parse_assertions(
        DbtRunContext({}, {"results": [{"unique_id": "seed.jaffle_shop.raw_orders"}]}),
        {},
    )
