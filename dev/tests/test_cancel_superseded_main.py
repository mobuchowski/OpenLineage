# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import io
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from urllib.error import URLError

from dev.cancel_superseded_main import REQUEST_TIMEOUT, CircleCI, cancel_superseded, main


def pipeline(number, *, branch="main", tag=None, trigger="webhook", revision=None):
    return {
        "id": f"pipeline-{number}",
        "number": number,
        "project_slug": "gh/OpenLineage/OpenLineage",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "trigger": {"type": trigger},
        "vcs": {"branch": branch, "tag": tag, "revision": revision or f"commit-{number}"},
    }


class CancellationTest(unittest.TestCase):
    def api(self, current, candidates, workflows=None):
        api = Mock()
        api.request.return_value = current

        def items(path, **_params):
            if path.startswith("project/"):
                return iter(candidates)
            return iter((workflows or {}).get(path, []))

        api.items.side_effect = items
        return api

    def test_cancels_only_active_older_automatic_main_workflows(self):
        current = pipeline(10)
        candidates = [
            pipeline(11),
            current,
            pipeline(9, tag="1.2.3"),
            pipeline(8, branch="feature"),
            pipeline(7, trigger="api"),
            pipeline(6, trigger="scheduled_pipeline"),
            pipeline(5, revision=current["vcs"]["revision"]),
            pipeline(4),
            pipeline(3),
        ]
        active_statuses = ["running", "failing", "on_hold", "queued"]
        terminal_statuses = ["success", "failed", "error", "canceled", "unauthorized", "not_run"]
        workflows = {
            "pipeline/pipeline-4/workflow": [
                {"id": status, "status": status} for status in active_statuses + terminal_statuses
            ],
            "pipeline/pipeline-3/workflow": [{"id": "older", "status": "running"}],
        }
        api = self.api(current, candidates, workflows)
        with redirect_stdout(io.StringIO()):
            cancel_superseded(api, current["id"])

        canceled = [
            call.args[0] for call in api.request.call_args_list if call.kwargs.get("method") == "POST"
        ]
        assert canceled == [f"workflow/{status}/cancel" for status in [*active_statuses, "older"]]
        assert [call.args[0] for call in api.items.call_args_list] == [
            "project/gh/OpenLineage/OpenLineage/pipeline",
            "pipeline/pipeline-4/workflow",
            "pipeline/pipeline-3/workflow",
        ]

    def test_ineligible_current_pipeline_does_not_list_or_cancel_workflows(self):
        for overrides in [
            {"tag": "1.2.3"},
            {"branch": "feature"},
            {"trigger": "api"},
            {"trigger": "scheduled_pipeline"},
        ]:
            with self.subTest(overrides=overrides):
                current = pipeline(10, **overrides)
                api = self.api(current, [pipeline(9)])
                cancel_superseded(api, current["id"])
                api.items.assert_not_called()
                api.request.assert_called_once_with(f"pipeline/{current['id']}")

    def test_missing_metadata_is_not_eligible(self):
        for key in ["vcs", "trigger"]:
            with self.subTest(key=key):
                candidate = pipeline(9)
                del candidate[key]
                api = self.api(pipeline(10), [candidate])
                cancel_superseded(api, "pipeline-10")
                api.request.assert_called_once_with("pipeline/pipeline-10")

    def test_stops_before_expired_pipelines(self):
        expired = pipeline(9)
        expired["created_at"] = (datetime.now(timezone.utc) - timedelta(days=91)).isoformat()
        api = self.api(pipeline(10), [expired])
        cancel_superseded(api, "pipeline-10")
        api.request.assert_called_once_with("pipeline/pipeline-10")

    def test_paginates_pipeline_and_workflow_lists(self):
        for path, params in [
            ("project/gh/OpenLineage/OpenLineage/pipeline", {"branch": "main"}),
            ("pipeline/pipeline-9/workflow", {}),
        ]:
            with self.subTest(path=path):
                api = CircleCI("test-token")
                with patch.object(api, "request") as request:
                    request.side_effect = [
                        {"items": [1], "next_page_token": "next page/+"},
                        {"items": [2], "next_page_token": None},
                    ]
                    assert list(api.items(path, **params)) == [1, 2]
                    assert request.call_args_list[0].kwargs == params
                    assert request.call_args_list[1].kwargs == {**params, "page-token": "next page/+"}

    def test_request_authentication_and_pagination_encoding(self):
        with patch("dev.cancel_superseded_main.urlopen") as urlopen:
            response = urlopen.return_value.__enter__.return_value
            response.read.return_value = b'{"items": []}'
            CircleCI("test-token").request("project/gh/org/repo/pipeline", **{"page-token": "next page/+"})
            request = urlopen.call_args.args[0]
            assert request.get_header("Circle-token") == "test-token"
            assert request.full_url.endswith("?page-token=next+page%2F%2B")
            assert urlopen.call_args.kwargs["timeout"] == REQUEST_TIMEOUT

    def test_environment_guard_skips_tags_other_branches_and_missing_token(self):
        for env in [
            {"CIRCLE_BRANCH": "main", "CIRCLE_TAG": "1.2.3", "CIRCLE_TOKEN": "test-token"},
            {"CIRCLE_BRANCH": "feature", "CIRCLE_TOKEN": "test-token"},
            {"CIRCLE_BRANCH": "main"},
        ]:
            with (
                self.subTest(env=env),
                patch.dict(os.environ, env, clear=True),
                patch("dev.cancel_superseded_main.CircleCI") as api,
                redirect_stdout(io.StringIO()),
            ):
                main()
                api.assert_not_called()

    def test_api_failure_does_not_block_ci(self):
        env = {"CIRCLE_BRANCH": "main", "CIRCLE_TOKEN": "test-token", "PIPELINE_ID": "pipeline-10"}
        with (
            patch.dict(os.environ, env, clear=True),
            patch("dev.cancel_superseded_main.cancel_superseded", side_effect=URLError("unavailable")),
            redirect_stdout(io.StringIO()) as output,
        ):
            main()
        assert "continuing CI" in output.getvalue()


if __name__ == "__main__":
    unittest.main()
