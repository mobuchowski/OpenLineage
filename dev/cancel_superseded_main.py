# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Cancel older push-triggered main workflows, preserving tag and API pipelines."""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://circleci.com/api/v2"
REQUEST_TIMEOUT = 30
ACTIVE_STATUSES = {"running", "failing", "on_hold", "queued"}


class CircleCI:
    def __init__(self, token):
        self.token = token

    def request(self, path, method="GET", **params):
        url = f"{API_URL}/{path}"
        if params:
            url += "?" + urlencode(params)
        request = Request(  # noqa: S310 -- fixed HTTPS API URL
            url, headers={"Circle-Token": self.token}, method=method
        )
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:  # noqa: S310 -- fixed HTTPS API URL
            return json.load(response)

    def items(self, path, **params):
        while True:
            page = self.request(path, **params)
            yield from page["items"]
            if not page.get("next_page_token"):
                return
            params["page-token"] = page["next_page_token"]


def automatic_main(pipeline):
    vcs = pipeline.get("vcs") or {}
    return (
        vcs.get("branch") == "main"
        and not vcs.get("tag")
        and bool(vcs.get("revision"))
        and pipeline.get("trigger", {}).get("type") == "webhook"
    )


def cancel_superseded(api, pipeline_id):
    current = api.request(f"pipeline/{pipeline_id}")
    if not automatic_main(current):
        return

    # CircleCI cancels non-terminal workflows after 90 days. Older pipelines
    # therefore only contain completed workflows or deliberate reruns.
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    for pipeline in api.items(f"project/{current['project_slug']}/pipeline", branch="main"):
        created_at = datetime.fromisoformat(pipeline["created_at"].replace("Z", "+00:00"))
        if created_at < cutoff:
            break
        if (
            not automatic_main(pipeline)
            or pipeline["number"] >= current["number"]
            or pipeline["vcs"]["revision"] == current["vcs"]["revision"]
        ):
            continue

        for workflow in api.items(f"pipeline/{pipeline['id']}/workflow"):
            if workflow["status"] in ACTIVE_STATUSES:
                api.request(f"workflow/{workflow['id']}/cancel", method="POST")
                print(f"Canceled workflow {workflow['id']} in superseded pipeline {pipeline['number']}")


def main():
    # Tag pipelines can also report a branch. Always check the tag first.
    if os.environ.get("CIRCLE_TAG") or os.environ.get("CIRCLE_BRANCH") != "main":
        return
    token = os.environ.get("CIRCLE_TOKEN")
    if not token:
        print("CIRCLE_TOKEN is not configured; skipping cancellation of superseded main workflows.")
        return
    try:
        cancel_superseded(CircleCI(token), os.environ["PIPELINE_ID"])
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        # Cancellation is an optimization; an API outage must not block CI.
        print(f"Could not cancel superseded workflows; continuing CI: {error}")


if __name__ == "__main__":
    main()
