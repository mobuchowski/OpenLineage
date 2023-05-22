import re
from typing import Optional

from openlineage.client.run import RunEvent


class Filter:
    def filter(self, event: RunEvent) -> Optional[RunEvent]:
        ...


class ExactMatchFilter(Filter):
    def __init__(self, match: str, *args, **kwargs):
        self.match = match

    def filter(self, event: RunEvent) -> Optional[RunEvent]:
        if self.match == event.job.name:
            return None
        return event


class RegexFilter(Filter):
    def __init__(self, regex: str, *args, **kwargs):
        self.pattern = re.compile(regex)

    def filter(self, event: RunEvent) -> Optional[RunEvent]:
        if self.pattern.match(event.job.name):
            return None
        return event


def create_filter(conf: dict) -> Optional[Filter]:
    if 'type' not in conf:
        return None
    # Switch in 3.10 🙂
    if conf['type'] == "exact":
        return ExactMatchFilter(match=conf['match'])
    elif conf['type'] == "regex":
        return RegexFilter(regex=conf['regex'])