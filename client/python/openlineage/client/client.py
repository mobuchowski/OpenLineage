# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import typing
from typing import Optional

import attr
import attrs

from openlineage.client.serde import Serde

if typing.TYPE_CHECKING:
    from requests import Session
    from requests.adapters import HTTPAdapter

from typing import List

from openlineage.client.run import RunEvent
from openlineage.client.transport import Transport, get_default_factory
from openlineage.client.transport.http import HttpConfig, HttpTransport


@attrs.define
class OpenLineageClientOptions:
    timeout: float = attrs.field(default=5.0)
    verify: bool = attrs.field(default=True)
    api_key: str = attrs.field(default=None)
    adapter: "HTTPAdapter" = attrs.field(default=None)


log = logging.getLogger(__name__)


@attrs.define
class Filter:
    


class OpenLineageClient:
    def __init__(
        self,
        url: Optional[str] = None,
        options: Optional[OpenLineageClientOptions] = None,
        session: Optional["Session"] = None,
        transport: Optional[Transport] = None,
    ):
        if url:
            # Backwards compatibility: if URL or options is set, use old path to initialize
            # HTTP transport.
            if not options:
                options = OpenLineageClientOptions()
            if not session:
                from requests import Session
                session = Session()
            self._initialize_url(url, options, session)
        elif transport:
            self.transport = transport
        else:
            self.transport = get_default_factory().create()

    def _initialize_url(
        self,
        url: str,
        options: OpenLineageClientOptions,
        session: 'Session'
    ):
        self.transport = HttpTransport(HttpConfig.from_options(
            url=url,
            options=options,
            session=session
        ))

    def emit(self, event: RunEvent):
        if not isinstance(event, RunEvent):
            raise ValueError("`emit` only accepts RunEvent class")
        if not self.transport:
            log.error("Tried to emit OpenLineage event, but transport is not configured.")
            return
        else:
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    f"OpenLineageClient will emit event {Serde.to_json(event).encode('utf-8')}"
                )
        event = self.filter_event(self.transport.filter, event)
        if event:
            self.transport.emit(event)

    def filter_event(self, filters: List[Filter], event: RunEvent):
        for filter

    @classmethod
    def from_environment(cls):
        return cls(transport=get_default_factory().create())

    @classmethod
    def from_dict(cls, config: dict):
        return cls(transport=get_default_factory().create(config=config))
