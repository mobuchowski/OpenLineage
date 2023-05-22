# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import re
import typing
from typing import Optional

import attrs

from openlineage.client.filter import create_filter
from openlineage.client.serde import Serde
from openlineage.client.utils import load_config

if typing.TYPE_CHECKING:
    from requests import Session
    from requests.adapters import HTTPAdapter

from openlineage.client.run import RunEvent
from openlineage.client.transport import Transport, get_default_factory, TransportFactory
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
    pass


class OpenLineageClient:
    def __init__(
        self,
        url: Optional[str] = None,
        options: Optional[OpenLineageClientOptions] = None,
        session: Optional["Session"] = None,
        transport: Optional[Transport] = None,
        factory: TransportFactory = get_default_factory()
    ):
        # Make config ellipsis - as a guard value to not try to reload yaml each time config is referred to.
        self._config = None
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
            transport_config = None if 'transport' not in self.config else self.config['transport']
            self.transport = factory.create(transport_config)

        self._filters = []
        print(self.config)
        if 'filters' in self.config:
            for filter in self.config['filters']:
                filter = create_filter(filter)
                if filter:
                    self._filters.append(filter)

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
        print("????")
        print(event)
        print(self._filters)
        print("????")
        if self._filters:
            event = self.filter_event(event)
            print(event)
        if event:
            self.transport.emit(event)

    def filter_event(self, event: RunEvent) -> Optional[RunEvent]:
        """Filters jobs according to config-defined events"""
        for filter in self._filters:
            if filter.filter(event) is None:
                return None
        return event

    @property
    def config(self) -> dict:
        if self._config is None:
            self._config = load_config()
        return self._config

    @classmethod
    def from_environment(cls):
        return cls()

