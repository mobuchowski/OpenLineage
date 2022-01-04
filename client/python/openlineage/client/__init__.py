from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import set_producer

from . import _version
__version__ = _version.get_versions()['version']
