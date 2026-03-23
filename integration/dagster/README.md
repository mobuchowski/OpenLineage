# OpenLineage Dagster Integration

This package provides an OpenLineage integration for Dagster.

## Usage

```python
from openlineage.dagster import openlineage_sensor
from dagster import Definitions

defs = Definitions(
    assets=[...],
    sensors=openlineage_sensor(),
)
```

Configure transport via `OPENLINEAGE_CONFIG` environment variable:

```bash
export OPENLINEAGE_CONFIG='{"transport":{"type":"file","log_file_path":"/tmp/ol-events.ndjson"}}'
```
