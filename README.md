# Installation

## Core Library
To install the core library for development, run:
```bash
uv sync
```

## Adapters
### Dataframe adapter
To install the dataframe adapter, run:
```bash
uv sync --extra dataframe_adapter
```

## Tests

```bash
make test-core
make test-dataframe
make test-rocrate
```
