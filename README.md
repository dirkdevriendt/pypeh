# Installation

## Core Library
To install the core library, run:
```bash
poetry install pypeh -E core
```

## Adapters
### Dataframe adapter
To install the dataframe adapter, run:
```bash
poetry install pypeh -E dataframe_adapter
```

## Development
```bash
poetry install --with test,dev
```

# Central design ideas: 
* core module: specifies a series of **interfaces** 
    - AIM: entry and/or exit point with no knowledge of the concrete implementation
    - the interface can be thought of as a type hint
* Each of interface can have **adapters**.
    - AIM: concrete implementation of an interface
* Inversion of control: Appropriate should be passed to the core module