# Adapters

Adapters provide concrete implementations behind the `Session` workflow.

Current adapter areas include:

- validation
- enrichment
- aggregation
- dataops

The `Session` can use default adapters where available, or you can register a
custom adapter with `session.register_adapter(...)`.
