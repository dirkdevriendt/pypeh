# Core Concepts

`pypeh` centers workflows around PEH model resources

The main entry point is `pypeh.Session`. A session owns:

- a connection manager for local or remote persisted resources
- an in-memory cache of PEH model resources
- adapter registration for validation, data operations, enrichment, and
  aggregation
- an optional namespace manager for minting identifiers

More detailed core concept documentation can grow here as the public API
settles.
