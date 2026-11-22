# Internal packages

The source tree is grouped by runtime responsibility:

- `agent/` — the XC_VM cluster agent, its protocol and node-side runtime.
- `fanout/` — the live fan-out daemon, HTTP front, gateway and stream lifecycle.
- `media/` — transport-stream, HLS, fMP4, source and remux primitives.
- `shared/` — small packages used by more than one runtime.
- `testkit/` — fixtures and adapters used only by tests.

Dependencies should point down this list: applications may compose domain packages;
domain packages may use `media` and `shared`; production packages must not import
`testkit`. The agent and fan-out runtimes communicate through their socket protocols,
not by importing one another.

`agent/runtime` is the remaining migration seam. New unrelated behaviour should not
be added directly to its `Agent` type: prefer a focused package with state owned by
that package, then let the runtime compose it.
