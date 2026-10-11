# Agent Framework Hosting Control Plane (`agent-framework-hosting-controlplane`)

Hosted DAG control-plane MVP: compile a **static** Agent Framework `Workflow`
(or a hand-authored declarative DAG) into a **GitHub Actions** workflow whose
jobs call a configurable reusable/composite action to invoke and poll
**Foundry-hosted agents**, then launch, observe, cancel, retry, and resume
runs of that workflow while tracking normalized run/node/event state.

> **Foundry does the compute.** This package never executes agent logic. The
> generated Ubuntu Actions jobs only carry identifiers/URIs and call out to
> Foundry; node outputs are exchanged as output/checkpoint *references*, never
> as inline payloads.

## Install

```bash
pip install agent-framework-hosting-controlplane
```

## Architecture

| Concern | Module | Notes |
| --- | --- | --- |
| Typed models | `_models` | Frozen Pydantic models for DAG definitions, nodes, edges, runs, node state, events, checkpoints, failures. |
| Compiler | `_compiler` | `Workflow` + per-executor `FoundryNodeSpec`, or a declarative `DagDefinition`, -> validated, content-hashed `DagVersion` with a rendered GitHub Actions workflow (JSON text; JSON is valid YAML). |
| DAG registry | `_registry` | `DagRegistry` protocol + thread-safe `InMemoryDagRegistry`. Registrations are immutable and versioned by content hash; registering identical content twice is a no-op. |
| Run store | `_store` | `RunStore` protocol + thread-safe `InMemoryRunStore`. Normalizes run/node/event state; launches are idempotent by `(dag_id, idempotency_key)`. |
| GitHub client | `_github_client` | `GitHubActionsClient` protocol + `HttpxGitHubActionsClient`. Dispatches, correlates, polls, cancels, and reruns workflow runs. |
| Webhook HMAC | `_webhooks` | Constant-time `X-Hub-Signature-256` validation. |
| Control plane | `_control_plane` | `ControlPlane` service: register, launch, observe, cancel, retry, resume, webhook ingestion, reconciliation. `EventBroker` is the bounded in-process SSE pub/sub. |
| HTTP API | `_app` | `create_app(control_plane)` FastAPI app factory. |

## Compiling a DAG

```python
from agent_framework_hosting_controlplane import (
    CompilerOptions,
    FoundryNodeSpec,
    GitHubRepoRef,
    ReusableActionRef,
    compile_workflow,
)

options = CompilerOptions(
    repo=GitHubRepoRef(owner="my-org", repo="my-dags", default_ref="main"),
    action=ReusableActionRef(uses="my-org/foundry-invoke-action@v1"),
    workflow_file="foundry-dag.yml",
    # runs_on defaults to "ubuntu-slim"; see "Runner label" below.
)

dag_version = compile_workflow(
    my_workflow,  # an agent_framework.Workflow
    {
        "extract": FoundryNodeSpec(agent_id="extract-agent"),
        "summarize": FoundryNodeSpec(agent_id="summarize-agent", input_mapping={"text": "extract.output"}),
    },
    dag_id="daily-report",
    options=options,
)
print(dag_version.workflow_json)  # commit this as .github/workflows/foundry-dag.yml
```

Or compile a hand-authored, declarative DAG directly with `compile_definition(definition, options=options)`
using `DagDefinition`/`DagNode`/`DagEdge` from the same package, with no dependency on
`agent_framework`'s `Workflow` type at all.

### Static-DAG boundary

The compiler rejects, with a `DagCompilationError`:

- Duplicate or unsafe node ids (ids must match GitHub Actions job id syntax: start with a
  letter or `_`, then letters/digits/`-`/`_`, max 64 characters) and ids that only differ by case.
- Edges that reference an undeclared node, or a node that depends on an undeclared node.
- Cycles (the dependency graph, from both `DagEdge`s and `DagNode.depends_on`, must be acyclic).
- Conditional or dynamically-selected edges (`SwitchCaseEdgeGroup`, or any edge with a condition).
- Nested workflows (`WorkflowExecutor`) and, by extension, superstep semantics: every node is a
  single GitHub Actions job, run exactly once per run.

### Versioning

`DagVersion.version` is a `sha256`-based content hash (`v1-<16 hex chars>`) over the canonical
JSON of the `DagDefinition` plus the compiler options that affect rendering (`action.uses`,
`action.with_inputs`, `runs_on`). Compiling identical content always yields the identical version,
and `DagRegistry.register` treats re-registering an existing `(dag_id, version)` as a no-op that
returns the original registration — safe to call from CI on every push.

### Runner label

Generated jobs default to `runs-on: ubuntu-slim`, the 1 vCPU / 5 GB GitHub-hosted Linux runner.
Set `CompilerOptions(runs_on="ubuntu-latest", ...)` for GitHub Enterprise Server or runner fleets
that do not yet support the slim label; any other valid runner label also works.

## GitHub Actions workflow shape

Every compiled workflow:

- Declares `workflow_dispatch` inputs `framework_run_id`, `dag_id`, `dag_version`, `inputs_json`,
  and `resume_checkpoint_uri`.
- Sets `run-name: foundry-dag/${{ inputs.dag_id }}/${{ inputs.framework_run_id }}`. GitHub Actions
  does not return a run id from a `workflow_dispatch` REST call, so this templated `run-name` is
  the documented, race-free way to correlate the run it creates with the launch that requested it;
  the control plane and `GitHubActionsClient.find_run_by_name` both rely on it.
- Declares top-level `permissions: {id-token: write, contents: read}` for OIDC federation (see
  below) and read-only repo access; grant any additional Foundry-specific permissions through the
  reusable action itself, not by widening these.
- Emits one job per DAG node, keyed by the node id, with `needs:` derived from the DAG's edges.
  Each job's single step `uses:` the configured `ReusableActionRef` and passes only identifiers
  (`agent-id`, `agent-version`, `project-endpoint`, `node-id`, `framework-run-id`), an
  `input-mapping-json`/`upstream-outputs-json` of *references* to upstream nodes' `output_ref`,
  and retry/timeout configuration. No node ever receives another node's actual output payload.
  Job display names are left at their GitHub default (the job key), so `GET
  .../actions/runs/{run}/jobs` responses correlate 1:1 back to DAG node ids via `job.name`.

### OIDC

`permissions.id-token: write` lets the reusable/composite action exchange a short-lived OIDC token
for Foundry/Azure credentials instead of a stored secret. The action referenced by
`CompilerOptions.action.uses` owns the actual OIDC exchange (for example via
`azure/login@v2` or a custom token-exchange step); this package only guarantees the permission is
granted at the workflow level.

## Control plane

```python
from agent_framework_hosting_controlplane import (
    ControlPlane,
    HttpxGitHubActionsClient,
    InMemoryDagRegistry,
    InMemoryRunStore,
)

github_client = HttpxGitHubActionsClient(owner="my-org", repo="my-dags", token=get_installation_token)
control_plane = ControlPlane(
    dag_registry=InMemoryDagRegistry(),
    run_store=InMemoryRunStore(),
    github_client=github_client,
    webhook_secret="...",  # required for production; omit only for local development
)

await control_plane.register_dag(dag_version)
run = await control_plane.launch("daily-report", inputs={"source_uri": "blob://..."})
```

`launch()` returns as soon as the attempt is recorded and (if dispatch succeeded) the GitHub
Actions `workflow_dispatch` call returns — it never waits for the DAG itself to make progress.
Supplying the same `idempotency_key` for the same `dag_id` again returns the original `Run`
unchanged and never dispatches a second workflow run; this idempotency is enforced by the
`RunStore`, not just the in-memory default, so a durable backend gets it too as long as it
implements `RunStore.create_run` per its documented contract.

### State machine

`RunStatus`/`NodeStatus` transitions are validated against an explicit monotonic transition table
(`RUN_TRANSITIONS`/`NODE_TRANSITIONS` in `_models.py`). Terminal statuses
(`succeeded`/`failed`/`cancelled`/`skipped`) never transition again. A transition to the run/node's
*current* status is always accepted as a no-op (duplicate delivery idempotency); an invalid
*different*-status transition (for example a stale `running` webhook arriving after a `succeeded`
one) is logged and dropped rather than applied, so out-of-order delivery cannot corrupt state.

### Webhook ingestion

`POST /webhooks/github` (or `ControlPlane.ingest_webhook` directly) handles `workflow_run` and
`workflow_job` events:

- `workflow_run.display_title` is parsed against the `run-name` template to find the
  `framework_run_id`.
- `workflow_job.name` *is* the DAG node id (see "job display names" above), and
  `workflow_job.run_id` correlates back to the framework run via the previously-recorded
  `github_run.run_id`.

When `ControlPlane.webhook_secret` is set, every delivery's `X-Hub-Signature-256` header is
validated with constant-time HMAC-SHA256 (`hmac.compare_digest`) before any state changes; an
invalid signature raises `WebhookSignatureError` (mapped to HTTP 401 by `create_app`). Set
`webhook_secret=None` only for local development or when an app-level gateway already validates
deliveries.

### Reconciliation

`ControlPlane.reconcile(run_id)` polls GitHub Actions directly (find-by-name if the run is not yet
correlated, then `get_run`/`list_jobs`) and applies the same validated, idempotent transition logic
as webhook ingestion. Call it periodically (for example every 1-5 minutes) for in-flight runs to
recover from missed or delayed webhook deliveries; it is a no-op (no API calls) for runs already in
a terminal status.

### Cancel / retry / resume

- **Cancel** is idempotent (cancelling an already-cancelling or terminal run is a no-op) and
  best-effort on GitHub's side: `cancel_run` asks GitHub to stop scheduling new jobs and stop
  in-progress runners, but a job that has already handed work to Foundry may continue running
  until the reusable action's own polling loop observes the cancellation and returns. **The
  control plane cannot force Foundry to stop mid-invocation** — the reusable action is responsible
  for checking for cancellation and/or for Foundry providing its own cancellation API.
- **Retry** re-runs only the failed jobs of a `failed` run (`rerun-failed-jobs`) and resets the
  locally-tracked `failed` node states to `queued` with an incremented `attempt`. This reset is an
  explicit, operator-authorized exception to the generic transition validator (which exists to
  reject *out-of-order webhooks*, not deliberate operator actions).
- **Resume** launches a brand-new, linked run (`Run.parent_run_id` set) using the most recently
  recorded `CheckpointRef` across the prior run's nodes, passed through as the
  `resume_checkpoint_uri` workflow input. There is no in-place continuation of a run: resuming
  always produces a new `framework_run_id` and a new GitHub Actions dispatch.

## HTTP API

`create_app(control_plane)` builds a FastAPI app with exactly these routes:

- `POST /dags/{dag_id}/runs`
- `GET /runs/{run_id}`
- `GET /runs/{run_id}/nodes`
- `GET /runs/{run_id}/events`
- `GET /runs/{run_id}/events/stream` (SSE)
- `POST /runs/{run_id}/cancel`
- `POST /runs/{run_id}/retry`
- `POST /runs/{run_id}/resume`
- `POST /webhooks/github`

There is **no** DAG-registration HTTP route: compile and register DAGs ahead of time (for example
from a CI job that also commits the generated workflow file), then launch runs against the
resulting `dag_id`/`version` through this API.

Authentication/authorization are entirely app-provided: pass `dependencies=[Depends(...)]` to
`create_app` to apply (for example) an API-key or OAuth dependency to every route.

### SSE

`GET /runs/{run_id}/events/stream` first replays persisted events after `Last-Event-ID` (or all
events, if omitted) from the `RunStore`, then subscribes to live events, sending a `: heartbeat`
comment every ~15 seconds of inactivity so intermediate proxies do not close the connection.
Resource usage is bounded: each subscription uses a fixed-size `asyncio.Queue`
(`EventBroker(max_queue_size=...)`); a subscriber that cannot keep up has new events dropped rather
than buffered without bound, and can always catch up by reconnecting with `Last-Event-ID`. The
number of live subscribers per run is capped (`EventBroker(max_subscribers_per_run=...)`); once
the cap is hit, new subscriptions fail with `SubscriberLimitExceededError` (HTTP 429).

## Storage protocols

`DagRegistry` and `RunStore` are `typing.Protocol`s. `InMemoryDagRegistry`/`InMemoryRunStore` are
thread-safe (guarded by `threading.RLock`) and adequate for tests and single-process deployments,
but neither is durable: process restart loses all state. Implement both protocols against a
database for production use; `ControlPlane` depends only on the protocols, not the in-memory
implementations.

## Static-only boundary

This package intentionally does not support conditional routing, dynamic fan-out, nested
workflows, or superstep/iterative semantics — see "Static-DAG boundary" above. Workflows that need
those capabilities are out of scope for this control plane; it targets only workflows whose graph
shape is fully known at compile time.

## Security

- GitHub tokens are never logged or included in exception messages (`GitHubClientError` messages
  include only the HTTP method, path, and status code).
- `HttpxGitHubActionsClient` accepts a static token or a zero-argument callable, so short-lived
  GitHub App installation tokens can be refreshed out-of-band without this package caching a stale
  credential.
- Webhook signature validation uses `hmac.compare_digest` (constant-time) against
  `X-Hub-Signature-256`.
- Node-to-node data exchange is reference-only (`OutputRef`/`CheckpointRef` URIs); large payloads
  never transit the control plane, the generated jobs, or GitHub Actions `needs.*.outputs`.
