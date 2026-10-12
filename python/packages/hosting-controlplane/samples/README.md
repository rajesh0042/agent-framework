# Ten-stage GitHub Actions DAG

This sample uses `agent-framework-hosting-controlplane` to compile a static DAG
containing ten sequential nodes. The generated workflow calls a local composite
action that prints each node's stage number, allowing the DAG structure to be
tested without deploying Foundry agents.

## Generate the workflow

From the `python` directory, run:

```bash
uv run python packages/hosting-controlplane/samples/generate_echo_dag.py
```

This writes `.github/workflows/echo-dag.yml` and prints its content-hash version.
Commit the generated workflow whenever the DAG definition changes.

## Run the workflow

In the repository's **Actions** tab, select **Foundry DAG: ten-stage-echo** and
choose **Run workflow**. Supply:

- a unique value for `framework_run_id`;
- `ten-stage-echo` for `dag_id`; and
- the generated content hash for `dag_version`.

The remaining inputs can keep their defaults. The run executes `stage_1`
through `stage_10` in order, and each job logs `Running stage N`.

The local echo action is only a structural demonstration. Production DAGs should
reference an action that authenticates to Foundry and invokes the configured
hosted agent for each node.
