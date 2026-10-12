# Copyright (c) Microsoft. All rights reserved.

from pathlib import Path

from agent_framework_hosting_controlplane import (
    CompilerOptions,
    DagDefinition,
    DagNode,
    FoundryNodeSpec,
    GitHubRepoRef,
    ReusableActionRef,
    compile_definition,
)

"""Generate a GitHub Actions workflow containing ten sequential echo jobs."""

STAGE_COUNT = 10
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "echo-dag.yml"


def main() -> None:
    """Compile and write the ten-stage echo DAG."""
    definition = DagDefinition(
        dag_id="ten-stage-echo",
        description="Ten sequential echo stages",
        nodes=tuple(
            DagNode(
                id=f"stage_{stage}",
                foundry=FoundryNodeSpec(agent_id=f"echo-stage-{stage}"),
                depends_on=() if stage == 1 else (f"stage_{stage - 1}",),
            )
            for stage in range(1, STAGE_COUNT + 1)
        ),
    )
    options = CompilerOptions(
        repo=GitHubRepoRef(owner="rajesh0042", repo="agent-framework", default_ref="main"),
        action=ReusableActionRef(uses="./.github/actions/echo-stage"),
        workflow_file=WORKFLOW_PATH.name,
        runs_on="ubuntu-latest",
    )

    compiled = compile_definition(definition, options=options)
    WORKFLOW_PATH.write_text(f"{compiled.workflow_json}\n", encoding="utf-8")
    print(f"Generated {WORKFLOW_PATH.relative_to(REPOSITORY_ROOT)}")
    print(f"DAG version: {compiled.version}")


if __name__ == "__main__":
    main()
