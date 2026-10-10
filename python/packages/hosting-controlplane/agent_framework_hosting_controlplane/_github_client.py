# Copyright (c) Microsoft. All rights reserved.

"""httpx-based GitHub Actions REST client used to drive compiled DAG workflows.

Only the operations the control plane needs are exposed: dispatching a
workflow run, correlating the run that a dispatch created, polling run/job
status, cancelling a run, and re-running failed jobs. The `GitHubActionsClient`
protocol exists so tests (and alternative transports, such as a GitHub App
installation-token client) can substitute a fake without depending on httpx.

Token handling: the client accepts a static token string or a zero-argument
callable returning one, so callers can plug in short-lived GitHub App
installation tokens that are refreshed out-of-band. The token is never
included in logged messages or in raised exceptions; `GitHubClientError`
messages only ever include the HTTP method, URL path, and status code.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

import httpx

from ._exceptions import GitHubClientError
from ._models import GitHubJobRef, GitHubRunRef

__all__ = ["GitHubActionsClient", "HttpxGitHubActionsClient"]

_API_VERSION = "2022-11-28"
_ACCEPT = "application/vnd.github+json"


@runtime_checkable
class GitHubActionsClient(Protocol):
    """Protocol for the subset of the GitHub Actions REST API the control plane uses."""

    async def dispatch_workflow(self, *, workflow_file: str, ref: str, inputs: Mapping[str, str]) -> None:
        """Trigger a `workflow_dispatch` event. Does not return a run id (GitHub does not provide one here)."""
        ...

    async def find_run_by_name(
        self, *, workflow_file: str, run_name: str, created_after: datetime | None = None
    ) -> GitHubRunRef | None:
        """Find a `workflow_dispatch`-triggered run whose rendered `run-name` equals `run_name`."""
        ...

    async def get_run(self, *, run_id: int) -> GitHubRunRef:
        """Fetch the current status/conclusion for a workflow run."""
        ...

    async def list_jobs(self, *, run_id: int) -> list[GitHubJobRef]:
        """List jobs for a workflow run. Job `name` equals the DAG node id (the job key)."""
        ...

    async def cancel_run(self, *, run_id: int) -> None:
        """Request cancellation of a workflow run.

        This is best-effort and asynchronous: GitHub stops scheduling new
        jobs and asks in-progress runners to stop, but an in-progress step
        (including one that already dispatched work to Foundry) may continue
        running until it reaches its next checkpoint.
        """
        ...

    async def rerun_failed_jobs(self, *, run_id: int) -> None:
        """Re-run only the failed jobs of a completed workflow run."""
        ...


class HttpxGitHubActionsClient:
    """`GitHubActionsClient` implementation backed by `httpx.AsyncClient`."""

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        token: str | Callable[[], str],
        base_url: str = "https://api.github.com",
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
    ) -> None:
        """Create a GitHub Actions client scoped to a single repository.

        Args:
            owner: Repository owner (user or organization).
            repo: Repository name.
            token: A static GitHub token, or a zero-argument callable that
                returns a current one (for short-lived installation tokens).
            base_url: GitHub REST API base URL; override for GitHub Enterprise Server.
            client: Optional pre-configured `httpx.AsyncClient` (for example
                one with custom proxies or retries); if omitted, one is created.
            timeout: Request timeout in seconds, used only when `client` is omitted.
        """
        self._owner = owner
        self._repo = repo
        self._token = token
        self._client = client or httpx.AsyncClient(base_url=base_url, timeout=timeout)

    def _resolve_token(self) -> str:
        return self._token() if callable(self._token) else self._token

    def _headers(self) -> dict[str, str]:
        # Resolved immediately before each request so short-lived installation tokens stay fresh;
        # never cached on the instance and never included in logs or raised exceptions.
        return {
            "Authorization": "Bearer " + self._resolve_token(),
            "Accept": _ACCEPT,
            "X-GitHub-Api-Version": _API_VERSION,
        }

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = await self._client.request(method, path, headers=self._headers(), **kwargs)
        if response.status_code >= 400:
            # Deliberately omit response headers/body from the message: GitHub error bodies can
            # sometimes echo back request metadata, and we never want a token to leak into logs.
            raise GitHubClientError(f"GitHub API request {method} {path} failed with status {response.status_code}.")
        return response

    async def dispatch_workflow(self, *, workflow_file: str, ref: str, inputs: Mapping[str, str]) -> None:
        path = f"/repos/{self._owner}/{self._repo}/actions/workflows/{workflow_file}/dispatches"
        await self._request("POST", path, json={"ref": ref, "inputs": dict(inputs)})

    async def find_run_by_name(
        self, *, workflow_file: str, run_name: str, created_after: datetime | None = None
    ) -> GitHubRunRef | None:
        path = f"/repos/{self._owner}/{self._repo}/actions/workflows/{workflow_file}/runs"
        params: dict[str, str] = {"event": "workflow_dispatch", "per_page": "30"}
        if created_after is not None:
            params["created"] = f">={created_after.isoformat()}"
        response = await self._request("GET", path, params=params)
        for run in response.json().get("workflow_runs", []):
            if run.get("display_title") == run_name or run.get("name") == run_name:
                return _parse_run_ref(run)
        return None

    async def get_run(self, *, run_id: int) -> GitHubRunRef:
        path = f"/repos/{self._owner}/{self._repo}/actions/runs/{run_id}"
        response = await self._request("GET", path)
        return _parse_run_ref(response.json())

    async def list_jobs(self, *, run_id: int) -> list[GitHubJobRef]:
        path = f"/repos/{self._owner}/{self._repo}/actions/runs/{run_id}/jobs"
        response = await self._request("GET", path, params={"per_page": "100"})
        return [_parse_job_ref(job) for job in response.json().get("jobs", [])]

    async def cancel_run(self, *, run_id: int) -> None:
        path = f"/repos/{self._owner}/{self._repo}/actions/runs/{run_id}/cancel"
        await self._request("POST", path)

    async def rerun_failed_jobs(self, *, run_id: int) -> None:
        path = f"/repos/{self._owner}/{self._repo}/actions/runs/{run_id}/rerun-failed-jobs"
        await self._request("POST", path)

    async def aclose(self) -> None:
        """Close the underlying `httpx.AsyncClient`."""
        await self._client.aclose()


def _parse_run_ref(payload: Mapping[str, Any]) -> GitHubRunRef:
    return GitHubRunRef(
        run_id=payload.get("id"),
        html_url=payload.get("html_url"),
        status=payload.get("status"),
        conclusion=payload.get("conclusion"),
    )


def _parse_job_ref(payload: Mapping[str, Any]) -> GitHubJobRef:
    return GitHubJobRef(
        job_id=payload.get("id"),
        name=payload.get("name"),
        html_url=payload.get("html_url"),
        status=payload.get("status"),
        conclusion=payload.get("conclusion"),
    )
