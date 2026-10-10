# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from agent_framework_hosting_controlplane import GitHubClientError, HttpxGitHubActionsClient
from agent_framework_hosting_controlplane._webhooks import verify_github_signature

_FAKE_TOKEN = "fixture-auth-token-value"  # noqa: S105 - test fixture only, not a real credential


def _make_client(handler: httpx.MockTransport, *, token: str = _FAKE_TOKEN) -> HttpxGitHubActionsClient:
    transport_client = httpx.AsyncClient(transport=handler, base_url="https://api.github.com")
    return HttpxGitHubActionsClient(owner="my-org", repo="my-dags", token=token, client=transport_client)


async def test_dispatch_workflow_posts_inputs_and_ref() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        captured["auth"] = request.headers["Authorization"]
        return httpx.Response(204)

    client = _make_client(httpx.MockTransport(handler))
    await client.dispatch_workflow(workflow_file="dag.yml", ref="main", inputs={"framework_run_id": "run-1"})

    assert captured["method"] == "POST"
    assert captured["path"] == "/repos/my-org/my-dags/actions/workflows/dag.yml/dispatches"
    assert captured["body"] == {"ref": "main", "inputs": {"framework_run_id": "run-1"}}
    assert captured["auth"] == "Bearer " + _FAKE_TOKEN


async def test_dispatch_workflow_token_never_appears_in_error_message() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "Bad credentials"})

    client = _make_client(httpx.MockTransport(handler))
    with pytest.raises(GitHubClientError) as exc_info:
        await client.dispatch_workflow(workflow_file="dag.yml", ref="main", inputs={})

    assert _FAKE_TOKEN not in str(exc_info.value)
    assert "401" in str(exc_info.value)


async def test_dispatch_workflow_supports_callable_token() -> None:
    calls: list[str] = []
    expected_token = "rotating-token-value"

    def token_provider() -> str:
        calls.append("called")
        return expected_token

    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["Authorization"]
        return httpx.Response(204)

    transport_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://api.github.com")
    client = HttpxGitHubActionsClient(owner="o", repo="r", token=token_provider, client=transport_client)
    await client.dispatch_workflow(workflow_file="dag.yml", ref="main", inputs={})

    assert captured["auth"] == "Bearer " + expected_token
    assert calls == ["called"]


async def test_find_run_by_name_matches_display_title() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "workflow_runs": [
                    {
                        "id": 1,
                        "display_title": "foundry-dag/other/run-x",
                        "status": "completed",
                        "conclusion": "success",
                    },
                    {
                        "id": 2,
                        "display_title": "foundry-dag/report/run-1",
                        "status": "in_progress",
                        "conclusion": None,
                    },
                ]
            },
        )

    client = _make_client(httpx.MockTransport(handler))
    found = await client.find_run_by_name(workflow_file="dag.yml", run_name="foundry-dag/report/run-1")

    assert found is not None
    assert found.run_id == 2
    assert found.status == "in_progress"


async def test_find_run_by_name_returns_none_when_absent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"workflow_runs": []})

    client = _make_client(httpx.MockTransport(handler))
    assert await client.find_run_by_name(workflow_file="dag.yml", run_name="foundry-dag/report/run-1") is None


async def test_get_run_parses_status_and_conclusion() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": 99, "status": "completed", "conclusion": "failure", "html_url": "u"})

    client = _make_client(httpx.MockTransport(handler))
    run_ref = await client.get_run(run_id=99)
    assert run_ref.run_id == 99
    assert run_ref.conclusion == "failure"


async def test_list_jobs_returns_job_refs_keyed_by_name() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"jobs": [{"id": 1, "name": "extract", "status": "completed", "conclusion": "success"}]},
        )

    client = _make_client(httpx.MockTransport(handler))
    jobs = await client.list_jobs(run_id=99)
    assert jobs[0].name == "extract"
    assert jobs[0].conclusion == "success"


async def test_cancel_run_posts_to_cancel_endpoint() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(202)

    client = _make_client(httpx.MockTransport(handler))
    await client.cancel_run(run_id=7)
    assert captured["path"] == "/repos/my-org/my-dags/actions/runs/7/cancel"


async def test_rerun_failed_jobs_posts_to_rerun_endpoint() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(201)

    client = _make_client(httpx.MockTransport(handler))
    await client.rerun_failed_jobs(run_id=7)
    assert captured["path"] == "/repos/my-org/my-dags/actions/runs/7/rerun-failed-jobs"


# --------------------------------------------------------------------------- #
# Webhook signature validation
# --------------------------------------------------------------------------- #


def test_verify_github_signature_accepts_valid_signature() -> None:
    secret = "topsecret"
    payload = b'{"hello": "world"}'
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    header = f"sha256={digest}"
    assert verify_github_signature(secret=secret, payload=payload, signature_header=header) is True


def test_verify_github_signature_rejects_wrong_secret() -> None:
    payload = b'{"hello": "world"}'
    digest = hmac.new(b"wrong-secret", payload, hashlib.sha256).hexdigest()
    header = f"sha256={digest}"
    assert verify_github_signature(secret="topsecret", payload=payload, signature_header=header) is False


def test_verify_github_signature_rejects_tampered_payload() -> None:
    secret = "topsecret"
    digest = hmac.new(secret.encode(), b'{"hello": "world"}', hashlib.sha256).hexdigest()
    header = f"sha256={digest}"
    assert verify_github_signature(secret=secret, payload=b'{"hello": "mallory"}', signature_header=header) is False


def test_verify_github_signature_rejects_missing_or_malformed_header() -> None:
    assert verify_github_signature(secret="s", payload=b"x", signature_header=None) is False
    assert verify_github_signature(secret="s", payload=b"x", signature_header="") is False
    assert verify_github_signature(secret="s", payload=b"x", signature_header="not-sha256=abc") is False
