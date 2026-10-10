# Copyright (c) Microsoft. All rights reserved.

"""Exception hierarchy for the hosted DAG control-plane."""

from __future__ import annotations

from agent_framework.exceptions import AgentFrameworkException

__all__ = [
    "ControlPlaneError",
    "DagCompilationError",
    "DagNotFoundError",
    "GitHubClientError",
    "InvalidRunStateError",
    "RunNotFoundError",
    "SubscriberLimitExceededError",
    "WebhookSignatureError",
]


class ControlPlaneError(AgentFrameworkException):
    """Base class for all hosted DAG control-plane errors."""


class DagCompilationError(ControlPlaneError):
    """Raised when a `Workflow` or declarative `DagDefinition` fails static-DAG validation."""


class DagNotFoundError(ControlPlaneError):
    """Raised when a requested DAG id/version is not present in the registry."""


class RunNotFoundError(ControlPlaneError):
    """Raised when a requested run id is not present in the run store."""


class InvalidRunStateError(ControlPlaneError):
    """Raised when an operation is not valid for a run's current state.

    For example: retrying a run that has not failed, or resuming a run with
    no recorded checkpoint reference.
    """


class WebhookSignatureError(ControlPlaneError):
    """Raised when an inbound GitHub webhook fails HMAC signature validation."""


class SubscriberLimitExceededError(ControlPlaneError):
    """Raised when an SSE subscription would exceed the per-run subscriber cap.

    This bounds the server's memory/connection footprint for a single run;
    callers should surface this as a retryable error (for example HTTP 429).
    """


class GitHubClientError(ControlPlaneError):
    """Raised when the GitHub Actions REST client receives an unexpected response.

    The exception message intentionally never includes request headers so
    that an `Authorization` token cannot leak into logs or error payloads.
    """
