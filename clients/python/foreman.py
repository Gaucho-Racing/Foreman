"""foreman.py — single-file Python client for the Foreman REST API.

Drop this file into your project. Only dependency is `requests`.

Quick start
-----------

    import foreman

    c = foreman.Client("http://foreman:7011")

    # Producer
    res = c.enqueue(kind="send-email",
                    params={"to": "x@y.com"},
                    idempotency_key="email-42",
                    max_attempts=3)
    print(res.job.id, "created" if res.created else "already existed")

    # Worker
    claimed = c.claim(kinds=["send-email"], worker_id="worker-1", lease_sec=60)
    if claimed is None:
        print("queue empty")
    else:
        job, run = claimed
        # ... do the work, calling c.heartbeat(run.id, worker_id="worker-1") periodically
        c.complete(run.id, worker_id="worker-1", result={"sent": True})

Errors
------

Non-2xx responses raise foreman.HTTPError; the server's ``{"error":
"..."}`` field shows up in ``HTTPError.message``. A 409 on
``enqueue`` is the one exception: it returns ``EnqueueResult(created=
False, job=existing)`` so idempotent re-sends are not exceptional.

No-op mode
----------

Constructing ``Client(endpoint="")`` makes every method a no-op
returning sensible empty values. Handy for opt-in deployments where
Foreman is configured per-environment.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Union

import requests

__all__ = [
    "Client",
    "HTTPError",
    "EnqueueResult",
    "Job",
    "Run",
    "Schedule",
]


# ---------- Types ----------


@dataclass
class Job:
    id: str = ""
    kind: str = ""
    queue: str = ""
    service: str = ""
    idempotency_key: Optional[str] = None
    params: Optional[Any] = None
    priority: int = 0
    max_attempts: int = 1
    scheduled_at: Optional[str] = None
    status: str = ""
    cancel_requested: bool = False
    attempt_count: int = 0
    result: Optional[Any] = None
    enqueued_at: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    updated_at: str = ""
    # Present only when get/list was called with include_current_run=True.
    current_run: Optional["Run"] = None


@dataclass
class Run:
    id: str = ""
    job_id: str = ""
    attempt: int = 0
    worker_id: str = ""
    status: str = ""  # running | succeeded | failed | abandoned
    lease_expires_at: Optional[str] = None
    progress_current: int = 0
    progress_total: int = 0
    progress_message: str = ""
    result: Optional[Any] = None
    error: str = ""
    started_at: str = ""
    finished_at: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""


@dataclass
class Schedule:
    id: str = ""
    kind: str = ""
    queue: str = ""
    service: str = ""
    params: Optional[Any] = None
    priority: int = 0
    max_attempts: int = 1
    cron_expr: str = ""
    timezone: str = "UTC"
    enabled: bool = True
    next_fire_at: str = ""
    last_fire_at: Optional[str] = None
    last_job_id: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass
class EnqueueResult:
    """Result of ``Client.enqueue``.

    ``created`` is True on a 201 (fresh insert), False on a 409
    idempotency-key collision (``job`` then points at the existing row).
    """

    created: bool
    job: Job


# ---------- Errors ----------


class HTTPError(Exception):
    """Raised on any non-2xx response. ``message`` is the server's
    ``{"error": "..."}`` field when present, otherwise empty."""

    def __init__(self, op: str, status_code: int, message: str = ""):
        self.op = op
        self.status_code = status_code
        self.message = message
        super().__init__(f"foreman {op}: {status_code}" + (f" {message}" if message else ""))


# ---------- Client ----------


class Client:
    """Talks to a Foreman server. Construct with an endpoint URL.

    An empty endpoint disables every method (no-op success returning
    sensible empty values) — useful for opt-in deployments where
    Foreman is configured per environment.
    """

    def __init__(self, endpoint: str, *, timeout: float = 30.0,
                 session: Optional[requests.Session] = None):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout
        self._session = session or requests.Session()

    # -- producer -------------------------------------------------------

    def enqueue(
        self,
        *,
        kind: str,
        queue: str = "",
        service: str = "",
        idempotency_key: Optional[str] = None,
        params: Optional[Any] = None,
        priority: int = 0,
        max_attempts: int = 0,
        scheduled_at: Optional[Union[str, datetime]] = None,
    ) -> EnqueueResult:
        """Create a Job.

        Returns ``EnqueueResult(created=True, job=...)`` on success.
        On a 409 idempotency conflict, returns ``EnqueueResult(created=
        False, job=existing)`` — same shape, not an exception.
        """
        if not self.endpoint:
            return EnqueueResult(created=False, job=Job())
        body = _clean({
            "kind": kind,
            "queue": queue,
            "service": service,
            "idempotency_key": idempotency_key,
            "params": params,
            "priority": priority,
            "max_attempts": max_attempts,
            "scheduled_at": _iso(scheduled_at),
        })
        resp = self._request("POST", "/foreman/jobs", json=body, op="enqueue",
                             allowed=(201, 409))
        if resp.status_code == 201:
            return EnqueueResult(created=True, job=_to(Job, resp.json()))
        # 409: body is {"error": "...", "job": {...}}
        body = resp.json() or {}
        return EnqueueResult(created=False, job=_to(Job, body.get("job", {})))

    # -- worker ---------------------------------------------------------

    def claim(
        self,
        *,
        kinds: list[str],
        worker_id: str,
        queues: Optional[list[str]] = None,
        lease_sec: int = 0,
    ) -> Optional[tuple[Job, Run]]:
        """Lease one job matching any of ``kinds``. Returns ``None`` if
        the queue is empty (server 204) — the "nothing to do" signal."""
        if not self.endpoint:
            return None
        body = _clean({
            "kinds": kinds,
            "queues": queues,
            "worker_id": worker_id,
            "lease_seconds": lease_sec,
        })
        resp = self._request("POST", "/foreman/jobs/claim", json=body, op="claim",
                             allowed=(200, 204))
        if resp.status_code == 204:
            return None
        body = resp.json()
        return _to(Job, body["job"]), _to(Run, body["run"])

    def heartbeat(
        self,
        run_id: str,
        *,
        worker_id: str,
        progress_current: Optional[int] = None,
        progress_total: Optional[int] = None,
        progress_message: Optional[str] = None,
        lease_sec: int = 0,
    ) -> Run:
        """Extend the lease on the calling worker's in-flight run and
        optionally report progress. Returns the updated Run."""
        if not self.endpoint:
            return Run()
        body = _clean({
            "worker_id": worker_id,
            "progress_current": progress_current,
            "progress_total": progress_total,
            "progress_message": progress_message,
            "lease_seconds": lease_sec,
        })
        return _to(Run, self._json("POST", f"/foreman/runs/{run_id}/heartbeat",
                                   body=body, op="heartbeat"))

    def complete(self, run_id: str, *, worker_id: str,
                 result: Optional[Any] = None) -> Job:
        """Mark the run succeeded and terminalize the parent job."""
        if not self.endpoint:
            return Job()
        body = _clean({"worker_id": worker_id, "result": result})
        return _to(Job, self._json("POST", f"/foreman/runs/{run_id}/complete",
                                   body=body, op="complete"))

    def fail(
        self,
        run_id: str,
        *,
        worker_id: str,
        error: str = "",
        retryable: bool = False,
        backoff_sec: int = 0,
        result: Optional[Any] = None,
    ) -> Job:
        """Record a failed attempt. If ``retryable`` and attempts
        remain, the parent job goes back to pending with backoff;
        otherwise it terminalizes. ``result`` is preserved on the
        JobRun (not Job.result) for partial-data reporting."""
        if not self.endpoint:
            return Job()
        body = _clean({
            "worker_id": worker_id,
            "error": error,
            "retryable": retryable,
            "backoff_seconds": backoff_sec,
            "result": result,
        })
        return _to(Job, self._json("POST", f"/foreman/runs/{run_id}/fail",
                                   body=body, op="fail"))

    def cancel(self, job_id: str) -> Job:
        """Cancel a pending job immediately, or set ``cancel_requested``
        on a running one. Terminal jobs return unchanged."""
        if not self.endpoint:
            return Job()
        return _to(Job, self._json("POST", f"/foreman/jobs/{job_id}/cancel",
                                   body=None, op="cancel"))

    # -- reads ----------------------------------------------------------

    def get_job(self, job_id: str, *, include_current_run: bool = False) -> Job:
        if not self.endpoint:
            return Job()
        params = {"include": "current_run"} if include_current_run else None
        return _to(Job, self._json("GET", f"/foreman/jobs/{job_id}",
                                   params=params, op="get-job"))

    def list_jobs(
        self,
        *,
        status: str = "",
        kind: str = "",
        service: str = "",
        queue: str = "",
        limit: int = 0,
        cursor: str = "",
        include_current_run: bool = False,
    ) -> list[Job]:
        if not self.endpoint:
            return []
        params = _clean({
            "status": status,
            "kind": kind,
            "service": service,
            "queue": queue,
            "limit": limit or None,
            "cursor": cursor,
            "include": "current_run" if include_current_run else None,
        })
        body = self._json("GET", "/foreman/jobs", params=params, op="list-jobs")
        return [_to(Job, j) for j in (body or [])]

    def get_run(self, run_id: str) -> Run:
        if not self.endpoint:
            return Run()
        return _to(Run, self._json("GET", f"/foreman/runs/{run_id}", op="get-run"))

    def list_job_runs(self, job_id: str) -> list[Run]:
        """Every attempt at a job, oldest first."""
        if not self.endpoint:
            return []
        body = self._json("GET", f"/foreman/jobs/{job_id}/runs", op="list-job-runs")
        return [_to(Run, r) for r in (body or [])]

    def list_runs(
        self,
        *,
        status: str = "",
        job_id: str = "",
        worker_id: str = "",
        kind: str = "",
        limit: int = 0,
        cursor: str = "",
    ) -> list[Run]:
        """Global view across all jobs."""
        if not self.endpoint:
            return []
        params = _clean({
            "status": status,
            "job_id": job_id,
            "worker_id": worker_id,
            "kind": kind,
            "limit": limit or None,
            "cursor": cursor,
        })
        body = self._json("GET", "/foreman/runs", params=params, op="list-runs")
        return [_to(Run, r) for r in (body or [])]

    # -- schedules ------------------------------------------------------

    def create_schedule(
        self,
        *,
        kind: str,
        cron_expr: str,
        queue: str = "",
        service: str = "",
        params: Optional[Any] = None,
        priority: int = 0,
        max_attempts: int = 0,
        timezone: str = "",
        enabled: Optional[bool] = None,
    ) -> Schedule:
        if not self.endpoint:
            return Schedule()
        body = _clean({
            "kind": kind,
            "queue": queue,
            "service": service,
            "params": params,
            "priority": priority,
            "max_attempts": max_attempts,
            "cron_expr": cron_expr,
            "timezone": timezone,
            "enabled": enabled,
        })
        return _to(Schedule, self._json("POST", "/foreman/schedules",
                                        body=body, op="create-schedule",
                                        success=201))

    def get_schedule(self, schedule_id: str) -> Schedule:
        if not self.endpoint:
            return Schedule()
        return _to(Schedule, self._json("GET", f"/foreman/schedules/{schedule_id}",
                                        op="get-schedule"))

    def list_schedules(
        self,
        *,
        kind: str = "",
        enabled: Optional[bool] = None,
        limit: int = 0,
        cursor: str = "",
    ) -> list[Schedule]:
        if not self.endpoint:
            return []
        params: dict[str, Any] = _clean({
            "kind": kind,
            "limit": limit or None,
            "cursor": cursor,
        })
        if enabled is not None:
            params["enabled"] = "true" if enabled else "false"
        body = self._json("GET", "/foreman/schedules", params=params, op="list-schedules")
        return [_to(Schedule, s) for s in (body or [])]

    def update_schedule(
        self,
        schedule_id: str,
        *,
        kind: str,
        cron_expr: str,
        queue: str = "",
        service: str = "",
        params: Optional[Any] = None,
        priority: int = 0,
        max_attempts: int = 0,
        timezone: str = "",
        enabled: Optional[bool] = None,
    ) -> Schedule:
        if not self.endpoint:
            return Schedule()
        body = _clean({
            "kind": kind,
            "queue": queue,
            "service": service,
            "params": params,
            "priority": priority,
            "max_attempts": max_attempts,
            "cron_expr": cron_expr,
            "timezone": timezone,
            "enabled": enabled,
        })
        return _to(Schedule, self._json("PUT", f"/foreman/schedules/{schedule_id}",
                                        body=body, op="update-schedule"))

    def delete_schedule(self, schedule_id: str) -> None:
        if not self.endpoint:
            return None
        self._request("DELETE", f"/foreman/schedules/{schedule_id}",
                      op="delete-schedule", allowed=(200,))
        return None

    def fire_schedule(self, schedule_id: str) -> Job:
        """Manual fire — enqueues the schedule's recipe without
        touching ``next_fire_at``."""
        if not self.endpoint:
            return Job()
        return _to(Job, self._json("POST", f"/foreman/schedules/{schedule_id}/fire",
                                   body=None, op="fire-schedule"))

    # -- internals ------------------------------------------------------

    def _json(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Any] = None,
        params: Optional[dict] = None,
        op: str,
        success: int = 200,
    ) -> Any:
        """Standard "send body, decode 2xx JSON, raise HTTPError on
        non-2xx" path. Used everywhere except enqueue + claim, which
        have multiple acceptable status codes."""
        resp = self._request(method, path, json=body, params=params,
                             op=op, allowed=(success,))
        return resp.json() if resp.content else None

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Any] = None,
        params: Optional[dict] = None,
        op: str,
        allowed: tuple[int, ...],
    ) -> requests.Response:
        url = self.endpoint + path
        resp = self._session.request(method, url, json=json, params=params,
                                     timeout=self.timeout)
        if resp.status_code not in allowed:
            raise HTTPError(op, resp.status_code, _extract_error(resp))
        return resp


# ---------- helpers ----------


def _clean(d: dict) -> dict:
    """Drop entries whose value is None or "" — keeps request bodies
    minimal and matches the server's "omitempty" expectations."""
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, (str, list, dict)) and len(v) == 0:
            continue
        if isinstance(v, int) and not isinstance(v, bool) and v == 0:
            continue
        out[k] = v
    return out


def _iso(t: Optional[Union[str, datetime]]) -> Optional[str]:
    if t is None:
        return None
    if isinstance(t, datetime):
        return t.isoformat().replace("+00:00", "Z")
    return t


def _to(cls, data: Any):
    """Decode a JSON dict into a dataclass instance, ignoring unknown
    fields so server additions don't break old clients."""
    if data is None:
        return cls()
    known = {f.name for f in dataclasses.fields(cls)}
    kwargs = {k: v for k, v in data.items() if k in known}
    obj = cls(**kwargs)
    # Recurse into nested current_run on Job.
    if cls is Job and isinstance(data.get("current_run"), dict):
        obj.current_run = _to(Run, data["current_run"])
    return obj


def _extract_error(resp: requests.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict) and isinstance(body.get("error"), str):
            return body["error"]
    except Exception:
        pass
    return ""
