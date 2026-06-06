"""Foreman Worker — the canonical claim → handle → heartbeat → terminate
loop packaged as a single class. Drop this file in alongside
``foreman.py`` and write only the handler bodies.

Quick start
-----------

    from foreman import Client
    from worker import Worker, PermanentError

    c = Client("http://foreman.local:7011")
    w = Worker(c, worker_id="emailer-1", lease_sec=60)

    @w.handle("send-email")
    def send_email(job, progress, cancel):
        progress.set(0, 1, "sending")
        if cancel.is_set():
            return None
        # ...do work...
        return {"sent": True}

    w.run()  # blocks until SIGINT or stop_event

What the Worker does for you, automatically
-------------------------------------------

* Atomic claim across all registered kinds, with empty-queue backoff.
* Background heartbeat thread pinned to ``lease_sec / 3`` by default.
* Progress updates (``progress.set(...)``) piggyback on the next
  heartbeat — no extra HTTP request per progress tick.
* Cooperative cancel: when the server flags ``cancel_requested`` on the
  parent job, the next heartbeat surfaces it and the Worker sets
  ``cancel`` (a :class:`threading.Event`). Handlers should check
  ``cancel.is_set()`` periodically and exit promptly.
* Handler exceptions become :meth:`Client.fail` calls. Plain exceptions
  are retryable with the worker's default backoff; raise
  :class:`PermanentError` (or its alias ``ErrPermanent``) to fail
  non-retryable, or :class:`FailError` for custom backoff/retryability.
* SIGINT / SIGTERM stop the loop cleanly — the in-flight handler is
  cancelled and its run terminalizes before ``run()`` returns.

One Worker handles one job at a time. For parallel execution, spin up
multiple Workers (each in its own thread or process) — the server
coordinates via ``SELECT ... FOR UPDATE SKIP LOCKED`` so they never
grab the same job.
"""

from __future__ import annotations

import signal
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from foreman import Client, HTTPError, Job


__all__ = [
    "Worker",
    "Progress",
    "FailError",
    "PermanentError",
    "ErrPermanent",
    "Handler",
]


# ---------- Errors ----------


class PermanentError(Exception):
    """Raise from a handler to fail the run non-retryably."""


# Compatibility alias: matches the Go client's ErrPermanent name.
ErrPermanent = PermanentError


@dataclass
class FailError(Exception):
    """Raise from a handler for precise control over the Fail call.

    ``message`` becomes the run's ``error`` field. ``retryable=False``
    terminalizes immediately; ``True`` bounces back to pending with
    ``backoff_sec`` (0 → the Worker's ``default_backoff_sec``).
    """
    message: str = ""
    retryable: bool = True
    backoff_sec: int = 0

    def __str__(self) -> str:  # noqa: D401
        return self.message


# ---------- Progress ----------


class Progress:
    """Thread-safe progress reporter. ``set()`` updates the latest
    values; the heartbeat thread drains them and folds them into the
    next heartbeat.

    Calling ``set()`` repeatedly between heartbeats is cheap — only the
    most recent value goes over the wire.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current = 0
        self._total = 0
        self._message = ""
        self._dirty = False

    def set(self, current: int = 0, total: int = 0, message: str = "") -> None:
        with self._lock:
            self._current = int(current)
            self._total = int(total)
            self._message = message
            self._dirty = True

    def _drain(self):
        with self._lock:
            if not self._dirty:
                return None
            snap = (self._current, self._total, self._message)
            self._dirty = False
            return snap


# ---------- Worker ----------


Handler = Callable[[Job, Progress, threading.Event], Optional[Any]]


class Worker:
    """Foreman worker loop.

    Parameters
    ----------
    client:
        A configured :class:`foreman.Client`.
    worker_id:
        Stable identifier the server records against claims and runs.
        Use something machine-unique (hostname+pid, k8s pod name, etc.)
        so abandoned runs are attributable.
    kinds:
        Restrict what this worker will claim. Defaults to the set of
        kinds registered via :meth:`handle`.
    queues:
        Restrict to specific named queues; empty matches any.
    lease_sec:
        How long the server holds the lease after each claim or
        heartbeat. Default 60. The heartbeat cadence is
        ``lease_sec / 3`` unless ``heartbeat_interval`` is set.
    heartbeat_interval:
        Override the default ``lease_sec / 3`` cadence (seconds, float).
    poll_interval:
        Empty-queue backoff in seconds. Default 5.
    default_backoff_sec:
        Retry delay applied when a Handler raises a plain Exception
        (no :class:`FailError` wrapping). Default 30.
    on_error:
        Optional callback invoked for background-loop errors (claim
        failures, heartbeat failures, terminal-call failures). Useful
        for plugging in logging or metrics. ``None`` swallows silently.
    """

    def __init__(
        self,
        client: Client,
        *,
        worker_id: str,
        kinds: Optional[List[str]] = None,
        queues: Optional[List[str]] = None,
        lease_sec: int = 60,
        heartbeat_interval: Optional[float] = None,
        poll_interval: float = 5.0,
        default_backoff_sec: int = 30,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        if not client:
            raise ValueError("foreman.Worker: client is required")
        if not worker_id:
            raise ValueError("foreman.Worker: worker_id is required")

        self.client = client
        self.worker_id = worker_id
        self.kinds = list(kinds) if kinds else []
        self.queues = list(queues) if queues else []
        self.lease_sec = int(lease_sec)
        self.heartbeat_interval = (
            float(heartbeat_interval)
            if heartbeat_interval is not None
            else self.lease_sec / 3.0
        )
        self.poll_interval = float(poll_interval)
        self.default_backoff_sec = int(default_backoff_sec)
        self.on_error = on_error or (lambda _: None)

        self._handlers: Dict[str, Handler] = {}
        self._stop = threading.Event()

    # ---------- Registration ----------

    def handle(self, kind: str) -> Callable[[Handler], Handler]:
        """Decorator: register a handler for jobs of the given kind.

            @w.handle("send-email")
            def send_email(job, progress, cancel):
                ...
        """
        def deco(fn: Handler) -> Handler:
            self._handlers[kind] = fn
            return fn
        return deco

    def register(self, kind: str, fn: Handler) -> None:
        """Imperative form of :meth:`handle`."""
        self._handlers[kind] = fn

    # ---------- Loop control ----------

    def stop(self) -> None:
        """Signal the loop to exit cleanly. Any in-flight handler is
        cancelled (its ``cancel`` event is set) and gets a chance to
        terminalize before :meth:`run` returns.
        """
        self._stop.set()

    def run(self, *, install_signal_handlers: bool = True) -> None:
        """Block, claiming and executing one job at a time, until
        :meth:`stop` is called or SIGINT/SIGTERM is received.

        Set ``install_signal_handlers=False`` if you embed the Worker
        inside a larger app that owns signal handling.
        """
        if not self._handlers:
            raise RuntimeError(
                "foreman.Worker: no handlers registered (call Worker.handle)"
            )

        kinds = self.kinds or list(self._handlers.keys())

        if install_signal_handlers:
            self._install_signals()

        while not self._stop.is_set():
            try:
                claimed = self.client.claim(
                    kinds=kinds,
                    queues=self.queues or None,
                    worker_id=self.worker_id,
                    lease_sec=self.lease_sec,
                )
            except Exception as exc:  # noqa: BLE001
                self.on_error(exc)
                self._stop.wait(self.poll_interval)
                continue

            if claimed is None:
                self._stop.wait(self.poll_interval)
                continue

            job, run = claimed
            self._handle_one(job, run)

    # ---------- Per-job execution ----------

    def _handle_one(self, job: Job, run) -> None:
        fn = self._handlers.get(job.kind)
        if fn is None:
            # We claimed it but have no handler. Fail non-retryable so
            # the queue doesn't loop the job back to us.
            self._safe_fail(
                run.id,
                message=f"no handler registered for kind {job.kind}",
                retryable=False,
                backoff_sec=0,
            )
            return

        cancel = threading.Event()
        progress = Progress()
        hb_stop = threading.Event()
        hb_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(run.id, progress, cancel, hb_stop),
            name=f"foreman-hb-{run.id[:12]}",
            daemon=True,
        )
        hb_thread.start()

        result: Optional[Any] = None
        err: Optional[Exception] = None
        try:
            result = fn(job, progress, cancel)
        except Exception as exc:  # noqa: BLE001
            err = exc

        # Stop heartbeats and wait for the thread to drain before
        # terminalizing — keeps logs/metrics tidy.
        hb_stop.set()
        hb_thread.join(timeout=self.heartbeat_interval + 5.0)

        # Cancel observed → Fail regardless of how the handler exited.
        # The server's Fail honors cancel_requested and terminalizes the
        # job as 'cancelled', so we don't need a special status from the
        # worker. This sidesteps the ambiguity of "handler returned None"
        # — was that a successful empty result, or did it bail on cancel?
        # We don't have to know.
        if cancel.is_set():
            self._safe_fail(run.id, message="cancelled by request",
                            retryable=False, backoff_sec=0)
            return

        if err is None:
            try:
                self.client.complete(run.id, worker_id=self.worker_id, result=result)
            except Exception as exc:  # noqa: BLE001
                self.on_error(exc)
            return

        # Map exception → Fail semantics.
        retryable, backoff_sec, message = self._classify_error(err)
        self._safe_fail(run.id, message=message, retryable=retryable,
                        backoff_sec=backoff_sec)

    def _classify_error(self, err: Exception):
        if isinstance(err, FailError):
            return (
                err.retryable,
                err.backoff_sec or self.default_backoff_sec,
                err.message or err.__class__.__name__,
            )
        if isinstance(err, PermanentError):
            return False, 0, str(err) or "PermanentError"
        # Fallback: include the exception type so logs are useful.
        message = f"{err.__class__.__name__}: {err}" if str(err) else err.__class__.__name__
        return True, self.default_backoff_sec, message

    def _safe_fail(self, run_id: str, *, message: str, retryable: bool,
                   backoff_sec: int) -> None:
        try:
            self.client.fail(
                run_id,
                worker_id=self.worker_id,
                error=message,
                retryable=retryable,
                backoff_sec=backoff_sec,
            )
        except Exception as exc:  # noqa: BLE001
            self.on_error(exc)

    # ---------- Heartbeat loop ----------

    def _heartbeat_loop(self, run_id: str, progress: Progress,
                        cancel: threading.Event, stop: threading.Event) -> None:
        while not stop.is_set():
            # Wait until the interval elapses OR stop is signalled.
            if stop.wait(self.heartbeat_interval):
                return

            kwargs: Dict[str, Any] = {
                "worker_id": self.worker_id,
                "lease_sec": self.lease_sec,
            }
            snap = progress._drain()
            if snap is not None:
                cur, tot, msg = snap
                kwargs["progress_current"] = cur
                kwargs["progress_total"] = tot
                if msg:
                    kwargs["progress_message"] = msg

            try:
                res = self.client.heartbeat(run_id, **kwargs)
            except HTTPError as exc:
                # Lease lost / job already terminalized / network blip.
                # The worker can't trust its lease anymore — cancel the
                # handler and bail.
                self.on_error(exc)
                cancel.set()
                return
            except Exception as exc:  # noqa: BLE001
                self.on_error(exc)
                cancel.set()
                return

            if res.cancel_requested:
                cancel.set()
                return

    # ---------- Signals ----------

    def _install_signals(self) -> None:
        def _handler(_signum, _frame):
            self._stop.set()
        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except ValueError:
            # Not on the main thread — caller handles signals itself.
            pass


# ---------- Internal: tiny helpers exported for tests ----------


def _format_traceback(err: Exception) -> str:
    """Return a short traceback suitable for the Foreman error field.
    Not used by Worker today; exposed for callers that want to log it
    alongside their own observability.
    """
    return "".join(traceback.format_exception(type(err), err, err.__traceback__))


# Re-export for convenience so callers can `from worker import Job`
# without also importing from foreman. Useful when worker.py is the
# only file they care about.
__all__.append("Job")
