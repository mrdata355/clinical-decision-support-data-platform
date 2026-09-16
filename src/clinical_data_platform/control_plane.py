"""Production control-plane primitives for pipeline reliability.

The data plane moves records; this module governs *whether* and *how aggressively* a pipeline
may move them. It keeps operational policy separate from transformation code and supplies
thread-safe primitives for idempotency, circuit breaking, adaptive backpressure, explicit run
state transitions, and SLO burn-rate evaluation.

The classes are intentionally infrastructure-neutral. They can be driven by Airflow, a Vercel
operations surface, a CLI, or a long-running worker without embedding provider-specific APIs in
core reliability logic.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import threading
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

UTC = dt.UTC


class RunState(StrEnum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ControlAction(StrEnum):
    START = "start"
    STOP = "stop"
    DRAIN = "drain"
    DEGRADE = "degrade"
    RECOVER = "recover"
    FAIL = "fail"
    RESET = "reset"


class InvalidTransitionError(RuntimeError):
    """Raised when an operator command violates the run state machine."""


class CircuitOpenError(RuntimeError):
    """Raised when a protected dependency is not permitted to receive traffic."""


@dataclasses.dataclass(frozen=True, slots=True)
class SLOPolicy:
    availability_target: float = 0.999
    latency_target_ms: int = 1_500
    max_error_ratio: float = 0.02
    fast_burn_threshold: float = 14.4
    slow_burn_threshold: float = 6.0

    def __post_init__(self) -> None:
        if not 0.0 < self.availability_target < 1.0:
            raise ValueError("availability_target must be between 0 and 1")
        if self.latency_target_ms <= 0:
            raise ValueError("latency_target_ms must be positive")
        if not 0.0 <= self.max_error_ratio < 1.0:
            raise ValueError("max_error_ratio must be between 0 and 1")

    @property
    def error_budget_ratio(self) -> float:
        return 1.0 - self.availability_target


@dataclasses.dataclass(frozen=True, slots=True)
class BackpressurePolicy:
    min_concurrency: int = 1
    max_concurrency: int = 64
    target_lag_ms: int = 750
    queue_high_watermark: int = 25_000
    error_ratio_high_watermark: float = 0.03
    increase_step: int = 2
    decrease_factor: float = 0.55

    def __post_init__(self) -> None:
        if self.min_concurrency < 1:
            raise ValueError("min_concurrency must be >= 1")
        if self.max_concurrency < self.min_concurrency:
            raise ValueError("max_concurrency must be >= min_concurrency")
        if not 0 < self.decrease_factor <= 1:
            raise ValueError("decrease_factor must be in (0, 1]")


@dataclasses.dataclass(frozen=True, slots=True)
class PipelineSignal:
    observed_at: dt.datetime
    queue_depth: int
    p95_lag_ms: int
    error_ratio: float
    throughput_eps: float

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.queue_depth < 0 or self.p95_lag_ms < 0 or self.throughput_eps < 0:
            raise ValueError("queue_depth, lag, and throughput must be non-negative")
        if not 0.0 <= self.error_ratio <= 1.0:
            raise ValueError("error_ratio must be between 0 and 1")


@dataclasses.dataclass(frozen=True, slots=True)
class BackpressureDecision:
    previous_concurrency: int
    next_concurrency: int
    mode: str
    reasons: tuple[str, ...]
    observed_at: dt.datetime


@dataclasses.dataclass(frozen=True, slots=True)
class BurnRateResult:
    window_name: str
    requests: int
    failures: int
    observed_error_ratio: float
    burn_rate: float
    severity: str


@dataclasses.dataclass(frozen=True, slots=True)
class ControlEvent:
    event_id: str
    run_id: str
    action: ControlAction
    previous_state: RunState
    next_state: RunState
    actor: str
    occurred_at: dt.datetime
    idempotency_key: str
    metadata: Mapping[str, Any]

    def canonical_json(self) -> str:
        payload = {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "action": self.action,
            "previous_state": self.previous_state,
            "next_state": self.next_state,
            "actor": self.actor,
            "occurred_at": self.occurred_at.isoformat(),
            "idempotency_key": self.idempotency_key,
            "metadata": dict(sorted(self.metadata.items())),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class IdempotencyLedger:
    """Bounded TTL ledger for operator and workflow commands.

    This prevents duplicate control mutations when an orchestrator retries a request after a
    network timeout. Entries are kept in insertion order so expiration and capacity eviction are
    deterministic and O(k) only for stale prefixes.
    """

    def __init__(self, *, ttl_seconds: float = 3_600.0, max_entries: int = 20_000) -> None:
        if ttl_seconds <= 0 or max_entries <= 0:
            raise ValueError("ttl_seconds and max_entries must be positive")
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self._lock = threading.RLock()

    def _evict(self, now: float) -> None:
        while self._entries:
            key, (created_at, _) = next(iter(self._entries.items()))
            if now - created_at <= self._ttl_seconds and len(self._entries) <= self._max_entries:
                break
            self._entries.pop(key, None)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def remember(self, key: str, result_digest: str) -> bool:
        if not key.strip():
            raise ValueError("idempotency key cannot be blank")
        now = time.monotonic()
        with self._lock:
            self._evict(now)
            if key in self._entries:
                return False
            self._entries[key] = (now, result_digest)
            self._evict(now)
            return True

    def get(self, key: str) -> str | None:
        now = time.monotonic()
        with self._lock:
            self._evict(now)
            value = self._entries.get(key)
            return value[1] if value else None

    def __len__(self) -> int:
        with self._lock:
            self._evict(time.monotonic())
            return len(self._entries)


class CircuitBreaker:
    """Thread-safe count-based circuit breaker with half-open recovery probes."""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        half_open_successes: int = 2,
    ) -> None:
        if failure_threshold <= 0 or recovery_timeout_seconds <= 0 or half_open_successes <= 0:
            raise ValueError("circuit breaker thresholds must be positive")
        self._failure_threshold = failure_threshold
        self._recovery_timeout_seconds = recovery_timeout_seconds
        self._half_open_successes_required = half_open_successes
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._opened_at: float | None = None
        self._lock = threading.RLock()

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._refresh_state(time.monotonic())
            return self._state

    def _refresh_state(self, now: float) -> None:
        if (
            self._state is BreakerState.OPEN
            and self._opened_at is not None
            and now - self._opened_at >= self._recovery_timeout_seconds
        ):
            self._state = BreakerState.HALF_OPEN
            self._half_open_successes = 0

    def allow_request(self) -> bool:
        with self._lock:
            self._refresh_state(time.monotonic())
            return self._state is not BreakerState.OPEN

    def require_request_allowed(self) -> None:
        if not self.allow_request():
            raise CircuitOpenError("dependency circuit is open")

    def record_success(self) -> None:
        with self._lock:
            self._refresh_state(time.monotonic())
            if self._state is BreakerState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self._half_open_successes_required:
                    self._state = BreakerState.CLOSED
                    self._consecutive_failures = 0
                    self._opened_at = None
            else:
                self._consecutive_failures = 0

    def record_failure(self) -> None:
        with self._lock:
            self._refresh_state(time.monotonic())
            if self._state is BreakerState.HALF_OPEN:
                self._trip()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_successes = 0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_state(time.monotonic())
            return {
                "state": self._state.value,
                "consecutive_failures": self._consecutive_failures,
                "half_open_successes": self._half_open_successes,
                "failure_threshold": self._failure_threshold,
            }


class AdaptiveBackpressureController:
    """AIMD-style concurrency controller driven by queue, lag, and error signals."""

    def __init__(self, policy: BackpressurePolicy, *, initial_concurrency: int | None = None) -> None:
        self._policy = policy
        initial = initial_concurrency if initial_concurrency is not None else policy.min_concurrency
        self._concurrency = max(policy.min_concurrency, min(policy.max_concurrency, initial))
        self._lock = threading.RLock()

    @property
    def concurrency(self) -> int:
        with self._lock:
            return self._concurrency

    def evaluate(self, signal: PipelineSignal) -> BackpressureDecision:
        reasons: list[str] = []
        with self._lock:
            previous = self._concurrency
            overloaded = False

            if signal.error_ratio >= self._policy.error_ratio_high_watermark:
                overloaded = True
                reasons.append("error_ratio_high")
            if signal.p95_lag_ms >= self._policy.target_lag_ms * 2:
                overloaded = True
                reasons.append("lag_critical")
            if signal.queue_depth >= self._policy.queue_high_watermark:
                overloaded = True
                reasons.append("queue_high")

            if overloaded:
                candidate = int(max(self._policy.min_concurrency, previous * self._policy.decrease_factor))
                self._concurrency = max(self._policy.min_concurrency, min(previous - 1, candidate))
                mode = "decrease"
            elif (
                signal.p95_lag_ms <= self._policy.target_lag_ms
                and signal.error_ratio < self._policy.error_ratio_high_watermark / 2
                and signal.queue_depth < self._policy.queue_high_watermark / 2
            ):
                self._concurrency = min(self._policy.max_concurrency, previous + self._policy.increase_step)
                mode = "increase" if self._concurrency > previous else "hold"
                reasons.append("headroom_available")
            else:
                mode = "hold"
                reasons.append("within_control_band")

            return BackpressureDecision(
                previous_concurrency=previous,
                next_concurrency=self._concurrency,
                mode=mode,
                reasons=tuple(reasons),
                observed_at=signal.observed_at,
            )


class SLOBurnRateEvaluator:
    """Evaluates multi-window error-budget burn without provider-specific metrics APIs."""

    def __init__(self, policy: SLOPolicy) -> None:
        self._policy = policy

    def evaluate(self, *, window_name: str, requests: int, failures: int) -> BurnRateResult:
        if requests < 0 or failures < 0 or failures > requests:
            raise ValueError("requests/failures are inconsistent")
        observed = failures / requests if requests else 0.0
        budget = max(self._policy.error_budget_ratio, 1e-12)
        burn = observed / budget
        if burn >= self._policy.fast_burn_threshold:
            severity = "critical"
        elif burn >= self._policy.slow_burn_threshold:
            severity = "warning"
        else:
            severity = "normal"
        return BurnRateResult(
            window_name=window_name,
            requests=requests,
            failures=failures,
            observed_error_ratio=observed,
            burn_rate=burn,
            severity=severity,
        )


class PipelineStateMachine:
    """Explicit operator state machine; illegal transitions fail closed."""

    _TRANSITIONS: dict[RunState, dict[ControlAction, RunState]] = {
        RunState.IDLE: {ControlAction.START: RunState.STARTING, ControlAction.RESET: RunState.IDLE},
        RunState.STARTING: {ControlAction.RECOVER: RunState.RUNNING, ControlAction.FAIL: RunState.FAILED},
        RunState.RUNNING: {
            ControlAction.DRAIN: RunState.DRAINING,
            ControlAction.DEGRADE: RunState.DEGRADED,
            ControlAction.FAIL: RunState.FAILED,
            ControlAction.STOP: RunState.STOPPED,
        },
        RunState.DEGRADED: {
            ControlAction.RECOVER: RunState.RUNNING,
            ControlAction.DRAIN: RunState.DRAINING,
            ControlAction.FAIL: RunState.FAILED,
            ControlAction.STOP: RunState.STOPPED,
        },
        RunState.DRAINING: {ControlAction.STOP: RunState.STOPPED, ControlAction.FAIL: RunState.FAILED},
        RunState.STOPPED: {ControlAction.START: RunState.STARTING, ControlAction.RESET: RunState.IDLE},
        RunState.FAILED: {ControlAction.RESET: RunState.IDLE, ControlAction.START: RunState.STARTING},
    }

    def __init__(self, initial: RunState = RunState.IDLE) -> None:
        self._state = initial
        self._lock = threading.RLock()

    @property
    def state(self) -> RunState:
        with self._lock:
            return self._state

    def transition(self, action: ControlAction) -> tuple[RunState, RunState]:
        with self._lock:
            previous = self._state
            allowed = self._TRANSITIONS.get(previous, {})
            if action not in allowed:
                raise InvalidTransitionError(f"{action.value} is not valid from {previous.value}")
            self._state = allowed[action]
            return previous, self._state


class ControlPlane:
    """Coordinates command idempotency, state transitions, breakers, and audit events."""

    def __init__(
        self,
        *,
        run_id: str | None = None,
        idempotency_ledger: IdempotencyLedger | None = None,
        dependency_breaker: CircuitBreaker | None = None,
        event_history_limit: int = 500,
        response_cache_limit: int = 20_000,
    ) -> None:
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:16]}"
        self.state_machine = PipelineStateMachine()
        self.idempotency = idempotency_ledger or IdempotencyLedger()
        self.dependency_breaker = dependency_breaker or CircuitBreaker()
        if response_cache_limit <= 0:
            raise ValueError("response_cache_limit must be positive")
        self._events: deque[ControlEvent] = deque(maxlen=event_history_limit)
        self._event_by_digest: OrderedDict[str, ControlEvent] = OrderedDict()
        self._response_cache_limit = response_cache_limit
        self._lock = threading.RLock()

    def execute(
        self,
        action: ControlAction,
        *,
        actor: str,
        idempotency_key: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ControlEvent:
        if not actor.strip():
            raise ValueError("actor cannot be blank")
        if not idempotency_key.strip():
            raise ValueError("idempotency_key cannot be blank")

        with self._lock:
            prior_digest = self.idempotency.get(idempotency_key)
            if prior_digest is not None:
                prior = self._event_by_digest.get(prior_digest)
                if prior is None:
                    raise RuntimeError("idempotency ledger references an evicted audit event")
                return prior

            previous, next_state = self.state_machine.transition(action)
            event = ControlEvent(
                event_id=f"ctl_{uuid.uuid4().hex}",
                run_id=self.run_id,
                action=action,
                previous_state=previous,
                next_state=next_state,
                actor=actor,
                occurred_at=dt.datetime.now(UTC),
                idempotency_key=idempotency_key,
                metadata=dict(metadata or {}),
            )
            digest = event.digest
            if not self.idempotency.remember(idempotency_key, digest):
                raise RuntimeError("idempotency race detected")
            self._events.append(event)
            self._event_by_digest[digest] = event
            self._event_by_digest.move_to_end(digest)
            while len(self._event_by_digest) > self._response_cache_limit:
                self._event_by_digest.popitem(last=False)
            return event

    @staticmethod
    def _serialize_event(event: ControlEvent) -> dict[str, Any]:
        """Return an API-safe representation without leaking enum/datetime objects."""
        return {
            "event_id": event.event_id,
            "run_id": event.run_id,
            "action": event.action.value,
            "previous_state": event.previous_state.value,
            "next_state": event.next_state.value,
            "actor": event.actor,
            "occurred_at": event.occurred_at.isoformat(),
            "idempotency_key": event.idempotency_key,
            "metadata": dict(event.metadata),
            "digest": event.digest,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            events = list(self._events)
            return {
                "run_id": self.run_id,
                "state": self.state_machine.state.value,
                "breaker": self.dependency_breaker.snapshot(),
                "idempotency_entries": len(self.idempotency),
                "audit_event_count": len(events),
                "latest_event": self._serialize_event(events[-1]) if events else None,
            }
