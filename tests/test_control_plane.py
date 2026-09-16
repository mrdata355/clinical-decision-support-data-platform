from __future__ import annotations

import datetime as dt
import time

import pytest

from clinical_data_platform.control_plane import (
    AdaptiveBackpressureController,
    BackpressurePolicy,
    BreakerState,
    CircuitBreaker,
    ControlAction,
    ControlPlane,
    IdempotencyLedger,
    InvalidTransitionError,
    PipelineSignal,
    RunState,
    SLOBurnRateEvaluator,
    SLOPolicy,
)

UTC = dt.UTC


def signal(*, queue: int, lag: int, errors: float, eps: float = 10.0) -> PipelineSignal:
    return PipelineSignal(
        observed_at=dt.datetime.now(UTC),
        queue_depth=queue,
        p95_lag_ms=lag,
        error_ratio=errors,
        throughput_eps=eps,
    )


def test_control_plane_transitions_are_explicit_and_idempotent() -> None:
    plane = ControlPlane(run_id="run_test")
    first = plane.execute(
        ControlAction.START,
        actor="airflow",
        idempotency_key="dag-run-1:start",
        metadata={"dag_id": "clinical_platform"},
    )
    duplicate = plane.execute(
        ControlAction.START,
        actor="airflow",
        idempotency_key="dag-run-1:start",
    )

    assert first is duplicate
    assert first.previous_state is RunState.IDLE
    assert first.next_state is RunState.STARTING
    assert plane.snapshot()["state"] == "starting"

    running = plane.execute(
        ControlAction.RECOVER,
        actor="airflow",
        idempotency_key="dag-run-1:recover",
    )
    assert running.next_state is RunState.RUNNING


def test_invalid_transition_fails_closed() -> None:
    plane = ControlPlane()
    with pytest.raises(InvalidTransitionError):
        plane.execute(ControlAction.STOP, actor="operator", idempotency_key="stop-too-early")


def test_idempotency_ledger_expires_entries() -> None:
    ledger = IdempotencyLedger(ttl_seconds=0.01, max_entries=10)
    assert ledger.remember("k", "digest") is True
    assert ledger.remember("k", "digest-2") is False
    assert ledger.get("k") == "digest"
    time.sleep(0.02)
    assert ledger.get("k") is None
    assert ledger.remember("k", "digest-3") is True


def test_circuit_breaker_trips_and_recovers_through_half_open() -> None:
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout_seconds=0.01,
        half_open_successes=2,
    )
    breaker.record_failure()
    assert breaker.state is BreakerState.CLOSED
    breaker.record_failure()
    assert breaker.state is BreakerState.OPEN
    assert breaker.allow_request() is False

    time.sleep(0.02)
    assert breaker.state is BreakerState.HALF_OPEN
    breaker.record_success()
    assert breaker.state is BreakerState.HALF_OPEN
    breaker.record_success()
    assert breaker.state is BreakerState.CLOSED


def test_backpressure_uses_additive_increase_multiplicative_decrease() -> None:
    controller = AdaptiveBackpressureController(
        BackpressurePolicy(min_concurrency=2, max_concurrency=20, increase_step=3),
        initial_concurrency=8,
    )
    healthy = controller.evaluate(signal(queue=100, lag=200, errors=0.001))
    assert healthy.mode == "increase"
    assert healthy.next_concurrency == 11

    overloaded = controller.evaluate(signal(queue=40_000, lag=2_000, errors=0.08))
    assert overloaded.mode == "decrease"
    assert overloaded.next_concurrency < healthy.next_concurrency
    assert {"error_ratio_high", "lag_critical", "queue_high"}.issubset(overloaded.reasons)


def test_slo_burn_rate_classifies_budget_consumption() -> None:
    evaluator = SLOBurnRateEvaluator(SLOPolicy(availability_target=0.999))
    normal = evaluator.evaluate(window_name="5m", requests=100_000, failures=20)
    critical = evaluator.evaluate(window_name="5m", requests=100_000, failures=2_000)

    assert normal.severity == "normal"
    assert critical.severity == "critical"
    assert critical.burn_rate > normal.burn_rate


def test_control_event_digest_is_stable_for_same_object() -> None:
    plane = ControlPlane(run_id="run_digest")
    event = plane.execute(ControlAction.START, actor="operator", idempotency_key="start-1")
    assert event.digest == event.digest
    assert len(event.digest) == 64
