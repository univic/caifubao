from app.jobs.signal_runner import (
    MODE_FORCE,
    MODE_STALE,
    SIGNAL_MA_CROSS,
    SignalConfig,
    run_signal,
)
import pytest


class FakeSignalService:
    def __init__(self):
        self.calls = []

    def get_codes_requiring_update(self, market=None):
        return ["sh600000"]

    def update_code(self, code, *, force=False):
        self.calls.append((code, force))
        return {"code": "GOOD", "written_count": 1, "message": None}

    def update_market(self, market=None, selected_codes=None):
        self.calls.append((list(selected_codes or []), False))
        return {
            "written_count": len(selected_codes or []),
            "skipped_count": 0,
            "failed_count": 0,
            "failed_codes": [],
        }


def _configs(service):
    return {SIGNAL_MA_CROSS: SignalConfig(lambda: service, "ma_factor")}


def test_stale_signal_run_uses_incremental_update():
    service = FakeSignalService()

    result = run_signal(
        SIGNAL_MA_CROSS,
        mode=MODE_STALE,
        configs=_configs(service),
        market_loader=lambda name: object(),
    )

    assert service.calls == [(["sh600000"], False)]
    assert result["written_count"] == 1


def test_force_signal_run_requests_authoritative_rebuild():
    service = FakeSignalService()

    result = run_signal(
        SIGNAL_MA_CROSS,
        mode=MODE_FORCE,
        codes=["sh600000"],
        configs=_configs(service),
        market_loader=lambda name: object(),
    )

    assert service.calls == [("sh600000", True)]
    assert result["written_count"] == 1


def test_force_signal_run_raises_when_service_returns_fail():
    from app.lib.signal_factory import SignalUpdateError

    class FailingSignalService(FakeSignalService):
        def update_code(self, code, *, force=False):
            return {"code": "FAIL", "written_count": 0, "message": "missing"}

    with pytest.raises(SignalUpdateError) as excinfo:
        run_signal(
            SIGNAL_MA_CROSS,
            mode=MODE_FORCE,
            codes=["sh600000"],
            configs=_configs(FailingSignalService()),
            market_loader=lambda name: object(),
        )

    assert "sh600000" in str(excinfo.value)
    # No signals were persisted before the failure, so written_count is 0.
    assert excinfo.value.written_count == 0


def test_force_signal_run_preserves_partial_written_count_on_failure():
    from app.lib.signal_factory import SignalUpdateError

    class PartiallyFailingSignalService(FakeSignalService):
        def __init__(self):
            super().__init__()
            self.partial_written = 42

        def update_code(self, code, *, force=False):
            if code == "sh600000":
                self.partial_written += 1
                return {"code": "GOOD", "written_count": 1, "message": None}
            return {"code": "FAIL", "written_count": 0, "message": "missing"}

    with pytest.raises(SignalUpdateError) as excinfo:
        run_signal(
            SIGNAL_MA_CROSS,
            mode=MODE_FORCE,
            codes=["sh600000", "sz000001"],
            configs=_configs(PartiallyFailingSignalService()),
            market_loader=lambda name: object(),
        )

    # The exception carries the signals persisted before the failure so the
    # job-run record keeps them for downstream scoring gates.
    assert excinfo.value.written_count == 1


class _FakeRunRecord:
    """Minimal stand-in for a DatahubJobRun used by _check_dependency."""

    def __init__(self, status="SUCCESS", phase_stats=None, written_total=0):
        self.status = status
        self.phase_stats = phase_stats or {}
        self.written_total = written_total


def test_signal_dependency_accepts_success_record(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [_FakeRunRecord(status="SUCCESS")]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        raise AssertionError("success path must not fall through to the record query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert signal_runner._check_dependency() is True


def test_signal_dependency_accepts_persisted_data_despite_failed_run(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [
        None,  # no SUCCESS record
        _FakeRunRecord(
            status="FAILED",
            phase_stats={
                "check_stock_data_integrity": {
                    "written_count": 5209,
                    "validated_count": 5209,
                },
                "update_ma_factor": {"written_count": 9836},
            },
        ),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    # The quote run died after persisting quotes AND MA factors; signals may
    # still be computed from the real data instead of being skipped.
    assert signal_runner._check_dependency() is True


def test_signal_dependency_accepts_running_upstream_with_full_evidence(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [
        None,
        # Killed by deadline at 20:00, still RUNNING when the signal job
        # checks at 18:30 the same evening, but quotes and MA factors were
        # already persisted (progress checkpoints).
        _FakeRunRecord(
            status="RUNNING",
            phase_stats={
                "check_stock_data_integrity": {
                    "written_count": 5209,
                    "validated_count": 5209,
                },
                "update_ma_factor": {"written_count": 9836},
            },
        ),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert signal_runner._check_dependency() is True


def test_signal_dependency_rejects_running_upstream_without_evidence(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [
        None,
        # Still RUNNING and only the prerequisite phase completed: signals
        # would be computed from stale factors, so the gate must stay closed.
        _FakeRunRecord(
            status="RUNNING",
            phase_stats={"check_prerequisite": {}},
        ),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert signal_runner._check_dependency() is False


def test_signal_dependency_rejects_missing_ma_factor_phase(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [
        None,
        # Quotes validated but the MA factor phase never completed: signals
        # would be computed from stale factors, so the gate must stay closed.
        _FakeRunRecord(
            status="FAILED",
            phase_stats={
                "check_stock_data_integrity": {
                    "written_count": 5209,
                    "validated_count": 5209,
                }
            },
        ),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert signal_runner._check_dependency() is False


def test_signal_dependency_rejects_run_without_written_quotes(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    records = [
        None,
        _FakeRunRecord(status="FAILED", phase_stats={"check_prerequisite": {}}),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert signal_runner._check_dependency() is False


def test_signal_dependency_rejects_missing_record(monkeypatch):
    import app.jobs.signal_runner as signal_runner

    monkeypatch.setattr(
        "app.jobs.signal_runner.job_run_helper.latest_job_run", lambda **kwargs: None
    )

    assert signal_runner._check_dependency() is False
