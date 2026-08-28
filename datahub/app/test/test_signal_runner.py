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
    class FailingSignalService(FakeSignalService):
        def update_code(self, code, *, force=False):
            return {"code": "FAIL", "written_count": 0, "message": "missing"}

    with pytest.raises(RuntimeError, match="sh600000"):
        run_signal(
            SIGNAL_MA_CROSS,
            mode=MODE_FORCE,
            codes=["sh600000"],
            configs=_configs(FailingSignalService()),
            market_loader=lambda name: object(),
        )
