from app.jobs.factor_runner import (
    FACTOR_ALL,
    FACTOR_FQ,
    FACTOR_MA,
    MODE_STALE,
    FactorConfig,
    parse_args,
    run_factor,
)


class FakeFactorService:
    def __init__(self):
        self.updated_codes = []

    def get_codes_requiring_update(self, market=None):
        return ["sh600000", "sz000001", "skip-code", "fail-code"]

    def update_code(self, code):
        self.updated_codes.append(code)
        if code == "skip-code":
            return {"code": "SKIP", "written_count": 0, "message": "unsupported"}
        if code == "fail-code":
            raise RuntimeError("boom")
        return {"code": "GOOD", "written_count": 3, "message": None}


def _configs(service):
    return {
        FACTOR_MA: FactorConfig(lambda: service, "ma_factor"),
        FACTOR_FQ: FactorConfig(lambda: service, "fq_factor"),
    }


def test_parse_args_defaults_to_safe_ma_stale_mode():
    args = parse_args([])

    assert args.factor == FACTOR_MA
    assert args.mode == MODE_STALE
    assert args.codes == []


def test_run_factor_dry_run_selects_stale_codes_without_writes():
    service = FakeFactorService()

    result = run_factor(
        FACTOR_MA,
        dry_run=True,
        limit=2,
        configs=_configs(service),
        market_loader=lambda name: object(),
    )

    assert result["dry_run"] is True
    assert result["pulled_count"] == 2
    assert result["codes"] == ["sh600000", "sz000001"]
    assert service.updated_codes == []


def test_run_factor_updates_codes_and_counts_skip_and_failures():
    service = FakeFactorService()

    result = run_factor(
        FACTOR_MA,
        configs=_configs(service),
        market_loader=lambda name: object(),
    )

    assert result["pulled_count"] == 4
    assert result["written_count"] == 6
    assert result["skipped_count"] == 1
    assert result["failed_count"] == 1
    assert result["failed_codes"] == ["fail-code"]


def test_run_all_combines_fq_and_ma_results():
    service = FakeFactorService()

    result = run_factor(
        FACTOR_ALL,
        limit=1,
        configs=_configs(service),
        market_loader=lambda name: object(),
    )

    assert result["factor"] == FACTOR_ALL
    assert result["pulled_count"] == 2
    assert result["written_count"] == 6
    assert len(result["results"]) == 2


def test_stale_fq_uses_market_snapshot_batch_path():
    class FakeSnapshotService(FakeFactorService):
        def __init__(self):
            super().__init__()
            self.market_calls = []

        def update_market(self, market=None, selected_codes=None):
            self.market_calls.append((market, selected_codes))
            return {"written_count": 2, "failed_count": 0, "failed_codes": []}

    service = FakeSnapshotService()
    market = object()
    result = run_factor(
        FACTOR_FQ,
        mode=MODE_STALE,
        configs=_configs(service),
        market_loader=lambda name: market,
    )

    assert service.market_calls == [
        (market, ["sh600000", "sz000001", "skip-code", "fail-code"])
    ]
    assert service.updated_codes == []
    assert result["written_count"] == 2
